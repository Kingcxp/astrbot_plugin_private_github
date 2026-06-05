import asyncio
import re
import os
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import aiohttp
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.event import MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger, AstrBotConfig

# GitHub API
REST_API_BASE = "https://api.github.com"
GRAPHQL_API = "https://api.github.com/graphql"

# 游标存储前缀
KV_LAST_CURSOR_PREFIX = "ghp_cursor_"
# 订阅数据存储文件名
SUBS_FILE = "subscriptions.json"

# 扫描窗口硬上限
MAX_SCAN_ENTRIES = 50

# 正则
RE_REPO = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")
RE_WATCH_ITEM = re.compile(r"^([a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+):(issues|commits|releases)$")
RE_PROJECT_ITEM = re.compile(r"^([a-zA-Z0-9._-]+)/(\d+)$")


def is_user_allowed(plugin, event: AstrMessageEvent) -> bool:
    """权限检查：管理员或白名单内用户"""
    try:
        if event.is_admin():
            return True
        whitelist = getattr(plugin, "whitelist", None)
        if whitelist is None:
            whitelist = plugin.config.get("whitelist", [])
        if not whitelist:
            return True
        return event.get_sender_id() in whitelist
    except Exception:
        return True


def get_session_id(event: AstrMessageEvent) -> str:
    """获取当前会话的 unified_msg_origin"""
    return event.unified_msg_origin


@register(
    "astrbot_plugin_private_github",
    "CecilyGao",
    "通过 GitHub API 定时获取私有仓库动态（Issues/Commits/Releases/Projects）并推送到聊天会话，支持多会话独立订阅",
    "2.0.0",
    "https://github.com/CecilyGao/astrbot_plugin_private_github",
)
class GitHubPrivateListenPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.github_token: str = config.get("github_token", "")
        if not self.github_token:
            logger.warning("[Private GitHub] 未配置 github_token，插件将无法访问私有数据")

        self.poll_interval: int = max(config.get("poll_interval", 1800), 60)
        self.max_entries: int = max(config.get("max_entries", 5), 0)
        self.cfg_timezone: str = config.get("timezone", "Asia/Shanghai")
        self.at_enabled: bool = config.get("at_enabled", False)
        self.whitelist: List[str] = config.get("whitelist", [])

        # 用户名 -> QQ 映射（管理员在配置中设置 username_qq）
        # 支持键为 github login 或 display name，值为 QQ 字符串或数字
        self.username_qq_map: Dict[str, str] = self._load_username_qq_map()

        # 数据目录
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_private_github")
        os.makedirs(self.data_dir, exist_ok=True)
        self.subs_file = os.path.join(self.data_dir, SUBS_FILE)

        # 会话订阅数据结构:
        # {
        #   "session_origin": [
        #       {"type": "repo", "repo": "owner/repo", "event": "issues"},
        #       {"type": "project", "org": "org", "number": 1}
        #   ]
        # }
        self.subscriptions: Dict[str, List[Dict]] = {}

        self._poll_task: Optional[asyncio.Task] = None
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._load_subscriptions()

    # ==================== 数据持久化 ====================

    def _load_subscriptions(self):
        """从文件加载订阅数据（不含 cursor，cursor 单独存在 KV 中）"""
        if not os.path.exists(self.subs_file):
            self.subscriptions = {}
            return
        try:
            with open(self.subs_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.subscriptions = {}
            else:
                self.subscriptions = data
            logger.info(f"[Private GitHub] 已加载 {len(self.subscriptions)} 个会话的订阅数据")
        except Exception as e:
            logger.error(f"[Private GitHub] 加载订阅数据失败: {e}")
            self.subscriptions = {}

    def _save_subscriptions(self):
        """保存订阅数据（不含 cursor）"""
        try:
            to_save = {}
            for session, items in self.subscriptions.items():
                to_save[session] = []
                for item in items:
                    copy_item = item.copy()
                    copy_item.pop("cursor", None)
                    to_save[session].append(copy_item)
            with open(self.subs_file, "w", encoding="utf-8") as f:
                json.dump(to_save, f, ensure_ascii=False, indent=2)
            logger.debug("[Private GitHub] 订阅数据已保存")
        except Exception as e:
            logger.error(f"[Private GitHub] 保存订阅数据失败: {e}")

    def _get_cursor_key(self, session: str, sub: Dict) -> str:
        """生成 KV 中游标的唯一 key"""
        if sub["type"] == "repo":
            return f"{KV_LAST_CURSOR_PREFIX}{session}_{sub['repo']}_{sub['event']}"
        else:
            return f"{KV_LAST_CURSOR_PREFIX}{session}_project_{sub['org']}_{sub['number']}"

    async def _get_cursor(self, session: str, sub: Dict) -> str:
        """获取订阅项的游标"""
        key = self._get_cursor_key(session, sub)
        return await self.get_kv_data(key, "")

    async def _set_cursor(self, session: str, sub: Dict, cursor: str):
        """设置订阅项的游标"""
        key = self._get_cursor_key(session, sub)
        await self.put_kv_data(key, cursor)

    async def _init_subscription_cursor(self, session: str, sub: Dict):
        """初始化新订阅的游标（最新一条动态的 ID）"""
        try:
            if sub["type"] == "repo":
                latest = await self._fetch_latest_repo_entry(sub["repo"], sub["event"])
            else:
                latest = await self._fetch_latest_project_item(sub["org"], sub["number"])
            if latest:
                cursor = self._extract_cursor_from_entry(latest, sub["type"] if sub["type"] == "repo" else "project")
                if cursor:
                    await self._set_cursor(session, sub, cursor)
                    logger.info(f"[Private GitHub] 初始化游标成功: {self._get_cursor_key(session, sub)} -> {cursor[:20]}...")
                    return
            await self._set_cursor(session, sub, "__EMPTY__")
            logger.warning(f"[Private GitHub] 无法获取最新条目，设置占位游标: {self._get_cursor_key(session, sub)}")
        except Exception as e:
            logger.error(f"[Private GitHub] 初始化游标失败: {e}")

    # ==================== 生命周期 ====================

    async def initialize(self):
        if not self.github_token:
            logger.error("[Private GitHub] github_token 未配置，插件将无法正常工作")
        logger.info(
            f"[Private GitHub] 插件初始化，轮询间隔: {self.poll_interval} 秒，"
            f"已加载 {len(self.subscriptions)} 个会话的订阅"
        )
        self._http_session = aiohttp.ClientSession(
            headers={
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json",
            },
            timeout=aiohttp.ClientTimeout(total=30)
        )
        await self._ensure_all_cursors()
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def _ensure_all_cursors(self):
        """确保所有订阅项都有游标"""
        for session, items in self.subscriptions.items():
            for sub in items:
                cursor = await self._get_cursor(session, sub)
                if not cursor:
                    await self._init_subscription_cursor(session, sub)

    async def terminate(self):
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        logger.info("[Private GitHub] 插件已卸载")

    # ==================== 辅助函数 ====================

    @staticmethod
    def _extract_cursor_from_entry(entry: Dict[str, Any], item_type: str) -> str:
        if item_type in ("issue", "issues", "repo"):
            return str(entry.get("id", ""))
        elif item_type in ("commit", "commits"):
            return entry.get("sha", "")
        elif item_type in ("release", "releases"):
            return str(entry.get("id", ""))
        elif item_type == "project":
            return entry.get("raw_updated_at", "2000-01-01T00:00:00Z")
        return ""

    def _convert_time(self, time_str: str) -> str:
        if not time_str:
            return ""
        try:
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(ZoneInfo(self.cfg_timezone)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return time_str

    def _load_username_qq_map(self) -> Dict[str, str]:
        raw = []
        try:
            raw = self.config.get("username_qq", []) or []
        except Exception:
            raw = []

        mapping: Dict[str, str] = {}

        def store(key_raw: Any, qq_raw: Any):
            try:
                key = str(key_raw).strip().lstrip("@").lower()
                if not key:
                    return
            except Exception:
                return
            try:
                qq_str = str(qq_raw).strip()
            except Exception:
                return
            import re

            digits = re.sub(r"\D+", "", qq_str)
            if not digits:
                logger.warning(f"[Private GitHub] username_qq: QQ for '{key}' is invalid or contains no digits: '{qq_str}' - skipped")
                return
            if digits != qq_str:
                logger.warning(f"[Private GitHub] username_qq: QQ '{qq_str}' for '{key}' contained non-digits; using '{digits}'")
            mapping[key] = digits

        if isinstance(raw, dict):
            for k, v in raw.items():
                store(k, v)
            return mapping

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    username = item.get("username") or item.get("login") or item.get("name") or item.get("user")
                    qq = item.get("qq") or item.get("QQ") or item.get("q")
                    if username and qq is not None:
                        store(username, qq)
                        continue
                elif isinstance(item, str):
                    s = item.strip()
                    if ":" in s:
                        a, b = s.split(":", 1)
                        store(a, b)
            return mapping

        return mapping

    def _resolve_qq_for_username(self, username: str) -> Optional[str]:
        if not username:
            return None
        key = str(username).strip().lstrip("@").lower()
        return self.username_qq_map.get(key)

    def _normalize_user_display(self, user_obj: Dict[str, Any]) -> str:
        """优先返回用户的 name（非空且不为字符串 'None'），否则回退到 login。"""
        if not user_obj:
            return ""
        name = user_obj.get("name")
        login = user_obj.get("login")
        if isinstance(name, str) and name.strip() and name.strip().lower() != "none":
            return name
        if login:
            return login
        return ""

    def _extract_assignee_logins(self, assignees_field: Any) -> List[str]:
        """从 GraphQL/REST 的 assignees 字段中提取用户显示名或登录名。

        优先使用 `name`（非空且不为字符串 'None'），否则回退到 `login`。
        返回值为字符串列表，方便用于消息展示与 QQ 映射查找。
        """
        nodes = []
        if not assignees_field:
            return []
        # 支持两种常见结构：{ 'nodes': [...] } 或直接为列表
        if isinstance(assignees_field, dict):
            nodes = assignees_field.get("nodes", [])
        elif isinstance(assignees_field, list):
            nodes = assignees_field
        else:
            return []

        res: List[str] = []
        for u in nodes:
            if not isinstance(u, dict):
                continue
            name = u.get("name")
            login = u.get("login")
            if isinstance(name, str) and name.strip() and name.strip().lower() != "none":
                res.append(name.strip())
            elif login:
                res.append(login)
        return res

    @staticmethod
    def _extract_content(entry: Dict[str, Any], event_type: str, max_len: int = 200) -> str:
        if event_type == "issue":
            body = entry.get("body", "") or ""
            title = entry.get("title", "")
            return f"{title}: {body[:max_len]}".strip()
        elif event_type == "commit":
            commit = entry.get("commit", {})
            message = commit.get("message", "")
            return message.split("\n")[0][:max_len]
        elif event_type == "release":
            name = entry.get("name", "") or entry.get("tag_name", "")
            body = entry.get("body", "") or ""
            return f"{name}: {body[:max_len]}".strip()
        elif event_type == "project":
            return ""
        return ""

    # ==================== GitHub API 请求 ====================

    async def _rest_api_get(self, url: str) -> Optional[List[Dict]]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30)
            )
        try:
            async with self._http_session.get(url) as resp:
                if resp.status == 404:
                    logger.warning(f"[Private GitHub] API 404: {url}")
                    return None
                if resp.status != 200:
                    logger.warning(f"[Private GitHub] API 请求失败: {url} -> HTTP {resp.status}")
                    return None
                data = await resp.json()
                if isinstance(data, list):
                    return data
                else:
                    return [data]
        except Exception as e:
            logger.error(f"[Private GitHub] API 请求异常: {url} -> {e}")
            return None

    async def _graphql_request(self, query: str, variables: Dict = None) -> Optional[Dict]:
        if not self._http_session or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                headers={"Authorization": f"token {self.github_token}"},
                timeout=aiohttp.ClientTimeout(total=30)
            )
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            async with self._http_session.post(GRAPHQL_API, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(f"[Private GitHub] GraphQL 请求失败: HTTP {resp.status}")
                    return None
                data = await resp.json()
                if "errors" in data:
                    logger.warning(f"[Private GitHub] GraphQL 错误: {data['errors']}")
                    return None
                return data.get("data")
        except Exception as e:
            logger.error(f"[Private GitHub] GraphQL 请求异常: {e}")
            return None

    # ==================== 仓库监听 (REST) ====================

    async def _fetch_latest_repo_entry(self, repo: str, event_type: str) -> Optional[Dict]:
        url = self._build_repo_api_url(repo, event_type, per_page=1)
        data = await self._rest_api_get(url)
        if data and len(data) > 0:
            return data[0]
        return None

    async def _fetch_new_repo_entries(self, repo: str, event_type: str, last_cursor: str) -> Tuple[List[Dict], str]:
        per_page = MAX_SCAN_ENTRIES
        url = self._build_repo_api_url(repo, event_type, per_page=per_page)
        items = await self._rest_api_get(url)
        if not items:
            return [], last_cursor

        new_entries = []
        for item in items:
            cursor = self._extract_cursor_from_entry(item, event_type)
            if cursor == last_cursor:
                break
            entry = self._build_repo_entry_dict(item, event_type)
            if entry:
                new_entries.append(entry)

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[:self.max_entries]

        if new_entries and items:
            latest_cursor = self._extract_cursor_from_entry(items[0], event_type)
            return new_entries, latest_cursor
        return new_entries, last_cursor

    def _build_repo_api_url(self, repo: str, event_type: str, per_page: int = 30) -> str:
        base = f"{REST_API_BASE}/repos/{repo}"
        if event_type == "issues":
            return f"{base}/issues?state=all&sort=created&direction=desc&per_page={per_page}"
        elif event_type == "commits":
            return f"{base}/commits?per_page={per_page}"
        elif event_type == "releases":
            return f"{base}/releases?per_page={per_page}"
        raise ValueError(f"Unknown event_type: {event_type}")

    def _build_repo_entry_dict(self, raw: Dict, event_type: str) -> Dict:
        if event_type == "issues":
            user_obj = raw.get("user") or {}
            author = self._normalize_user_display(user_obj) or None
            assignees = self._extract_assignee_logins(raw.get("assignees") or [])
            return {
                "title": f"[Issue] #{raw['number']}: {raw['title']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw["created_at"]),
                "content": self._extract_content(raw, "issue"),
                "id": str(raw["id"]),
                "type": "issue",
                "author": author,
                "assignees": assignees,
            }
        elif event_type == "commits":
            return {
                "title": f"[Commit] {raw['sha'][:7]}: {raw['commit']['message'].splitlines()[0][:100]}",
                "link": raw["html_url"],
                "published": self._convert_time(raw["commit"]["committer"]["date"]),
                "content": self._extract_content(raw, "commit"),
                "id": raw["sha"],
                "type": "commit"
            }
        elif event_type == "releases":
            return {
                "title": f"[Release] {raw['tag_name']}: {raw['name'] or raw['tag_name']}",
                "link": raw["html_url"],
                "published": self._convert_time(raw["published_at"] or raw["created_at"]),
                "content": self._extract_content(raw, "release"),
                "id": str(raw["id"]),
                "type": "release"
            }
        return {}

    # ==================== 组织项目监听 (GraphQL) ====================

    async def _fetch_latest_project_item(self, org: str, number: int) -> Optional[Dict]:
        items = await self._fetch_project_items(org, number, first=1)
        if items:
            return items[0]
        return None

    async def _fetch_project_items(self, org: str, number: int, first: int = 50) -> List[Dict]:
        query = """
        query($org: String!, $number: Int!, $first: Int!) {
            organization(login: $org) {
                projectV2(number: $number) {
                    id
                    title
                    shortDescription
                    number
                    items(first: $first) {
                        nodes {
                            id
                            createdAt
                            updatedAt
                            content {
                                __typename
                                ... on Issue {
                                    id
                                    number
                                    title
                                    createdAt
                                    updatedAt
                                    url
                                    bodyText
                                    state
                                    author { login ... on User { name } }
                                    assignees(first: 50) { nodes { login name } }
                                    comments(last: 5) {
                                        nodes {
                                            author { login ... on User { name } }
                                            bodyText
                                            createdAt
                                        }
                                    }
                                }
                                ... on PullRequest {
                                    id
                                    number
                                    title
                                    createdAt
                                    updatedAt
                                    url
                                    bodyText
                                    state
                                    merged
                                    mergedAt
                                    author { login ... on User { name } }
                                    assignees(first: 50) { nodes { login name } }
                                    comments(last: 5) {
                                        nodes {
                                            author { login ... on User { name } }
                                            bodyText
                                            createdAt
                                        }
                                    }
                                }
                                ... on DraftIssue {
                                    id
                                    title
                                    bodyText
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"org": org, "number": number, "first": first}
        data = await self._graphql_request(query, variables)
        if not data:
            return []
        org_data = data.get("organization")
        if not org_data:
            return []
        project = org_data.get("projectV2")
        if not project:
            return []
        items = project.get("items", {}).get("nodes", [])
        items.sort(key=lambda x: x.get("updatedAt", ""), reverse=True)
        return items

    async def _fetch_new_project_entries(self, org: str, number: int, last_cursor: str) -> Tuple[List[Dict], str]:
        items = await self._fetch_project_items(org, number, first=MAX_SCAN_ENTRIES)
        if not items:
            return [], last_cursor

        if "T" not in last_cursor:
            last_cursor = "2000-01-01T00:00:00Z"

        new_entries = []
        max_time = last_cursor

        for item in items:
            entry = self._build_project_entry_dict(item, org, number)
            if not entry:
                continue
            
            entry_time = entry.get("raw_updated_at", "")
            if entry_time > last_cursor:
                new_entries.append(entry)
                if entry_time > max_time:
                    max_time = entry_time

        new_entries.sort(key=lambda x: x.get("raw_updated_at", ""))

        if self.max_entries > 0 and len(new_entries) > self.max_entries:
            new_entries = new_entries[-self.max_entries:]

        return new_entries, max_time

    def _build_project_entry_dict(self, item: Dict, org: str, number: int) -> Dict:
        content = item.get("content")
        if not content:
            return {}

        typename = content.get("__typename")
        title = content.get("title", "无标题")
        url = content.get("url", "")
        item_type = "Card"
        number_str = ""
        author_display = ""
        assignees = []
        state = ""
        merged = False
        latest_comment = None

        if typename == "Issue":
            item_type = "Issue"
            number_str = f"#{content.get('number', '')}"
            author_obj = content.get("author") or {}
            author_display = self._normalize_user_display(author_obj)
            assignees = self._extract_assignee_logins(content.get("assignees") or {})
            state = content.get("state", "")
            comments = content.get("comments", {}).get("nodes", []) or []
            if comments:
                c = comments[-1]
                c_author_obj = c.get("author") or {}
                latest_comment = {
                    "author": self._normalize_user_display(c_author_obj),
                    "body": c.get("bodyText", "") or "",
                    "createdAt": self._convert_time(c.get("createdAt", "")),
                }
        elif typename == "PullRequest":
            item_type = "PR"
            number_str = f"#{content.get('number', '')}"
            author_obj = content.get("author") or {}
            author_display = self._normalize_user_display(author_obj)
            assignees = self._extract_assignee_logins(content.get("assignees") or {})
            state = content.get("state", "")
            merged = bool(content.get("merged", False))
            comments = content.get("comments", {}).get("nodes", []) or []
            if comments:
                c = comments[-1]
                c_author_obj = c.get("author") or {}
                latest_comment = {
                    "author": self._normalize_user_display(c_author_obj),
                    "body": c.get("bodyText", "") or "",
                    "createdAt": self._convert_time(c.get("createdAt", "")),
                }
        elif typename == "DraftIssue":
            item_type = "Draft Issue"
            number_str = ""

        # 计算最新活跃时间
        item_updated_at = item.get("updatedAt", "")
        content_updated_at = content.get("updatedAt", "")
        content_created_at = content.get("createdAt", "")
        
        # 获取最新评论的时间和作者
        comment_time = ""
        comment_author = ""
        if latest_comment:
            comment_time = latest_comment.get("createdAt", "")
            comment_author = latest_comment.get("author", "")

        # 选出最晚的一个时间戳作为该卡片的“最后活跃时间”
        valid_times = [t for t in [item_updated_at, content_updated_at, comment_time] if t]
        last_active_time = max(valid_times) if valid_times else ""

        # 到底更新了什么？是谁更新的？
        update_type = "📌 状态/属性更新"
        actor = author_display # 默认触发者是卡片提出者

        if last_active_time == comment_time and comment_time:
            update_type = "💬 新评论"
            actor = comment_author
        elif last_active_time == content_created_at and content_created_at:
            update_type = "🆕 新建卡片"

        return {
            "title": f"[{item_type} {number_str}] {title}".replace("[] ", ""), # 稍微优化标题格式
            "link": url,
            "published": self._convert_time(last_active_time),
            "raw_updated_at": last_active_time,
            "id": item.get("id", ""),
            "type": "project_item",
            "author": author_display,
            "assignees": assignees,
            "state": state,
            "merged": merged,
            "latest_comment": latest_comment,
            "update_type": update_type,
            "actor": actor,
        }

    # ==================== 轮询与推送 ====================

    async def _poll_loop(self):
        await asyncio.sleep(10)
        while True:
            try:
                await self._do_poll()
            except asyncio.CancelledError:
                logger.info("[Private GitHub] 轮询任务已取消")
                return
            except Exception as e:
                logger.error(f"[Private GitHub] 轮询出错: {e}")
            await asyncio.sleep(self.poll_interval)

    async def _do_poll(self):
        if not self.github_token:
            return

        messages_by_session: Dict[str, List[Tuple[str, List[str]]]] = {}

        for session, items in list(self.subscriptions.items()):
            for sub in items:
                try:
                    last_cursor = await self._get_cursor(session, sub)
                    if not last_cursor:
                        await self._init_subscription_cursor(session, sub)
                        continue
                    if last_cursor == "__EMPTY__":
                        continue

                    if sub["type"] == "repo":
                        new_entries, new_cursor = await self._fetch_new_repo_entries(
                            sub["repo"], sub["event"], last_cursor
                        )
                    else:
                        new_entries, new_cursor = await self._fetch_new_project_entries(
                            sub["org"], sub["number"], last_cursor
                        )

                    if new_entries:
                        if sub["type"] == "repo":
                            msg = self._format_repo_entries(sub["repo"], sub["event"], new_entries)
                        else:
                            msg = self._format_project_entries(f"{sub['org']}/{sub['number']}", new_entries)
                        if msg:
                            assignees_to_notify = set()
                            for entry in new_entries:
                                actor = entry.get("actor", "")
                                for a in entry.get("assignees", []):
                                    if a and a != actor:
                                        assignees_to_notify.add(a)
                            
                            messages_by_session.setdefault(session, []).append((msg, list(assignees_to_notify)))

                    if new_cursor and new_cursor != last_cursor:
                        await self._set_cursor(session, sub, new_cursor)
                except Exception as e:
                    logger.error(f"[Private GitHub] 处理订阅 {sub} 失败: {e}")

        for session, msg_list in messages_by_session.items():
            full_msg = "\n\n".join([m[0] for m in msg_list])
            chain = MessageChain().message(full_msg)
            logger.info(f"[Private GitHub] 推送到 {session}: {full_msg}")
            seen = set()
            for _, ass in msg_list:
                for a in ass or []:
                    if not a:
                        continue
                    if a in seen:
                        continue
                    seen.add(a)
                    qq = self._resolve_qq_for_username(a)
                    if qq and self.at_enabled:
                        try:
                            chain.at(a, qq)
                        except Exception:
                            pass
            try:
                await self.context.send_message(session, chain)
            except Exception as e:
                logger.error(f"[Private GitHub] 推送到 {session} 失败: {e}")

    # ==================== 消息格式化 ====================

    @staticmethod
    def _format_repo_entries(repo: str, event_type: str, entries: List[Dict]) -> str:
        type_icon = {
            "issues": "🐛",
            "commits": "📝",
            "releases": "📦"
        }.get(event_type, "🔔")
        lines = [f"{type_icon} 仓库 {repo} 的新 {event_type} 动态（{len(entries)} 条）：\n"]
        for i, entry in enumerate(entries, 1):
            lines.append(f"  {i}. {entry.get('title')}")
            if entry.get("published"):
                lines.append(f"     🕐 时间: {entry.get('published')}")
            # 作者与指派者
            if entry.get("author"):
                lines.append(f"     🙋 提出者: @{entry.get('author')}")
            if entry.get("assignees"):
                ass = entry.get("assignees") or []
                if ass:
                    lines.append("     🔔 指派给: " + " ".join([f"@{a}" for a in ass]))
            if entry.get("state"):
                lines.append(f"     📊 状态: {entry.get('state')}")
            if entry.get("link"):
                lines.append(f"     🔗 {entry.get('link')}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_project_entries(project_id: str, entries: List[Dict]) -> str:
        lines = [f"📢 组织项目 {project_id} 有 {len(entries)} 个新动态：\n"]
        for i, entry in enumerate(entries, 1):
            update_type = entry.get("update_type", "📌 状态/属性更新")
            lines.append(f"  {i}. {update_type} | {entry.get('title')}")
            
            if entry.get("published"):
                lines.append(f"     🕐 时间: {entry.get('published')}")
                
            # 如果是新评论，突出展示评论内容
            if "新评论" in update_type and entry.get("latest_comment"):
                lc = entry.get("latest_comment")
                snippet = (lc.get("body", "") or "").replace("\n", " ")[:150]
                lines.append(f"     💬 @{lc.get('author','')}: {snippet}")
            else:
                # 不是纯评论时，才展示状态和提出者
                if entry.get("state"):
                    lines.append(f"     📊 当前状态: {entry.get('state')}")
                if entry.get("author"):
                    lines.append(f"     🙋 提出者: @{entry.get('author')}")

            # 被指派的人（提醒用）
            ass = entry.get("assignees") or []
            if ass:
                lines.append("     🔔 指派给: " + " ".join([f"@{a}" for a in ass]))
                
            if entry.get("link"):
                lines.append(f"     🔗 {entry.get('link')}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_single_check_repo(repo: str, event_type: str, entries: List[Dict]) -> str:
        type_icon = {
            "issues": "🐛",
            "commits": "📝",
            "releases": "📦"
        }.get(event_type, "🔔")
        lines = [f"{type_icon} 仓库 {repo} 最近的 {event_type} 动态：\n"]
        for i, entry in enumerate(entries, 1):
            lines.append(f"  {i}. {entry['title']}")
            if entry.get("published"):
                lines.append(f"     🕐 时间: {entry['published']}")
            if entry.get("author"):
                lines.append(f"     🙋 提出者: @{entry.get('author')}")
            if entry.get("assignees"):
                ass = entry.get("assignees") or []
                if ass:
                    lines.append("     🔔 指派给: " + " ".join([f"@{a}" for a in ass]))
            if entry.get("state"):
                lines.append(f"     📊 状态: {entry.get('state')}")
            if entry.get("link"):
                lines.append(f"     🔗 {entry.get('link')}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_single_check_project(project_id: str, entries: List[Dict]) -> str:
        lines = [f"📢 组织项目 {project_id} 最近的卡片动态：\n"]
        for i, entry in enumerate(entries, 1):
            update_type = entry.get("update_type", "📌 状态/属性更新")
            lines.append(f"  {i}. {update_type} | {entry.get('title')}")
            
            if entry.get("published"):
                lines.append(f"     🕐 时间: {entry.get('published')}")
                
            if "新评论" in update_type and entry.get("latest_comment"):
                lc = entry.get("latest_comment")
                snippet = (lc.get("body", "") or "").replace("\n", " ")[:150]
                lines.append(f"     💬 @{lc.get('author','')}: {snippet}")
            else:
                if entry.get("state"):
                    lines.append(f"     📊 当前状态: {entry.get('state')}")
                if entry.get("author"):
                    lines.append(f"     🙋 提出者: @{entry.get('author')}")

            ass = entry.get("assignees") or []
            if ass:
                lines.append("     🔔 指派给: " + " ".join([f"@{a}" for a in ass]))
                
            if entry.get("link"):
                lines.append(f"     🔗 {entry.get('link')}")
            lines.append("")
        return "\n".join(lines)

    # ==================== 订阅管理指令 ====================

    async def _add_subscription(self, event: AstrMessageEvent, sub_type: str, *args):
        """通用添加订阅逻辑"""
        session = get_session_id(event)
        if sub_type == "repo":
            if len(args) < 2:
                yield event.plain_result("❌ 用法: ghp_subscribe repo <owner/repo> <issues|commits|releases>")
                return
            repo = args[0]
            event_type = args[1].lower()
            if not RE_REPO.match(repo):
                yield event.plain_result("❌ 仓库格式不正确，应为 owner/repo")
                return
            if event_type not in ("issues", "commits", "releases"):
                yield event.plain_result("❌ 事件类型必须为 issues / commits / releases 之一")
                return
            for sub in self.subscriptions.get(session, []):
                if sub.get("type") == "repo" and sub["repo"] == repo and sub["event"] == event_type:
                    yield event.plain_result(f"⚠️ 当前会话已订阅 {repo} 的 {event_type} 动态")
                    return
            new_sub = {"type": "repo", "repo": repo, "event": event_type}
        elif sub_type == "project":
            if len(args) < 1:
                yield event.plain_result("❌ 用法: ghp_subscribe project <org/number>")
                return
            proj_str = args[0]
            match = RE_PROJECT_ITEM.match(proj_str)
            if not match:
                yield event.plain_result("❌ 项目格式不正确，应为 org/number，如 my-org/1")
                return
            org, num = match.group(1), int(match.group(2))
            for sub in self.subscriptions.get(session, []):
                if sub.get("type") == "project" and sub["org"] == org and sub["number"] == num:
                    yield event.plain_result(f"⚠️ 当前会话已订阅项目 {org}/{num}")
                    return
            new_sub = {"type": "project", "org": org, "number": num}
        else:
            yield event.plain_result("❌ 未知订阅类型")
            return

        if session not in self.subscriptions:
            self.subscriptions[session] = []
        self.subscriptions[session].append(new_sub)
        self._save_subscriptions()
        await self._init_subscription_cursor(session, new_sub)
        yield event.plain_result(f"✅ 已成功订阅：{self._format_sub(new_sub)}")

    @filter.command("ghp_subscribe")
    async def ghp_subscribe(self, event: AstrMessageEvent):
        """订阅 GitHub 动态
        用法:
          ghp_subscribe repo <owner/repo> <issues|commits|releases>
          ghp_subscribe project <org/number>
        """
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        parts = event.message_str.strip().split()
        if len(parts) < 3:
            yield event.plain_result("❌ 用法错误，详见 /help ghp_subscribe")
            return
        sub_type = parts[1]
        args = parts[2:]
        async for result in self._add_subscription(event, sub_type, *args):
            yield result

    @filter.command("ghp_unsubscribe")
    async def ghp_unsubscribe(self, event: AstrMessageEvent, index: int = None):
        """取消订阅
        用法: ghp_unsubscribe <序号>   (序号通过 ghp_list_subs 查看)
        """
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if not items:
            yield event.plain_result("当前会话没有任何订阅")
            return

        if index is None:
            msg = "📋 当前会话的订阅列表：\n"
            for i, sub in enumerate(items, 1):
                msg += f"{i}. {self._format_sub(sub)}\n"
            msg += "请使用 ghp_unsubscribe <序号> 取消对应的订阅"
            yield event.plain_result(msg)
            return

        try:
            idx = int(index) - 1
            if idx < 0 or idx >= len(items):
                yield event.plain_result(f"❌ 序号无效，请输入 1-{len(items)} 之间的数字")
                return
            removed = items.pop(idx)
            if not items:
                del self.subscriptions[session]
            self._save_subscriptions()
            key = self._get_cursor_key(session, removed)
            await self.put_kv_data(key, "")
            yield event.plain_result(f"✅ 已取消订阅：{self._format_sub(removed)}")
        except ValueError:
            yield event.plain_result("❌ 序号必须为数字")

    @filter.command("ghp_list_subs")
    async def ghp_list_subs(self, event: AstrMessageEvent):
        """列出当前会话的所有订阅"""
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return
        session = get_session_id(event)
        items = self.subscriptions.get(session, [])
        if not items:
            yield event.plain_result("当前会话没有任何订阅。使用 ghp_subscribe 添加订阅。")
            return
        msg = "📋 当前会话的订阅列表：\n"
        for i, sub in enumerate(items, 1):
            msg += f"{i}. {self._format_sub(sub)}\n"
        yield event.plain_result(msg)

    def _format_sub(self, sub: Dict) -> str:
        if sub["type"] == "repo":
            return f"仓库 {sub['repo']} 的 {sub['event']} 动态"
        else:
            return f"项目 {sub['org']}/{sub['number']} 的卡片动态"

    @filter.command("ghp_pushnow")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def ghp_pushnow(self, event: AstrMessageEvent):
        """立即执行一次全局推送（检查所有会话的所有订阅）"""
        if not self.github_token:
            yield event.plain_result("❌ 未配置 github_token，无法执行推送")
            return
        yield event.plain_result("🔄 正在立即检查所有订阅并推送...")
        try:
            await self._do_poll()
            yield event.plain_result("✅ 推送完成（如有新动态已发送）")
        except Exception as e:
            logger.error(f"[Private GitHub] 手动推送失败: {e}")
            yield event.plain_result(f"❌ 推送过程中发生错误: {e}")

    @filter.command("ghp_check")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def ghp_check(self, event: AstrMessageEvent):
        """手动检查指定仓库或项目的最新动态（不推送，仅查看）
        用法：
          /ghp_check repo owner/repo issues|commits|releases
          /ghp_check project org/number
        """
        if not is_user_allowed(self, event):
            yield event.plain_result("❌ 你没有权限使用此指令")
            return

        parts = event.message_str.strip().split()
        if len(parts) < 3:
            yield event.plain_result(
                "❌ 用法错误，示例：\n"
                "  /ghp_check repo nju-mc-org/server_document issues\n"
                "  /ghp_check project my-org/1"
            )
            return

        sub_type = parts[1].lower()
        if sub_type == "repo":
            if len(parts) < 4:
                yield event.plain_result("用法: /ghp_check repo <owner/repo> <issues|commits|releases>")
                return
            repo = parts[2]
            event_type = parts[3].lower()
            if not RE_REPO.match(repo) or event_type not in ("issues", "commits", "releases"):
                yield event.plain_result("❌ 仓库格式或事件类型错误")
                return

            yield event.plain_result(f"🔄 正在获取 {repo} 的 {event_type} 动态...")
            url = self._build_repo_api_url(repo, event_type, per_page=self.max_entries or 10)
            items = await self._rest_api_get(url)
            if items is None:
                yield event.plain_result("❌ 无法获取数据，请检查仓库名及 token 权限")
                return

            entries = []
            for item in items[:self.max_entries or 10]:
                entry = self._build_repo_entry_dict(item, event_type)
                if entry:
                    entries.append(entry)

            if not entries:
                yield event.plain_result(f"🔍 仓库 {repo} 暂无最近的 {event_type} 动态。")
            else:
                msg = self._format_single_check_repo(repo, event_type, entries)
                yield event.plain_result(msg)

        elif sub_type == "project":
            if len(parts) < 3:
                yield event.plain_result("用法: /ghp_check project <org/number>")
                return
            proj_str = parts[2]
            match = RE_PROJECT_ITEM.match(proj_str)
            if not match:
                yield event.plain_result("❌ 项目格式错误，应为 org/number")
                return
            org, num = match.group(1), int(match.group(2))

            yield event.plain_result(f"🔄 正在获取项目 {org}/{num} 的最新卡片...")
            items = await self._fetch_project_items(org, num, first=self.max_entries or 10)
            if not items:
                yield event.plain_result("❌ 无法获取项目数据，请检查组织名、项目编号及 token 权限")
                return

            entries = []
            for item in items[:self.max_entries or 10]:
                entry = self._build_project_entry_dict(item, org, num)
                if entry:
                    entries.append(entry)

            if not entries:
                yield event.plain_result(f"🔍 项目 {org}/{num} 暂无最近的卡片动态。")
            else:
                msg = self._format_single_check_project(f"{org}/{num}", entries)
                yield event.plain_result(msg)

        else:
            yield event.plain_result("❌ 第二个参数必须是 repo 或 project")

    # 兼容旧指令
    @filter.command("ghp_list")
    async def ghp_list(self, event: AstrMessageEvent):
        """兼容旧指令，现在推荐使用 ghp_list_subs 查看本会话订阅"""
        yield event.plain_result(
            "💡 新版插件支持多会话独立订阅，请使用 ghp_list_subs 查看当前会话的订阅。"
            "全局监听列表已废弃。"
        )

    @filter.command("ghp_bindhere")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def ghp_bindhere(self, event: AstrMessageEvent):
        yield event.plain_result(
            "⚠️ 此指令已废弃。现在每个会话自动独立管理订阅，无需绑定。请使用订阅指令：\n"
            "ghp_subscribe repo owner/repo issues\n"
            "ghp_subscribe project org/number"
        )