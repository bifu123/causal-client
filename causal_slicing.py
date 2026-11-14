#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
causal_slicing.py —— 元龙因果 Agent 的「切片器」(Slicer)（PyPI 单文件版）

自包含的单文件实现：把对话流切分为事件粒子，按 source_id（场景会话）隔离，
以物理时间先后组织成因果链，贞入（写入）因果星空；并以 bot_id 为主键跨 source_id
全局共享可见，接受因果星空的语义时空弯曲。

实现依据：Memory.md §2.1（切片器）与 §5（贞入流程 / 写入机制）。

贞入流程核心（「瓶子」模型）：
    1. 累加优先：必答/非必答一视同仁，先看当前瓶子（焦点节点）是否装得下
       —— 装得下就 update 累加进去（不看 node_id 是否冲突）；累加时必须显式
       回传原 previous_ids / action_tag / block_tag，否则服务端默认值会把当前
       瓶子重置为首贞。每次判断都显式输出容量明细。
    2. 满则又贞：当前瓶子 event_tuple 大小 + 欲贞内容超过上限 →「封盖」，另起
       新瓶（又贞接龙到链尾）；此时才处理 node_id 冲突（换唯一 ID 接龙）。
    3. 先核对再接续：Redis 有记录则回星空核对同 serial_id 节点；记录缺失或节点已失效
       则回星空找最近一个 [chat] 瓶子；两者都找不到才「首贞」。
    4. 又贞强制父节点非空：满瓶又贞前先确认 node_id 是否重复，重复则换唯一 ID，
       但 previous_node 始终保留当前瓶子的 node_id；写入后回读校验父链，
       若服务端未记住 previous_node，则立即用 update 补回，避免旧瓶变成孤立节点。
    5. 切分：单条内容超上限则按块切分（一块一瓶），保持断句完整、不跨句切瓶。
    6. 回写：serial_id / 计数写入 redis（必答/非必答都必须记住最后一瓶 serial_id）。
    7. 必答 vs 非必答：必答贞入/累加后 click 拉取事件视界（作 AI 历史上下文），
       非必答不点（省星空计算开销）。
    8. 坍塌器：整链 event_tuple 之和超阈值时，另起线程触发坍塌器。

配置方式（无本地配置模块依赖，直接通过构造函数参数传入）：
    BotSlicer(base_url=..., owner_id=..., actor_id=..., token=...,
              bot_id=..., source_id=..., max_tuple_size=..., collapse_threshold=..., timeout=...)
    必填参数：base_url（因果星空服务地址）、bot_id（机器人 ID，跨场共享主键）。
    关键可调项：
        max_tuple_size      瓶子大小——单个事件节点 event_tuple 大小上限，默认 2000
        collapse_threshold  整链大小上限——所有瓶子 event_tuple 之和超此值触发坍塌器，默认 10000
        timeout             HTTP 超时（秒），默认 120
    其余可调项（max_eyes / redis_*）均有默认值，需要时按名传入即可，
    详见 BotSlicer.__init__ 的注释。

依赖：requests；redis 为可选（Redis 5.x 需 RESP2，protocol=2）。

用法：
    from causal_slicing import BotSlicer

    slicer = BotSlicer(
        base_url="http://192.168.66.39:8094",
        owner_id="dev",
        actor_id="415135222",
        token="123456|...",
        bot_id="3787687088",
        source_id="454652835",
        max_tuple_size=2000,        # 瓶子大小
        collapse_threshold=10000,   # 整链大小上限
    )
    slicer.zhenru(event_tuple="你好，今天是一个晴天。", node_id="问好", user_name="廉颇", is_must_answer="yes")
    horizon = slicer.present()   # 当下（Agent Memory）
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests

try:
    import redis as _redis_mod  # type: ignore
    _HAS_REDIS = True
except ImportError:  # pragma: no cover
    _redis_mod = None  # type: ignore
    _HAS_REDIS = False

__version__ = "0.1.1"
__all__ = ["CausalClient", "BotSlicer", "field_owner_id"]

logger = logging.getLogger("causal_slicing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# 断句结束符（切分时尽量在句末断开，保证断句完整）
_SENTENCE_END_RE = re.compile(r"[。！？!?；;…\n.]")

# 节点级 [chat] 标记 与 切片分隔符（累加时用 --- 分隔多轮切片，便于日后转译成 JSON）
_CHAT_TAG = "[chat]"
_SLICE_SEP = "\n\n---\n\n"

# 坍塌触发倍率（默认值）：整链 event_tuple 之和达到 collapse_threshold × 该倍率时才触发坍塌器。
# 配合「只坍 collapse_threshold 大小」形成迟滞，避免坍完立刻再次触发，并保留最近节点的连续上下文。
_COLLAPSE_TRIGGER_RATIO = 1.5

# 坍塌器线程池（进程级单例）：多个 BotSlicer 实例共享，避免每次新建切片器都泄漏一个常驻线程。
# max_workers 为并发上限；同一实例内仍由 _collapse_lock 保证「同 bot 同时只跑一个坍塌」。
_COLLAPSE_MAX_WORKERS = 4
_collapse_executor_lock = threading.Lock()
_collapse_executor: Optional[ThreadPoolExecutor] = None


def _get_collapse_executor() -> ThreadPoolExecutor:
    """返回共享的坍塌器线程池（懒加载单例）。"""
    global _collapse_executor
    if _collapse_executor is None:
        with _collapse_executor_lock:
            if _collapse_executor is None:
                _collapse_executor = ThreadPoolExecutor(
                    max_workers=_COLLAPSE_MAX_WORKERS, thread_name_prefix="collapse",
                )
    return _collapse_executor


def _strip_chat(text: str) -> str:
    """去掉结尾唯一的节点级 [chat] 标记（仅去尾，不误删正文里字面的 [chat]）。"""
    if text.endswith(_CHAT_TAG):
        return text[: -len(_CHAT_TAG)]
    return text

import requests

# 取得因果星空中最后贞入的事件节点
def get_max_serial_id_from_api(base_url: str, owner_id: str, token: Optional[str] = None,
                                actor_id: Optional[str] = None) -> int:
    
    """取得因果星空中最后贞入的事件节点"""
    url = f"{base_url.rstrip('/')}/api/v1/causal/history"
    params: Dict[str, Any] = {"owner_id": owner_id}
    if actor_id:
        params["actor_id"] = actor_id
    if token:
        params["token"] = token
    try:
        # 发起 HTTP GET 请求，设置适当的超时时间
        response = requests.get(url, params=params, timeout=10)
        # 检查响应状态码，若不是 2xx 会抛出 HTTPError 异常
        response.raise_for_status()
        
        # 解析 JSON 响应
        content = response.json()
        
        # 获取 data 列表
        data_list = content.get("data", [])
        if not data_list:
            return 0
            
        # 提取所有非空的 serial_id
        serial_ids = [
            item.get("serial_id") 
            for item in data_list 
            if isinstance(item, dict) and item.get("serial_id") is not None and _CHAT_TAG in (item.get("event_tuple") or "")
        ]
        
        if not serial_ids:
            return 0
            
        # 返回最大值并确保为数字类型（int）
        return int(max(serial_ids))
        
    except Exception:
        # 在网络请求失败、JSON 解析错误或任何其他异常情况下均返回 0
        return 0


def field_owner_id(source_id: str, bot_id: str) -> str:
    """一个 source_id（会话，如 QQ/微信联系人）对应一个 owner_id 场：{source_id}_{bot_id}。

    即「一 source 一场一链」的 1:1 映射；切片器与坍塌器都用它算场，保证接龙/坍塌落在同一场，
    不会接错链。真实运行时 owner_id 不是写死的 "dev"，而是按 source_id 现算。
    """
    return f"{source_id}_{bot_id}"


# ============================================================
# 因果星空 HTTP 客户端
# ============================================================
class CausalClient:
    """对 /api/v1/causal/* 接口的最小封装，统一注入 token 与鉴权。"""

    def __init__(self, base_url: str, token: Optional[str], owner_id: str,
                 actor_id: str, timeout: float = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.owner_id = owner_id
        self.actor_id = actor_id
        self.timeout = timeout
        self.session = requests.Session()

    def _request(self, method: str, path: str, *, params: Optional[Dict] = None,
                 json_body: Optional[Dict] = None) -> Dict[str, Any]:
        url = self.base_url + path
        params = dict(params or {})
        if self.token and "token" not in params:
            params["token"] = self.token
        kwargs: Dict[str, Any] = {"timeout": self.timeout, "params": params}
        if json_body is not None:
            kwargs["json"] = json_body
        try:
            resp = self.session.request(method, url, **kwargs)
        except requests.exceptions.Timeout as exc:
            raise RuntimeError(f"{method} {path} 请求超时（timeout={self.timeout}s）") from exc
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError(f"{method} {path} 连接失败：{exc}") from exc
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            return {"data": data}
        if data.get("status") == "error":
            message = data.get("message") or ""
            # 「当下」视界自愈：该 actor 尚未对当前场实施观测（无大股东节点）时，
            # 自动补一次「过往」点击最近贞入的事件，建立观测并返回视界。
            if path == "/api/v1/causal/horizon" and ("未找到大股东节点" in message or "还没有事件" in message):
                owner = (params or {}).get("owner_id") or self.owner_id
                latest_serial = get_max_serial_id_from_api(
                    self.base_url, owner, token=self.token, actor_id=self.actor_id
                )
                if latest_serial > 0:
                    click_params = dict(params or {})
                    click_params["serial_id"] = latest_serial
                    logger.info(
                        "「当下」视界自愈：场 %s 无大股东节点，点击最近事件 serial_id=%s 实施观测",
                        owner, latest_serial,
                    )
                    return self._request("GET", "/api/v1/causal/click", params=click_params)
                logger.warning("「当下」视界自愈：场 %s 为空场（尚未贞入任何事件），返回空视界。", owner)
                return {"data": {}, "event_horizon": []}
            raise RuntimeError(f"{method} {path} 失败: {message or data}")
        return data

    # ---- 写入 ----
    def genesis(self, node_id: str, event_tuple: str, previous_node: Optional[Any] = None,
                block_tag: str = "因", action_tag: str = "贞",
                full_image_url: Optional[str] = None, is_share: bool = False,
                return_serial_id: bool = True,
                owner_id: Optional[str] = None) -> Dict[str, Any]:
        """首贞 / 又贞：新建事件节点（严格 INSERT，node_id 重复会报错）。"""
        body: Dict[str, Any] = {
            "node_id": node_id,
            "event_tuple": event_tuple,
            "block_tag": block_tag,
            "action_tag": action_tag,
            "owner_id": owner_id or self.owner_id,
            "is_share": is_share,
            "return_serial_id": return_serial_id,
            "token": self.token,
        }
        if previous_node is not None:
            body["previous_node"] = previous_node
        if full_image_url:
            body["full_image_url"] = full_image_url
        return self._request("POST", "/api/v1/causal/genesis", json_body=body)

    def update(self, old_node_id: str, new_node_id: Optional[str] = None,
               event_tuple: Optional[str] = None, is_share: bool = False,
               previous_ids: Optional[Any] = None,
               action_tag: Optional[str] = None,
               block_tag: Optional[str] = None,
               owner_id: Optional[str] = None) -> Dict[str, Any]:
        """累加 / 编辑：修改既有节点（event_tuple 整体替换；previous_ids 可重连因果链）。"""
        body: Dict[str, Any] = {
            "old_node_id": old_node_id,
            "new_node_id": new_node_id or old_node_id,
            "owner_id": owner_id or self.owner_id,
            "is_share": is_share,
            "token": self.token,
        }
        if event_tuple is not None:
            body["event_tuple"] = event_tuple
        if previous_ids is not None:
            body["previous_ids"] = previous_ids
        if action_tag is not None:
            body["action_tag"] = action_tag
        if block_tag is not None:
            body["block_tag"] = block_tag
        return self._request("POST", "/api/v1/causal/update", json_body=body)

    def delete(self, node_id: str, owner_id: Optional[str] = None) -> Dict[str, Any]:
        """删除事件节点（子事件自动继承被删事件的父ID，因果链不断裂）。"""
        body: Dict[str, Any] = {
            "node_id": node_id,
            "owner_id": owner_id or self.owner_id,
            "token": self.token,
        }
        return self._request("POST", "/api/v1/causal/delete", json_body=body)

    # ---- 读取 ----
    def horizon(self, bot_id: Optional[str] = None, max_eyes: Optional[float] = None,
                owner_id: Optional[str] = None) -> Dict[str, Any]:
        """当下：获取当前大股东的事件视界。带 bot_id 即 Agent Memory（跨场 [chat] 合并）。"""
        params: Dict[str, Any] = {"owner_id": owner_id or self.owner_id, "actor_id": self.actor_id}
        if bot_id:
            params["bot_id"] = bot_id
        if max_eyes is not None:
            params["max_eyes"] = max_eyes
        return self._request("GET", "/api/v1/causal/horizon", params=params)

    def click(self, serial_id: int, bot_id: Optional[str] = None,
              max_eyes: Optional[float] = None,
              owner_id: Optional[str] = None) -> Dict[str, Any]:
        """过往：点击指定事件，提升权重并返回视界。带 bot_id 即 Agent Memory。"""
        params: Dict[str, Any] = {
            "serial_id": serial_id,
            "owner_id": owner_id or self.owner_id,
            "actor_id": self.actor_id,
        }
        if bot_id:
            params["bot_id"] = bot_id
        if max_eyes is not None:
            params["max_eyes"] = max_eyes
        return self._request("GET", "/api/v1/causal/click", params=params)

    def history(self, bot_id: Optional[str] = None,
                owner_id: Optional[str] = None) -> Dict[str, Any]:
        """全量历史（带 bot_id 合并异场 [chat] 节点）。"""
        params: Dict[str, Any] = {"owner_id": owner_id or self.owner_id, "actor_id": self.actor_id}
        if bot_id:
            params["bot_id"] = bot_id
        return self._request("GET", "/api/v1/causal/history", params=params)



# ============================================================
# 切片器
# ============================================================
class BotSlicer:
    """元龙因果 Agent 的切片器：对话流 → 事件粒子 → 贞入因果星空。"""

    def __init__(self, base_url: str, bot_id: str, owner_id: str = "default",
                 actor_id: str = "", token: Optional[str] = None,
                 source_id: Optional[str] = None,
                 max_tuple_size: int = 2000, collapse_threshold: int = 10000,
                 collapse_trigger_ratio: float = _COLLAPSE_TRIGGER_RATIO,
                 max_eyes: float = 30, timeout: float = 120,
                 redis_url: str = "redis://127.0.0.1:6379/0",
                 redis_protocol: int = 2, redis_key_prefix: str = "causal_slicing") -> None:
        """
        参数说明（全部通过构造参数传入，无环境变量/配置文件依赖）：

        必填：
            base_url    因果星空服务地址，如 "http://192.168.66.39:8094"
            bot_id      元龙因果 Agent 的机器人 ID（跨场共享主键）

        连接/鉴权（可选，访问私密场时需提供 token）：
            owner_id    事件拥有者 ID（当前视角场），默认 "default"
            actor_id    用户/观测者 ID，用于个性化权重与定位大股东
            token       私密场钥匙 token（"防盗门"鉴权），默认 None
            source_id   当前场景会话 ID（默认 None，可在 zhenru() 逐条传入）

        可调项（有默认值）：
            max_tuple_size      单个瓶子（事件节点）event_tuple 大小上限（字符），默认 2000
            collapse_threshold      整链大小限制：所有瓶子 event_tuple 之和达到 collapse_threshold × collapse_trigger_ratio 即触发坍塌器，默认 10000
            collapse_trigger_ratio  坍塌触发倍率（collapse_threshold 的倍数），默认 1.5（即 150%）
            max_eyes                望远镜功率 / 事件视界半径，默认 30
            timeout             HTTP 超时（秒），默认 120（大文本语义向量化较慢）
            redis_url           Redis 状态库地址，默认 "redis://127.0.0.1:6379/0"
            redis_protocol      Redis 协议版本，Redis 5.x 需 2（RESP2），默认 2
            redis_key_prefix    Redis 键前缀，默认 "causal_slicing"
        """
        if not base_url:
            raise ValueError("base_url 不能为空：切片器需要因果星空服务地址。")
        if not bot_id:
            raise ValueError("bot_id 不能为空：切片器需要机器人 ID 作为跨场主键。")
        self.client = CausalClient(base_url, token, owner_id, actor_id, timeout=timeout)
        self.bot_id = bot_id
        self.source_id = source_id
        self.max_tuple_size = int(max_tuple_size)
        self.collapse_threshold = int(collapse_threshold)
        self.collapse_trigger_ratio = float(collapse_trigger_ratio)
        self.max_eyes = max_eyes

        self._redis_url = redis_url
        self._redis_protocol = redis_protocol
        self._prefix = redis_key_prefix
        self._redis = None
        self._redis_down = False          # 连接失败后置 True，避免每次操作都重连 + 重复告警
        self._redis_lock = threading.Lock()
        self._mem: Dict[str, Any] = {}          # redis 不可用时的进程内兜底
        self._collapse_callback: Optional[Callable] = None
        self._collapse_lock = threading.Lock()  # 坍塌器互斥锁：同一 bot 同时只跑一个坍塌
        # 共享线程池（进程级单例）执行坍塌；任务提交即返回 Future，供监听返回/状态
        self._collapse_executor = _get_collapse_executor()
        self._collapse_future: Optional[Future] = None


    # ---------------- Redis 状态库 ----------------
    def _get_redis(self):
        if self._redis is None and not self._redis_down:
            with self._redis_lock:
                if self._redis is None and not self._redis_down:
                    if not _HAS_REDIS:
                        logger.warning("未安装 redis 库，状态库退化为进程内字典（重启即失）。")
                        self._redis_down = True
                        return None
                    try:
                        self._redis = _redis_mod.Redis.from_url(
                            self._redis_url, protocol=self._redis_protocol, decode_responses=True
                        )
                        self._redis.ping()
                        logger.info(f"已连接 redis：{self._redis_url}（protocol={self._redis_protocol}）")
                    except Exception as exc:
                        logger.warning(f"redis 连接失败，退化为进程内字典：{exc}")
                        self._redis = None
                        self._redis_down = True
        return self._redis

    def _focus_key(self, source_id: str) -> str:
        return f"{self._prefix}:focus:{self.bot_id}:{source_id}"

    def _chain_key(self, source_id: str) -> str:
        return f"{self._prefix}:chain:{self.bot_id}:{source_id}"

    def _field_owner(self, source_id: str) -> str:
        """当前 source_id 对应的复合 owner_id 场（与 field_owner_id 一致）。"""
        return field_owner_id(source_id, self.bot_id)

    def _get_focus(self, source_id: str) -> Optional[Dict[str, Any]]:
        """读取当下焦点：{"serial_id", "node_id", "event_tuple", "size"}。"""
        r = self._get_redis()
        if r is None:
            return self._mem.get(self._focus_key(source_id))
        raw = r.get(self._focus_key(source_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    def _set_focus(self, source_id: str, serial_id: Optional[int], node_id: str,
                   event_tuple: str) -> None:
        data = {
            "serial_id": serial_id,
            "node_id": node_id,
            "event_tuple": event_tuple,
            "size": len(event_tuple),
        }
        payload = json.dumps(data, ensure_ascii=False)
        r = self._get_redis()
        if r is None:
            self._mem[self._focus_key(source_id)] = data
        else:
            r.set(self._focus_key(source_id), payload)

    # def _add_chain_count(self, n: int, source_id: str) -> None:
    #     """整链 event_tuple 之和增量累加（redis INCRBY）；超阈值时另起线程触发坍塌器。"""
    #     if n <= 0:
    #         return
    #     r = self._get_redis()
    #     key = self._chain_key(source_id)
    #     if r is None:
    #         total = self._mem.get(key, 0) + n
    #         self._mem[key] = total
    #     else:
    #         total = r.incrby(key, n)
    #     logger.debug(f"整链 event_tuple 之和 += {n}，当前 = {total}")
    #     if total >= self.collapse_threshold:
    #         if r is not None:
    #             r.set(key, 0)
    #         else:
    #             self._mem[key] = 0
    #         threading.Thread(target=self._run_collapse, args=(source_id, int(total)), daemon=True).start()
    
    def _collapse_trigger(self) -> int:
        """坍塌触发阈值 = collapse_threshold × collapse_trigger_ratio（默认 1.5 倍）。"""
        return int(self.collapse_threshold * self.collapse_trigger_ratio)

    def _add_chain_count(self, n: int, source_id: str) -> None:
        """整链 event_tuple 之和增量累加（redis INCRBY）；超阈值时触发坍塌器。"""
        if n <= 0:
            return
        r = self._get_redis()
        key = self._chain_key(source_id)
        if r is None:
            total = self._mem.get(key, 0) + n
            self._mem[key] = total
        else:
            total = r.incrby(key, n)
        logger.debug(f"整链 event_tuple 之和 += {n}，当前 = {total}")
        if total >= self._collapse_trigger():
            # 先把计数清零，避免 collapse 过程中新事件继续把 total 顶上去
            if r is not None:
                r.set(key, 0)
            else:
                self._mem[key] = 0
            # 另起线程执行坍塌，避免阻塞切片器主流程（串行化仍由 _collapse_lock 保证）
            self._submit_collapse(source_id, int(total))
    

    def get_chain_size(self, source_id: Optional[str] = None) -> int:
        """读取某 source 当前整链 event_tuple 之和（真实瓶子总和，非新增累计）。"""
        sid = source_id or self.source_id
        if not sid:
            return 0
        r = self._get_redis()
        key = self._chain_key(sid)
        if r is None:
            return int(self._mem.get(key, 0))
        val = r.get(key)
        return int(val) if val is not None else 0

    def set_collapse_callback(self, callback: Callable) -> None:
        """注册坍塌器回调（叙事提炼/影响感受），由独立线程调用。"""
        self._collapse_callback = callback

    def _submit_collapse(self, source_id: str, chain_size: int) -> Future:
        """把坍塌任务提交到线程池，立即返回可监听的 Future（不阻塞切片器主流程）。

        返回的 Future 可用于：
            future.done()                 —— 是否已结束
            future.result(timeout=...)    —— 阻塞等待并取回结果（或抛异常）
            future.add_done_callback(fn)  —— 完成时回调
        """
        future = self._collapse_executor.submit(self._run_collapse, source_id, chain_size)
        self._collapse_future = future
        return future

    def collapse_future(self) -> Optional[Future]:
        """返回最近一次坍塌任务的 Future（供监听返回/状态；无任务时为 None）。"""
        return self._collapse_future

    def collapse_is_running(self) -> bool:
        """坍塌是否正在执行中。"""
        fut = self._collapse_future
        return fut is not None and not fut.done()

    def collapse_done(self) -> bool:
        """最近一次坍塌是否已结束。"""
        fut = self._collapse_future
        return fut is not None and fut.done()

    def collapse_result(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """等待并返回最近一次坍塌的结果（Future.result 的封装）。

        timeout 为 None 时无限等待；超时抛 concurrent.futures.TimeoutError。
        返回 dict，含 status（done/skipped/no_callback/error）等字段；无任务时为 None。
        """
        fut = self._collapse_future
        if fut is None:
            return None
        return fut.result(timeout=timeout)

    def _run_collapse(self, source_id: str, chain_size: int) -> Dict[str, Any]:
        logger.info(f"整链大小 {chain_size} 超过触发阈值 {self._collapse_trigger()}，另起线程提炼压缩（坍塌器）。")
        if self._collapse_callback is None:
            return {"status": "no_callback", "source_id": source_id, "chain_size": chain_size}
        if not self._collapse_lock.acquire(blocking=False):
            logger.info("坍塌器已在运行，跳过本次触发（待下次计数再触发）。")
            return {"status": "skipped", "source_id": source_id, "chain_size": chain_size}
        try:
            result = self._collapse_callback(bot_id=self.bot_id, chain_size=chain_size,
                                             source_id=source_id)
            return {"status": "done", "source_id": source_id, "chain_size": chain_size,
                    "result": result}
        except Exception as exc:
            logger.exception("坍塌器回调执行失败")
            return {"status": "error", "source_id": source_id, "chain_size": chain_size,
                    "error": str(exc)}
        finally:
            self._collapse_lock.release()

    def reset_state(self, source_id: Optional[str] = None) -> None:
        """清空 redis 中本 bot 的焦点与整链计数（仅清指针，不删星空中的事件节点）。"""
        source_id = source_id or self.source_id
        r = self._get_redis()
        if r is None:
            if source_id:
                self._mem.pop(self._focus_key(source_id), None)
                self._mem.pop(self._chain_key(source_id), None)
            return
        if source_id:
            r.delete(self._focus_key(source_id))
            r.delete(self._chain_key(source_id))


    # ---------------- 切片辅助 ----------------
    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _gen_node_id(source_id: str) -> str:
        """生成全局唯一的 node_id（source_id_<纳秒时间戳>）。"""
        return f"{source_id}_{time.time_ns()}"

    @staticmethod
    def make_node_id(text: str) -> str:
        """由文本内容生成节点 ID：去除所有空白与保留分隔符 | 后截取前 8 字符 + 当前时间戳（| 是服务端多父节点分隔符，含 | 的 node_id 作 previous_node 会被拆分导致找不到）。"""
        processed = "".join(text.split()).replace("|", "")[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{processed}_{timestamp}"

    def _build_slice(self, user_name: str, user_id: str, content: str,
                     source_id: str) -> str:
        """构造一个事件粒子（时间字段不含 [chat]，[chat] 在节点级统一追加一次）。"""
        return self._wrap_slice_content(user_name, user_id, content, source_id)

    def _wrap_slice_content(self, user_name: str, user_id: str, content: str,
                            source_id: str) -> str:
        """把单块内容包上固定元信息，生成一个完整事件粒子文本。

        每个分块都必须带上 bot_id/user/user_id/source_id/内容/时间，
        否则分块后的后续块会只剩裸内容、丢失元信息。
        """
        lines = [
            f"**机器人ID(bot_id)**: {self.bot_id}",
            f"**用户(user)**: {user_name}",
            f"**用户ID(user_id)**: {user_id}",
            f"**来源ID(source_id)**: {source_id}",
            f"**内容(content)**: {content}",
            f"**时间(time)**: {self._now()}",
        ]
        return "\n".join(lines)

    def _slice_meta_size(self, user_name: str, user_id: str, source_id: str) -> int:
        """固定元信息开销（内容留空时的长度），用于计算分块时内容的可用上限。

        时间戳固定 19 字符（%Y-%m-%d %H:%M:%S），故开销与调用时刻无关。
        """
        return len(self._wrap_slice_content(user_name, user_id, "", source_id))

    def _split_text(self, text: str, limit: int) -> List[str]:
        """按上限切分文本为多块，尽量在句末（。！？；\n 等）断开以保持断句完整。"""
        limit = max(1, int(limit))
        if len(text) <= limit:
            return [text]
        chunks: List[str] = []
        rest = text
        while len(rest) > limit:
            window = rest[:limit]
            last = None
            for m in _SENTENCE_END_RE.finditer(window):
                last = m
            cut = last.end() if last else limit
            chunk = rest[:cut].strip()
            if not chunk:  # 找不到断句点，硬切
                cut = limit
                chunk = rest[:cut].strip()
            chunks.append(chunk)
            rest = rest[cut:].lstrip()
        if rest.strip():
            chunks.append(rest.strip())
        return chunks

    # ---------------- 贞入核心 ----------------
    @staticmethod
    def _parent_ids(node: Dict[str, Any]) -> List[str]:
        """汇总节点记录中的父节点字段（兼容 previous_node 单值/数组）。"""
        ids: List[str] = []
        for key in ("previous_node", "previous_ids"):
            value = node.get(key)
            if isinstance(value, (list, tuple, set)):
                candidates = [str(x) for x in value if x]
            elif value:
                candidates = [str(value)]
            else:
                candidates = []
            for candidate in candidates:
                for parent_id in candidate.split("|"):
                    parent_id = parent_id.strip()
                    if parent_id and parent_id not in ids:
                        ids.append(parent_id)
        return ids

    def _wait_for_node(self, owner_id: str, node_id: str,
                       attempts: int = 5, delay: float = 0.2) -> Optional[Dict[str, Any]]:
        """等待节点在 history 中可见，规避写入成功后短暂读不到/读到旧值。"""
        for attempt in range(attempts):
            node = self._find_node_by_node_id(owner_id, node_id)
            if node is not None:
                return node
            if attempt + 1 < attempts:
                time.sleep(delay * (attempt + 1))
        return None

    def _restore_parent_ids(self, owner_id: str, node_id: str,
                            parent_ids: List[str], action_tag: str = "又贞") -> bool:
        """把既有节点重新接回快照中的父节点，并回读确认。"""
        parent_ids = [str(x) for x in parent_ids if x]
        if not parent_ids:
            return True
        if not self._ensure_previous_link(owner_id, node_id, parent_ids[0]):
            return False
        node = self._wait_for_node(owner_id, node_id)
        if node is None:
            return False
        actual = set(self._parent_ids(node))
        if not set(parent_ids).issubset(actual):
            logger.error(
                "父链恢复后仍未确认：节点 '%s' 期望父节点=%s，实际=%s。",
                node_id, parent_ids, sorted(actual),
            )
            return False
        return True

    def _ensure_node_link_unchanged(self, owner_id: str, node_id: str,
                                    expected_parent_ids: List[str]) -> bool:
        """校验既有节点（通常是当前父瓶子）没有被本次写入改回首贞。"""
        expected = [str(x) for x in expected_parent_ids if x]
        if not expected:
            return True
        node = self._wait_for_node(owner_id, node_id)
        if node is None:
            logger.error("父链保护：既有节点 '%s' 写入后不可读。", node_id)
            return False
        actual = set(self._parent_ids(node))
        if set(expected).issubset(actual):
            return True
        logger.warning(
            "父链被改写：既有节点 '%s' 原父节点=%s，写入后=%s；立即恢复。",
            node_id, expected, sorted(actual),
        )
        return self._restore_parent_ids(owner_id, node_id, expected)

    def _ensure_previous_link(self, owner_id: str, node_id: str,
                              previous_node: str) -> bool:
        """校验新节点确实记住了父节点；若服务端丢链，则用 update 补回。

        某些情况下 genesis 返回 success，但落库记录可能是 action_tag='贞'、
        previous_node=None。若不校验，旧瓶会变成孤立节点。这里显式回读并自愈。
        """
        node = self._wait_for_node(owner_id, node_id)
        if node is None:
            logger.error("父链校验：新节点 '%s' 写入后仍不可读。", node_id)
            return False
        if previous_node in self._parent_ids(node):
            return True

        logger.warning(
            "父链丢失：节点 '%s' 未记录 previous_node='%s'，尝试用 update 补回。",
            node_id, previous_node,
        )
        try:
            self.client.update(
                old_node_id=node_id, new_node_id=node_id,
                previous_ids=[previous_node], action_tag="又贞", block_tag="因",
                owner_id=owner_id,
            )
        except Exception as exc:
            logger.error(
                "父链补回失败：节点 '%s' previous_node='%s'：%s",
                node_id, previous_node, exc,
            )
            return False

        node = self._wait_for_node(owner_id, node_id)
        if node is not None and previous_node in self._parent_ids(node):
            logger.info("父链已补回：节点 '%s' previous_node='%s'。", node_id, previous_node)
            return True
        logger.error("父链补回后仍未确认：节点 '%s' previous_node='%s'。", node_id, previous_node)
        return False

    def _genesis_chain_safe(self, new_node_id: str, node_tuple: str,
                            previous_node: Optional[str], source_id: str) -> tuple:
        """又贞 / 首贞：新建事件节点。

        又贞时 previous_node 必须非空；若星空返回父节点不存在，不再静默降级为首贞。
        node_id 冲突时换用自动生成的唯一 ID 重试，父节点保持不变。
        """
        if previous_node is not None and not str(previous_node).strip():
            raise ValueError(f"又贞时 previous_node 必须非空，收到：{previous_node!r}")
        use_node_id = new_node_id
        use_prev = previous_node
        owner = field_owner_id(source_id, self.bot_id)
        for _ in range(3):
            if use_prev is not None and not str(use_prev).strip():
                raise ValueError(f"又贞时父事件（前事件）不能为空，收到：{use_prev!r}")
            api_tag = "贞" if use_prev is None else "又贞"
            label = "首贞" if use_prev is None else "又贞"
            parent_snapshot = None
            if use_prev is not None:
                parent_snapshot = self._wait_for_node(owner, str(use_prev))
                if parent_snapshot is None:
                    raise RuntimeError(
                        f"又贞失败：父节点 '{use_prev}' 写入前不可读，拒绝建立断链。"
                    )
            try:
                resp = self.client.genesis(
                    node_id=use_node_id, event_tuple=node_tuple, previous_node=use_prev,
                    block_tag="因", action_tag=api_tag, is_share=False, return_serial_id=True,
                    owner_id=owner,
                )
                if use_prev is not None:
                    if not self._ensure_previous_link(owner, use_node_id, str(use_prev)):
                        raise RuntimeError(
                            f"又贞失败：新节点 '{use_node_id}' 未能接到父节点 '{use_prev}'。"
                        )
                    parent_ids = self._parent_ids(parent_snapshot)
                    if not self._ensure_node_link_unchanged(owner, str(use_prev), parent_ids):
                        raise RuntimeError(
                            f"又贞失败：父节点 '{use_prev}' 的父链在写入 '{use_node_id}' 后被改写。"
                        )
                return resp, use_node_id, label
            except Exception as exc:
                msg = str(exc)
                if "找不到" in msg and use_prev is not None:
                    raise RuntimeError(
                        f"又贞失败：previous_node '{use_prev}' 在星空中找不到（又贞要求父节点非空）"
                    ) from exc
                if ("已存在" in msg or "冲突" in msg) and use_node_id == new_node_id:
                    use_node_id = self._gen_node_id(source_id)
                    logger.warning(
                        f"node_id '{new_node_id}' 已存在，改用自动生成的唯一 ID：{use_node_id}"
                    )
                    continue
                raise
        raise RuntimeError("又贞 / 首贞重试次数用尽")

    def _find_latest_bottle(self, owner_id: str) -> Optional[Dict[str, Any]]:
        """找回星空中最后贞入的瓶子（event_tuple 带 [chat] 且 serial_id 最大的节点）。

        用于「无焦点（focus=None）」场景：判断是否还有可累加的瓶子，而非直接又贞。
        返回与焦点一致的 dict 结构，找不到（或查询失败）则返回 None。
        """
        try:
            resp = self.client.history(owner_id=owner_id)
        except Exception as exc:
            logger.warning(f"找回最后瓶子失败：{exc}")
            return None
        if not isinstance(resp, dict):
            return None
        latest = None
        for item in resp.get("data") or []:
            if not isinstance(item, dict):
                continue
            event_tuple = item.get("event_tuple") or ""
            node_id = item.get("node_id")
            if _CHAT_TAG not in event_tuple or not node_id:
                continue
            sid = item.get("serial_id")
            if sid is None:
                continue
            try:
                sid = int(sid)
            except (TypeError, ValueError):
                continue
            if latest is None or sid > latest["serial_id"]:
                latest = {"serial_id": sid, "node_id": str(node_id),
                          "event_tuple": event_tuple, "size": len(event_tuple)}
        return latest

    def _find_node_by_serial_id(self, owner_id: str, serial_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if serial_id is None:
            return None
        try:
            serial_id = int(serial_id)
        except (TypeError, ValueError):
            return None
        try:
            resp = self.client.history(owner_id=owner_id)
        except Exception as exc:
            logger.warning(f"find node failed: {exc}")
            return None
        if not isinstance(resp, dict):
            return None
        for item in resp.get("data") or []:
            if not isinstance(item, dict):
                continue
            try:
                item_serial_id = int(item.get("serial_id"))
            except (TypeError, ValueError):
                continue
            if item_serial_id == serial_id:
                event_tuple = item.get("event_tuple") or ""
                node_id = item.get("node_id")
                if not node_id:
                    return None
                return {"serial_id": serial_id, "node_id": str(node_id),
                        "event_tuple": event_tuple, "size": len(event_tuple)}
        return None

    def _find_node_by_node_id(self, owner_id: str, node_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """在星空中按 node_id 查找节点（用于又贞 / 首贞前的重名检查）。"""
        if not node_id:
            return None
        try:
            resp = self.client.history(owner_id=owner_id)
        except Exception as exc:
            logger.warning(f"按 node_id 查找失败：{exc}")
            return None
        if not isinstance(resp, dict):
            return None
        for item in resp.get("data") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("node_id") or "") != str(node_id):
                continue
            event_tuple = item.get("event_tuple") or ""
            try:
                serial_id = int(item.get("serial_id"))
            except (TypeError, ValueError):
                serial_id = None
            return {"serial_id": serial_id, "node_id": str(node_id),
                    "event_tuple": event_tuple, "size": len(event_tuple),
                    "previous_node": item.get("previous_node"),
                    "previous_ids": item.get("previous_ids") or []}
        return None

    def _resolve_bottle(self, source_id: str, owner_id: str) -> Optional[Dict[str, Any]]:
        """按 do.txt 的 REDIS 分支定位当前可接续的瓶子。

        - Redis 有记录：回星空核对同 serial_id 的节点；节点仍在才使用。
        - Redis 无记录 / 同 serial_id 节点找不到：查找最近一个带 [chat] 的瓶子。
        - 两处都找不到：返回 None，交给首贞。
        """
        focus = self._get_focus(source_id)
        if focus is not None:
            serial_id = focus.get("serial_id")
            node = self._find_node_by_serial_id(owner_id, serial_id)
            if node is not None:
                logger.info(
                    "Redis 焦点 serial_id=%s 在星空中存在，进入流程块（node_id=%s）。",
                    serial_id, node["node_id"],
                )
                return node
            logger.warning(
                "Redis 焦点 serial_id=%s 在星空中找不到，回退查找最近 [chat] 瓶子。",
                serial_id,
            )

        latest = self._find_latest_bottle(owner_id)
        if latest is not None:
            logger.info(
                "找到最近 [chat] 瓶子 serial_id=%s（node_id=%s），进入流程块。",
                latest["serial_id"], latest["node_id"],
            )
            return latest

        logger.info("Redis 无有效记录且星空无 [chat] 瓶子，走首贞。")
        # 星空链已空/断：焦点、计数全部失效，一并清空
        self.reset_state(source_id)
        return None

    def _ensure_unique_node_id(self, owner_id: str, source_id: str,
                               node_id: Optional[str]) -> tuple:
        """返回 (可用 node_id, 是否因重名而替换)。"""
        desired = node_id or self._gen_node_id(source_id)
        if self._find_node_by_node_id(owner_id, desired) is None:
            return desired, False
        replacement = self._gen_node_id(source_id)
        while self._find_node_by_node_id(owner_id, replacement) is not None:
            replacement = self._gen_node_id(source_id)
        return replacement, True

    def _bottle_fits(self, current: Dict[str, Any], block: str) -> bool:
        """流程块的「检查装得下不」：累加后不得超过瓶子上限，并显式输出判断依据。"""
        existing = _strip_chat(str(current.get("event_tuple") or ""))
        existing_len = len(existing)
        separator_len = len(_SLICE_SEP)
        block_len = len(block)
        chat_tag_len = len(_CHAT_TAG)
        total_len = existing_len + separator_len + block_len + chat_tag_len
        fits = total_len <= self.max_tuple_size
        logger.info(
            "容量判断：%s（serial_id=%s node_id=%s；"
            "已用=%d + 分隔符=%d + 本块=%d + [chat]=%d = %d，上限=%d）",
            "装得下 → 累加" if fits else "装不下 → 又贞",
            current.get("serial_id"), current.get("node_id"),
            existing_len, separator_len, block_len, chat_tag_len,
            total_len, self.max_tuple_size,
        )
        return fits

    def _try_accumulate(self, current: Dict[str, Any], block: str,
                        owner_id: str) -> Optional[Dict[str, Any]]:
        """还能装：累加到当前瓶子；失败（节点已消失）返回 None。"""
        node_id = str(current.get("node_id") or "").strip()
        if not node_id:
            return None
        new_tuple = _strip_chat(str(current.get("event_tuple") or "")) + _SLICE_SEP + block + _CHAT_TAG
        if not self._accumulate(node_id, new_tuple, owner_id):
            return None
        return {"serial_id": current.get("serial_id"), "node_id": node_id,
                "event_tuple": new_tuple, "size": len(new_tuple)}

    def _accumulate(self, focus_node_id: str, new_tuple: str, owner_id: str) -> bool:
        """累加到 focus 节点；若 focus 已不存在（找不到事件），返回 False 以重新定位瓶子。"""
        before = self._wait_for_node(owner_id, focus_node_id)
        if before is None:
            logger.warning(f"focus 节点 '{focus_node_id}' 已不存在，回退为首贞。")
            return False
        parent_ids = self._parent_ids(before)
        action_tag = str(before.get("action_tag") or ("贞" if not parent_ids else "又贞"))
        block_tag = str(before.get("block_tag") or "因")
        try:
            # update 接口会把未携带的 previous_ids/action_tag 重置为默认值（首贞），
            # 因此累加时必须显式回传原父链与动作标签，否则当前瓶子会被清成孤根。
            self.client.update(
                old_node_id=focus_node_id, new_node_id=focus_node_id,
                event_tuple=new_tuple, is_share=False,
                previous_ids=parent_ids, action_tag=action_tag,
                block_tag=block_tag, owner_id=owner_id,
            )
            after = self._wait_for_node(owner_id, focus_node_id)
            if after is not None:
                actual_parents = set(self._parent_ids(after))
                if not set(parent_ids).issubset(actual_parents):
                    logger.warning(
                        "累加后父链丢失：节点 '%s' 原父节点=%s，实际=%s；立即恢复。",
                        focus_node_id, parent_ids, sorted(actual_parents),
                    )
                    if not self._restore_parent_ids(owner_id, focus_node_id, parent_ids):
                        raise RuntimeError(
                            f"累加后无法恢复节点 '{focus_node_id}' 的父链。"
                        )
            return True
        except Exception as exc:
            if "找不到" in str(exc):
                logger.warning(f"focus 节点 '{focus_node_id}' 已不存在，回退为首贞。")
                return False
            raise

    def zhenru(self, event_tuple: str, node_id: Optional[str] = None,
               source_id: Optional[str] = None, user_name: str = "用户",
               user_id: Optional[str] = None,
               is_must_answer: str = "no") -> List[Dict[str, Any]]:
        """
        贞入一条消息到因果星空，执行「累加优先 → 满则又贞 / 无瓶首贞 → 切分 → 回写」。

        参数：
            event_tuple 消息内容（事件粒子的叙述文本，会包装为带 [chat] 标记的元组）
            node_id     事件节点的提炼标题（event_tuple 的标题）；未提供时自动生成
            source_id   场景会话 ID（默认用构造时的 source_id）
            user_name   说话者名称（用户消息为用户名；AI 消息可传机器人名）
            user_id     用户 ID（默认 actor_id）
            is_must_answer 是否为「用户必答消息」，取值 "yes"/"no"：yes 则贞入/累加后 click 拉取事件视界（AI 历史上下文）；no 则不点

        返回：本轮的贞入动作列表，每项含 action / serial_id / node_id。
        """
        if not event_tuple:
            return []
        source_id = source_id or self.source_id
        if not source_id:
            raise ValueError("source_id 不能为空：切片器需要场景会话 ID 作隔离。")
        user_id = user_id or self.client.actor_id
        field_owner = self._field_owner(source_id)

        # sanitize node_id: strip ALL whitespace so a dirty id
        # (e.g. leading \n from streamed answer) cannot break previous_node lookup
        if node_id is not None:
            node_id = "".join(node_id.split()) or None

        # 切分：只对「内容」分块，每个分块都重新包上固定元信息，避免后续分块只剩裸内容。
        meta_size = self._slice_meta_size(user_name, user_id, source_id)
        content_limit = max(1, self.max_tuple_size - meta_size)
        content_blocks = self._split_text(event_tuple, content_limit)
        blocks = [self._wrap_slice_content(user_name, user_id, chunk, source_id)
                  for chunk in content_blocks]

        # REDIS 里有没有记录：
        #   有 → 回星空核对同 serial_id 节点；仍在则进入流程块。
        #   没有 / 节点已失效 → 到星空找最近 [chat] 瓶子；找到进入流程块，找不到首贞。
        current = self._resolve_bottle(source_id, field_owner)
        results: List[Dict[str, Any]] = []
        # 本轮对整链 event_tuple 之和的真实增量（含分隔符与 [chat] 标记）
        delta = 0

        for block in blocks:
            try:
                # 流程块：检查装得下不。
                accumulated = None
                if current is not None and self._bottle_fits(current, block):
                    accumulated = self._try_accumulate(current, block, field_owner)
                    if accumulated is None:
                        # Redis/快照与星空之间发生并发删除，重新走一次定位分支。
                        logger.warning("累加时焦点已失效，重新定位当前瓶子。")
                        current = self._resolve_bottle(source_id, field_owner)
                        if current is not None and self._bottle_fits(current, block):
                            accumulated = self._try_accumulate(current, block, field_owner)
                            if accumulated is None:
                                # 星空也确认该瓶子已不可写，按无瓶处理（首贞）。
                                current = None

                # e-还能装：累加。
                if accumulated is not None:
                    current = accumulated
                    results.append({"action": "累加", "serial_id": current["serial_id"],
                                    "node_id": current["node_id"]})
                    delta += len(_SLICE_SEP) + len(block)
                    continue

                # f-不能装：又贞；previous_node 必须非空。
                previous_node = str(current["node_id"]) if current is not None else None
                if current is None:
                    logger.info("容量判断：没有可用瓶子，跳过累加，走首贞。")

                # g-node_id 重复没：没重复直接又贞 / 首贞；重复则先处理重名。
                new_node_id, replaced = self._ensure_unique_node_id(
                    field_owner, source_id, node_id,
                )
                if replaced:
                    logger.warning(f"node_id '{node_id}' 在星空中已存在，改用 '{new_node_id}'。")

                # h / i：又贞（或 d-找不到瓶子时的首贞）。
                node_tuple = block + _CHAT_TAG
                resp, actual_node_id, actual_action = self._genesis_chain_safe(
                    new_node_id, node_tuple, previous_node, source_id,
                )
                data = resp.get("data")
                serial_id = data.get("serial_id") if isinstance(data, dict) else None
                current = {"serial_id": serial_id, "node_id": actual_node_id,
                           "event_tuple": node_tuple, "size": len(node_tuple)}
                results.append({"action": actual_action,
                                "serial_id": serial_id, "node_id": actual_node_id})
                delta += len(block) + len(_CHAT_TAG)
            except Exception as exc:
                logger.error(f"贞入失败，已跳过该块（程序继续运行）：{exc}")
                results.append({"action": "失败", "error": str(exc)})

        # 本轮未成功写入任何节点（首贞失败且无焦点），跳过回写，避免崩溃
        if current is None:
            logger.error("本轮贞入未成功写入任何节点，已跳过回写。")
            return results

        # 回写：serial_id / 计数写入 redis
        self._set_focus(source_id, current["serial_id"], current["node_id"], current["event_tuple"])

        # 整链计数 + 坍塌器触发（真实 event_tuple 增量）
        self._add_chain_count(delta, source_id)

        # 用户必答消息：点击刷新「当下」视界
        if is_must_answer == "yes":
            # 坍塌可能已把 focus 替换为新果节点，故用最新 focus，而非本轮累加的 current
            focus = self._get_focus(source_id)
            sid = (focus or {}).get("serial_id") or current.get("serial_id")
            if sid is not None:
                try:
                    self.client.click(serial_id=sid, bot_id=self.bot_id,
                                      max_eyes=self.max_eyes, owner_id=field_owner)
                except Exception as exc:
                    logger.warning(f"刷新「当下」视界失败：{exc}")

        return results

    # ---------------- 读取（Agent Memory 当下/过往） ----------------
    @staticmethod
    def _remove_focus_node_from_horizon(horizon: Dict[str, Any]) -> Dict[str, Any]:
        """去除事件视界内焦点事件冗余（distance=0 的本节点 + serial_id 等于焦点的节点）。"""
        focus_serial_id = (horizon.get("data") or {}).get("serial_id")
        if "event_horizon_details" in horizon:
            horizon["event_horizon_details"] = [
                node for node in horizon["event_horizon_details"]
                if node.get("distance", 0.0) != 0.0 and node.get("serial_id") != focus_serial_id
            ]
        return horizon

    def present(self, max_eyes: Optional[float] = None,
                source_id: Optional[str] = None) -> Dict[str, Any]:
        """当下：带 bot_id 拉取当前事件视界（系统客体推送），并去除焦点节点冗余。"""
        sid = source_id or self.source_id
        owner = self._field_owner(sid) if sid else None
        try:
            horizon = self.client.horizon(
                bot_id=self.bot_id,
                max_eyes=max_eyes or self.max_eyes,
                owner_id=owner,
            )
            return self._remove_focus_node_from_horizon(horizon)
        except Exception as exc:
            logger.warning("「当下」视界拉取失败，降级为空视界：%s", exc)
            return {"data": {}, "event_horizon": []}

    def past(self, serial_id: int, max_eyes: Optional[float] = None,
             source_id: Optional[str] = None) -> Dict[str, Any]:
        """过往：带 bot_id 点击某个事件（Agent 主动发起）。"""
        sid = source_id or self.source_id
        owner = self._field_owner(sid) if sid else None
        return self.client.click(serial_id=serial_id, bot_id=self.bot_id,
                                 max_eyes=max_eyes or self.max_eyes, owner_id=owner)



# ============================================================
# 命令行演示（直接参数示例，运行前请替换为你的真实值）
# ============================================================
def _print_result(r: Dict[str, Any]) -> None:
    """打印单个贞入结果，兼容成功与失败两种情况。"""
    if r.get("error"):
        print(f"    {r['action']}: {r['error']}")
    else:
        print(f"    {r['action']}: serial_id={r.get('serial_id')}")


def main() -> None:
    notice = """
    ////////////////////////////////////////
    //          因果星空 Memory           //
    ///////////////////////////////////////
    """
    print(notice)
    from causal_config import (SLICER_KWARGS, COLLAPSER_KWARGS,
                               USER_NICK_NAME, BOT_NICK_NAME, USER_ID, BOT_ID)
    from causal_collapse import Collapser

    slicer = BotSlicer(**SLICER_KWARGS)
    print(f"切片器已就绪：bot_id={slicer.bot_id}  source_id={slicer.source_id}")

    # 注册坍塌器回调：真实环境用 Collapser（LLM 提炼 + 删除 + 首贞 + focus 回写）
    collapser = Collapser(**COLLAPSER_KWARGS)
    slicer.set_collapse_callback(collapser.collapse)
    
    user_event_tuple = "你好，今天是一个晴天，我就是一直发，你敢拿我怎么样！"
    ai_event_tuple = "你好呀！是个适合出门走走的好天气。"

    # 1. 首贞（用户必答消息）
    print("\n[1] 首贞 —— 用户消息（必答）")
    for r in slicer.zhenru(event_tuple="你好，今天是一个晴天，我就是一直发，你敢拿我怎么样！", node_id=slicer.make_node_id(user_event_tuple), user_name=USER_NICK_NAME, user_id=USER_ID, is_must_answer="yes"):
        _print_result(r)

    # 2. AI 回复（短消息 → 上限判断自动「累加」）
    print("\n[2] AI 回复 —— 短消息，自动「累加」")
    for r in slicer.zhenru(event_tuple=ai_event_tuple, node_id=slicer.make_node_id(ai_event_tuple), user_name=BOT_NICK_NAME, user_id=BOT_ID):
        _print_result(r)

    # 3. 当下视界（带 bot_id 即 Agent Memory）
    print("\n[3] 当下视界（bot_id 并入跨场 [chat] 记忆）")
    focus_serial_id = None
    try:
        horizon = slicer.present()
        data = horizon.get("data") or {}
        focus_serial_id = data.get("serial_id")
        print(f"    大股东 serial_id={data.get('serial_id')}")
        print(f"    视界节点数={len(horizon.get('event_horizon') or [])}")
        # print(json.dumps(horizon, ensure_ascii=False, indent=4))
    except Exception as exc:
        print(f"    获取当下视界失败：{exc}")

    # # 4. 点击事件节点（提升权重并返回视界）
    # print("\n[4] 点击事件节点 focus_serial_id")
    # time.sleep(3)
    # if focus_serial_id is not None:
    #     try:
    #         clicked = slicer.past(serial_id=focus_serial_id)
    #         cdata = clicked.get("data") or {}
    #         print(f"    已点击 serial_id={focus_serial_id}，大股东 serial_id={cdata.get('serial_id')}")
    #         print(f"    视界节点数={len(clicked.get('event_horizon') or [])}")
    #     except Exception as exc:
    #         print(f"    点击失败：{exc}")
    # else:
    #     print("    未取得可点击的 serial_id，跳过点击示例。")

    print("\n演示完成。")


if __name__ == "__main__":
    # for i in range(1, 7):
    #     main()
    #     print(f'第 {i} 次写入')
    #     # time.sleep(2)
    main()

