#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
causal_collapse.py —— 元龙因果 Agent 的「坍塌器」(Collapser)

在整链 event_tuple 之和超过阈值时，由切片器触发本坍塌器，把一条 source_id 链上「最旧的一段」
瓶子（事件节点）提炼压缩成一个以第一人称「我」视角的果节点，保留最近节点不坍。

工作流程（部分坍塌，6 步）：
    1. 通过 history 收集整条链上所有 [chat] 瓶子（从链首到链尾，不依赖台账）。
    2. 从根节点（serial_id 最小）升序累计 event_tuple 大小，累计达到 collapse_threshold 为止，
       拆出「要坍塌的旧前缀」与「保留的最近节点」。
    3. 将旧前缀的 event_tuple 按 serial_id 升序拼合成 event_all_temp，交给 LLM 提炼出
       event_new（叙事 + 领悟）与标题 event_node_new（≤15 字，重名加时间戳）；
       event_new 结尾带 [chat] 标记。
    4. 删除被坍塌的旧前缀节点（依赖 API「删除连锁」保持链不断裂）。
    5. 对贞、果（previous=None）新建果节点 Sn+1，serial_id=x（成为新根）。
    6. 把保留链的第一个节点 previous_node 重连到果节点，维持最近节点的连续完整。

好处：整链永远只会「坍掉最旧的一段」，不会把整链压成一个果节点，保留最近节点的
完整性连续性，便于 Agent 理解最近上下文。

注意：
    - 坍塌由切片器另起线程执行；串行化由切片器的 _collapse_lock 保证（同一 bot 同时只跑一个）。
    - LLM 用 openai 库，指向本地 OpenAI 兼容端点。

用法（参数统一在 causal_config.py，注册为坍塌回调即可）：
    from causal_config import SLICER_KWARGS, COLLAPSER_KWARGS
    from causal_slicing import BotSlicer
    from causal_collapse import Collapser

    slicer = BotSlicer(**SLICER_KWARGS)
    collapser = Collapser(**COLLAPSER_KWARGS)   # redis_key_prefix 须与切片器一致（同一 focus 键）
    slicer.set_collapse_callback(collapser.collapse)   # 整链超阈值时自动坍塌
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from causal_slicing import CausalClient, field_owner_id

try:
    import redis as _redis_mod  # type: ignore
    _HAS_REDIS = True
except ImportError:  # pragma: no cover
    _redis_mod = None  # type: ignore
    _HAS_REDIS = False

logger = logging.getLogger("causal_collapse")

_CHAT_TAG = "[chat]"

# LLM 默认（本地 OpenAI 兼容端点）
DEFAULT_LLM_BASE_URL = "http://192.168.66.43:9382/llm/v1"
DEFAULT_LLM_MODEL = "local-llm"
DEFAULT_LLM_API_KEY = "not-needed"

# 叙事提炼系统提示词（按机器人昵称动态生成，让 LLM 以该身份展开第一人称叙事）
def _build_system_prompt(bot_nick_name: str, length: Optional[int] = None) -> str:
    rules = [
        "叙事骨架：以「谁做了什么」为骨架，按「时间、地点、人物、起因、冲突发展、结局」展开。",
        "结尾补充领悟：通过这些事，我对（谁/这些事）有了什么领悟、感受、懂得了什么技能、或知道了谁擅长什么。",
    ]
    if length:
        rules.append(f"内容不超过 {int(length)} 个字。")
    rules.extend([
        "内容结尾追加标记 [chat]。",
        "标题不超过 15 个字。",
        "**表达感受时请不要一律强制对齐正面表达，你可以负面表达，如愤怒、悲伤、厌恶......(人类可以产生的负面感受）**",
    ])
    numbered = "\n".join(f"{i}. {r}" for i, r in enumerate(rules, 1))

    result = (
        f"你是元龙因果 Agent 的记忆坍塌器，你的名字叫 `{bot_nick_name}`。请把用户提供的一段对话事件流，提炼成一个以第一人称「我」"
        "视角的叙事总结，并给出一个简短标题。要求：\n"
        f"{numbered}\n"
        "只输出 JSON，不要输出其它文字：{\"title\": \"标题\", \"content\": \"叙事内容\"}"
    )
    return result


class Collapser:
    """坍塌器：整链超阈值时，把链上「最旧的一段」提炼压缩成一个果节点（首贞），保留最近节点。"""

    def __init__(self, base_url: str, token: Optional[str], owner_id: str,
                 bot_id: str, bot_nick_name: str = "元龙",
                 source_id: Optional[str] = None,
                 max_eyes: float = 30, timeout: float = 120,
                 redis_url: str = "redis://127.0.0.1:6379/0",
                 redis_protocol: int = 2,
                 redis_key_prefix: str = "causal_slicing",
                 llm_base_url: str = DEFAULT_LLM_BASE_URL,
                 llm_model: str = DEFAULT_LLM_MODEL,
                 llm_api_key: str = DEFAULT_LLM_API_KEY,
                 length: Optional[int] = None,
                 collapse_threshold: Optional[int] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.bot_id = bot_id
        self.bot_nick_name = bot_nick_name
        self.source_id = source_id
        self.max_eyes = max_eyes
        self.timeout = timeout
        # 用 actor_id="collapse" 点击/写入，避免与切片器抢大股东
        self.client = CausalClient(base_url, token, owner_id, actor_id="collapse", timeout=timeout)
        self.llm_base_url = llm_base_url
        self.llm_model = llm_model
        self.llm_api_key = llm_api_key
        self.length = length
        # 坍塌范围大小：从根节点升序累计到此值为止的旧前缀会被坍掉；None 则回退为整链坍塌
        self.collapse_threshold = collapse_threshold

        # redis（读/写切片器的 focus 焦点；键结构须与切片器一致）
        self._redis_url = redis_url
        self._redis_protocol = redis_protocol
        self._prefix = redis_key_prefix
        self._redis = None
        self._redis_down = False
        self._redis_lock = threading.Lock()


    # ---------------- Redis（与切片器同库同前缀，键结构一致） ----------------
    def _get_redis(self):
        if self._redis is None and not self._redis_down:
            with self._redis_lock:
                if self._redis is None and not self._redis_down:
                    if not _HAS_REDIS:
                        self._redis_down = True
                        return None
                    try:
                        self._redis = _redis_mod.Redis.from_url(
                            self._redis_url, protocol=self._redis_protocol, decode_responses=True)
                        self._redis.ping()
                    except Exception as exc:
                        logger.warning(f"坍塌器 redis 连接失败：{exc}")
                        self._redis = None
                        self._redis_down = True
        return self._redis

    def _chain_key(self, source_id: str) -> str:
        return f"{self._prefix}:chain:{self.bot_id}:{source_id}"

    def _field_owner(self, source_id: str) -> str:
        return field_owner_id(source_id, self.bot_id)

    def _incr_chain_size(self, source_id: str, n: int) -> None:
        """坍塌完成后把果节点大小计入整链计数（触发清零后补回真实大小）。"""
        if n <= 0:
            return
        r = self._get_redis()
        if r is None:
            logger.warning("redis 不可用，无法回写整链计数。")
            return
        r.incrby(self._chain_key(source_id), n)

    # ---------------- 主流程 ----------------
    def collapse(self, bot_id: str, chain_size: int, source_id: str) -> Dict[str, Any]:
        """坍塌器回调入口（由切片器调用，签名与 set_collapse_callback 约定一致）。

        返回结果 dict，会作为 Future 的结果回传给切片器（status=done/error）。
        """
        try:
            return self._do_collapse(bot_id, chain_size, source_id)
        except Exception as exc:
            logger.exception("坍塌执行失败")
            return {"status": "error", "error": str(exc), "source_id": source_id}

    def _collect_chain_bottles(self, owner: str) -> List[Tuple[int, str, str]]:
        """通过 history 收集整条链上所有 [chat] 瓶子，按 serial_id 升序（即链的先后顺序）。

        不依赖切片器台账，直接从星空实际状态出发，覆盖从链首到链尾的所有瓶子，
        避免台账漏记（星空删除、果节点漏记等）导致的「只坍塌一部分」。
        返回 [(serial_id, node_id, event_tuple), ...]。
        """
        try:
            resp = self.client.history(owner_id=owner)
        except Exception as exc:
            logger.warning(f"坍塌器获取 history 失败：{exc}")
            return []
        if not isinstance(resp, dict):
            return []
        bottles: List[Tuple[int, str, str]] = []
        for item in resp.get("data") or []:
            if not isinstance(item, dict):
                continue
            event_tuple = item.get("event_tuple") or ""
            if _CHAT_TAG not in event_tuple:
                continue
            node_id = item.get("node_id")
            if not node_id:
                continue
            try:
                serial_id = int(item.get("serial_id"))
            except (TypeError, ValueError):
                continue
            bottles.append((serial_id, str(node_id), event_tuple))
        bottles.sort(key=lambda x: x[0])
        return bottles

    @staticmethod
    def _split_collapse_range(bottles: List[Tuple[int, str, str]], range_size: int
                              ) -> Tuple[List[Tuple[int, str, str]], List[Tuple[int, str, str]]]:
        """把瓶子按根节点升序拆成「要坍塌的旧前缀」与「保留的最近节点」。

        从 serial_id 最小的瓶子（根）开始累计 event_tuple 大小，累计达到 range_size 为止；
        这部分作为坍塌范围，其余（更新的节点）保留，以维持最近上下文的连续完整。
        若整链都装不满 range_size（range_size 过大或未配置），则全部归入坍塌范围（退化为整链坍塌）。
        """
        if range_size <= 0:
            range_size = 1
        collapse: List[Tuple[int, str, str]] = []
        keep: List[Tuple[int, str, str]] = []
        acc = 0
        split_done = False
        for b in bottles:
            if split_done:
                keep.append(b)
                continue
            collapse.append(b)
            acc += len(b[2])
            if acc >= range_size:
                split_done = True
        return collapse, keep

    def _do_collapse(self, bot_id: str, chain_size: int, source_id: str) -> Dict[str, Any]:
        owner = self._field_owner(source_id)
        # 1. 直接沿链收集整条链上所有 [chat] 瓶子（从链首到链尾，不依赖台账，避免漏记）
        bottles = self._collect_chain_bottles(owner)
        if not bottles:
            logger.warning("坍塌器：链上无 [chat] 瓶子，跳过坍塌。")
            return {"status": "skipped", "reason": "no_bottles", "source_id": source_id}

        # 2. 确定坍塌范围：从根节点（serial_id 最小）升序累计，累计大小达到 collapse_threshold 为止。
        #    只坍这段「旧前缀」，保留最近节点，维持最近上下文的连续完整。
        range_size = int(self.collapse_threshold) if self.collapse_threshold else int(chain_size)
        collapse_bottles, keep_bottles = self._split_collapse_range(bottles, range_size)
        logger.info(
            f"坍塌器启动：source_id={source_id} 链大小={chain_size} 瓶子数={len(bottles)}，"
            f"坍塌范围={len(collapse_bottles)} 个（大小上限={range_size}），保留={len(keep_bottles)} 个"
        )

        # 3. 只对要坍塌的旧前缀拼合 event_tuple / node_id
        tuples = [b[2] for b in collapse_bottles]
        node_ids = [b[1] for b in collapse_bottles]
        for b in collapse_bottles:
            logger.info(f"坍塌器收集瓶子 serial_id={b[0]}，event_tuple 长度={len(b[2])}")
        event_all_temp = "\n".join(t for t in tuples if t)
        logger.info(
            f"坍塌器收集完成：坍塌 {len(collapse_bottles)} 个瓶子，event_all_temp 长度={len(event_all_temp)}"
        )

        # 4. LLM 提炼
        logger.info(f"坍塌器开始 LLM 提炼（event_all_temp 长度={len(event_all_temp)}）……")
        title, content = self._llm_summarize(event_all_temp, source_id)
        logger.info(f"坍塌器 LLM 提炼完成：标题='{title}' 内容长度={len(content)}")

        # 5. 删除被坍塌的旧前缀节点（依赖「删除连锁」不断链）
        logger.info(f"坍塌器删除被坍塌的 {len(node_ids)} 个节点……")
        for nid in node_ids:
            if not nid:
                continue
            try:
                self.client.delete(node_id=nid, owner_id=owner)
                logger.info(f"坍塌器删除节点 '{nid}' 成功")
            except Exception as exc:
                logger.warning(f"删除节点 '{nid}' 失败：{exc}")

        # 6. 首贞新建果节点（previous=None，成为新根）
        logger.info(f"坍塌器首贞新建果节点：标题='{title}'……")
        resp, actual_node_id = self._genesis_dedup(title, content, owner)
        data = resp.get("data") or {}
        new_serial = data.get("serial_id")
        actual_node_id = str(actual_node_id or data.get("node_id") or title)
        logger.info(f"坍塌器首贞成功：serial_id={new_serial} node_id={actual_node_id}")

        # 7. 把保留链的第一个节点重连到果节点，维持最近节点的连续完整
        if keep_bottles:
            self._relink_first_kept(owner, keep_bottles[0][1], actual_node_id)
        else:
            logger.info("坍塌器：保留链为空（整链坍塌），果节点作为合法单根，无需重连。")

        # 8. 整链计数回写 = 果节点 + 保留节点（保留节点仍在链上，须一并补回）
        if new_serial is not None:
            kept_size = sum(len(b[2]) for b in keep_bottles)
            self._incr_chain_size(source_id, len(content) + kept_size)

        logger.info(
            f"坍塌完成：source_id={source_id} 新节点 node_id={actual_node_id} serial_id={new_serial}，"
            f"坍塌={len(collapse_bottles)} 保留={len(keep_bottles)}"
        )
        return {
            "status": "done",
            "source_id": source_id,
            "bottles": len(collapse_bottles),
            "kept_bottles": len(keep_bottles),
            "title": title,
            "node_id": actual_node_id,
            "new_serial_id": new_serial,
        }

    @staticmethod
    def _parent_ids(node: Dict[str, Any]) -> List[str]:
        """汇总节点记录中的父节点字段（兼容 previous_node 单值/数组，与切片器保持一致）。"""
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

    def _find_node_by_node_id(self, owner: str, node_id: str) -> Optional[Dict[str, Any]]:
        """在星空中按 node_id 查找节点（用于重连后的回读校验）。"""
        if not node_id:
            return None
        try:
            resp = self.client.history(owner_id=owner)
        except Exception as exc:
            logger.warning(f"坍塌器按 node_id 查找失败：{exc}")
            return None
        if not isinstance(resp, dict):
            return None
        for item in resp.get("data") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("node_id") or "") != str(node_id):
                continue
            return item
        return None

    def _wait_for_node(self, owner: str, node_id: str,
                       attempts: int = 5, delay: float = 0.2) -> Optional[Dict[str, Any]]:
        """等待节点在 history 中可见，规避写入成功后短暂读不到/读到旧值。"""
        for attempt in range(attempts):
            node = self._find_node_by_node_id(owner, node_id)
            if node is not None:
                return node
            if attempt + 1 < attempts:
                time.sleep(delay * (attempt + 1))
        return None

    def _relink_first_kept(self, owner: str, first_kept_node_id: str, fruit_node_id: str) -> None:
        """把保留链第一个节点的 previous_node 显式重连到果节点。

        坍塌只删旧前缀，保留链的链首原本指向被删的前缀尾节点；删除连锁会让它级联
        继承到已删节点，必须用 update 把它的父节点改回新果节点，果节点才真正成为新根。
        写入后回读校验父链，未接上则重试，避免果节点沦为孤立节点。
        """
        first_kept_node_id = str(first_kept_node_id)
        fruit_node_id = str(fruit_node_id)
        for attempt in range(3):
            node = self._wait_for_node(owner, first_kept_node_id)
            if node is not None and fruit_node_id in self._parent_ids(node):
                logger.info(
                    f"坍塌器重连保留链链首 '{first_kept_node_id}' 已指向果节点 '{fruit_node_id}'"
                )
                return
            try:
                self.client.update(
                    old_node_id=first_kept_node_id, new_node_id=first_kept_node_id,
                    previous_ids=[fruit_node_id], action_tag="又贞", block_tag="因",
                    owner_id=owner,
                )
            except Exception as exc:
                logger.warning(
                    f"重连保留链链首 '{first_kept_node_id}' 到果节点 '{fruit_node_id}' 失败：{exc}"
                )
            if attempt + 1 < 3:
                time.sleep(0.2 * (attempt + 1))
        node = self._wait_for_node(owner, first_kept_node_id)
        if node is not None and fruit_node_id in self._parent_ids(node):
            logger.info(f"坍塌器重连保留链链首 '{first_kept_node_id}' 到果节点 '{fruit_node_id}' 成功")
        else:
            logger.error(
                f"坍塌器重连保留链链首 '{first_kept_node_id}' 到果节点 '{fruit_node_id}' 未确认，"
                f"果节点可能成为孤立节点。"
            )

    def _genesis_dedup(self, node_id: str, content: str, owner: str) -> Tuple[Dict[str, Any], str]:
        """首贞（previous=None）；若标题重名（已存在）则加时间戳后缀重试。

        返回 (resp, 实际写入的 node_id)。实际 node_id 以本方法最终提交的 use_id 为准，
        避免重名重试换了带时间戳的 id 后，调用方仍拿原标题当 node_id 导致后续重连错位。
        """
        use_id = node_id
        for _ in range(3):
            try:
                resp = self.client.genesis(
                    node_id=use_id, event_tuple=content, previous_node=None,
                    block_tag="果", action_tag="对贞", is_share=False,
                    return_serial_id=True, owner_id=owner,
                )
                return resp, use_id
            except Exception as exc:
                if "已存在" in str(exc) or "冲突" in str(exc):
                    use_id = f"{node_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    logger.warning(f"标题 '{node_id}' 已存在，加时间戳重试：{use_id}")
                    continue
                raise
        raise RuntimeError("坍塌首贞重试次数用尽")

    # ---------------- LLM ----------------
    def _llm_summarize(self, event_all_temp: str, source_id: str) -> Tuple[str, str]:
        """调用 openai 提炼出 (title, content)；LLM 失败时退化为原文直存。"""
        if not event_all_temp.strip():
            return (self._default_title(), self._wrap_event_tuple("（空链）", source_id))
        try:
            from openai import OpenAI
            client = OpenAI(
                api_key=self.llm_api_key, 
                base_url=self.llm_base_url, 
                timeout=self.timeout
            )
            system_prompt = _build_system_prompt(self.bot_nick_name, self.length)
            resp = client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": event_all_temp},
                ],
                temperature=0.3,
            )
            text = (resp.choices[0].message.content or "").strip()
            title, content = self._parse_json(text)
            if content:
                return (title, self._wrap_event_tuple(content, source_id))
        except Exception as exc:
            logger.warning(f"LLM 提炼失败，退化为原文直存：{exc}")
        text = event_all_temp.strip()
        if len(text) > 2000:
            text = text[:2000] + "……"
        return (self._default_title(), self._wrap_event_tuple(text, source_id))

    @staticmethod
    def _parse_json(text: str) -> Tuple[str, str]:
        """解析 LLM 返回的 JSON；尽量剥离可能的代码块围栏。"""
        text = text.strip()
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()
        try:
            obj = json.loads(text)
        except ValueError:
            m = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not m:
                return ("", "")
            try:
                obj = json.loads(m.group(0))
            except ValueError:
                return ("", "")
        title = str(obj.get("title") or "").strip()[:15]
        content = str(obj.get("content") or "").strip()
        return (title, content)

    def _wrap_event_tuple(self, content: str, source_id: str) -> str:
        """把提炼/兜底得到的叙事内容包装成标准 event_tuple 格式（说话者=机器人自己）。"""
        content = (content or "").strip()
        if content.endswith(_CHAT_TAG):
            content = content[:-len(_CHAT_TAG)].rstrip()
        lines = [
            f"**机器人ID(bot_id)**: {self.bot_id}",
            f"**用户(user)**: {self.bot_nick_name}",
            f"**用户ID(user_id)**: {self.bot_id}",
            f"**来源ID(source_id)**: {source_id}",
            f"**内容(content)**: {content}",
            f"**时间(time)**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        return "\n".join(lines) + _CHAT_TAG

    @staticmethod
    def _default_title() -> str:
        return f"记忆提炼_{datetime.now().strftime('%Y%m%d%H%M%S')}"

