#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
causal_config.py —— 元龙因果 Agent 的配置模块（切片器 + 坍塌器共用）

参数示例：
    base_url = http://192.168.66.39:8094
    owner_id = dev
    actor_id = 415135222
    token    = 123456|4f3359bc9e9e0b66bd4f00a8b992491c

用法：
    from causal_config import SLICER_KWARGS, COLLAPSER_KWARGS
    from causal_slicing import BotSlicer
    from causal_collapse import Collapser

    slicer = BotSlicer(**SLICER_KWARGS)
    collapser = Collapser(**COLLAPSER_KWARGS)
    slicer.set_collapse_callback(collapser.collapse)

"""

# http://192.168.66.39:8094/3d?owner_id=415135222_3787687088&actor_id=415135222&bot_id=3787687088&token=123456%7C4f3359bc9e9e0b66bd4f00a8b992491c&edit=true

# ---- 因果星空连接 ----
BASE_URL = "http://192.168.66.39:8094"
OWNER_ID = "415135222_3787687088"          # 兜底 owner_id；真实运行时按 source_id 现算复合场 {source_id}_{bot_id}
ACTOR_ID = "415135222"
TOKEN = "123456|4f3359bc9e9e0b66bd4f00a8b992491c"

# 元龙因果 Agent 机器人 ID（跨场共享主键，必填）
BOT_ID = "3787687088"
# 当前场景会话 ID（必填）
SOURCE_ID = "415135222"

# ---- 用户 / 机器人身份（区分说话者 + 坍塌格式包装）----
USER_NICK_NAME = "廉颇"      # 用户昵称（用户消息的说话者名称）
BOT_NICK_NAME = "孙膑"       # 机器人昵称（AI 消息 / 坍塌果节点的说话者名称）
USER_ID = "415135222"        # 用户 ID（用户消息的说话者 ID；同 ACTOR_ID / SOURCE_ID）

# 单个事件节点 event_tuple 大小上限（字符）
MAX_TUPLE_SIZE = 2000
# 整链 event_tuple 之和触发坍塌器的阈值
COLLAPSE_THRESHOLD = 8000
# 坍塌触发倍率：整链大小达到 COLLAPSE_THRESHOLD 的该倍数时才触发坍塌器（默认 150%）
COLLAPSE_TRIGGER_RATIO = 2
# 望远镜功率 / 事件视界半径
MAX_EYES = 30
# HTTP 超时（秒）；大文本语义向量化较慢，需放宽
TIMEOUT = 300

# Redis 状态库（Redis 5.x 仅支持 RESP2）
REDIS_URL = "redis://192.168.66.24:6379/2"
REDIS_PROTOCOL = 2
REDIS_KEY_PREFIX = "causal_slicing"


# # ---- 坍塌器 / LLM（本地 OpenAI 兼容端点）----
LLM_BASE_URL = "http://192.168.66.43:9382/llm/v1"
LLM_MODEL = "local-llm"
LLM_API_KEY = "not-needed"


# ---- 便捷 kwargs（切片器 / 坍塌器构造用）----
SLICER_KWARGS = {
    "base_url": BASE_URL,
    "owner_id": OWNER_ID,
    "actor_id": ACTOR_ID,
    "token": TOKEN,
    "bot_id": BOT_ID,
    "source_id": SOURCE_ID,
    "max_tuple_size": MAX_TUPLE_SIZE,
    "collapse_threshold": COLLAPSE_THRESHOLD,
    "collapse_trigger_ratio": COLLAPSE_TRIGGER_RATIO,
    "max_eyes": MAX_EYES,
    "timeout": TIMEOUT,
    "redis_url": REDIS_URL,
    "redis_protocol": REDIS_PROTOCOL,
    "redis_key_prefix": REDIS_KEY_PREFIX,
}

COLLAPSER_KWARGS = {
    "base_url": BASE_URL,
    "token": TOKEN,
    "owner_id": OWNER_ID,
    "bot_id": BOT_ID,
    "bot_nick_name": BOT_NICK_NAME,
    "source_id": SOURCE_ID,
    "max_eyes": MAX_EYES,
    "timeout": TIMEOUT,
    "redis_url": REDIS_URL,
    "redis_protocol": REDIS_PROTOCOL,
    "redis_key_prefix": REDIS_KEY_PREFIX,
    "llm_base_url": LLM_BASE_URL,
    "llm_model": LLM_MODEL,
    "llm_api_key": LLM_API_KEY,
    "collapse_threshold": COLLAPSE_THRESHOLD,
    "length": int(COLLAPSE_THRESHOLD/3)
}
