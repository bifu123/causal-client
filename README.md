# 元龙因果 Agent · causal_plugin

把对话流写进**因果星空**（一张以「事件」为节点、由前后 ID 串起来的因果链网），用这条链当 Agent 的记忆；
记忆过长时由**坍塌器**把最旧的一段提炼压缩成第一人称叙事的「果节点」，最近上下文保持不变。

一句话记法：**切片器负责「写」，坍塌器负责「压」，Agent 负责「读」**。

## 安装（已发布 PyPI）

```bash
pip install causal-slicing causal-collapse
# 需要 Redis 作为跨进程状态库时（Redis 5.x 仅支持 RESP2，传 redis_protocol=2）
pip install "causal-slicing[redis]" "causal-collapse[redis]"
```

| 发行版 | 顶层模块 | 依赖 | PyPI |
|---|---|---|---|
| `causal-slicing` | `causal_slicing` | `requests`（可选 `redis`） | https://pypi.org/project/causal-slicing/ |
| `causal-collapse` | `causal_collapse` | `causal-slicing`、`openai`（可选 `redis`） | https://pypi.org/project/causal-collapse/ |

## 最小用法

```python
from causal_slicing import BotSlicer
from causal_collapse import Collapser

# 实例化切片器和坍塌器
slicer = BotSlicer(**SLICER_KWARGS)
collapser = Collapser(**COLLAPSER_KWARGS)   # redis_key_prefix 必须与切片器一致（同一焦点键）

# 注册坍塌器：整链超阈值时另起线程自动坍塌
slicer.set_collapse_callback(collapser.collapse)

# 写入一条用户消息（必答 → 写完顺手 click 拉取事件视界）
for r in slicer.zhenru("你好，今天是一个晴天。",
                       node_id=slicer.make_node_id("你好，今天是一个晴天。"),
                       user_name="廉颇", user_id="415135222", is_must_answer="yes"):
    print(r.get("action"), r.get("serial_id"), r.get("error"))

# 读取当下（Agent Memory）
horizon = slicer.present()
```

## 目录结构

| 文件 / 目录 | 作用 |
|---|---|
| `causal_slicing.py` | **切片器**。`CausalClient`（HTTP 客户端）+ `BotSlicer`（贞入/累加/切分/回写）+ `field_owner_id`（场名）。自包含单文件，已发布为 `causal-slicing` |
| `causal_collapse.py` | **坍塌器**。整链超阈值时提炼旧前缀 → 删除 → 首贞果节点 → 重连保留链。已发布为 `causal-collapse` |
| `causal_config.py` | 两者共用的参数与身份配置，导出 `SLICER_KWARGS` / `COLLAPSER_KWARGS` |
| `casaul_agent.py` | 交互式 Demo：langchain `create_agent` + `click_event` 工具 |
| `tool_call.py` | 交互式 Demo：原生 OpenAI tool calling 循环（连接本地 Ollama） |
| `casaul_deepagents.py` | 交互式 Demo：`deepagents` 版，会从星空下载 `SKILL.md` 到 `./skills/causal/` |
| `causal_tools.py` | 8 个因果工具函数（搜索/贞入/更新/删除/骨架/视界…），自动聚合成 `tools` 列表 |
| `bot_agent.py` | 进阶异步多技能 Agent（依赖本仓库外的私有模块，见「本地运行」） |
| `skills/causal/SKILL.md` | Agent Skill：因果链术语与操作规范（由 `casaul_deepagents.py` 运行时生成/更新） |
| `pypi/` | 发布工程：两个 `pyproject.toml`、README、同步脚本、冒烟测试、构建产物 |
| `.causal_plugin/` | 项目自带的 Linux 虚拟环境（已装 `deepagents` 等），Windows 下不可用 |
| `pypi_token.txt` | PyPI 上传 token（不入库，见 `.gitignore`） |

## 核心概念

| 术语 | 含义 |
|---|---|
| 事件 / 瓶子 | 因果链上的一个节点，`event_tuple` 是一条带 `[chat]` 标记的叙事文本；切片器把多轮对话「累加」进同一个瓶子，满则封盖另起新瓶 |
| 贞入 | 写入动作的统称。**首贞**＝新建节点（`previous=None`，成为根）；**又贞**＝接龙到链尾；**对贞**＝结果确认（`block_tag="果"`）；坍塌产物就是「首贞+对贞」的果节点 |
| 累加优先 | 先判断当前瓶子装不装得下：装得下就 `update` 累加（必须回传原 `previous_ids`/`action_tag`/`block_tag`），装不下才封盖另起新瓶 |
| 场（owner_id） | 一个 `source_id`（会话/联系人）对应一个场：`field_owner_id(source_id, bot_id)` → `{source_id}_{bot_id}`，即「一 source 一场一链」，切片器与坍塌器必须落在同一场 |
| 当下 / 事件视界 | `present()` 返回焦点（大股东）事件及其「黑洞视界」内的节点；`max_eyes` 是望远镜功率（名义单位「光年」，建议 30 左右） |
| 大股东 | 视界里权重最高的节点，通常就是当下焦点 |
| 整链阈值 | 链上所有瓶子 `event_tuple` 之和；超过 `collapse_threshold × collapse_trigger_ratio` 触发坍塌 |
| 果节点 | 坍塌产物：第一人称「我」视角的叙事总结，`previous=None`，重连后成为新根 |

## 切片器 BotSlicer：写入流程（8 步）

```
对话流 ──► 包装成 slice（带 [chat] 标记）
             │
             ├─ 焦点瓶子装得下？──是──► update 累加（显式回传 previous_ids / action_tag / block_tag）
             │                                   │
             │                                   └─ 累加后整链超阈值？──是──► 提交坍塌任务（不阻塞）
             │
             └─ 装不下 ──► 封盖，又贞接龙到链尾（node_id 冲突则换唯一 ID，previous_node 始终指向当前瓶）
                             │
                             └─ 单条内容超上限 ──► 按句切分（一块一瓶，不断句）
```

1. **累加优先**：必答/非必答一视同仁，先看当前焦点瓶子是否装得下，装得下就 `update` 累加。
2. **满则又贞**：超过 `max_tuple_size` 则封盖，另起新瓶接龙到链尾，此时才处理 `node_id` 冲突。
3. **先核对再接续**：Redis 有记录就回星空核对同 `serial_id` 节点；记录缺失或节点失效则找最近的 `[chat]` 瓶子；都没有才首贞。
4. **又贞强制父节点非空**：写入后回读校验父链，服务端没记住 `previous_node` 就用 `update` 补回，避免旧瓶变孤立节点。
5. **切分**：单条内容超上限按块切分，保持断句完整。
6. **回写**：`serial_id` 与整链计数写回 Redis（不可用时退化为进程内字典）。
7. **必答 vs 非必答**：`is_must_answer="yes"` 贞入/累加后 `click` 拉取事件视界作 AI 上下文；`"no"` 不点（省星空算力）。
8. **坍塌触发**：整链超阈值时把任务丢进进程级线程池，立即返回可监听的 `Future`。

### 关键参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `base_url` / `bot_id` | 必填 | 星空服务地址 / 机器人 ID（跨场共享主键） |
| `owner_id` / `actor_id` / `token` | `"default"` / `""` / `None` | 视角场 / 观测者 / 私密场钥匙 |
| `source_id` | `None` | 当前会话 ID，也可逐条在 `zhenru()` 传入 |
| `max_tuple_size` | `2000` | **瓶子大小**：单个节点 `event_tuple` 上限（字符） |
| `collapse_threshold` | `10000` | **整链大小上限**，超 `× collapse_trigger_ratio` 触发坍塌 |
| `collapse_trigger_ratio` | `1.5` | 触发倍率（迟滞，避免坍完立刻再触发） |
| `max_eyes` | `30` | 望远镜功率 / 事件视界半径 |
| `timeout` | `120` | HTTP 超时（大文本语义向量化较慢） |
| `redis_url` / `redis_protocol` / `redis_key_prefix` | `127.0.0.1:6379/0` / `2` / `causal_slicing` | 状态库；`redis_key_prefix` 必须与坍塌器一致 |

### 常用方法

`zhenru()` 写入 · `present()` 当下 · `past(serial_id)` 点击并取视界 · `set_collapse_callback()` 注册坍塌器 ·
`collapse_future()/collapse_is_running()/collapse_done()/collapse_result()` 监听坍塌 · `get_chain_size()` 读整链大小 ·
`reset_state()` 清指针（不删星空节点） · `make_node_id(text)` 生成节点 ID · `field_owner_id(source_id, bot_id)` 算场名。

## 坍塌器 Collapser：部分坍塌（6 步）

```
history 收集整条链的 [chat] 瓶子（升序）
   └─ 从根累计大小到 collapse_threshold ──► 旧前缀（坍） + 最近节点（留）
        ├─ LLM 提炼：叙事 + 领悟 → content；≤15 字标题 → title（重名加时间戳）
        ├─ 删除旧前缀节点（依赖服务端「删除连锁」保持链不断裂）
        ├─ 首贞果节点（previous=None，成为新根）
        ├─ 把保留链链首 previous_node 重连到果节点（回读校验，未接上则重试）
        └─ 整链计数回写 = 果节点 + 保留节点
```

- **只坍最旧的一段**，不把整链压成一个果节点，保留最近上下文完整。
- LLM 用 `openai` 客户端指向本地 OpenAI 兼容端点（`llm_base_url` / `llm_model` / `llm_api_key`）；提炼失败**退化为原文直存**（截断 2000 字）。
- 同一实例内 `_collapse_lock` 保证「同 bot 同时只跑一个坍塌」，并发触发记为 `skipped`。

### 关键参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `base_url` / `token` / `owner_id` / `bot_id` | 必填 | 与切片器同源（`owner_id` 兜底，实际按 `field_owner_id` 现算） |
| `bot_nick_name` | `"元龙"` | 机器人昵称，用于拼 LLM 第一人称提示词 |
| `llm_base_url` / `llm_model` / `llm_api_key` | `http://192.168.66.43:9382/llm/v1` / `local-llm` / `not-needed` | 提炼用 LLM |
| `length` | `None` | 提炼内容字数上限 |
| `collapse_threshold` | `None` | **坍塌范围**：从根升序累计到此值为止的旧前缀被坍；`None` 退化为整链坍塌 |
| `redis_*` | 同切片器 | **必须与切片器一致**，否则读不到焦点、计数无法回写 |

返回值：`{"status": "done", "bottles", "kept_bottles", "title", "node_id", "new_serial_id"}`；无瓶子为 `skipped`，异常为 `error`。

## 本地运行

```bash
# 1) 依赖
pip install requests openai redis        # 直接在仓库里跑所需的最小依赖
# 外部项目则改为： pip install causal-slicing causal-collapse

# 2) 改配置
#    causal_config.py：BASE_URL / TOKEN / OWNER_ID / ACTOR_ID / BOT_ID / SOURCE_ID /
#                      USER_NICK_NAME / BOT_NICK_NAME / MAX_TUPLE_SIZE / COLLAPSE_THRESHOLD / REDIS_* / LLM_*

# 3) 跑起来
python causal_slicing.py        # 切片器 CLI 演示（首贞 → 累加 → 当下视界，需星空服务可达 + 真实 token）
python casaul_agent.py          # langchain create_agent + click_event 工具（需 langchain / langchain-openai）
python tool_call.py             # 原生 OpenAI tool calling 循环（默认连本地 Ollama，模型与地址写死在文件里）
python casaul_deepagents.py     # deepagents 版（需 deepagents；会把 SKILL.md 下载到 ./skills/causal/）
```

`causal_tools.py` 里定义了 8 个因果工具，模块末尾自动收集成 `tools` 列表，可直接喂给 `create_deep_agent(tools=tools, ...)`：

| 工具 | 作用 |
|---|---|
| `search_causal_by_keyword` | 关键字搜索事件（三级召回：全文 → 分词 → LIKE，按字面相关度排序） |
| `search_causal_by_embed` | 语义向量搜索事件 |
| `search_causal_by_serial` | 按 `serial_id` 取事件及其视界 |
| `trigger_causal_node` | 贞入新节点（首贞/又贞） |
| `update_causal_node` | 更新已有节点（改内容/改父节点） |
| `delete_causal_node` | 删除节点（服务端连锁） |
| `get_causal_skeleton` | 取骨架（链结构概览） |
| `get_current_event_horizon` | 取当下事件视界 |

> `bot_agent.py` 是进阶异步版（Redis checkpoint、多技能、HITL 待人工确认），
> 但依赖本仓库之外的私有模块 `causal_prompt` / `lmcp_tool_builder` / `bot_fun` / `causal_get_tag` /
> `bot_process` / `bot_llm` 及 `langgraph`、`deepagents`，**不能在本仓库内独立运行**，仅作参考实现。

## 配置说明（`causal_config.py`）

| 配置项 | 示例值 | 说明 |
|---|---|---|
| `BASE_URL` | `http://192.168.66.39:8094` | 因果星空服务地址 |
| `OWNER_ID` | `415135222_3787687088` | 兜底场名；真实运行按 `{source_id}_{bot_id}` 现算 |
| `ACTOR_ID` / `USER_ID` | `415135222` | 观测者 / 用户 ID |
| `TOKEN` | `123456/|<hex>` | 私密场钥匙，原样作为 `token` 参数发出 |
| `BOT_ID` / `SOURCE_ID` | `3787687088` / `415135222` | 机器人 ID（跨场主键）/ 当前会话 ID |
| `USER_NICK_NAME` / `BOT_NICK_NAME` | `廉颇` / `孙膑` | 说话者昵称（用户消息 / AI 消息与果节点） |
| `MAX_TUPLE_SIZE` | `2000` | 瓶子大小上限 |
| `COLLAPSE_THRESHOLD` | `8000` | 整链大小上限（坍塌范围） |
| `COLLAPSE_TRIGGER_RATIO` | `2` | 触发倍率：整链达 `8000×2=16000` 才触发 |
| `MAX_EYES` / `TIMEOUT` | `30` / `300` | 望远镜功率 / HTTP 超时 |
| `REDIS_URL` / `REDIS_PROTOCOL` / `REDIS_KEY_PREFIX` | `redis://192.168.66.24:6379/2` / `2` / `causal_slicing` | 状态库 |
| `LLM_BASE_URL` / `LLM_MODEL` / `LLM_API_KEY` | 本地 OpenAI 兼容端点 | 坍塌提炼用 |

**注意 `collapse_threshold` 在两个类里含义不同**：切片器里是「整链大小上限（触发阈值）」，
坍塌器里是「一次坍塌的范围大小」；二者配合 `collapse_trigger_ratio` 形成迟滞，避免坍完立刻再坍。

## Redis 状态键

| 键 | 内容 | 写入方 |
|---|---|---|
| `{prefix}:focus:{bot_id}:{source_id}` | 焦点瓶子 `{"serial_id","node_id","event_tuple","size"}` | 切片器 |
| `{prefix}:chain:{bot_id}:{source_id}` | 整链 `event_tuple` 累计大小 | 切片器写、坍塌器回写 |

- Redis 5.x 只支持 RESP2，必须 `redis_protocol=2`。
- 未安装 `redis` 库或连不上时自动退化为**进程内字典**（重启即失），并只告警一次。

## 打包与发布

两个模块已拆成两个 PyPI 发行版（模块源码保持原样，仅打包元数据/版本号维护），发布工程在 `pypi/`：

```powershell
python pypi/sync_sources.py                       # 根目录源码 → 包目录副本（字节级一致）
./.build_venv/Scripts/python -m build --outdir pypi/dist pypi/causal_slicing
./.build_venv/Scripts/python -m build --outdir pypi/dist pypi/causal_collapse
./.build_venv/Scripts/python -m twine check pypi/dist/*

$env:TWINE_USERNAME='__token__'; $env:TWINE_PASSWORD=(Get-Content -Raw pypi_token.txt).Trim()
./.build_venv/Scripts/python -m twine upload pypi/dist/causal_slicing-0.1.1*     # 先
./.build_venv/Scripts/python -m twine upload pypi/dist/causal_collapse-0.1.1*    # 后（依赖前者）
```

发布前/后的离线自检：`python pypi/smoke_test.py`；用法示例：`python pypi/example_usage.py`。
详见 [`pypi/README.md`](pypi/README.md)。

## 注意事项

- **`import causal_slicing` 会执行一次 `logging.basicConfig(level=INFO, ...)`**，即设置宿主的 root logger；
  需要自定义日志的项目请在 import 之前完成配置。
- `causal_slicing.py` 末尾的 CLI 演示 import 了 `causal_config` / `causal_collapse`，仅在**直接运行该文件**时触发，
  `import causal_slicing` 不受影响。
- `token` 只通过 `causal_config.py` / kwargs 传入：**不要把它提交进仓库**（`pypi_token.txt` 已被 `.gitignore` 忽略）。
- 星空删除是「连锁」的：坍塌器依赖它保持链不断裂，同时删除后必须重连保留链链首，否则新果节点会孤立。
- 两个 PyPI 包暂未声明 `LICENSE`（PyPI 页面显示无许可证）。

## 许可证

仓库与已发布的两个 PyPI 包均未声明许可证。如需分发/商用，请先与作者确认。


