import asyncio
import os
import re
import sys
import ast
from json import JSONDecodeError
from causal_prompt import meta_content

import json

# 导入依赖包
from langchain_openai import ChatOpenAI
from langchain_core.messages import ToolMessage
# from langchain.agents import create_agent

# deepagents 库
from deepagents import create_deep_agent
from langgraph.checkpoint.redis.aio import AsyncRedisSaver

# Skills本地文件支持  
from pathlib import Path

# from deepagents.backends.filesystem import FilesystemBackend, CompositeBackend
from deepagents.backends import LocalShellBackend
import httpx
from causal_config import SLICER_KWARGS
from causal_slicing import field_owner_id

# 工具构建器
from lmcp_tool_builder import LMCPToolBuilder # lmcp_tool_builder

# 导入文件
import bot_fun # 通用函数 

# 导入果核标签获取模块
from causal_get_tag import get_collapse

from bot_process import MAX_TUPLE_SIZE

# 读取环境变量  
from dotenv import load_dotenv
load_dotenv()

import logging

# 抑制httpx和openai的日志
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# 创建异步 llm
import bot_llm

# Windows 异步兼容补丁  
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    

def build_redis_uri() -> str:
    """从 .env 常量拼接 Redis URI"""
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")
    password = os.getenv("REDIS_PASSWORD", "")
    if password:
        return f"redis://:{password}@{host}:{port}/{db}"
    else:
        return f"redis://{host}:{port}/{db}"

# # 获取果核标签
# async def get_tag(keyword:str, owner_id:str, url:str, limit:int):
    
    

# LLM 执行 agent 过程
class AsyncAgentClient:
    def __init__(self, user_query, chat_history=[], rebuild_data:dict=None, skills_dir=None):
        self.user_query = user_query 
        self.chat_history = chat_history if chat_history != [] else "当前无对话历史记录"
        self.rebuild_data = rebuild_data
        # 新增：如果子代理HITL状态
        self.causal_state = {"human_pending": False, "pending_question": ""}
 
        # 路径处理
        if skills_dir is None:
            self.skills_dir = "./skills/"

        source_id = self.rebuild_data["source_id"]
        self.workspace = Path(f'./workspace/{source_id}').resolve()
        # 确保目录存在
        self.workspace.mkdir(parents=True, exist_ok=True)
        
        # 预留属性，稍后在异步方法中初始化
        self.model = None
        self.base_tools = []
        self.expert_tools = []
        self.social_tools = []
        self.subagents_officer = None
        self.subagents_social = None
        self.checkpointer = None
        self.prompt = ""
        self.config = {}
        self.result_tag = None # 果核标签
        
    # # 定一个获取果核标签的内部函数
    # async def _get_tag(self):
    #     keyword = self.rebuild_data["from_user"]["user_nick_name"]
    #     owner_id = f'{self.rebuild_data["source_id"]}_{self.rebuild_data["bot"]["bot_id"]}'
    #     url = f'{os.getenv("CAUSAL_URL")}/api/v1/causal/search/keyword'
    #     limit = 12
    #     from causal_get_tag import get_collapse
    #     tag = await get_collapse(keyword=keyword, owner_id=owner_id, url=url, limit=limit)
    #     if tag["status"] == "seccuss" and tag["event_tuples"] != '':
    #         result = f'\n你当前对{keyword}的看法是:\n{tag["event_tuples"].replace("[chat]","")}\n**请基于你的看法决定响应策略**\n---\n'
    #     else:
    #         result = ""
    #     print(f'[DEBUG] bot_agent.py:138: 果核标签：{result}')
    #     return result
    
    # 对类参数赋值
    async def _initialize(self):
        """在这里安全地加载 LLM 和工具"""
        if self.model is not None:
            return
        
        # 异步获取模型
        self.model = await bot_llm.get_llm_async()
        
        # 获取果核标签
        # self.result_tag = await self._get_tag()
        
        # 拼装prompt
        self.prompt = f"""你的名字叫**{self.rebuild_data["bot"]["bot_nick_name"]}**，请参照以下规则回答问题：
            
    ## 当前聊天环境
    """
        if self.rebuild_data["method"] == "private":
            self.prompt += f'你当前和**{self.rebuild_data["from_user"]["user_nick_name"]}**私聊\n\n---\n\n'
        if self.rebuild_data["method"] == "group":
            self.prompt += f'你当前处在聊天群环境中\n\n---\n\n'
        if self.rebuild_data["method"] == "group_at":
            self.prompt += f'你当前处在聊天群环境中，**{self.rebuild_data["from_user"]["user_nick_name"]}**在群中@你\n---'
            
        # self.prompt += self.result_tag
        
        self.prompt += f"""
        ## 当下
        
        {self.chat_history}
        `当下`是你目前处于因果链中的位置，如果要了解前因后果，或遇到语义断裂，你可以移动因果指针历经前后事件，或使用更多因果技能。

        ## 安全规则
        - 在执行 ls、rm、mkdir等文件操作时，请使用系统管理员子代理，请不要擅自执行。"""

        additional_prompt = f"""
        ## 输出规则
        - 尽量使用markdown格式输出，使之图文并茂。
        - 在总结答案时，重要的图片和链接请不要忽略，当你使用markdown格式输出图片或链接时，你可以这样: \n```\n[下载链接](http://example.com/download)```。
        - 可以加入一些emoji表情让回答更生动有趣。
        - 除非用户要求，请控制回复字数在 **{MAX_TUPLE_SIZE}** 以内"""
        if self.rebuild_data["source"] == "web":
            self.prompt += additional_prompt
        
        # if self.rebuild_data["method"] != "private" and self.rebuild_data.get("members_skill") and self.rebuild_data["members_skill"] not in self.prompt:
        #     self.prompt += "\n如果群成员技能不为空，你可以在 Agent 复杂任务规划时根据各人所长，@其它群成员分配任务"
        #     self.prompt += "\n\n## 群成员技能\n" + self.rebuild_data["members_skill"]
            
        if meta_content not in self.prompt:
            self.prompt = meta_content + self.prompt
        prompt_format = json.dumps(self.prompt, ensure_ascii=False, indent=4).replace(r'\n', '\n')
        # print(f'[DEBUG] bot_agent.py:160 prompt_format: system_prompt: \n{prompt_format}')
        print(f'[DEBUG] bot_agent.py:167 prompt_format大小: {len(self.prompt)}')
        
        # 3. 定义工具业务
        import bot_tools

        self.base_tools = [
            bot_tools.calculator,
            bot_tools.get_current_time, 
            bot_tools.get_url_content, 
            bot_tools.baidu_search,
            bot_tools.ddg_search,
            bot_tools.get_v6_search,
            bot_tools.get_notebook_content, 
            bot_tools.insert_note_content,
            bot_tools.delete_note_content,
            bot_tools.update_note_content,
            bot_tools.add_scheduled_task,
            bot_tools.remove_scheduled_task,
            bot_tools.list_scheduled_tasks,
            bot_tools.get_current_info,
            bot_tools.get_chat_history,
            bot_tools.interpret_history_image,
            bot_tools.gui_trask_invoke,
            bot_tools.gui_trask_invoke_android,
            bot_tools.gui_trask_flow,
            bot_tools.gui_trask_end,
            bot_tools.search_causal_by_embed,
            bot_tools.search_causal_by_keyword,
            bot_tools.search_causal_by_serial,
            bot_tools.get_causal_skeleton,
            bot_tools.trigger_causal_node,
            bot_tools.update_causal_node,
            bot_tools.delete_causal_node,
            bot_tools.get_current_event_horizon  
        ]
        
        if self.rebuild_data.get("source_key") == "napchat":
            self.base_tools.extend([
                bot_tools.send_private_msg,
                bot_tools.send_group_msg,
                bot_tools.get_nickname,
                bot_tools.get_friend_list,
                bot_tools.get_group_info,
                bot_tools.get_group_list,
                bot_tools.get_group_member_info,
                bot_tools.get_group_member_list,
                bot_tools.kick_group_member,
            ])
        
        self.ylbot_tools = [
            bot_tools.find_group_by_group_id,
            bot_tools.find_group_by_keyword,
            bot_tools.join_group,
            bot_tools.update_member_description,
            bot_tools.leave_group,
            bot_tools.register_group
        ]
        
        self.system_tools = [
            bot_tools.execute_command,
            # bot_tools.vps_execute_command,
            bot_tools.calculator,
            bot_tools.get_current_time
        ]
        
        self.subagents_ylbot = {
            "name": "subagents_ylbot",
            "description": """ylbot是一种agent群体合作生态，此子代理专注于本群在全网的注册、自已在群中的技能申明等业务""",
            "tools": self.ylbot_tools + self.base_tools,
            "system_prompt": "你是一个社会化合作的agent，负责处理主代理交给我的ylbot相关任务。"
        }

        self.system_manager = {
            "name": "system_manager",
            "description": """直接执行本地系统命令，以及在远程 VPS 上执行 Shell命令""",
            "tools": self.system_tools,
            "system_prompt": "执行操作系统的一切命令操作及 VPS Shell、python 脚本，处理主代理交给你的此类任务。"
        }
        

        # 5. 配置会话存储
        # redis_uri = build_redis_uri()
        # self.checkpointer = AsyncRedisSaver(redis_uri)
        
        # 6. 配置传递变量（必须包含 thread_id 以供 Redis checkpointer 识别）
        thread_id = str(self.rebuild_data.get("from_user", {}).get("user_id") or self.rebuild_data.get("source_id", "default_thread"))
        self.config = {
            "configurable": {
                "thread_id": thread_id,  # ✅ 必须项：作为检查点的 primary key
                "data": self.rebuild_data,
                "history": self.chat_history,
                "workspace": self.workspace,
                "causal_state": self.causal_state
            }
        }



    async def _get_agent(self):
        await self._initialize()
        backend = LocalShellBackend(
            root_dir=".",
            virtual_mode=False,
            env={"PATH": "/usr/bin:/bin"}
        )
        
        # 像 causal 那样：从 URL 在线下载 SKILL，替换参数后写入本地并上传到后端
        source_id = self.rebuild_data["source_id"]
        bot_id = self.rebuild_data["bot"]["bot_id"]
        owner_id = field_owner_id(source_id, bot_id)
        actor_id = bot_id
        
        skill_url = f"{SLICER_KWARGS['base_url']}/SKILL.md"
        response = httpx.get(skill_url)
        response.raise_for_status()
        skill_content = response.text
        skill_content = skill_content.replace("{owner_id}", owner_id)
        skill_content = skill_content.replace("{actor_id}", actor_id)
        skill_content = skill_content.replace("{token}", SLICER_KWARGS["token"])
        
        os.makedirs("./skills/causal", exist_ok=True)
        skill_file_path = "./skills/causal/SKILL.md"
        with open(skill_file_path, "w", encoding="utf-8") as f:
            f.write(skill_content)
        
        backend.upload_files([
            ("/skills/causal/SKILL.md", skill_content.encode("utf-8"))
        ])
        
        
        # if meta_content not in self.prompt:
        #     self.prompt = meta_content + self.prompt
        # prompt_format = json.dumps(self.prompt, ensure_ascii=False, indent=4).replace(r'\n', '\n')
        # print(f'[DEBUG] bot_agent.py:395 system_promt: \n{prompt_format}')
            
        agent = create_deep_agent(
            model=self.model,
            tools=self.base_tools,
            skills=["./skills/"],
            subagents=[self.subagents_ylbot, self.system_manager],
            backend=backend,
            system_prompt=self.prompt
            # checkpointer=self.checkpointer 
        )
        

        return agent
    
    def _ensure_plain_text_download_links(self, response):
        import re
        markdown_link_pattern = r'\[.*?\]\((/download/.*?)\)'
        def replace_markdown_link(match):
            link_url = match.group(1)
            return f"下载地址：{link_url}"
        response = re.sub(markdown_link_pattern, replace_markdown_link, response, flags=re.DOTALL)
        html_tag_pattern = r'<[^>]+>'
        response = re.sub(html_tag_pattern, '', response)
        return response
    
    async def _get_final_answer(self, response):
        if "[最终答案]" in response:
            final_answer_match = re.search(r'\[最终答案\](.*)', response, re.DOTALL)
            if final_answer_match:
                clean_response = final_answer_match.group(1).strip()
                clean_response = self._ensure_plain_text_download_links(clean_response)
                response = clean_response
        return response

    async def _handle_agent_error(self, e: Exception, source: str = "web"):
        """统一处理 Agent 执行过程中的异常"""
        error_str = str(e).lower()
        if "json" in error_str or "decode" in error_str or "unterminated string" in error_str:
            print(f'[ERROR] agent_{source} 内部检测到疑似 JSON 解析错误: {e}')
            import traceback
            traceback.print_exc()
            if source == "onebot":
                yield f"\n\n[系统检测到数据解析异常，正在自动重试...]\n"
            raise JSONDecodeError(f"包装后的JSON解析错误: {e}", "", 0)
        elif "insufficient balance" in error_str or "402" in error_str or "all llm_pay models failed" in error_str:
            print(f'[ERROR] 检测到 API 余额不足错误: {e}')
            error_msg = f"模型错误：{e}"
            if source == "onebot":
                yield await bot_fun.format_output_for_onebot(error_msg, self.rebuild_data) + os.getenv("AGENT_FLAG")
            else:
                yield f"\n\n{error_msg}"
            return
        else:
            print(f'[ERROR] agent_{source} 内部发生未知异常: {e}')
            import traceback
            traceback.print_exc()
            # raise e
            yield f'[ERROR] agent_{source} 内部发生未知异常: {e}'
            return
                
    async def agent_for_web(self):
        llm_agent_async = await self._get_agent()
        response = ""
        last_tool_call_snapshot = None

        try:
            async for item in llm_agent_async.astream(
                {"messages": [{"role": "user", "content": self.user_query}]}, 
                config=self.config,
                stream_mode="messages",
                subgraphs=True
            ):
                from bot_redis_helper import redis_helper
                cancel_key = f"agent_cancel_events:{self.rebuild_data['source_id']}"
                cancel_flag = await redis_helper.get_value(cancel_key)
                if cancel_flag == "1":
                    await redis_helper.delete_key(cancel_key)
                    print(f'[AGENT] 检测到 Redis 停止信号，已停止 agent 输出')
                    break
                
                node_path, (chunk, metadata) = item

                if hasattr(chunk, "tool_calls") and chunk.tool_calls:
                    current_tool_names = [tc.get("name") for tc in chunk.tool_calls]
                    if current_tool_names != last_tool_call_snapshot:
                        for tc in chunk.tool_calls:
                            name = tc.get("name")
                            if name and "todo" not in name.lower():
                                yield f"\n\n🛠️ 正在调用工具: {name}..."
                        last_tool_call_snapshot = current_tool_names

                if isinstance(chunk, ToolMessage) or (hasattr(chunk, "type") and getattr(chunk, "type", "") == "tool"):
                    result_content = chunk.content
                    if result_content:
                        if result_content.startswith("http"):
                            display_text = f"\n\n📦 工具返回结果: [点击查看详情]({result_content})"
                        else:
                            display_text = f"\n\n📦 工具返回结果: {result_content[:150]}..."
                        yield display_text

                if hasattr(chunk, "content") and chunk.content:
                    yield chunk.content
                    response += chunk.content
        except Exception as e:
            async for token in self._handle_agent_error(e, source="web"):
                yield token
            return

        clean_response = await bot_fun.get_after_fianl_answer(response)
        response = clean_response
        yield '[最终答案]' + response
        
    async def agent_for_onebot(self):
        print("✉️ 开始全流程推理过程 (支持子业务追踪)")
        print(f"[SKILLS] 技能目录：{self.skills_dir}")
        
        llm_agent_async = await self._get_agent()
        response = ""
        last_todo_snapshot = []
         
        try:
            async for namespace, chunk in llm_agent_async.astream(
                {"messages": [{"role": "user", "content": self.user_query}]},
                config=self.config,
                stream_mode="updates",
                subgraphs=True
            ):
                from bot_redis_helper import redis_helper
                cancel_key = f"agent_cancel_events:{self.rebuild_data['source_id']}"
                cancel_flag = await redis_helper.get_value(cancel_key)
                if cancel_flag == "1":
                    await redis_helper.delete_key(cancel_key)
                    print(f'[AGENT] 检测到 Redis 停止信号，已停止 agent 输出')
                    break

                actor = namespace[-1].split(":")[0] if namespace else "主业务"
                
                for node_name, output in chunk.items():
                    if not output or "Middleware" in node_name:
                        continue

                    if "todos" in output:
                        current_todos = output["todos"]
                        if isinstance(current_todos, list) and current_todos != last_todo_snapshot:
                            todo_text = f"\n📋 让我更新一下任务清单:"
                            has_valid_content = False
                            
                            for i, item in enumerate(current_todos, 1):
                                task_content = item.get('task') or item.get('todo') or item.get('content')
                                if task_content:
                                    has_valid_content = True
                                    status = item.get('status', 'pending')
                                    icon = "✅" if status == 'completed' else "⏳" if status == 'in_progress' else "⚪"
                                    todo_text += f"\n   {i}. {icon} {task_content}"
                            
                            if has_valid_content:
                                last_todo_snapshot = [dict(t) for t in current_todos]
                                yield todo_text + os.getenv("AGENT_FLAG")
                        continue

                    if "messages" in output:
                        msgs = output["messages"]
                        if not isinstance(msgs, list) and hasattr(msgs, 'value'): 
                            msgs = msgs.value
                        if not msgs: 
                            continue
                        
                        msg = msgs[-1]

                        if getattr(msg, "tool_calls", None):
                            if msg.content:
                                thought = f"\n🤔 {msg.content}"
                                yield thought + os.getenv("AGENT_FLAG")
                            continue

                        if getattr(msg, "type", None) == "tool":
                            continue

                        if msg.content:
                            if "[NEED_HUMAN]" in msg.content:
                                human_question = msg.content.split("[NEED_HUMAN]")[-1].strip()
                                self.causal_state["human_pending"] = True
                                self.causal_state["pending_question"] = human_question
                                hitl_msg = f"\n❓ **因果数据不足，需要您补充：**\n{human_question}"
                                yield hitl_msg + os.getenv("AGENT_FLAG")
                                continue

                            if not namespace and node_name == "agent":
                                response += msg.content
                                prefix = "💡 "
                            else:
                                prefix = ""
                            
                            raw_chunk = prefix + msg.content
                            if not await bot_fun.is_simple_repetition(raw_chunk, self.user_query):
                                yield raw_chunk + "\n"
        except Exception as e:
            async for token in self._handle_agent_error(e, source="onebot"):
                yield token
            return

        clean_response = await self._get_final_answer(response)
        print()
        print('✉️ ==================== result ======================') 
        print(clean_response)
        print('✉️ ====================== end =======================')
        
if __name__ == "__main__":
    async def main(rebuild_data:dict):
        user_query = rebuild_data["raw_message"]
        agent_client = AsyncAgentClient(user_query=user_query, rebuild_data=rebuild_data)
        
        try:
            import sys
            p = sys.argv[1]
        except:
            p = "web"
            
        if p == "onebot":
            async for token in agent_client.agent_for_onebot():
                print(token, end="", flush=True)
        else:
            async for token in agent_client.agent_for_web():
                print(token, end="", flush=True)

    rebuild_data = {
        "message_id": 796320063,
        "bot": {
            "bot_id": "3152246598",
            "bot_nick_name": "小雪"
        },
        "raw_message": "@小雪 你好",
        "message": [
            {
            "type": "text",
            "data": {
                "text": "@小雪 你好"
            }
            }
        ],
        "source_id": "815669761",
        "method": "group_at",
        "from_user": {
            "user_id": "415135222",
            "user_nick_name": "廉颇"
        },
        "source": "onebot",
        "source_key": "napchat",
        "name_space": "default",
        "method_allow": [
            "private",
            "group_at",
            "group"
        ],
        "is_must_answer": "yes",
        "callback_url": "http://192.168.68.15:8085"
    }
          
    asyncio.run(main(rebuild_data))