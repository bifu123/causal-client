# 1. 导入依赖库
## langchain相关
import httpx
from deepagents import create_deep_agent
from deepagents.backends import LocalShellBackend

##  langchain agent工具
from causal_tools import tools

## openai库
from langchain_openai import ChatOpenAI

## 元龙因果链相关
### 配置变量
from causal_config import (SLICER_KWARGS, COLLAPSER_KWARGS,
                            USER_NICK_NAME, BOT_NICK_NAME, USER_ID, BOT_ID)
### 切片器和坍塌器
from causal_slicing import BotSlicer
from causal_collapse import Collapser

## 其它
import json
import os


# 2. 定例化对象
## 实例化切片器和坍塌器
slicer = BotSlicer(**SLICER_KWARGS)
collapser = Collapser(**COLLAPSER_KWARGS)

## 注册坍塌器：整链超阈值时自动另起线程坍塌
slicer.set_collapse_callback(collapser.collapse)

## 定义本地后端文件系统
backend = LocalShellBackend(
    root_dir=".",
    virtual_mode=False,
    env={"PATH": "/usr/bin:/bin"}
)

## 定义SKILL
### 下载SKILL
skill_url = f"{SLICER_KWARGS['base_url']}/SKILL.md"
response = httpx.get(skill_url)
response.raise_for_status()
skill_content = response.text

### 替换参数
skill_content = skill_content.replace("{owner_id}", f'{USER_ID}_{BOT_ID}')
skill_content = skill_content.replace("{actor_id}", f'{BOT_ID}')
skill_content = skill_content.replace("{token}", f'{SLICER_KWARGS["token"]}')

### 将在线下载的 Skill 文件写入本地物理文件（或者直接通过 backend 上传）
os.makedirs("./skills/causal", exist_ok=True) #确保本地物理目录存在
skill_file_path = "./skills/causal/SKILL.md"
with open(skill_file_path, "w", encoding="utf-8") as f:
    f.write(skill_content)
    
### 将在线下载的 Skill 文件上传到后端的对应位置
backend.upload_files([
    ("/skills/causal/SKILL.md", skill_content.encode("utf-8"))
])


## 定义模型
model = ChatOpenAI(
    model=COLLAPSER_KWARGS["llm_model"], 
    base_url=COLLAPSER_KWARGS["llm_base_url"],
    api_key=COLLAPSER_KWARGS["llm_api_key"]
)

## 创建 DeepAgent
agent = create_deep_agent(
    model=model,
    backend=backend,
    tools=tools,
    skills=["./skills/"],  # 指定 skills 目录
    system_prompt=f"""你叫`{BOT_NICK_NAME}`，你将处理因果链上的事件操作、回答问题。
    **除非用户明确要求，请将回复限制在 `{SLICER_KWARGS["max_tuple_size"]}` 字内**
    """
)


## 定义传递变量（在工具层和子代理通信）
config = {"configurable":{
            "thread_id": USER_ID,
            "slicer_kwargs": SLICER_KWARGS
        }}

# 执行推理
if __name__ == "__main__":
    # 统计变量
    all = """|次数|是否必答|当前焦点|
    |---|---|---|"""
    counter = 0
    focus = "--"
    is_must_answer = "no"

    # 执行问答
    while True:
        notice = """
        ////////////////////////////////////////
        //          因果星空 Memory           //
        ///////////////////////////////////////
        """
        print(notice)
        

        
        # 是否必答
        
        print("1. 收集变量")
        question = input("请输入问题：")
        is_must_answer = input("是否必答？ yes|no(默认yes): ")
        if is_must_answer == "":
            is_must_answer = "yes"
        
        
        print("2. 将用户消息写入星空")
        for r in slicer.zhenru(event_tuple=question, node_id=slicer.make_node_id(question), user_id=USER_ID, user_name=USER_NICK_NAME, is_must_answer=is_must_answer):
            print(r)
            
        print("3. 处理是否必答")
        if is_must_answer == "yes":
            print("|----AI 必答") 
            # 获取事件视界
            max_eyes = SLICER_KWARGS["max_eyes"]
            
            while True:
                horizon = slicer.present(max_eyes)  # 或 slicer.present(max_eyes)
                print(f'[DEBUG] 获取当下...')
                horizon_len = len(str(horizon))
                if horizon_len <= SLICER_KWARGS["max_tuple_size"]:
                    print(f'[DEBUG] 视界符合大小，当前: {horizon_len}，限定：{SLICER_KWARGS["max_tuple_size"]}')
                    break
                max_eyes -= 5
                print(f'[DEBUG] 视界超过大小，当前: {horizon_len}，限定：{SLICER_KWARGS["max_tuple_size"]}，递减5光年为 {max_eyes} 光年')
                
                if max_eyes <= 0:
                    max_eyes = 1
                    break
            
            
            
            # print("#" * 40)
            print(f'|--------当前大股东：{horizon["data"]["serial_id"]}')
            # print(json.dumps(horizon, indent=4, ensure_ascii=False))
            
            focus = horizon["data"]["serial_id"]
            
            
            # print("#" * 40)

            # 构建请求
            query = f'''
            # 当下
            
            > 你现在正处在 `{focus}` 事件节点上：
            
            {horizon}
            
            ---
            
            # 用户问题
            {question}
            '''
            
            
            # # 执行推理问答（非流式）
            # result = agent.invoke(
            #     {"messages": [{"role": "user", "content": query}]},
            #     config=config
            # )
            
            # # 统计请求大小
            # print(f'|--------请求大小：{len(query)}')
            
            # # 执行问答
            # answer = result["messages"][-1].content


            # # 输出结果
            # print("=" * 45) 
            # print(answer)
            # print(f'|--------答案大小：{len(answer)}')
            # print("=" * 45)
            
            # # 将清洗后的纯净答案写入因果星空
            # for r in slicer.zhenru(event_tuple=answer, node_id=slicer.make_node_id(answer), user_id=BOT_ID, user_name=BOT_NICK_NAME):
            #     print(r)
            
            # 执行推理问答（流式）
            full_content = []
            
            # 使用最基础的 messages 模式，不开启 subgraphs，避免复杂的命名空间嵌套
            
            print("\n" + "=" * 45)
            for msg, metadata in agent.stream(
                {"messages": [{"role": "user", "content": query}]},
                config=config,
                stream_mode="messages"
            ):
                if hasattr(msg, "content") and msg.content:
                    text = msg.content
                    # 排除工具消息和内部 JSON
                    if "ToolMessage" in str(type(msg)) or (text.strip().startswith("{") and "status" in text):
                        continue
                    
                    print(text, end="", flush=True)
                    full_content.append(text)
            print("\n" + "=" * 45 + "\n")
            
            raw_answer = "".join(full_content).strip()
            
            # if "【最终答案】" in raw_answer:
            #     answer = raw_answer.split("【最终答案】")[-1].strip()
            # elif "[最终答案]" in raw_answer:
            #     answer = raw_answer.split("[最终答案]")[-1].strip()
            # else:
            #     answer = raw_answer.strip()

            # if not answer:
            #     answer = raw_answer if raw_answer else "（系统提示：暂无有效回复内容）"

            # print("【最终入库确认】")
            # print("#" * 50)
            # print(answer)
            # print(f"|----最终入库答案大小: {len(answer)}")
            # print("#" * 50)
            # print()
            
            for r in slicer.zhenru(event_tuple=raw_answer, node_id=slicer.make_node_id(raw_answer), user_id=BOT_ID, user_name=BOT_NICK_NAME, is_must_answer=is_must_answer):
                print(r)
            
            
        else:
            print("|----AI 非必答，略过") 
            focus = "----" 
        
        counter += 1  
        all += f"\n|{counter}|{is_must_answer}|{focus}|"
        print(all)    
        
                