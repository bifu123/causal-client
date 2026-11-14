from openai import OpenAI
import json

# 导入配置
from causal_config import (SLICER_KWARGS, COLLAPSER_KWARGS,
                            USER_NICK_NAME, BOT_NICK_NAME, USER_ID, BOT_ID)
# 导入坍塌器
from causal_slicing import BotSlicer
from causal_collapse import Collapser

# 实例化切片器和坍塌器
slicer = BotSlicer(**SLICER_KWARGS)
collapser = Collapser(**COLLAPSER_KWARGS)

# 注册坍塌器：整链超阈值时自动另起线程坍塌
slicer.set_collapse_callback(collapser.collapse)


# 定义纯 Python 工具函数
def click_event(serial_id: int) -> dict:
    '''
    点击因果链中事件节点，返回事件视界内容
    Args: serial_id 事件节点ID
    Return: 事件视界信息
    Notice: `previous_ids`字段为当前事件的前事件ID列表；`next_ids`为后事件ID列表
    '''
    try:
        clicked = slicer.past(serial_id=serial_id)
        print(f"    已点击 serial_id={serial_id}，大股东 serial_id={clicked['data'].get('serial_id')}")
        print(f"    视界节点数={len(clicked.get('event_horizon') or [])}")
        return clicked
    except Exception as exc:
        print(f"    点击失败：{exc}")
        return {"err": f'{exc}'}

# 声明 OpenAI 格式的工具 Schema
tools = [
    {
        "type": "function",
        "function": {
            "name": "click_event",
            "description": "点击因果链中事件节点，返回事件视界内容",
            "parameters": {
                "type": "object",
                "properties": {
                    "serial_id": {
                        "type": "integer",
                        "description": "事件节点ID"
                    }
                },
                "required": ["serial_id"]
            }
        }
    }
]

# 工具函数映射字典
available_tools = {
    "click_event": click_event
}

# 初始化原生 OpenAI 客户端（连接 Ollama）
client = OpenAI(
    base_url="http://192.168.68.28:11434/v1",
    api_key="ollama"
)

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
            horizon = slicer.present(max_eyes)
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
        
        print(f'|--------当前大股东：{horizon["data"]["serial_id"]}')
        
        focus = horizon["data"]["serial_id"]

        # 构建请求
        query = f'''
        # 当下
        
        > 你现在正处在 `{focus}` 事件节点上：
        
        {horizon}
        
        > **previous_ids**: 提供了前事件节点的 `serial_id`；**next_ids**: 提供了后事件节点的 `serial_id`，你可使用**点击工具**向前或向后读取节点内容，可以了解事件的前因后果，补齐断裂。
        ---
        
        # 用户问题
        {question}
        '''
        
        # 初始化消息列表
        messages = [
            {"role": "system", "content": f"你叫`{BOT_NICK_NAME}`，你将处理因果链上的事件操作、回答问题。"},
            {"role": "user", "content": query}
        ]
        
        print(f'|--------请求大小：{len(query)}')
        
        # 原生 Tool Calling 循环
        while True:
            response = client.chat.completions.create(
                model="gemma4:12b",
                messages=messages,
                tools=tools,
                tool_choice="auto",
                extra_body={
                    "reasoning_effort": "none",   # 关闭思考
                    "options": {
                        "num_ctx": 4096,           # 限制上下文长度，防止 OOM
                        "num_gpu": 99               # 确保所有层加载到 GPU
                    }
                }
            )
            
            response_message = response.choices[0].message
            messages.append(response_message)
            
            # 如果模型没有发起工具调用，说明已得到最终回答
            if not response_message.tool_calls:
                answer = response_message.content
                break
            
            # 依次执行模型请求调用的工具
            for tool_call in response_message.tool_calls:
                tool_name = tool_call.function.name
                tool_args = json.loads(tool_call.function.arguments)
                tool_call_id = tool_call.id
                
                print(f"    [Tool Calling] 正在调用工具: {tool_name}, 参数: {tool_args}")
                
                if tool_name in available_tools:
                    tool_result = available_tools[tool_name](**tool_args)
                else:
                    tool_result = {"err": f"未知工具: {tool_name}"}
                
                # 将工具执行结果以 tool 角色追加回消息历史
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": json.dumps(tool_result, ensure_ascii=False)
                })
        
        print(f'|--------AI回复：\n\n')
        print("=" * 45)
        print(answer)
        print("=" * 45, "\n\n")
        
        # 将AI回答写入星空
        for r in slicer.zhenru(event_tuple=answer, node_id=slicer.make_node_id(answer), user_id=BOT_ID, user_name=BOT_NICK_NAME):
            print(r)
    
    else:
        print("|----AI 非必答，略过") 
        focus = "----" 
    
    counter += 1  
    all += f"\n|{counter}|{is_must_answer}|{focus}|"
    print(all)