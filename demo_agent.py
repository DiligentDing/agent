# demo_agent.py
"""
示例：把所有 schema 注册给 Azure OpenAI，
收到 tool_call 请求后执行 tools.impl 中的真实函数，再回传。
"""
import os
import json, importlib
from openai import AzureOpenAI
from tools.schema import ALL_SCHEMAS
from tools import impl      as T  # 引入实现
from typing import Optional
import re
os.environ["AZURE_OPENAI_API_KEY"] = "5a1437f6ff2648b9b969507fb5a73276"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://ai-mistraleastus2753718354821.openai.azure.com/"
os.environ["YOUR_DEPLOYMENT"] = "gpt-4.1-noah"  # 替换为你的部署名
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-12-01-preview"
)

# 映射工具名 → Python 函数
TOOLS = {
    "pubmed.search":        T.pubmed_search,
    "ctgov_search":         T.ctgov_search,
    "opentargets.search":   T.ot_associated_diseases,
    "opentargets.tractability": T.ot_tractability,
    "opentargets.safety":   T.ot_safety,
    "umls.concept_lookup":  T.umls_concept_lookup,
    "umls.get_related":     T.umls_get_related,
    "oncology.path_query":  T.oncology_path_query,
}

# demo_agent.py  顶部 import 之后加
from copy import deepcopy

def ensure_function_wrapping(schemas: list[dict]):
    """确保每个 schema 都包含 {"type":"function", "function": ...} 结构"""
    new_list = []
    for sch in schemas:
        if "type" in sch:              # 已是新格式
            new_list.append(sch)
        else:                          # 旧格式 → 包装
            new_list.append({
                "type": "function",
                "function": deepcopy(sch)
            })
    return new_list

ALL_SCHEMAS = ensure_function_wrapping(ALL_SCHEMAS)   # ← 覆盖原变量

def sanitise_schemas(schemas):
    safe = []
    for sch in schemas:
        fn = deepcopy(sch["function"] if "function" in sch else sch)
        fn["name"] = fn["name"].replace(".", "_")
        assert re.match(r"^[a-zA-Z0-9_-]+$", fn["name"])
        safe.append({"type": "function", "function": fn})
    return safe

ALL_SCHEMAS_SAFE = sanitise_schemas(ALL_SCHEMAS)
def chat_once(
        user_text: str,
        model_name: Optional[str] = None      # ← 替换掉  str | None
    ) -> str:

    if model_name is None:
        model_name = os.getenv("YOUR_DEPLOYMENT")

    messages = [
        {"role": "system", "content": "You are an assistant."},
        {"role": "user",   "content": user_text}
    ]

    while True:
        rsp = client.chat.completions.create(
            model=model_name,          # ← 动态部署名
            messages=messages,
            tools=ALL_SCHEMAS,
            tool_choice="auto"
        )
        msg = rsp.choices[0].message
        if msg.tool_calls:                 # 有工具调用：执行并回传
            for tc in msg.tool_calls:
                fn   = TOOLS[tc.function.name]
                args = json.loads(tc.function.arguments)

                result = fn(**args)
                messages.append(msg)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "name": tc.function.name,
                    "content": json.dumps(result, ensure_ascii=False)
                })
            continue                       # 再让模型整合结果
        else:
            return msg.content.strip()


if __name__ == "__main__":
    print(chat_once("Which PMIDs match “A rare case of rectal malignant melanoma with long-term survival ...”?"))
