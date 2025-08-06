# prefix_completion_test.py
# prefix_completion_test_100.py
import os, json, difflib, argparse
from collections import defaultdict
from pathlib import Path

import tiktoken
import pandas as pd
from tqdm import tqdm              # 进度条
from rouge_score import rouge_scorer
from openai import AzureOpenAI
os.environ["OPENAI_API_KEY"] = 'sk-4jnd9yjoIXnQRQ5SXR2b3bVO1d3sHtuyegGMzAl6awSWDRNn' 
os.environ['OPENAI_BASE_URL'] = 'https://api2.aigcbest.top/v1' 

# os.environ["OPENAI_API_KEY"] = 'sk-OlimLcefr3MBSt08IrcZ9LrhP94qqni4w3u4qkOPFtAULcDD' 
# os.environ['OPENAI_BASE_URL'] = 'https://api.chatanywhere.tech' 

os.environ["AZURE_OPENAI_API_KEY"] = "5a1437f6ff2648b9b969507fb5a73276"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://ai-mistraleastus2753718354821.openai.azure.com/"
# ---------- 参数 ----------
JSON_PATH      = "../dataset/MAIA.json"   # ← 按需修改
MODEL_NAME     = "gpt-4.1-noah"
PREFIX_RATIO   = 0.5
MAX_TOKENS_GEN = 128

# ---------- 编码与分段 ----------
enc = tiktoken.encoding_for_model("gpt-4o-mini")
def prefix_by_ratio(text, ratio=PREFIX_RATIO):
    toks = enc.encode(text)
    k = max(1, int(len(toks) * ratio))
    return enc.decode(toks[:k]), enc.decode(toks[k:])

# ---------- Azure OpenAI 客户端 ----------
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

def complete(prefix: str) -> str:
    prompt = f"继续补全下面医学问题的剩余部分，仅输出补全内容，不要重复前缀：\n\n{prefix}"
    rsp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_TOKENS_GEN,
        temperature=0.0)
    return rsp.choices[0].message.content.strip()

# ---------- 读取数据 ----------
with open(JSON_PATH, encoding="utf-8") as f:
    dataset = json.load(f)["dataset"]

# ---------- 迭代 100 条并统计 ----------
scorer  = rouge_scorer.RougeScorer(["rougeL"], use_stemmer=True)
results = defaultdict(list)

for item in tqdm(dataset, desc="Prefix-completion test"):
    qid   = item["id"]
    full  = item["question"]
    pref, gold = prefix_by_ratio(full)

    try:
        gen = complete(pref)
    except Exception as e:
        print(f"[{qid}] API error → {e}")
        continue

    rougeL = scorer.score(gold, gen)["rougeL"].fmeasure
    lev    = difflib.SequenceMatcher(None, gold, gen).ratio()

    results["qid"].append(qid)
    results["rougeL"].append(rougeL)
    results["levsim"].append(lev)

    tqdm.write(f"{qid:>18} | rougeL={rougeL:.3f} | lev_sim={lev:.3f}")

# ---------- 汇总输出 ----------
df = pd.DataFrame(results)
print("\n=== 统计（前 100 条）===")
print(df.describe())