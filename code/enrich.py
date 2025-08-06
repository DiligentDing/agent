# ▶️ 在运行本单元前，请先 export 两个环境变量（或直接写死）
#   AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT
import os, json, textwrap, time
from openai import AzureOpenAI      # pip install openai>=1.30.3

# Azure OpenAI API configuration
os.environ["AZURE_OPENAI_API_KEY"] = "5a1437f6ff2648b9b969507fb5a73276"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://ai-mistraleastus2753718354821.openai.azure.com/"
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)
SYSTEM_MSG = """
You are a clinical pharmacologist who rewrites multiple-choice Q-and-A items for an educational dataset.

For every original entry you receive:
- Turn the stem into a realistic, anonymised clinical vignette (omit the target answer term).
- Design one single-best-answer question that requires readers to follow the full mechanistic pathway you describe.
- Provide a 150-250-word explanation (“reasoning”) that walks through the clinical and molecular logic.
- Return a *minified* JSON object with the keys: question, answer, reasoning, reasoning_path.

Stay factual, concise, and comply with professional and ethical standards.
""".strip()



# —— Few-shot 示例；一问一答 —— #
FEWSHOT_ORIG = {
  "question": "A male patient develops painful breast enlargement; his physician prescribes a selective estrogen receptor modulator. Which molecular target is primarily involved in mediating this drug's therapeutic effect in this condition?",
  "answer": "Estrogen Receptor; Tamoxifen alleviates gynecomastia by antagonizing estrogen receptors, thereby blocking estrogen-mediated breast tissue proliferation.",
  "reasoning_path": "Gynecomastia is treated with tamoxifen, a selective estrogen receptor modulator, whose therapeutic effect is mediated by antagonism of estrogen receptors in breast tissue."
}
FEWSHOT_REWRITE = {
  "question": "A 16-year-old male presents with a three-month history of a tender, rubbery, subareolar mass on the left chest. He is otherwise healthy, takes no medications, and has no signs of chronic illness. Physical examination reveals a 2-cm concentric, mobile mass beneath the areola with mild tenderness. After evaluation, his clinician recommends a medication that is not FDA-approved for this indication but is known to act as a competitive antagonist at a nuclear hormone receptor, blocking ligand-induced transcription of proliferative genes in breast tissue. Which pharmacologic agent is most likely to reduce the size of his palpable mass and alleviate symptoms, and what is the molecular basis for its effectiveness in this setting?",
  "answer": "Tamoxifen—competitive antagonism of the estrogen receptor in breast tissue",
  "reasoning": "1. Pubertal gynecomastia arises from estrogen-androgen imbalance (CUI:C0014936)... ",
    "reasoning_path": "Tamoxifen counters estrogen-driven proliferation via competitive ER blockade."
}

# —— 把“原始条目”模板化塞进 user message —— #
USER_TEMPLATE = (
    "ORIGINAL ENTRY (JSON):\n```json\n{entry_json}\n```\n\n"
    "Please rewrite this entry according to **all** rules stated above."
)
from openai import BadRequestError

def rewrite_entry(entry: dict,
                  model: str = "gpt-4.1-noah",
                  temperature: float = 0.7,
                  max_attempts: int = 3) -> dict:
    """Call Azure OpenAI to rewrite one item; if still failing, return original entry."""
    base_messages = [
        {"role": "system",    "content": SYSTEM_MSG},
        {"role": "user",      "content": json.dumps(FEWSHOT_ORIG, ensure_ascii=False)},
        {"role": "assistant", "content": json.dumps(FEWSHOT_REWRITE, ensure_ascii=False)},
    ]
    user_msg = USER_TEMPLATE.format(entry_json=json.dumps(entry, ensure_ascii=False))

    for attempt in range(1, max_attempts + 1):
        try:
            resp = client.chat.completions.create(
                model       = model,
                temperature = temperature,
                messages    = base_messages + [{"role": "user", "content": user_msg}],
                response_format = {"type": "json_object"},  # 强制 JSON 输出（2024-12-01-preview 支持）
                timeout     = 60,
            )
            new_fields = json.loads(resp.choices[0].message.content)
            assert all(k in new_fields for k in ("question", "answer", "reasoning", "reasoning_path"))
            return {**entry, **new_fields}

        except BadRequestError as e:
            print(f"[attempt {attempt}] HTTP 400 {e}")
        except Exception as e:
            print(f"[attempt {attempt}] {type(e).__name__}: {e}")

        time.sleep(2 * attempt)

    # — 连续失败：保留原条目并记日志 —
    print(f"⚠️  Skip id={entry.get('id')} after {max_attempts} failed attempts.")
    return entry | {"rewrite_error": True}

# 假设 data 原始结构是 {"dataset": [ {...}, {...} ]}
IN_FILE  = "umls_qa.json"
OUT_FILE = "umls_qa_rewritten.json"

from tqdm import tqdm
import json

with open(IN_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

rewritten_items = []

# 使用 tqdm 包装 enumerate，显示进度条
for i, item in enumerate(tqdm(data["dataset"], desc="Rewriting items"), 1):
    # 可选：在 tqdm 进度条上显示额外信息（如 ID）
    rewritten_items.append(rewrite_entry(item))

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump({"dataset": rewritten_items}, f, ensure_ascii=False, indent=2)

print("✅  完成！结果保存在:", OUT_FILE)
