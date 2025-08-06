import os
import json
import time
import random
import tqdm
from pathlib import Path
from openai import AzureOpenAI
from collections import defaultdict
import re

# Azure OpenAI设置
os.environ["AZURE_OPENAI_API_KEY"] = "5a1437f6ff2648b9b969507fb5a73276"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://ai-mistraleastus2753718354821.openai.azure.com/"

# 初始化Azure OpenAI客户端
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

# Configuration parameters
MODEL = "gpt-4.1-noah"  # Using advanced model to ensure high-quality questions
TEMPERATURE = 0.7       # Increased temperature for diversity
RATE_LIMIT_S = 1.2      # Request interval to avoid API rate limits
# Load merged_paths.json file
def load_paths_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        paths_data = json.load(f)
    return paths_data

# Path and output files
paths_file = Path("/home/xinding/dingxin/Agent/MAIA/code/merged_paths.json")
output_file = Path("/home/xinding/dingxin/Agent/MAIA/code/umls_qa_pairs.json")  # English version

# Load path data
paths_data = load_paths_data(paths_file)
print(f"Loaded {sum(len(paths) for paths in paths_data.values())} paths")

# View data structure
template_ids = list(paths_data.keys())
print(f"Template types: {template_ids}")

# Randomly select a path to view its structure
template = random.choice(template_ids)
if paths_data[template]:
    sample_path = paths_data[template][0]
    print(f"\nSample path (Template: {template}):")
    if 'path_strs' in sample_path:
        print(f"path_strs: {sample_path['path_strs']}")
    else:
        print("Path structure:", sample_path.keys())
else:
    print(f"Template {template} has no available paths")
def filter_valid_paths(paths_data, min_path_length=3):
    """Filter valid paths - keep only those with complete structure and sufficient length"""
    valid_paths = {}
    
    for template_id, paths in paths_data.items():
        valid_template_paths = []
        
        for path in paths:
            # 检查路径是否包含必要的字段并且长度足够
            if ('path_strs' in path and 
                isinstance(path['path_strs'], list) and 
                len(path['path_strs']) >= min_path_length):
                valid_template_paths.append(path)
        
        if valid_template_paths:
            valid_paths[template_id] = valid_template_paths
    
    return valid_paths

# 过滤有效路径
valid_paths = filter_valid_paths(paths_data)
print(f"过滤后共有 {sum(len(paths) for paths in valid_paths.values())} 条有效路径")
for template_id, paths in valid_paths.items():
    print(f"模板 {template_id}: {len(paths)} 条有效路径")
def create_qa_prompt(path_info, template_id):
    """Create a high-quality English prompt for generating complex medical Q-A pairs"""

    path_strs = path_info["path_strs"]
    template_map = {
        "Disease_Drug_Target": "Disease → Drug → Target",
        "Disease_Drug_moA":    "Disease → Drug → Mechanism-of-Action"
    }
    template_desc = template_map.get(template_id, template_id)

    prompt = f"""
You are a senior medical educator who must craft challenging Q&A pairs from UMLS multi-hop reasoning paths.

Path type: {template_desc}
UMLS path: {' -> '.join(path_strs)}

**Instructions**

1. Write a question that requires **multi-step professional reasoning** along this path.
2. You may reveal **at most one or two** intermediate concepts as clues, but do NOT expose the full path.
3. Frame the question in a realistic clinical, pharmacological, or research scenario—professional and concise, not overly detailed.
4. The answer should normally be the terminal entity **\"{path_strs[-1]}\"**; if another node is more clinically sensible, use it and explain why.
5. After the entity, add a **succinct (≤ 40 words) clinical/pharmacological rationale** introduced by a semicolon, so evaluators can easily judge correctness.
6. Provide a short (20–40 words) “reasoning_path” summary showing the key medical logic without disclosing every node.
7. Output **only** the following JSON structure—no extra text:

{{
  "question": "...",
  "answer": "<Entity>; <≤40-word rationale>",
  "reasoning_path": "<20–40-word reasoning summary>",
  "umls_path": {path_strs},
  "template_id": "{template_id}"
}}
"""
    return prompt
def generate_qa_pair(path_info, template_id):
    """Use OpenAI to generate Q&A pairs"""
    prompt = create_qa_prompt(path_info, template_id)
    
    try:
        response = client.chat.completions.create(
            model=MODEL,
            temperature=TEMPERATURE,
            messages=[
                {"role": "system", "content": "You are a medical education expert specializing in creating high-quality medical reasoning questions in English."},
                {"role": "user", "content": prompt}
            ]
        )
        
        result = response.choices[0].message.content.strip()
        
        # 尝试解析返回的JSON
        try:
            # 查找JSON部分 (可能会有额外的文本)
            json_match = re.search(r'({[\s\S]*})', result)
            if json_match:
                result = json_match.group(1)
            
            qa_pair = json.loads(result)
            
            # Ensure required fields are present
            if "question" not in qa_pair or "answer" not in qa_pair:
                return None
                
            # Add original path data and metadata
            qa_pair["umls_path"] = path_info["path_strs"]
            qa_pair["template_id"] = template_id
            
            return qa_pair
            
        except json.JSONDecodeError:
            print(f"Failed to parse JSON: {result[:100]}...")
            return None
            
    except Exception as e:
        print(f"Error generating Q&A pair: {e}")
        return None


# Define the number of samples per template (now this is no longer needed, as we generate for all paths)
# samples_per_template = 50  # This is no longer needed
output_qa_pairs = []

# Iterate through all the templates and their associated paths
for template_id, paths in valid_paths.items():
    print(f"Generating Q&A pairs for template '{template_id}'...")
    
    # Iterate through all paths for this template (no random sampling)
    for path in tqdm.tqdm(paths, desc=f"Template: {template_id}"):
        qa_pair = generate_qa_pair(path, template_id)
        
        if qa_pair:
            output_qa_pairs.append(qa_pair)
            # Add a short delay to avoid API rate limits
            time.sleep(RATE_LIMIT_S)
            
            # Save intermediate results every 10 samples
            if len(output_qa_pairs) % 10 == 0:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(output_qa_pairs, f, ensure_ascii=False, indent=2)
                print(f"Saved {len(output_qa_pairs)} Q&A pairs")

# Save final results
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(output_qa_pairs, f, ensure_ascii=False, indent=2)
print(f"Finished generating and saving {len(output_qa_pairs)} Q&A pairs.")
