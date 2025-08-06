import requests
import json

def call_slot_fill_tool(prompt, tool, language="en"):
    """
    Question -> query parameters, used to query Noah AI database, text in json out
    Expected time to run: < 15s
    Output: {'result': 'success', 'data': <dict>}
    Available tools include:
    [
        "General-Inference",
        "Medical-Search",
        "Web-Search",
        "Clinical-Trial-Result-Analysis",
        "Drug-Analysis",
        "Catalyst-Event-Analysis"
    """
    noah_api_url = f"https://staging.noahai.co/api/tool_test/"
    body = {"language":"en", "user_prompt":prompt, "tool":tool, "slot_fill":True}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Token ab2af44c17490f0c3c3b221b0f6fc2c20d62590a"}
    response = requests.post(noah_api_url, data=json.dumps(body), headers=headers, timeout=30, allow_redirects=True, verify=False)
    try: ret = response.json()
    except: ret = response.text
    return ret

def call_tool(prompt, tool, language="en"):
    """
    Question -> report, text in text out
    Expected time to run: ~2min
    Output: {'result': 'success', 'data': <txt>}
    Available tools include:
    [
        "General-Inference",
        "Medical-Search",
        "Web-Search",
        "Clinical-Trial-Result-Analysis",
        "Drug-Analysis",
        "Catalyst-Event-Analysis"
    """
    noah_api_url = f"https://staging.noahai.co/api/tool_test/"
    body = {"language": language, "user_prompt":prompt, "tool":tool, "slot_fill":False}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Token ab2af44c17490f0c3c3b221b0f6fc2c20d62590a"}
    response = requests.post(noah_api_url, data=json.dumps(body), headers=headers, timeout=240, allow_redirects=True, verify=False)
    try: ret = response.json()
    except: ret = response.text
    return ret

def call_agent(prompt, language="en"):
    """
    Question -> report, text in text out
    Expected time to run: ~10-12min
    Output: {'result': 'success', 'data': <txt>}
    """
    noah_api_url = f"https://staging.noahai.co/api/tool_test/"
    body = {"language": language, "user_prompt":prompt, "tool": "agent"}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Token ab2af44c17490f0c3c3b221b0f6fc2c20d62590a"}
    response = requests.post(noah_api_url, data=json.dumps(body), headers=headers, timeout=1200, allow_redirects=True, verify=False)
    try: ret = response.json()
    except: ret = response.text
    return ret

print(call_slot_fill_tool('减肥药的最新竞争格局', 'Drug-Analysis'))
# print(call_tool('减肥药的最新竞争格局', 'Drug-Analysis'))
# print(call_agent('减肥药的最新竞争格局'))
