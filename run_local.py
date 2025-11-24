import json
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lambda_function import lambda_handler

# 1) event.json 파일 로드
with open("integrations.json", encoding="utf-8") as f:
    event = json.load(f)

# 2) lambda_handler 실행
print("---- Running Lambda Locally ----")
result = lambda_handler(event, None)

# 3) 결과 출력
print("---- Result ----")
print(json.dumps(result, indent=2, ensure_ascii=False))