import os
import google.generativeai as genai
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    raise ValueError("GEMINI_API_KEY 환경 변수를 설정해주세요.")

genai.configure(api_key=API_KEY)

# ✅ 1.5 / pro 말고 2.0 계열로 변경
model = genai.GenerativeModel("gemini-2.0-flash-lite")

res = model.generate_content("안녕!")
print(res.text)
