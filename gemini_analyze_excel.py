import json
import time
import pandas as pd
import google.generativeai as genai
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# ✅ Gemini API 키 (환경 변수에서 가져오기)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

MODEL_NAME = "gemini-2.0-flash"

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY 환경 변수를 설정해주세요.")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(MODEL_NAME)

ANALYSIS_SYSTEM_PROMPT = """
당신은 이메일에서 '구독/정기결제/뉴스레터/멤버십' 정보를 추출하는 분석기입니다.

입력으로 이메일 전체 텍스트가 주어집니다.
이메일이 구독/정기결제/뉴스레터/멤버십과 무관하다면 다음과 같이 정확히 응답하세요:

{"is_subscription": false}

만약 관련이 있다면, 아래 JSON 포맷으로만 응답하세요(설명 X, 코드블럭 X):

{
  "is_subscription": true,
  "service_name": "서비스나 브랜드 이름",
  "plan_name": "요금제/플랜 이름 또는 null",
  "price": "금액 문자열 또는 null",
  "currency": "KRW, USD 등 혹은 null",
  "billing_cycle": "monthly / yearly / weekly / once / unknown",
  "start_date": "YYYY-MM-DD 형식 또는 null",
  "next_billing_date": "YYYY-MM-DD 형식 또는 null",
  "unsubscribe_link": "있다면 링크, 없다면 null",
  "category": "streaming / news / shopping / cloud / app / other",
  "raw_summary": "이메일에서 파악되는 구독 내용 한 줄 요약"
}
"""


def analyze_email_with_gemini(subject: str, sender: str, body: str):
    """
    Gemini로 메일 내용을 분석해서 구독정보 JSON(dict) 반환.
    구독 메일이 아니면 None 리턴.
    """
    if not body.strip():
        return None

    prompt = f"""
이메일 제목: {subject}
보낸 사람: {sender}
본문:
{body}
"""

    try:
        response = model.generate_content(
            ANALYSIS_SYSTEM_PROMPT + "\n\n### 이메일 내용\n" + prompt
        )
        text = response.text.strip()

        # 혹시 ```json ...``` 이런 식으로 나오면 정리
        text = text.strip("`").strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()

        data = json.loads(text)

        if not isinstance(data, dict):
            return None

        if not data.get("is_subscription"):
            return None

        return data

    except Exception as e:
        print("❗ Gemini 분석 중 오류:", e)
        return None

def main():
    if not GEMINI_API_KEY:
        print("❗ GEMINI_API_KEY를 코드 안에 넣어줘야 합니다.")
        return

    # 1) 네이버 메일 검색 결과 엑셀 읽기
    input_filename = "naver_mail_search_결제_20251124.xlsx"  # 파일명 맞게 수정
    if not os.path.exists(input_filename):
        print(f"❗ 엑셀 파일을 찾을 수 없습니다: {input_filename}")
        return

    df = pd.read_excel(input_filename)

    print("📊 엑셀에서 메일 데이터 로드 완료. 행 개수:", len(df))

    rows = []

    for idx, row in df.iterrows():
        subject = str(row.get("subject", "(제목 없음)"))
        from_name = str(row.get("from_name", "")) or "(보낸이 없음)"
        from_email = str(row.get("from_email", "")) or ""
        preview = str(row.get("preview", ""))
        body_snippet = str(row.get("body_snippet", ""))
        received_time = str(row.get("receivedTime", ""))

        sender = f"{from_name} <{from_email}>" if from_email else from_name

        # 네이버 메일 한 통을 하나의 텍스트로 구성
        body_text = f"""
[요약 정보]
제목: {subject}
보낸 사람: {sender}
수신 시각: {received_time}

[미리보기]
{preview}

[본문 일부]
{body_snippet}
"""

        print(f"[{idx+1}/{len(df)}] 분석 중: {subject[:60]}")

        analysis = analyze_email_with_gemini(
            subject=subject,
            sender=sender,
            body=body_text,
        )

        # 속도/요금 부담 줄이려면 슬립 조금 줘도 좋음
        time.sleep(1.5)

        # 구독과 무관하면 스킵
        if not analysis:
            continue

        # 원본 메일 정보 + 분석 결과 합쳐서 저장
        rows.append({
            "mailSN": row.get("mailSN"),
            "folderSN": row.get("folderSN"),
            "from_name": from_name,
            "from_email": from_email,
            "subject": subject,
            "receivedTime": received_time,

            "service_name": analysis.get("service_name"),
            "plan_name": analysis.get("plan_name"),
            "price": analysis.get("price"),
            "currency": analysis.get("currency"),
            "billing_cycle": analysis.get("billing_cycle"),
            "start_date": analysis.get("start_date"),
            "next_billing_date": analysis.get("next_billing_date"),
            "unsubscribe_link": analysis.get("unsubscribe_link"),
            "category": analysis.get("category"),
            "raw_summary": analysis.get("raw_summary"),
        })

    if not rows:
        print("✅ 구독/정기결제 관련으로 판단된 메일이 없습니다.")
        return

    result_df = pd.DataFrame(rows)
    output_filename = "naver_mail_subscriptions_결제_20251124.xlsx"
    result_df.to_excel(output_filename, index=False, engine="openpyxl")

    print(f"✅ 엑셀 저장 완료: {output_filename}")
    print("총 구독 관련 메일 수:", len(result_df))


if __name__ == "__main__":
    import os
    main()


