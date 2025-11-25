from __future__ import annotations
import os
import time
import json
from dotenv import load_dotenv

import pandas as pd
import google.generativeai as genai

# ============================================
# 1. Gemini 설정
# ============================================
load_dotenv()

# ✅ Gemini API 키 (환경 변수에서 가져오기)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 사용하는 Gemini 모델 이름
MODEL_NAME = "gemini-2.0-flash-lite"

if not GEMINI_API_KEY:
    print("❗ GEMINI_API_KEY를 코드 안에 넣어줘야 합니다.")
else:
    genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel(MODEL_NAME)

# ============================================
# 2. 배치 분석용 프롬프트
# ============================================

BATCH_PROMPT = """
당신은 이메일에서 '구독/정기결제/뉴스레터/멤버십' 정보를 추출하는 분석기입니다.

입력으로 여러 개의 이메일 목록이 주어집니다.
각 이메일은 JSON 한 줄로 표현되며, 형식은 다음과 같습니다.

{"id": 1, "subject": "...", "sender": "...", "body": "..."}

당신의 할 일:
1) 각 이메일이 구독/정기결제/뉴스레터/멤버십 관련인지 판별
2) 관련 있는 것만 아래 형식의 JSON 객체로 변환
3) 최종적으로 JSON 배열로만 출력 (설명 X, 코드블럭 X)

형식:

[
  {
    "id": 1,
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
  },
  ...
]

구독/정기결제와 전혀 무관한 이메일이라면 아예 배열에서 제외하세요.
구독 관련 이메일이 하나도 없다면 빈 배열 []만 출력하세요.
"""


# ============================================
# 3. Gemini 배치 분석 함수
# ============================================

def analyze_emails_batch_with_gemini(email_items):
    """
    email_items: [
      {"id": 1, "subject": "...", "sender": "...", "body": "..."},
      ...
    ]

    반환:
      { id: 분석결과(dict), ... }
      - 구독 관련이 아닌 건 자동으로 제외됨
    """
    if not email_items:
        return {}

    # 이메일들을 한 줄짜리 JSON로 이어붙임
    lines = []
    for item in email_items:
        body = item["body"] or ""
        # 토큰 아끼려고 너무 길면 자르기 (필요시 숫자 조정 가능)
        if len(body) > 3000:
            body = body[:3000]

        obj = {
            "id": item["id"],
            "subject": item["subject"],
            "sender": item["sender"],
            "body": body,
        }
        lines.append(json.dumps(obj, ensure_ascii=False))

    joined = "\n".join(lines)

    prompt = BATCH_PROMPT + "\n\n### 이메일 목록\n" + joined

    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()

        # 혹시 ```json ...``` 이런 식으로 나오면 정리
        text = text.strip("`").strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()

        arr = json.loads(text)

        if not isinstance(arr, list):
            print("⚠ 예기치 않은 응답 형식(리스트 아님):", type(arr))
            return {}

        result = {}
        for item in arr:
            if not isinstance(item, dict):
                continue
            _id = item.get("id")
            if _id is None:
                continue
            if not item.get("is_subscription"):
                continue
            result[_id] = item

        return result

    except Exception as e:
        print("❗ 배치 분석 JSON 파싱/요청 중 오류:", e)
        # 실패하면 그냥 빈 결과
        return {}


# ============================================
# 4. 메인: 네이버 메일 엑셀 → Gemini 분석 → 결과 엑셀 저장
# ============================================

def main():
    if not GEMINI_API_KEY:
        print("❗ GEMINI_API_KEY를 코드 안에 넣어줘야 합니다.")
        return

    # ★ 여기 파일명만 맞게 바꾸면 됨
    input_filename = "naver_mail_search_결제_20251124_filtered.xlsx"

    if not os.path.exists(input_filename):
        print(f"❗ 엑셀 파일을 찾을 수 없습니다: {input_filename}")
        return

    # 1) 엑셀 로드
    df = pd.read_excel(input_filename)
    print("📊 엑셀에서 메일 데이터 로드 완료. 행 개수:", len(df))

    # 인덱스를 id로 쓰기 위해 reset_index
    df = df.reset_index().rename(columns={"index": "row_id"})

    # 한 번에 20통씩 배치 분석
    BATCH_SIZE = 20

    all_rows = []

    total_rows = len(df)

    for start in range(0, total_rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_rows)
        chunk = df.iloc[start:end]

        email_items = []
        for _, row in chunk.iterrows():
            row_id = int(row["row_id"])
            subject = str(row.get("subject", "(제목 없음)"))
            from_name = str(row.get("from_name", "")) or "(보낸이 없음)"
            from_email = str(row.get("from_email", "")) or ""
            preview = str(row.get("preview", ""))
            body_snippet = str(row.get("body_snippet", ""))
            received_time = str(row.get("receivedTime", ""))

            sender = f"{from_name} <{from_email}>" if from_email else from_name

            body_text = f"""
[요약 정보]
제목: {subject}
보낸 사람: {sender}
수신 시각: {received_time}

[미리보기]
{preview}

[본문 일부]
{body_snippet}
""".strip()

            email_items.append({
                "id": row_id,
                "subject": subject,
                "sender": sender,
                "body": body_text,
            })

        print(f"🔎 {start+1} ~ {end}번째 메일 배치 분석 중...")

        analysis_map = analyze_emails_batch_with_gemini(email_items)

        # 너무 빡세면 여기서만 살짝 쉬어주자 (필요하면 조절)
        time.sleep(1.0)

        # 분석 결과를 원본 row와 매칭
        for _, row in chunk.iterrows():
            row_id = int(row["row_id"])
            if row_id not in analysis_map:
                continue
            analysis = analysis_map[row_id]

            all_rows.append({
                # 원본 메일 정보
                "mailSN": row.get("mailSN"),
                "folderSN": row.get("folderSN"),
                "from_name": row.get("from_name"),
                "from_email": row.get("from_email"),
                "subject": row.get("subject"),
                "receivedTime": row.get("receivedTime"),

                # Gemini 분석 결과
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

    if not all_rows:
        print("✅ 구독/정기결제 관련 메일이 하나도 없다고 판단됨.")
        return

    result_df = pd.DataFrame(all_rows)

    # 출력 파일명 (원하는 대로 바꿔도 됨)
    output_filename = "naver_mail_search_결제_20251124_filtered.xlsx"
    result_df.to_excel(output_filename, index=False)
    print(f"✅ 엑셀 저장 완료: {output_filename}")
    print("총 구독 관련 메일 수:", len(result_df))


if __name__ == "__main__":
    main()
