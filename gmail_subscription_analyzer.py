from __future__ import print_function
import os.path
import base64
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

import google.generativeai as genai
import pandas as pd

# ---------------------------------------------------
#  설정
# ---------------------------------------------------

import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# Gmail 읽기 전용 권한
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# 최근 6개월 기준 (gmail 검색쿼리용)
NEWER_THAN_DAYS = 180      # 6개월 ≈ 180일
MAX_EMAILS = 300           # 너무 많으면 상한선

# ✅ Gemini API 키 (환경 변수에서 가져오기)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Gemini 모델 (이걸로 테스트 성공했었지?)
MODEL_NAME = "gemini-2.0-flash"

# API 키가 있을 경우에만 genai 구성
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("❗ GEMINI_API_KEY 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")

model = genai.GenerativeModel(MODEL_NAME)

# ---------------------------------------------------
#  이메일 본문 파싱 함수
# ---------------------------------------------------
def get_plain_text_from_message(msg_detail):
    """
    MIME 구조에서 텍스트 본문만 깔끔하게 뽑아내는 함수
    """
    payload = msg_detail.get("payload", {})
    mime_type = payload.get("mimeType", "")
    body = payload.get("body", {})
    parts = payload.get("parts", [])

    # 1) 메일이 text/plain 한 덩어리일 때
    if mime_type == "text/plain" and "data" in body:
        data = body["data"]
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")

    # 2) multipart/alternative, multipart/mixed 등 파트가 나뉜 메일일 때
    text_parts = []

    def walk_parts(parts_list):
        for part in parts_list:
            part_mime = part.get("mimeType", "")
            part_body = part.get("body", {})
            sub_parts = part.get("parts", [])

            # 다시 하위 파트 있으면 재귀
            if sub_parts:
                walk_parts(sub_parts)

            # text/plain 파트만 모으기
            if part_mime == "text/plain" and "data" in part_body:
                data = part_body["data"]
                text = base64.urlsafe_b64decode(data).decode(
                    "utf-8", errors="ignore"
                )
                text_parts.append(text)

    if parts:
        walk_parts(parts)

    if text_parts:
        return "\n".join(text_parts)

    # 3) 못 찾으면 빈 문자열
    return ""


# ---------------------------------------------------
#  Gemini 프롬프트 (구독/정기결제/뉴스레터 분석)
# ---------------------------------------------------
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

        # 혹시 코드블럭, 'json' 이런거 붙어 나오면 제거
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


# ---------------------------------------------------
#  Gmail API 인증/클라이언트 생성
# ---------------------------------------------------
def get_gmail_service():
    creds = None

    # 이전에 로그인해둔 토큰이 있으면 token.json에서 불러오기
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # 토큰이 없거나 만료되었으면 새로 로그인 플로우 실행
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # 다음 실행을 위해 token.json에 저장
        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    # Gmail API 클라이언트 생성
    service = build("gmail", "v1", credentials=creds)
    return service


# ---------------------------------------------------
#  최근 6개월 메일 ID 목록 가져오기
# ---------------------------------------------------
def fetch_recent_messages(service):
    """
    Gmail API로 최근 NEWER_THAN_DAYS 일 메일 ID들 가져오기
    """
    q = f"newer_than:{NEWER_THAN_DAYS}d"
    messages = []

    request = service.users().messages().list(
        userId="me",
        q=q,
        maxResults=100,
    )

    while request is not None:
        response = request.execute()
        batch = response.get("messages", [])
        messages.extend(batch)

        if len(messages) >= MAX_EMAILS:
            break

        request = service.users().messages().list_next(request, response)

    return messages[:MAX_EMAILS]

import time
# ---------------------------------------------------
#  메인: 메일 → Gemini 분석 → 엑셀 저장
# ---------------------------------------------------
def main():
    if not GEMINI_API_KEY:
        print("❗ GEMINI_API_KEY를 코드 안에 넣어줘야 합니다.")
        return

    print("📥 Gmail 서비스 초기화 중...")
    service = get_gmail_service()

    print(f"📬 최근 {NEWER_THAN_DAYS}일(약 6개월) 메일 목록 가져오는 중...")
    msg_list = fetch_recent_messages(service)
    print("가져온 메일 수:", len(msg_list))

    if not msg_list:
        print("메일이 없습니다.")
        return

    rows = []

    for idx, msg in enumerate(msg_list, start=1):
        msg_id = msg["id"]

        msg_detail = (
            service.users()
            .messages()
            .get(userId="me", id=msg_id, format="full")
            .execute()
        )

        headers = msg_detail["payload"]["headers"]

        subject = next(
            (h["value"] for h in headers if h["name"] == "Subject"),
            "(제목 없음)",
        )
        from_addr = next(
            (h["value"] for h in headers if h["name"] == "From"),
            "(발신자 없음)",
        )
        date_raw = next(
            (h["value"] for h in headers if h["name"] == "Date"),
            "(날짜 없음)",
        )

        body_text = get_plain_text_from_message(msg_detail)

        print(f"[{idx}/{len(msg_list)}] 분석 중: {subject[:60]}")

        analysis = analyze_email_with_gemini(
            subject=subject,
            sender=from_addr,
            body=body_text,
        )
        time.sleep(2)

        # 구독과 무관하면 스킵
        if not analysis:
            continue

        row = {
            "email_subject": subject,
            "email_from": from_addr,
            "email_date_raw": date_raw,
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
        }

        rows.append(row)

    if not rows:
        print("✅ 최근 6개월 동안 구독/정기결제 관련 메일 분석 결과가 없습니다.")
        return

    df = pd.DataFrame(rows)
    output_filename = "gmail_subscriptions_last_6_months.xlsx"
    df.to_excel(output_filename, index=False)
    print(f"✅ 엑셀 저장 완료: {output_filename}")


if __name__ == "__main__":
    main()
