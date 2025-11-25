from __future__ import print_function
import os.path
import base64

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Gmail 읽기 전용 권한
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def get_plain_text_from_message(msg_detail):
    """
    MIME 구조에서 텍스트 본문만 깔끔하게 뽑아내는 헬퍼 함수
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


def main():
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

    # 최근 메일 5개만 가져와보자
    results = (
        service.users()
        .messages()
        .list(userId="me", maxResults=5)
        .execute()
    )
    messages = results.get("messages", [])

    if not messages:
        print("메일이 없습니다.")
        return

    print("최근 메일 5개:")

    for msg in messages:
        msg_detail = (
            service.users()
            .messages()
            .get(userId="me", id=msg["id"], format="full")
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

        body_text = get_plain_text_from_message(msg_detail)

        print("=" * 60)
        print("제목 :", subject)
        print("보낸 사람 :", from_addr)
        print("본문 일부 :")
        # 너무 길면 앞쪽 300자만 잘라보기
        print(body_text[:300].replace("\n", " ") + "...")
        print()

if __name__ == "__main__":
    main()
