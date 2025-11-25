import requests
import datetime
import time
import pandas as pd
import re

# ==============================
# 1) 네이버 쿠키 (브라우저에서 그대로 긁어와서 넣기)
# ==============================
COOKIE_STRING = """
NAC=94xYB0gvym8I; NNB=ZQNHYE7LZELGS; ASID=01d446030000019a9b2e2a8c0000001b; NACT=1; nid_inf=1437667095; NID_AUT=c4kbr938UMqkGRK1CXJBZLQdPHHpY3DUYovLN8mnA86MG0Hju6aT0+Iqb9naxxTp; NMUSER=uQn/FAEwaqbsKAK9FAEqp6p4WXidax2sKxbrKq2/FxKZKqn9KxUmaqgsaqRJaqt/axvXFAR5+6wnaw/qKARpa9vsyqvs6xRpa9vs6xnstonsarRTBdRLa9vstonsH405pzk/7xt5W4d5W4JrpBU5MreR7A2lKAgsbrkoWrlvMBil7605pzk/7xt5W4d5W4JrpBU5MreR7A2lKAgs; SRT30=1763911447; SRT5=1763925270; NID_SES=AAABlR5Ou2rYD9B6rWX/syl2i0A91mbHvZlryzRX5OlQEXTJTt/WdRakr2tSOe+vG6C9J6QuEswnU8+TJ570PVx6gVNyewbM6h7cT6QJN5keEMgGvZOeS4mkuD/5A93ukXs+mxgYc0fFnZGKY4VTSPNTVGL9cmh3SxXtmDU3zrT+h37fiS1VxCQbD8mf19BwUA4FCoyWunVJZTHABWdeVFWykfWCzX7PfXdN2gjvazca+PhLnteGIVgbLsNiQi7lgcVV08b9SOtjWpRzxZ5P4Wr41OiWZ+qubjjHRlpY5soDb1B4RHpHCRrAhCcusBiIaYo2oMI/udGN7AMLY7SWlrfqnl+VM3xZGHjEx5TvWO34+KqLOa4fwqinQTvqcxY38d2kTH1VnnTnmbdMIO7NQotUYXG0yWRMgDLAcHnU70hxl16CBIwE6yDniecv/Tn4hX3eZbDo9zTrnrQRYxJkifVSqD1X1GJKusZY+6d50YzH5nqRSqy3xg3eZs5euzqJeWYzeo3KLaYIw1dmvL+NDJx2yx6qVPOeP36/bXaYbEfZQaxa; BUC=_EO3PQBEb0wcRPFgHYytL9_mNdh4VgxY3FNGu7bJq9Q=^
""".strip()


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://mail.naver.com",
    "Referer": "https://mail.naver.com/v2/folders/-1/search?body=%EA%B2%B0%EC%A0%9C&bodyCond=0&page=1",
    "Cookie": COOKIE_STRING,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "charset": "utf-8",
    "X-Requested-With": "XMLHttpRequest",
}


BASE_URL = "https://mail.naver.com/json/search/"

TAG_RE = re.compile(r"<[^>]+>")

def strip_html(text: str) -> str:
    if not isinstance(text, str):
        return text
    return TAG_RE.sub("", text)

def fetch_page(page: int, keyword: str = "결제") -> dict:
    params = {
        "page": page,
        "from": "",
        "folderSN": -1,
        "to": "",
        "body": keyword,
        "bodyCond": 0,
        "exceptTrash": "true",
        "content": "",
        "periodStart": "",
        "periodEnd": "",
        "type": "all",
        "u": "effort98",
        "previewMode": 1,
        "useSearchHistory": "true",
    }

    # curl이랑 비슷하게: querystring + POST + 빈 바디
    resp = requests.post(
        BASE_URL,
        headers=HEADERS,
        params=params,
        data=b"",   # content-length: 0
    )

    print("HTTP status:", resp.status_code)
    print("요청 URL:", resp.url)

    # JSON이 아니면 바로 내용 찍고 죽이기
    try:
        data = resp.json()
    except ValueError:
        print("⚠ JSON 아님, 응답 내용 일부:", resp.text[:500])
        raise

    # FAIL이면 이유를 보기 위해 원본도 찍자
    if data.get("Result") != "OK":
        print("RAW JSON:", data)

    return data


def parse_mail_items(mail_data: list, page: int) -> list:
    rows = []
    for item in mail_data:
        received_ts = item.get("receivedTime")
        if received_ts:
            received_dt = datetime.datetime.fromtimestamp(received_ts)
            received_str = received_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            received_str = None

        from_info = item.get("from") or {}

        row = {
            "page": page,
            "mailSN": item.get("mailSN"),
            "folderSN": item.get("folderSN"),
            "from_name": from_info.get("name"),
            "from_email": from_info.get("email"),
            "subject": strip_html(item.get("subject", "")),
            "preview": strip_html(item.get("preview", "")),
            "body_snippet": strip_html(item.get("body", ""))[:200],
            "category": item.get("category"),
            "size": item.get("size"),
            "receivedTime_unix": received_ts,
            "receivedTime": received_str,
        }
        rows.append(row)
    return rows


def main():
    keyword = "결제"  # 검색어 바꾸고 싶으면 여기만 수정

    print("[1] 1페이지 먼저 호출해서 lastPage 확인 중...")
    first_data = fetch_page(1, keyword=keyword)

    # 디버깅용: 혹시 뭐가 나왔는지 한 번 확인
    print("Result:", first_data.get("Result"), "/ Message:", first_data.get("Message"))
    print("terms:", first_data.get("terms"))
    print("totalCount:", first_data.get("totalCount"))
    print("lastPage:", first_data.get("lastPage"))

    last_page = first_data.get("lastPage", 1)
    total_count = first_data.get("totalCount", 0)

    print(f"전체 페이지: {last_page} / 메일 개수: {total_count}")

    all_rows = []
    all_rows.extend(parse_mail_items(first_data.get("mailData", []), page=1))

    for page in range(2, last_page + 1):
        print(f"[2] {page} 페이지 처리 중...")
        try:
            data = fetch_page(page, keyword=keyword)
            rows = parse_mail_items(data.get("mailData", []), page=page)
            all_rows.extend(rows)
        except Exception as e:
            print(f"!! {page} 페이지에서 오류 발생: {e}")
        time.sleep(0.2)

    df = pd.DataFrame(all_rows)

    today_str = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"naver_mail_search_{keyword}_{today_str}.xlsx"

    print(f"[3] 엑셀로 저장 중... → {filename}")
    df.to_excel(filename, index=False)
    print("[완료] 엑셀 파일 생성 끝!")
    print(f"총 {len(df)}행 저장됨")


if __name__ == "__main__":
    main()
