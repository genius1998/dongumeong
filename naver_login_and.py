import requests
import datetime
import time
import pandas as pd
import re

from getpass import getpass  # 비밀번호 안 보이게 입력받고 싶으면 사용

# ===== Selenium 관련 =====
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ==============================
# 0) 전역 설정 (쿠키는 나중에 채움)
# ==============================
COOKIE_STRING = ""  # 나중에 Selenium 로그인 후 채움

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://mail.naver.com",
    "Referer": "https://mail.naver.com/v2/folders/-1/search",
    "Cache-Control": "no-cache",
    "charset": "utf-8",
    # "Cookie": COOKIE_STRING  # ← 나중에 main()에서 동적으로 넣음
}

BASE_URL = "https://mail.naver.com/json/search/"

TAG_RE = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    if not isinstance(text, str):
        return text
    return TAG_RE.sub("", text)


# ==============================
# 1) Selenium으로 로그인 + 쿠키 얻기
# ==============================
def get_naver_cookie_string_with_selenium(naver_id: str, naver_pw: str) -> str:
    """
    1) 네이버 로그인 페이지 접속
       https://nid.naver.com/nidlogin.login?mode=form&url=https://mail.naver.com/
    2) 아이디/비밀번호 입력 후 Enter로 로그인
    3) 로그인 성공 후 mail.naver.com 접속
    4) driver.get_cookies()로 쿠키 전부 가져와서 "name=value; ..." 형태 문자열로 반환
    """
    options = webdriver.ChromeOptions()
    # 디버깅하면서 브라우저 보고 싶으면 주석 처리 유지 (headless 안씀)
    # options.add_argument("--headless=new")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    try:
        login_url = "https://nid.naver.com/nidlogin.login?mode=form&url=https://mail.naver.com/"
        driver.get(login_url)

        wait = WebDriverWait(driver, 20)

        # 아이디/비밀번호 입력창 대기
        id_input = wait.until(EC.presence_of_element_located((By.ID, "id")))
        pw_input = driver.find_element(By.ID, "pw")

        # 입력
        id_input.clear()
        id_input.send_keys(naver_id)

        pw_input.clear()
        pw_input.send_keys(naver_pw)
        pw_input.send_keys(Keys.ENTER)  # 엔터로 로그인

        # 로그인 성공해서 mail.naver.com 으로 넘어갈 때까지 대기
        wait.until(EC.url_contains("naver.com"))

        # 혹시 바로 메일 안 뜨면 강제로 메일 페이지 한 번 접속
        driver.get("https://mail.naver.com")
        wait.until(EC.url_contains("mail.naver.com"))

        # 여기까지 왔으면 로그인 성공 + 메일 쿠키 세팅 완료
        cookies = driver.get_cookies()  # [{'name': 'NID_SES', 'value': '...'}, ...]

        cookie_string = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        return cookie_string

    finally:
        # 브라우저 닫기
        driver.quit()


# ==============================
# 2) 네이버 메일 검색 API 호출
# ==============================
def fetch_page(page: int, keyword: str = "결제") -> dict:
    """
    네이버 메일 검색 페이지 호출
    keyword = 검색어 (제목+본문 등)
    """
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
        "u": "effort98",      # 네이버 메일 아이디(앞부분)
        "previewMode": 1,
        "useSearchHistory": "true",
    }

    resp = requests.post(BASE_URL, headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()

    if data.get("Result") != "OK":
        print("⚠ Result:", data.get("Result"), "Message:", data.get("Message"))

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


# ==============================
# 3) 메인 로직
# ==============================
def main():
    # 1) 네이버 아이디/비밀번호 입력
    naver_id = input("네이버 아이디를 입력하세요 (예: effort98): ").strip()
    # 비밀번호 안 보이게 입력하고 싶으면 getpass 사용
    naver_pw = getpass("네이버 비밀번호를 입력하세요: ").strip()
    # 그냥 input 쓰고 싶으면:
    # naver_pw = input("네이버 비밀번호를 입력하세요: ").strip()

    print("[0] 셀레니움으로 네이버 로그인 중... (브라우저 창이 뜰 수 있음)")
    global COOKIE_STRING, HEADERS
    COOKIE_STRING = get_naver_cookie_string_with_selenium(naver_id, naver_pw)

    HEADERS["Cookie"] = COOKIE_STRING
    time.sleep(1000)
    keyword = "결제"  # 검색어 바꾸고 싶으면 여기만 수정

    print("[1] 1페이지 먼저 호출해서 lastPage 확인 중...")
    first_data = fetch_page(1, keyword=keyword)

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
