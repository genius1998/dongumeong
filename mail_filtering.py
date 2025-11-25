import pandas as pd

# 원본 엑셀 파일 이름
INPUT_FILE = "naver_mail_search_결제_20251124.xlsx"

# 필터링 후 새로 만들 엑셀 파일 이름
OUTPUT_FILE = "naver_mail_search_결제_20251124_filtered.xlsx"


def is_ad_mail(row) -> bool:
    """
    한 행(메일) 기준으로 광고 메일인지 판단하는 함수.
    필요하면 키워드 마음대로 추가/삭제하면 됨.
    """
    # 안전하게 컬럼이 없을 경우도 대비해서 row.get 사용
    subject = str(row.get("subject", "") or "")
    from_name = str(row.get("from_name", "") or "")
    from_email = str(row.get("from_email", "") or "")
    preview = str(row.get("preview", "") or "")
    body_snippet = str(row.get("body_snippet", "") or "")

    # 한 덩어리로 합쳐서 소문자 변환
    full_text = " ".join(
        [subject, from_name, from_email, preview, body_snippet]
    ).lower().strip()

    # 1) 제목/내용이 (광고), [광고] 로 시작하는 경우
    if full_text.startswith("(광고") or full_text.startswith("[광고"):
        return True

    # 2) 광고/프로모션스러운 키워드들
    ad_keywords = [
        "(광고", "[광고",
        "뉴스레터", "newsletter",
        "프로모션", "promotion",
        "특가", "세일", "할인",
        "이벤트", "쿠폰",
        "멤버스데이", "핫딜", "[네이버 웹툰]"
    ]

    if any(kw in full_text for kw in ad_keywords):
        return True

    # 필요하면 여기서 발신자 기반 필터도 추가 가능
    # 예시:
    # blocked_senders = ["jobkorea.co.kr", "bananamall.co.kr", "trip.com"]
    # if any(bs in from_email.lower() for bs in blocked_senders):
    #     return True

    return False


def main():
    print(f"📂 엑셀 로드 중... → {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    print(f"총 행 개수: {len(df)}")

    # 광고 여부 판별
    print("🔎 광고 메일 필터링 중...")
    ad_mask = df.apply(is_ad_mail, axis=1)

    df_ads = df[ad_mask].reset_index(drop=True)
    df_for_gemini = df[~ad_mask].reset_index(drop=True)

    print(f"🧹 광고(필터 아웃) 행 수: {len(df_ads)}")
    print(f"✅ Gemini 분석 대상 행 수: {len(df_for_gemini)}")

    # 새 엑셀 파일로 저장 (시트 2개)
    print(f"💾 새 엑셀로 저장 중... → {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        df_for_gemini.to_excel(writer, sheet_name="for_gemini", index=False)
        df_ads.to_excel(writer, sheet_name="filtered_out", index=False)

    print("🎉 완료! 필터링된 엑셀 파일이 생성되었습니다.")


if __name__ == "__main__":
    main()
