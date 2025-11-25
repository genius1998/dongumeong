import pandas as pd
import numpy as np
import datetime as dt
import re

# 입력 / 출력 파일 이름
INPUT_FILE = "is_subscribe.xlsx"
TODAY_STR = dt.date.today().strftime("%Y%m%d")
OUTPUT_FILE = f"is_subscribe_analyzed_{TODAY_STR}.xlsx"

# "진행중" 판단 기준 일수 (원하면 30 → 60/90 등으로 바꿔도 됨)
ACTIVE_THRESHOLD_DAYS = 30


def guess_column(df: pd.DataFrame, candidates):
    """
    주어진 후보 컬럼명 리스트 중에서,
    실제 df에 존재하는 첫 번째 컬럼명을 골라줌.
    없으면 None 리턴.
    """
    cols_lower = {c.lower(): c for c in df.columns}

    for cand in candidates:
        for col_lower, real_col in cols_lower.items():
            if cand.lower() == col_lower:
                return real_col
    return None


_num_pattern = re.compile(r"(\d+(?:\.\d+)?)")


def parse_amount(val):
    """
    '₩29,000/1개월', '29,000원', '156,630원 (즉시할인가 151,630원)' 같은 문자열에서
    제일 앞에 나오는 숫자만 뽑아서 float로 변환.
    숫자 없으면 None.
    """
    if pd.isna(val):
        return None
    s = str(val)
    # 쉼표, 공백 제거는 굳이 안 해도 정규식이 숫자만 잡음
    m = _num_pattern.search(s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def main():
    print(f"📂 엑셀 로드 중... → {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    print("✅ 컬럼 목록:", list(df.columns))

    # 1) 컬럼 추론: 서비스명 / 결제일 / billing_cycle / 금액 / 통화
    service_col = guess_column(
        df,
        ["service_name", "서비스명", "service", "brand", "업체명"],
    )
    billing_col = guess_column(
        df,
        ["billing_cycle", "billingcycle", "cycle", "주기"],
    )
    date_col = guess_column(
        df,
        [
            "payment_date",
            "email_date",
            "email_date_raw",
            "receivedTime",
            "date",
            "결제일",
            "날짜",
        ],
    )
    amount_col = guess_column(
        df,
        ["amount", "price", "결제금액", "금액", "paid_amount", "총금액"],
    )
    currency_col = guess_column(
        df,
        ["currency", "통화"],
    )

    # 필수 컬럼 체크
    if service_col is None:
        raise ValueError("서비스명을 나타내는 컬럼을 찾지 못했습니다. "
                         "예: service_name, 서비스명, service 등")

    if billing_col is None:
        raise ValueError("billing_cycle 컬럼을 찾지 못했습니다. "
                         "예: billing_cycle, billingcycle, cycle, 주기 등")

    if date_col is None:
        raise ValueError("결제/날짜 컬럼을 찾지 못했습니다. "
                         "예: payment_date, email_date_raw, receivedTime, 결제일, 날짜 등")

    print(f"✅ 서비스 컬럼: {service_col}")
    print(f"✅ billing_cycle 컬럼: {billing_col}")
    print(f"✅ 날짜 컬럼: {date_col}")
    print(f"✅ 금액 컬럼: {amount_col if amount_col else '없음(합계/평균은 NaN)'}")
    print(f"✅ 통화 컬럼: {currency_col if currency_col else '없음'}")

    # 2) 날짜 파싱
    df["_pay_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df_clean = df.dropna(subset=[service_col, "_pay_date"]).copy()

    if df_clean.empty:
        raise ValueError("유효한 서비스명 + 날짜 데이터가 없습니다.")

    # 3) 금액 파싱
    if amount_col:
        df_clean["_amount"] = df_clean[amount_col].apply(parse_amount)
    else:
        df_clean["_amount"] = np.nan

    # 4) 오늘 날짜
    today = dt.date.today()
    print(f"📅 기준 날짜(오늘): {today}")

    summary_rows = []

    # 5) 서비스별 그룹핑
    for service, g in df_clean.groupby(service_col):
        g_sorted = g.sort_values("_pay_date")
        dates = g_sorted["_pay_date"].dt.date.values

        num_pay = len(dates)
        first_date = dates[0]
        last_date = dates[-1]

        # billing_cycle 값들
        billing_values = (
            g_sorted[billing_col]
            .astype(str)
            .str.strip()
            .str.lower()
            .tolist()
        )

        # 이 서비스에서 한 번이라도 monthly면 → 정기구독으로 간주
        has_monthly = any(bv == "monthly" for bv in billing_values)

        # 통화 추정 (가장 많이 등장하는 값)
        if currency_col:
            cur_series = (
                g_sorted[currency_col]
                .dropna()
                .astype(str)
                .str.strip()
            )
            currency = cur_series.mode().iloc[0] if not cur_series.empty else None
        else:
            currency = None

        # 금액 관련
        valid_amounts = g_sorted["_amount"].dropna()
        if not valid_amounts.empty:
            total_amount = float(valid_amounts.sum())
            avg_amount = float(valid_amounts.mean())
        else:
            total_amount = None
            avg_amount = None

        # 구독 판단 + 상태
        if has_monthly:
            is_subscription = True
            days_since_last = (today - last_date).days

            if days_since_last <= ACTIVE_THRESHOLD_DAYS:
                status = "진행중"
            else:
                status = "종료됨"
        else:
            is_subscription = False
            status = "비구독"

        summary_rows.append(
            {
                "service_name": service,
                "num_payments": num_pay,
                "first_payment_date": first_date,
                "last_payment_date": last_date,
                "currency": currency,
                "total_amount": total_amount,
                # 구독형인 것들 회당 금액 (비구독이어도 참고용으로 그냥 채워둬도 됨)
                "avg_amount_per_payment": avg_amount,
                "has_monthly_billing": has_monthly,
                "is_subscription": is_subscription,
                "status": status,
            }
        )

    summary_df = pd.DataFrame(summary_rows)

    print("📊 서비스 요약 (앞부분):")
    print(summary_df.head())

    # 6) 원본 df에 서비스별 구독정보 병합 + parsed_amount 붙이기
    merged = df.merge(
        summary_df[["service_name", "is_subscription", "status", "total_amount", "avg_amount_per_payment"]],
        left_on=service_col,
        right_on="service_name",
        how="left",
    )

    # raw에도 파싱된 금액 보여주고 싶으면:
    if amount_col:
        # 원본 df에도 parsed_amount를 맞춰 붙이기 위해 다시 파싱
        merged["parsed_amount"] = merged[amount_col].apply(parse_amount)
    else:
        merged["parsed_amount"] = np.nan

    # 7) 엑셀 저장
    print(f"💾 분석 결과 엑셀 저장 중... → {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        summary_df.to_excel(writer, sheet_name="service_summary", index=False)
        merged.to_excel(writer, sheet_name="raw_with_status", index=False)

    print("🎉 완료! 서비스별 총 사용금액 + 회당 금액 + 구독상태까지 정리 끝!")


if __name__ == "__main__":
    main()
