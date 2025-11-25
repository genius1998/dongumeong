import pandas as pd
import numpy as np
import datetime as dt

# 입력 / 출력 파일 이름
INPUT_FILE = "is_subscribe.xlsx"
TODAY_STR = dt.date.today().strftime("%Y%m%d")
OUTPUT_FILE = f"is_subscribe_analyzed_{TODAY_STR}.xlsx"

# "진행중" 판단 기준 일수 (원하면 30 → 60, 90 같은 걸로 바꿔도 됨)
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


def main():
    print(f"📂 엑셀 로드 중... → {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    print("✅ 컬럼 목록:", list(df.columns))

    # 1) 컬럼 추론: 서비스명 / 결제일 / billing_cycle
    service_col = guess_column(
        df,
        ["service_name", "서비스명", "service", "brand", "업체명"]
    )
    billing_col = guess_column(
        df,
        ["billing_cycle", "billingcycle", "cycle", "주기"]
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
        ]
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

    # 2) 날짜 파싱
    df["_pay_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df_clean = df.dropna(subset=[service_col, "_pay_date"]).copy()

    if df_clean.empty:
        raise ValueError("유효한 서비스명 + 날짜 데이터가 없습니다.")

    # 3) 오늘 날짜
    today = dt.date.today()
    print(f"📅 기준 날짜(오늘): {today}")

    summary_rows = []

    # 4) 서비스별 그룹핑 후 billing_cycle, 마지막 결제일 기반으로 판정
    for service, g in df_clean.groupby(service_col):
        g_sorted = g.sort_values("_pay_date")
        dates = g_sorted["_pay_date"].dt.date.values

        num_pay = len(dates)
        first_date = dates[0]
        last_date = dates[-1]

        # billing_cycle 값들 (문자열로 정리)
        billing_values = (
            g_sorted[billing_col]
            .astype(str)
            .str.strip()
            .str.lower()
            .tolist()
        )

        # 이 서비스에서 한 번이라도 monthly면 → 정기구독으로 간주
        has_monthly = any(bv == "monthly" for bv in billing_values)

        # 필요하면 여기서 "month", "매월" 같은 것도 같이 허용 가능
        # has_monthly = any(bv in ["monthly", "month", "매월", "월정액"] for bv in billing_values)

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
                "has_monthly_billing": has_monthly,
                "is_subscription": is_subscription,
                "status": status,
            }
        )

    summary_df = pd.DataFrame(summary_rows)

    print("📊 서비스 요약 (앞부분):")
    print(summary_df.head())

    # 5) 원본 df에 서비스별 구독정보 병합해서 같이 저장
    merged = df.merge(
        summary_df[["service_name", "is_subscription", "status"]],
        left_on=service_col,
        right_on="service_name",
        how="left",
    )

    # 6) 엑셀 저장
    print(f"💾 분석 결과 엑셀 저장 중... → {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        summary_df.to_excel(writer, sheet_name="service_summary", index=False)
        merged.to_excel(writer, sheet_name="raw_with_status", index=False)

    print("🎉 완료! billing_cycle 기반 정기구독 + 진행중/종료 판별 끝!")


if __name__ == "__main__":
    main()
