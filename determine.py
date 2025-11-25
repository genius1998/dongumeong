import pandas as pd
import numpy as np
import datetime as dt

# 입력 / 출력 파일 이름
INPUT_FILE = "is_subscribe.xlsx"
TODAY_STR = dt.date.today().strftime("%Y%m%d")
OUTPUT_FILE = f"is_subscribe_analyzed_{TODAY_STR}.xlsx"


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

    print("존재하는 컬럼:", list(df.columns))

    # 1) 서비스명 / 결제일 / 금액 컬럼 추론
    service_col = guess_column(
        df,
        ["service_name", "서비스명", "service", "brand", "업체명"]
    )
    date_col = guess_column(
        df,
        ["payment_date", "email_date", "email_date_raw",
         "receivedTime", "date", "결제일", "날짜"]
    )
    amount_col = guess_column(
        df,
        ["amount", "price", "금액", "결제금액", "총결제금액"]
    )

    if service_col is None:
        raise ValueError("서비스명을 나타내는 컬럼을 찾지 못했습니다. "
                         "예: service_name, 서비스명, service 등")

    if date_col is None:
        raise ValueError("결제/날짜 정보를 나타내는 컬럼을 찾지 못했습니다. "
                         "예: payment_date, email_date_raw, receivedTime, 결제일, 날짜 등")

    print(f"✅ 사용 서비스 컬럼: {service_col}")
    print(f"✅ 사용 날짜 컬럼: {date_col}")
    print(f"✅ 사용 금액 컬럼: {amount_col}")

    # 2) 날짜 파싱
    df["_pay_date"] = pd.to_datetime(df[date_col], errors="coerce")

    # 서비스명 결측/날짜 결측은 제거
    df_clean = df.dropna(subset=[service_col, "_pay_date"]).copy()

    if df_clean.empty:
        raise ValueError("유효한 서비스명 + 날짜 데이터가 없습니다.")

    # 금액 컬럼 숫자화 (없으면 NaN)
    if amount_col is not None:
        df_clean["_amount"] = pd.to_numeric(df_clean[amount_col], errors="coerce")
    else:
        df_clean["_amount"] = np.nan

    # 오늘 날짜
    today = dt.date.today()
    print(f"📅 기준 날짜(오늘): {today}")

    # 3) 서비스명별 그룹핑 후 정기구독 여부 / 상태 판단
    summary_rows = []

    for service, g in df_clean.groupby(service_col):
        g_sorted = g.sort_values("_pay_date")
        dates = g_sorted["_pay_date"].dt.date.values

        num_pay = len(dates)
        first_date = dates[0]
        last_date = dates[-1]

        # 금액 관련
        amounts = g_sorted["_amount"].dropna().values
        total_amount = float(np.nansum(amounts)) if len(amounts) > 0 else None
        avg_amount = float(np.nanmean(amounts)) if len(amounts) > 0 else None
        std_amount = float(np.nanstd(amounts)) if len(amounts) > 0 else None

        # 기본값
        is_subscription = False
        billing_cycle_days = None
        status = "비구독"

        if num_pay >= 2:
            # 결제 간격(일) 계산
            date_times = g_sorted["_pay_date"].values
            diffs = np.diff(date_times)  # numpy timedelta64
            intervals = np.array([d.astype("timedelta64[D]").astype(int) for d in diffs])

            if len(intervals) > 0:
                median_interval = float(np.median(intervals))
                std_interval = float(np.std(intervals))
            else:
                median_interval = None
                std_interval = None

            billing_cycle_days = median_interval

            # --- 정기구독 판단 로직 ---
            # 1) 결제 간격이 대략 월단위 (20~45일)
            cond_interval = (median_interval is not None) and (20 <= median_interval <= 45)

            # 2) 간격 변동이 너무 크지 않음 (표준편차 <= 10일 정도)
            cond_interval_stable = (std_interval is not None) and (std_interval <= 10)

            # 3) 금액이 비슷하게 반복 (있을 경우)
            if len(amounts) >= 2 and not np.isnan(avg_amount):
                cond_amount_stable = (std_amount <= avg_amount * 0.3)  # 변동 30% 이내
            else:
                # 금액 데이터가 없으면 이 조건은 패스 (True로 인정)
                cond_amount_stable = True

            if cond_interval and cond_interval_stable and cond_amount_stable:
                is_subscription = True

        # --- 현재 진행중/종료 상태 ---
        if is_subscription:
            days_since_last = (today - last_date).days

            if days_since_last <= 30:
                status = "진행중"   # 최근 한달 내 결제 → 아직 살아있는 구독
            else:
                status = "종료됨"   # 한달 넘게 결제 없음 → 끊긴 구독으로 간주

        summary_rows.append(
            {
                "service_name": service,
                "num_payments": num_pay,
                "first_payment_date": first_date,
                "last_payment_date": last_date,
                "total_amount": total_amount,
                "avg_amount": avg_amount,
                "std_amount": std_amount,
                "median_interval_days": billing_cycle_days,
                "is_subscription": is_subscription,
                "status": status,
            }
        )

    summary_df = pd.DataFrame(summary_rows)

    print("📊 서비스 요약 분석 결과 (앞부분):")
    print(summary_df.head())

    # 4) 원본 df에 서비스별 구독정보 병합해서 같이 저장
    merged = df.merge(
        summary_df[["service_name", "is_subscription", "status"]],
        left_on=service_col,
        right_on="service_name",
        how="left",
    )

    # 5) 엑셀 저장
    print(f"💾 분석 결과 엑셀 저장 중... → {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        summary_df.to_excel(writer, sheet_name="service_summary", index=False)
        merged.to_excel(writer, sheet_name="raw_with_status", index=False)

    print("🎉 완료! 정기구독 여부 + 진행중/종료 상태 판별이 끝났습니다.")


if __name__ == "__main__":
    main()
