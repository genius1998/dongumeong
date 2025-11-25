import os
import base64
import json
import time
import datetime as dt
import pandas as pd
import numpy as np
import google.generativeai as genai
from googleapiclient.discovery import build
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete

import models

# --- (Helper functions and BATCH_PROMPT remain the same) ---
def get_plain_text_from_message(msg_detail):
    payload = msg_detail.get("payload", {})
    mime_type = payload.get("mimeType", "")
    body = payload.get("body", {})
    parts = payload.get("parts", [])

    if mime_type == "text/plain" and "data" in body:
        data = body["data"]
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")

    text_parts = []
    def walk_parts(parts_list):
        for part in parts_list:
            part_mime = part.get("mimeType", "")
            part_body = part.get("body", {})
            sub_parts = part.get("parts", [])
            if sub_parts:
                walk_parts(sub_parts)
            if part_mime == "text/plain" and "data" in part_body:
                data = part_body["data"]
                text = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
                text_parts.append(text)

    if parts:
        walk_parts(parts)
    
    return "\n".join(text_parts) if text_parts else ""

BATCH_PROMPT = """
You are an analyzer that extracts 'subscription/recurring payment/newsletter/membership' information from emails.
A list of multiple emails will be given as input.
Each email is represented as a single line of JSON, in the format:
{"id": 1, "subject": "...", "sender": "...", "body": "..."}

Your tasks:
1) Determine if each email is related to a subscription/recurring payment/newsletter/membership.
2) Convert only the relevant ones into a JSON object with the format below.
3) Finally, output only a JSON array (No explanations, no code blocks).

Format:
[
  {
    "id": 1,
    "is_subscription": true,
    "service_name": "Service or brand name",
    "plan_name": "Plan name or null",
    "price": "Price as a string or null",
    "currency": "KRW, USD, etc., or null",
    "billing_cycle": "monthly / yearly / weekly / once / unknown",
    "start_date": "YYYY-MM-DD format or null",
    "next_billing_date": "YYYY-MM-DD format or null"
  },
  ...
]

If an email is completely unrelated to subscriptions/recurring payments, exclude it from the array entirely.
If there are no related emails, output an empty array [].
"""

def analyze_emails_batch_with_gemini(email_items, gemini_api_key):
    if not email_items:
        return {}

    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-2.0-flash-lite") # Using a more robust model

    lines = []
    for item in email_items:
        body = item["body"] or ""
        if len(body) > 4000:
            body = body[:4000]
        obj = {
            "id": item["id"],
            "subject": item["subject"],
            "sender": item["sender"],
            "body": body,
        }
        lines.append(json.dumps(obj, ensure_ascii=False))

    joined = "\n".join(lines)
    prompt = BATCH_PROMPT + "\n\n### Email List\n" + joined

    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        text = text.strip("`").strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()

        arr = json.loads(text)
        if not isinstance(arr, list):
            return {}

        result = {item.get("id"): item for item in arr if isinstance(item, dict) and item.get("id") is not None and item.get("is_subscription")}
        return result
    except Exception as e:
        print(f"Error during Gemini batch analysis: {e}")
        return {}

async def run_analysis(credentials, gemini_api_key, db: AsyncSession, user_id: int):
    # 1. Fetch Emails
    print("Step 1: Fetching emails...")
    service = build('gmail', 'v1', credentials=credentials)
    
    six_months_ago = (dt.date.today() - dt.timedelta(days=180)).strftime('%Y/%m/%d')
    query = f'-category:promotions -category:social in:anywhere after:{six_months_ago}'
    
    results = service.users().messages().list(userId='me', q=query, maxResults=500).execute()
    messages = results.get('messages', [])
    
    email_data = []
    for i, msg in enumerate(messages):
        # Removed print spam for cleaner logs
        msg_detail = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        headers = msg_detail["payload"]["headers"]
        subject = next((h["value"] for h in headers if h["name"].lower() == "subject"), "")
        
        print(f"--> Fetched email {i+1}/{len(messages)}: {subject[:50]}...")

        from_addr = next((h["value"] for h in headers if h["name"].lower() == "from"), "")
        date_str = next((h["value"] for h in headers if h["name"].lower() == "date"), "")
        
        
        try:
            # Handle timezone information correctly
            parsed_date = pd.to_datetime(date_str, errors='coerce').to_pydatetime()
        except Exception:
            parsed_date = None

        body_text = get_plain_text_from_message(msg_detail)
        
        email_data.append({
            "id": i, "message_id": msg["id"], "subject": subject,
            "sender": from_addr, "body": body_text, "receivedTime": parsed_date,
        })
    print(f"Fetched {len(email_data)} emails.")

    # 2. Analyze with Gemini in Batches
    print("Step 2: Analyzing emails with Gemini...")
    BATCH_SIZE = 20
    all_analyzed_items = []
    total_batches = (len(email_data) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(email_data), BATCH_SIZE):
        chunk = email_data[i:i + BATCH_SIZE]
        print(f"--> Sending batch {i//BATCH_SIZE + 1}/{total_batches}...")
        analysis_map = analyze_emails_batch_with_gemini(chunk, gemini_api_key)
        time.sleep(1.0) # Respect API rate limits

        for item in chunk:
            if item["id"] in analysis_map:
                all_analyzed_items.append({**item, **analysis_map[item["id"]]})
    
    if not all_analyzed_items:
        return []
    
    df = pd.DataFrame(all_analyzed_items)
    print(f"Gemini identified {len(df)} potential subscription emails.")

    # 3. Determine Status
    print("Step 3: Determining subscription status...")
    df["receivedTime"] = pd.to_datetime(df["receivedTime"], errors="coerce", utc=True)
    df_clean = df.dropna(subset=["service_name", "receivedTime"]).copy()
    
    if df_clean.empty: return []

    status_map = {}
    for service, group in df_clean.groupby("service_name"):
        last_payment_date = group["receivedTime"].max().date()
        is_subscription = any(cycle in ["monthly", "yearly", "weekly"] for cycle in group["billing_cycle"].astype(str).str.lower())
        status = "구독중" if is_subscription and (dt.date.today() - last_payment_date).days <= 35 else ("구독종료" if is_subscription else "일회성 결제")
        status_map[service] = status

    df["status"] = df["service_name"].map(status_map)
    
    # 4. Prepare and Save Final Result to DB
    print("Step 4: Saving analysis to database...")
    df_final = df.rename(columns={"sender": "from_name"})
    result_cols = ["from_name", "price", "billing_cycle", "receivedTime", "status", "service_name"]
    df_final = df_final.replace({np.nan: None})
    final_data = df_final.to_dict(orient='records')
    
    # Clear old analysis for the user
    await db.execute(delete(models.GmailAnalysis).where(models.GmailAnalysis.user_id == user_id))
    
    new_analysis_entries = []
    for record in final_data:
        # Convert datetime to string for JSON serialization
        if 'receivedTime' in record and isinstance(record['receivedTime'], (dt.datetime, pd.Timestamp)):
            record['receivedTime'] = record['receivedTime'].isoformat()
            
        new_entry = models.GmailAnalysis(
            user_id=user_id,
            analysis_result=json.dumps(record, ensure_ascii=False)
        )
        new_analysis_entries.append(new_entry)

    db.add_all(new_analysis_entries)
    await db.commit()
    
    print("Analysis complete and saved.")
    return final_data
