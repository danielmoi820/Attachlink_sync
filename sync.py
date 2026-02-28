import gspread
from oauth2client.service_account import ServiceAccountCredentials
from supabase import create_client
import os

# ----------------- Supabase Setup -----------------
SUPABASE_URL = os.getenv("https://cmfahrwxychnjkjcxugd.supabase.co")
SUPABASE_KEY = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNtZmFocnd4eWNobmpramN4dWdkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxNTA1NzgsImV4cCI6MjA4NzcyNjU3OH0.h14tFrpORE7FLDoBkM_1eyooTWiknNv3HgpXqzFTBp8")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------- Google Sheets Setup -----------------
SCOPE = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "service_account.json"  # Upload this to Render

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
gc = gspread.authorize(creds)

SHEET_ID = os.getenv("1t1_TYML1_ROPHFLUth2Ji96SebyWOIkpGViPiqml4EQ")
sheet = gc.open_by_key(SHEET_ID).sheet1

# ----------------- Track Processed Rows -----------------
# Avoid duplicates using the Timestamp column
processed_timestamps = set()

# Fetch existing timestamps from Supabase
existing = supabase.table("students").select("timestamp").execute()
if existing.data:
    processed_timestamps = set([row['timestamp'] for row in existing.data])

# ----------------- Read Sheet -----------------
rows = sheet.get_all_records()
new_rows_count = 0

for row in rows:
    timestamp = row.get("Timestamp")
    if not timestamp or timestamp in processed_timestamps:
        continue  # Skip duplicates or missing timestamp

    # Map Google Sheet columns to Supabase table
    data = {
        "full_name": row.get("Full name"),
        "email_address": row.get("Email address"),
        "phone_number": row.get("Phone number"),
        "year_of_study": row.get("Year of Study"),
        "browse_internship": row.get("Browse internship"),
        "college_university": row.get("College/University"),
        "course_program": row.get("Course/Program"),
        "portfolio_github": row.get("Portfolio/GitHub"),
        "why_should_we_select_you": row.get("Why should we select you"),
        "cv_link": row.get("Upload CV"),  # Just the link from Google Form
        "timestamp": timestamp
    }

    # Insert into Supabase
    supabase.table("students").insert(data).execute()
    new_rows_count += 1

print(f"Sync complete! {new_rows_count} new rows inserted.")