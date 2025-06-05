import requests
import psycopg2
import pandas as pd
import os
import smtplib
from email.message import EmailMessage

# ---------------------- Fetch Data ----------------------
def fetch_opensky():
    url = "https://opensky-network.org/api/states/all"
    username = os.environ.get("OPENSKY_USER")     # Optional: Use if added
    password = os.environ.get("OPENSKY_PASS")
    
    auth = (username, password) if username and password else None
    response = requests.get(url, auth=auth)
    data = response.json()

    return pd.DataFrame(data['states'], columns=[
        'icao24', 'callsign', 'origin_country', 'time_position',
        'last_contact', 'longitude', 'latitude', 'baro_altitude',
        'on_ground', 'velocity', 'true_track', 'vertical_rate'
    ])

# ---------------------- Insert to DB ----------------------
def insert_to_db(df):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database="postgres",
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=5432
    )
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO flights (
                icao24, callsign, origin_country, time_position,
                last_contact, longitude, latitude, baro_altitude,
                on_ground, velocity, true_track, vertical_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

# ---------------------- Send Email Notification ----------------------
def send_email(subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = os.environ['EMAIL_SENDER']
    msg['To'] = os.environ['EMAIL_RECIPIENT']

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login(os.environ['EMAIL_SENDER'], os.environ['EMAIL_PASSWORD'])
    server.send_message(msg)
    server.quit()

# ---------------------- Main ETL Runner ----------------------
try:
    df = fetch_opensky()
    df.dropna(inplace=True)
    insert_to_db(df)
    send_email("✅ ETL Success", f"{len(df)} records loaded into Supabase.")
except Exception as e:
    send_email("❌ ETL Failed", f"Error during ETL run:\n\n{str(e)}")
    raise
