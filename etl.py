import requests
import psycopg2
import pandas as pd
import os
import smtplib
from email.message import EmailMessage

# ---------------------- Fetch Data ----------------------
def fetch_opensky():
    url = "https://opensky-network.org/api/states/all"
    username = os.environ.get("OPENSKY_USER")
    password = os.environ.get("OPENSKY_PASS")
    auth = (username, password) if username and password else None

    response = requests.get(url, auth=auth)
    data = response.json()
    return pd.DataFrame(data['states'], columns=[
        'icao24', 'callsign', 'origin_country', 'time_position',
        'last_contact', 'longitude', 'latitude', 'baro_altitude',
        'on_ground', 'velocity', 'true_track', 'vertical_rate'
    ])

# ---------------------- Insert to Supabase ----------------------
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

# ---------------------- Email Notification ----------------------
def send_email(subject, body):
    try:
        sender = os.environ['EMAIL_SENDER']
        recipient = os.environ['EMAIL_RECIPIENT']
        password = os.environ['EMAIL_PASSWORD']

        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
    except KeyError:
        print("⚠️ Email skipped — missing environment variables.")
    except Exception as e:
        print(f"⚠️ Email failed to send: {e}")

# ---------------------- Main Runner ----------------------
try:
    df = fetch_opensky()
    df.dropna(inplace=True)
    insert_to_db(df)
    send_email("✅ ETL Success", f"{len(df)} records inserted into Supabase.")
except Exception as e:
    send_email("❌ ETL Failed", f"Error:\n{str(e)}")
    raise
