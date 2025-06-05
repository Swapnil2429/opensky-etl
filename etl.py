import requests
import psycopg2
import pandas as pd
import os

def fetch_opensky():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data['states'], columns=[
        'icao24', 'callsign', 'origin_country', 'time_position',
        'last_contact', 'longitude', 'latitude', 'baro_altitude',
        'on_ground', 'velocity', 'true_track', 'vertical_rate'
    ])

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

df = fetch_opensky()
df.dropna(inplace=True)
insert_to_db(df)
