import psycopg2
import os

DB_CONFIG = {
    "host": "localhost",
    "dbname": "tiktok_crawler",
    "user": "postgres",
    "password": "admin123@", 
    "port": 5432
}

def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print("❌ DB connection error:", e)
        return None


def validate_yt_post(title, url) -> bool:
    conn = get_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM yt_post WHERE url = %s", (url,))
            return cur.fetchone() is None
    finally:
        conn.close()


def insert_yt_post(video_id, title, url, audio_path):
    conn = get_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO yt_post (video_id, title, url, audio_path)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (video_id, title, url, audio_path))
            conn.commit()
            return True
    finally:
        conn.close()
