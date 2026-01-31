import os
import psycopg2

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "tiktok_crawler"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "your_password_here"),
    "port": int(os.getenv("DB_PORT", 5432))
}


def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB connection error: {e}")
        return None


def validate_yt_post(title: str, url: str) -> bool:
    conn = get_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM yt_post WHERE url = %s", (url,))
            return cur.fetchone() is None
    finally:
        conn.close()


def insert_yt_post(video_id: str, title: str, url: str, audio_path: str) -> bool:
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
