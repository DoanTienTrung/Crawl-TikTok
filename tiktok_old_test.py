import os
import sys
import time
from datetime import datetime
import yt_dlp
from db import db_adapter as db
import logging

AUDIO_DIR = "downloads/audio"

def get_latest_tiktok_video(username: str) -> dict:
    """
    Lấy metadata video mới nhất của TikTok user (KHÔNG dùng cookies).
    """
    profile_url = f"https://www.tiktok.com/@{username}"
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(profile_url, download=False)

        if "entries" not in info or not info["entries"]:
            raise RuntimeError(f"Không tìm thấy video nào cho {username}")

        entries = info["entries"]

        # Sắp xếp theo timestamp mới nhất
        entries = sorted(
            entries,
            key=lambda e: e.get("upload_date") or e.get("timestamp") or 0,
            reverse=True
        )

        return entries[0]


def download_audio(video_url: str, video_id: str) -> str:
    """Download audio từ video (KHÔNG dùng cookies)."""
    os.makedirs(AUDIO_DIR, exist_ok=True)

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(AUDIO_DIR, f"{video_id}.%(ext)s"),
        "nocheckcertificate": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "quiet": False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")


def main():
    print("🔍 [Code cũ - Không cookies] Đang tìm TikTok video mới nhất...")

    conn = db.get_connection()
    if conn is None:
        logging.error("Không thể kết nối DB")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT tt_id, tt_link, tt_name FROM tt_group ORDER BY RANDOM()")
            rows = cursor.fetchall()
            data_list = list(rows)
    finally:
        conn.close()

    for id, link, name in data_list:
        print(f"\n🎵 Xử lý: {name} (@{link})")
        start_time = time.time()

        try:
            # Lấy video mới nhất (không cookies)
            latest = get_latest_tiktok_video(link)

            # Build URL
            vid_id = latest.get("id")
            video_url = latest.get("url") or f"https://www.tiktok.com/@{link}/video/{vid_id}"
            title = latest.get("title", "")

            ts = latest.get("timestamp")
            ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "N/A"

            print(f"📹 Video: {video_url}")
            print(f"📅 Thời gian: {ts_str}")

            # Kiểm tra DB
            if not db.validate_yt_post(title, video_url):
                elapsed = time.time() - start_time
                print(f"⏭️ Đã tồn tại, bỏ qua. ({elapsed:.1f}s)")
                continue

            # Download
            video_id_db = f"t_{link}_{vid_id}"
            audio_path = download_audio(video_url, video_id_db)

            elapsed = time.time() - start_time
            print(f"✅ Thành công: {audio_path} ({elapsed:.1f}s)")

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ Lỗi: {e} ({elapsed:.1f}s)")
            logging.error(f"Lỗi {link}: {e}")

        time.sleep(5)


if __name__ == "__main__":
    main()
