import os
import time
import random
import yt_dlp
import logging
from datetime import datetime
from db import db_adapter as db

AUDIO_DIR = "downloads/audio"
COOKIES_FILE = "cookies_loi.txt"

def resolve_tiktok_channel(username: str):
    """Lấy ID kênh để vượt rào chặn username của TikTok"""
    if username.startswith("tiktokuser:"):
        return username

    profile_url = f"https://www.tiktok.com/@{username}"
    ydl_opts = {
        "quiet": True,
        "extract_flat": "first",
        "skip_download": True,
        "cookies": COOKIES_FILE,
        "no_warnings": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(profile_url, download=False)
            if "entries" in info and len(info["entries"]) > 0:
                first_entry = info["entries"][0]
                channel_id = first_entry.get("channel_id")
                if channel_id:
                    return f"tiktokuser:{channel_id}"
    except Exception:
        pass

    return f"https://www.tiktok.com/@{username}"

def get_latest_video_url(username: str):
    resolved_target = resolve_tiktok_channel(username)
    print(f"🔍 Đang quét mục tiêu: {resolved_target}")

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "cookies": COOKIES_FILE,
        "nocheckcertificate": True,
        "noplaylist": False,
        "simulate": True,
        "playlistend": 5,  # Giảm xuống 5 video để tránh rate limit
        "no_warnings": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(resolved_target, download=False)
        entries = info.get("entries", [])
        if not entries:
            raise RuntimeError("Không lấy được danh sách video.")

        # Lấy 3 video đầu tiên (tránh video ghim cũ)
        top_entries = entries[:3]

        # Chọn video mới nhất dựa vào timestamp
        latest = max(top_entries, key=lambda e: e.get("timestamp") or 0)

        # Build lại URL chuẩn
        vid_id = latest.get("id")
        return f"https://www.tiktok.com/@{username}/video/{vid_id}", latest.get("title")

def download_audio(video_url: str, video_id: str):
    os.makedirs(AUDIO_DIR, exist_ok=True)
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(AUDIO_DIR, f"{video_id}.%(ext)s"),
        "cookies": COOKIES_FILE,
        "nocheckcertificate": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "quiet": False,
        "no_warnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])
    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")

def main():
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT tt_link, tt_name FROM tt_group")
        groups = cur.fetchall()
    conn.close()

    for username, name in groups:
        username = username.replace("@", "")
        start_time = time.time()
        print(f"\n🎵 Xử lý: {name}")

        # Delay trước mỗi account
        print(f"⏳ Đợi 45s để tránh rate limit...")
        time.sleep(45)

        try:
            video_url, title = get_latest_video_url(username)

            # Kiểm tra DB
            if not db.validate_yt_post(title, video_url):
                elapsed = time.time() - start_time
                print(f"⏭️ Đã tồn tại, bỏ qua. (Mất {elapsed:.1f}s)")
                time.sleep(45)
                continue

            video_id_db = f"t_{username}_{int(time.time())}"
            audio_path = download_audio(video_url, video_id_db)

            db.insert_yt_post(video_id_db, title, video_url, audio_path)
            elapsed = time.time() - start_time
            print(f"✅ Thành công: {audio_path} (Mất {elapsed:.1f}s)")

            # Delay sau khi thành công
            print(f"⏳ Đợi 45s trước khi tiếp tục...")
            time.sleep(45)

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ Lỗi: {e} (Mất {elapsed:.1f}s)")
            logging.error(f"Lỗi xử lý {username}: {e}")

            # Nếu là 429, đợi lâu hơn
            if "429" in str(e):
                wait_time = random.randint(300, 600)  # 5-10 phút
                print(f"⚠️ Rate limit! Đợi {wait_time//60} phút...")
                time.sleep(wait_time)
            else:
                time.sleep(45)

if __name__ == "__main__":
    main()