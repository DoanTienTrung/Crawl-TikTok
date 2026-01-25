import os
import sys
import json
import time
import random
import yt_dlp
import logging
import traceback
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from db import db_adapter as db

AUDIO_DIR = "downloads/audio"
COOKIES_FILE = "cookies_loi.txt"
CONFIG_FILE = "scheduler_config.json"


def load_config():
    """Load scheduler config from JSON file"""
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_tiktok_target(username: str) -> str:
    return f"https://www.tiktok.com/@{username}"


def get_latest_video_url(username: str, max_retries: int = 3):
    target = resolve_tiktok_target(username)
    print(f"🔍 Đang quét: {target}")

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "playlistend": 5,
        "no_warnings": True,
        "playlist_items": "1-10",
        "verbose": True,
        "extractor_args": {"tiktok": {"api_hostname": "api16-normal-c-useast1a.tiktokv.com", "skip": "web"}},
    }

    entries = []
    for attempt in range(max_retries):
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target, download=False)
            entries = info.get("entries", [])

            if entries:
                break

            if attempt < max_retries - 1:
                wait = random.randint(30, 60)
                print(f"⚠️ Không lấy được video, retry {attempt + 1}/{max_retries} sau {wait}s...")
                time.sleep(wait)

    if not entries:
        raise RuntimeError("Không lấy được danh sách video sau nhiều lần thử")

    # Bỏ video ghim + thiếu timestamp
    valid = [
        e for e in entries
        if e.get("timestamp") and not e.get("is_pinned")
    ]

    if not valid:
        raise RuntimeError("Không có video hợp lệ")

    latest = max(valid, key=lambda e: e["timestamp"])
    video_id = latest["id"]
    title = latest.get("title", "")

    return f"https://www.tiktok.com/@{username}/video/{video_id}", title


def download_audio(video_url: str, video_id: str):
    os.makedirs(AUDIO_DIR, exist_ok=True)

    ydl_opts = {
        "format": "bestaudio/best[acodec!=none]/best",
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
        # "impersonate": "chrome",
        "referer": "https://www.tiktok.com/",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "http_headers": {
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "*/*",
        },
        "verbose": True,
        "extractor_args": {"tiktok": {"api_hostname": "api16-normal-c-useast1a.tiktokv.com", "skip": "web"}},
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
        start = time.time()

        print(f"\n🎵 Xử lý: {name}")
        print(f"⏳ Đợi random 40-50s để tránh rate limit...")
        time.sleep(random.randint(50, 60))

        try:
            video_url, title = get_latest_video_url(username)

            # Kiểm tra DB
            if not db.validate_yt_post(title, video_url):
                print("⏭️ Đã tồn tại, bỏ qua")
                time.sleep(random.randint(50, 60))
                continue

            video_id_db = f"t_{username}_{int(time.time())}"
            audio_path = download_audio(video_url, video_id_db)

            db.insert_yt_post(video_id_db, title, video_url, audio_path)

            elapsed = time.time() - start
            print(f"✅ Thành công: {audio_path} ({elapsed:.1f}s)")
            print(f"⏳ Đợi random 40-50s trước khi tiếp tục...")
            time.sleep(random.randint(50, 60))

        except Exception as e:
            elapsed = time.time() - start
            print(f"❌ Lỗi chi tiết cho {username}: {repr(e)} - {str(e)} ({elapsed:.1f}s)")
            traceback.print_exc()  # In stack trace đầy đủ
            logging.error(f"Lỗi {username}: {repr(e)}\n{traceback.format_exc()}")

            if "429" in str(e):
                wait = random.randint(300, 600)
                print(f"⚠️ Rate limit, đợi {wait//60} phút")
                time.sleep(wait)
            else:
                time.sleep(random.randint(50, 60))


def run_scheduler():
    """Run with APScheduler based on config"""
    config = load_config()
    scheduler_cfg = config["scheduler"]

    if not scheduler_cfg.get("enabled", True):
        print("Scheduler is disabled in config")
        return

    scheduler = BlockingScheduler(timezone=scheduler_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
    schedule_type = scheduler_cfg.get("type", "interval")
    settings = scheduler_cfg.get("settings", {})

    # Create trigger based on type
    if schedule_type == "interval":
        interval_cfg = settings.get("interval", {})
        trigger = IntervalTrigger(
            hours=interval_cfg.get("hours", 0),
            minutes=interval_cfg.get("minutes", 0),
            seconds=interval_cfg.get("seconds", 0) or 3600  # default 1 hour
        )
        print(f"Scheduled: every {interval_cfg.get('hours', 0)}h {interval_cfg.get('minutes', 0)}m")

    elif schedule_type == "cron":
        cron_cfg = settings.get("cron", {})
        trigger = CronTrigger(
            hour=cron_cfg.get("hour", "*"),
            minute=cron_cfg.get("minute", "0"),
            day_of_week=cron_cfg.get("day_of_week", "*")
        )
        print(f"Scheduled: cron hour={cron_cfg.get('hour')}, minute={cron_cfg.get('minute')}")

    elif schedule_type == "date":
        date_cfg = settings.get("date", {})
        trigger = DateTrigger(run_date=date_cfg.get("run_date"))
        print(f"Scheduled: one-time at {date_cfg.get('run_date')}")

    else:
        print(f"Unknown schedule type: {schedule_type}")
        return

    scheduler.add_job(main, trigger, id="tiktok_downloader", replace_existing=True)

    # Run immediately on startup if configured
    if scheduler_cfg.get("run_on_startup", False):
        print("Running immediately on startup...")
        main()

    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        main()  # Run once without scheduler
    else:
        run_scheduler()