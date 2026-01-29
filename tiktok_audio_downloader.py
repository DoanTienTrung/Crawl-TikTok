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

# Cấu hình logging: ghi file + in console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("tiktok_crawl.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("yt_dlp").setLevel(logging.WARNING)

AUDIO_DIR = "downloads/audio"
COOKIES_DIR = "cookies"
COOKIES_FILES = []  # Sẽ load sau
CONFIG_FILE = "scheduler_config.json"

# Load cookies files một lần khi script khởi động
if os.path.exists(COOKIES_DIR):
    COOKIES_FILES = [
        os.path.join(COOKIES_DIR, f)
        for f in os.listdir(COOKIES_DIR)
        if f.endswith(".txt") and os.path.isfile(os.path.join(COOKIES_DIR, f))
    ]
    if COOKIES_FILES:
        logging.info(f"Đã load {len(COOKIES_FILES)} file cookies từ '{COOKIES_DIR}'")
    else:
        logging.warning("Không tìm thấy file .txt nào trong thư mục cookies/")
else:
    logging.warning(f"Thư mục '{COOKIES_DIR}' không tồn tại → download không dùng cookies!")

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

SECUID_CACHE_FILE = "secuid_cache.json"

# ================= SECUID CACHE =================
def load_secuid_cache():
    if os.path.exists(SECUID_CACHE_FILE):
        with open(SECUID_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_secuid_cache(cache: dict):
    with open(SECUID_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def get_cached_target(username: str):
    cache = load_secuid_cache()
    info = cache.get(username)

    if not info:
        return None

    if info["status"] == "ok" and info.get("sec_uid"):
        logging.info(f"🔁 Dùng secUid cache cho @{username}")
        return f"tiktokuser:{info['sec_uid']}"

    if info["status"] == "broken":
        logging.info(f"🔁 @{username} bị đánh dấu broken → dùng WEB")
        return f"https://www.tiktok.com/@{username}"

    return None

# ================= RESOLVE TARGET =================
def resolve_tiktok_target(username: str) -> str:
    # 1️⃣ Ưu tiên cache
    cached = get_cached_target(username)
    if cached:
        return cached

    profile_url = f"https://www.tiktok.com/@{username}"
    logging.info(f"🔍 Thử lấy secUid cho @{username}")

    configs = [
        {"tiktok": {"api_hostname": "api16-normal-c-useast1a.tiktokv.com", "skip": "web"}},
        {"tiktok": {"api_hostname": "api22-normal-c-useast1a.tiktokv.com", "skip": "web"}},
        {"tiktok": {"api_hostname": "api.tiktokv.com"}},
        {},
    ]

    for idx, extractor_args in enumerate(configs, 1):
        ydl_opts = {
            "quiet": True,
            "extract_flat": True,
            "skip_download": True,
            "playlistend": 1,
            "no_warnings": True,
            "verbose": True,
        }

        if extractor_args:
            ydl_opts["extractor_args"] = extractor_args

        if COOKIES_FILES:
            ydl_opts["cookies"] = random.choice(COOKIES_FILES)

        try:
            logging.info(f"  Thử config {idx}/{len(configs)}...")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(profile_url, download=False)
                entries = info.get("entries", [])
                if entries:
                    e = entries[0]
                    sec_uid = (
                        e.get("uploader_id")
                        or e.get("channel_id")
                        or e.get("creator_id")
                    )
                    if sec_uid:
                        logging.info(f"  ✓ Lấy được secUid: {sec_uid}")
                        cache = load_secuid_cache()
                        cache[username] = {
                            "sec_uid": sec_uid,
                            "status": "ok",
                            "source": "auto",
                            "updated_at": datetime.now().isoformat(),
                        }
                        save_secuid_cache(cache)
                        return f"tiktokuser:{sec_uid}"
        except Exception:
            continue

    # ❌ Không lấy được → đánh dấu broken
    logging.warning(f"⚠️ @{username} không lấy được secUid → fallback WEB")
    cache = load_secuid_cache()
    if username not in cache:
        cache[username] = {
            "sec_uid": None,
            "status": "broken",
            "source": "auto",
            "updated_at": datetime.now().isoformat(),
        }
        save_secuid_cache(cache)

    return profile_url

# ================= GET LATEST VIDEO =================
def get_latest_video_url(username: str):
    target = resolve_tiktok_target(username)
    logging.info(f"🔍 Đang quét: {target}")

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "playlistend": 10,
        "playlist_items": "1-10",
        "no_warnings": True,
        "verbose": True,
    }

    if target.startswith("tiktokuser:"):
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "web"}}
        logging.info("  → Dùng TikTok API")
    else:
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "api"}}
        logging.info("  → Dùng WEB")

    if COOKIES_FILES:
        ydl_opts["cookies"] = random.choice(COOKIES_FILES)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(target, download=False)
        entries = info.get("entries", [])

    if not entries:
        raise RuntimeError("Không lấy được danh sách video")

    valid = [e for e in entries if e.get("timestamp") and not e.get("is_pinned")]
    if not valid:
        raise RuntimeError("Không có video hợp lệ")

    latest = max(valid, key=lambda e: e["timestamp"])
    return f"https://www.tiktok.com/@{username}/video/{latest['id']}", latest.get("title", "")




def download_audio(video_url: str, video_id: str):
    os.makedirs(AUDIO_DIR, exist_ok=True)

    if not COOKIES_FILES:
        logging.warning("Không có cookies → thử download mà không cookies (có thể fail)")
        cookies_list = [None]
    else:
        cookies_list = COOKIES_FILES.copy()
        random.shuffle(cookies_list)
        logging.info(f"Sử dụng {len(cookies_list)} bộ cookies (random order)")

    max_attempts = len(cookies_list) if cookies_list else 1

    for attempt, cookies_file in enumerate(cookies_list, 1):
        cookies_name = os.path.basename(cookies_file) if cookies_file else "Không cookies"
        logging.info(f"Thử download attempt {attempt}/{max_attempts} với cookies: {cookies_name}")

        ydl_opts = {
            "format": "bestaudio/best[acodec!=none]/best",
            "outtmpl": os.path.join(AUDIO_DIR, f"{video_id}.%(ext)s"),
            "nocheckcertificate": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "quiet": False,
            "no_warnings": True,
            "referer": "https://www.tiktok.com/",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "http_headers": {
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "*/*",
            },
            "verbose": True,
        }

        if cookies_file:
            ydl_opts["cookies"] = cookies_file

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            logging.info(f"✅ Download thành công với {cookies_name}")
            return os.path.join(AUDIO_DIR, f"{video_id}.mp3")

        except Exception as e:
            error_str = str(e).lower()
            logging.warning(f"Fail với {cookies_name}: {str(e)[:150]}")

            retry_keywords = ["429", "too many requests", "rate limit", "sign in", "bot", "private", "login required", "cookies", "forbidden", "403"]
            if any(kw in error_str for kw in retry_keywords) and attempt < max_attempts:
                wait = random.randint(15, 45)
                logging.info(f"→ Lỗi rate-limit/cookies → thử cookies tiếp theo sau {wait}s...")
                time.sleep(wait)
                continue
            else:
                raise RuntimeError(f"Download fail sau {attempt} attempts: {str(e)}")

    raise RuntimeError(f"Tất cả {max_attempts} bộ cookies đều fail cho {video_url}")

def main():
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT tt_link, tt_name FROM tt_group")
        groups = cur.fetchall()
    conn.close()

    success_list = []
    failed_list = []
    skipped_list = []

    if not COOKIES_FILES:
        logging.warning("Không có cookies → crawl có thể fail nhiều do rate-limit!")

    for username, name in groups:
        username = username.replace("@", "")
        start = time.time()

        logging.info(f"\n🎵 Xử lý: {name}")
        logging.info(f"⏳ Đợi random 40-50s để tránh rate limit...")
        time.sleep(random.randint(40, 50))

        try:
            video_url, title = get_latest_video_url(username)

            if not db.validate_yt_post(title, video_url):
                logging.info("⏭️ Đã tồn tại, bỏ qua")
                skipped_list.append((username, name, "Đã tồn tại"))
                time.sleep(random.randint(40, 50))
                continue

            video_id_db = f"t_{username}_{int(time.time())}"
            audio_path = download_audio(video_url, video_id_db)

            db.insert_yt_post(video_id_db, title, video_url, audio_path)

            elapsed = time.time() - start
            logging.info(f"✅ Thành công: {audio_path} ({elapsed:.1f}s)")
            success_list.append((username, name, title[:50]))
            logging.info(f"⏳ Đợi random 40-50s trước khi tiếp tục...")
            time.sleep(random.randint(40, 50))

        except Exception as e:
            elapsed = time.time() - start
            error_str = str(e)
            logging.error(f"❌ Lỗi chi tiết cho {username}: {repr(e)} - {error_str} ({elapsed:.1f}s)")
            traceback.print_exc(file=sys.stdout)
            logging.error(traceback.format_exc())

            if "Unable to extract secondary user ID" in error_str:
                logging.warning(f"⚠️ Bỏ qua kênh {username}: Profile private hoặc bị block")
                skipped_list.append((username, name, "Không lấy được secUid"))
            elif "429" in error_str or "Too Many Requests" in error_str:
                wait = random.randint(300, 900)
                logging.warning(f"⚠️ Rate limit toàn cục, đợi {wait//60} phút...")
                time.sleep(wait)
            elif "fail sau" in error_str and "cookies" in error_str.lower():
                failed_list.append((username, name, "Tất cả cookies fail (rate-limit/login?)"))
            else:
                failed_list.append((username, name, error_str[:80]))

            time.sleep(random.randint(40, 80))

    # Summary
    logging.info("\n" + "="*60)
    logging.info("📊 KẾT QUẢ CRAWL")
    logging.info("="*60)
    logging.info(f"\n✅ THÀNH CÔNG: {len(success_list)}")
    for u, n, t in success_list:
        logging.info(f"   - @{u} ({n}): {t}")
    logging.info(f"\n⏭️ BỎ QUA: {len(skipped_list)}")
    for u, n, reason in skipped_list:
        logging.info(f"   - @{u} ({n}): {reason}")
    logging.info(f"\n❌ THẤT BẠI: {len(failed_list)}")
    for u, n, err in failed_list:
        logging.info(f"   - @{u} ({n}): {err}")
    logging.info("\n" + "="*60)

def run_scheduler():
    config = load_config()
    scheduler_cfg = config["scheduler"]
    if not scheduler_cfg.get("enabled", True):
        logging.info("Scheduler is disabled in config")
        return

    scheduler = BlockingScheduler(timezone=scheduler_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
    schedule_type = scheduler_cfg.get("type", "interval")
    settings = scheduler_cfg.get("settings", {})

    if schedule_type == "interval":
        interval_cfg = settings.get("interval", {})
        trigger = IntervalTrigger(
            hours=interval_cfg.get("hours", 0),
            minutes=interval_cfg.get("minutes", 0),
            seconds=interval_cfg.get("seconds", 0) or 3600
        )
        logging.info(f"Scheduled: every {interval_cfg.get('hours', 0)}h {interval_cfg.get('minutes', 0)}m")
    elif schedule_type == "cron":
        cron_cfg = settings.get("cron", {})
        trigger = CronTrigger(
            hour=cron_cfg.get("hour", "*"),
            minute=cron_cfg.get("minute", "0"),
            day_of_week=cron_cfg.get("day_of_week", "*")
        )
        logging.info(f"Scheduled: cron hour={cron_cfg.get('hour')}, minute={cron_cfg.get('minute')}")
    elif schedule_type == "date":
        date_cfg = settings.get("date", {})
        trigger = DateTrigger(run_date=date_cfg.get("run_date"))
        logging.info(f"Scheduled: one-time at {date_cfg.get('run_date')}")
    else:
        logging.error(f"Unknown schedule type: {schedule_type}")
        return

    scheduler.add_job(main, trigger, id="tiktok_downloader", replace_existing=True)

    if scheduler_cfg.get("run_on_startup", False):
        logging.info("Running immediately on startup...")
        main()

    logging.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        main()
    else:
        run_scheduler()