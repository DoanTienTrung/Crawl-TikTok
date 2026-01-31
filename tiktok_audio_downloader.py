import os
import sys
import json
import time
import random
import subprocess
import traceback
import logging

import yt_dlp
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from db import db_adapter as db

AUDIO_DIR = "downloads/audio"
COOKIES_DIR = "cookies"
COOKIES_FILE = os.path.join(COOKIES_DIR, "tiktok_refreshed.txt")
CONFIG_FILE = "scheduler_config.json"

DELAY_MIN = 40
DELAY_MAX = 50
RATE_LIMIT_DELAY_MIN = 300
RATE_LIMIT_DELAY_MAX = 900
PLAYLIST_LIMIT = 10

AUTH_ERROR_KEYWORDS = ["private", "login", "sign in", "auth", "embedding disabled", "comfortable"]
LIVESTREAM_KEYWORDS = ["livestream", "live stream", "đang live", "live now"]

auth_error_count = 0

try:
    from cookie_refresher import auto_refresh_if_needed, PLAYWRIGHT_AVAILABLE
    COOKIE_REFRESH_ENABLED = PLAYWRIGHT_AVAILABLE
except ImportError:
    COOKIE_REFRESH_ENABLED = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("tiktok_crawl.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("yt_dlp").setLevel(logging.WARNING)


def get_cookies_file():
    if os.path.exists(COOKIES_FILE):
        return COOKIES_FILE

    if COOKIE_REFRESH_ENABLED:
        logging.info("🔄 Cookies not found, refreshing...")
        try:
            auto_refresh_if_needed(force=True)
            if os.path.exists(COOKIES_FILE):
                return COOKIES_FILE
        except Exception as e:
            logging.error(f"❌ Cookie refresh failed: {e}")

    return None


def try_refresh_cookies():
    global auth_error_count
    if not COOKIE_REFRESH_ENABLED:
        return False

    try:
        logging.info("🔄 Refreshing cookies...")
        new_cookies = auto_refresh_if_needed(force=True)
        if new_cookies and os.path.exists(COOKIES_FILE):
            auth_error_count = 0
            logging.info("✅ Cookie refresh successful")
            return True
    except Exception as e:
        logging.error(f"❌ Cookie refresh failed: {e}")
    return False


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_api_configs():
    return [
        {"tiktok": {"api_hostname": "api16-normal-c-useast1a.tiktokv.com", "skip": "web"}},
        {"tiktok": {"api_hostname": "api22-normal-c-useast1a.tiktokv.com", "skip": "web"}},
        {"tiktok": {"api_hostname": "api.tiktokv.com"}},
        {},
    ]


def resolve_tiktok_target(username: str) -> str:
    profile_url = f"https://www.tiktok.com/@{username}"
    logging.info(f"🔍 Resolving secUid for @{username}")

    for idx, extractor_args in enumerate(build_api_configs(), 1):
        ydl_opts = {
            "quiet": True,
            "extract_flat": True,
            "skip_download": True,
            "playlistend": 1,
            "no_warnings": True,
        }

        if extractor_args:
            ydl_opts["extractor_args"] = extractor_args

        cookies = get_cookies_file()
        if cookies:
            ydl_opts["cookies"] = cookies

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(profile_url, download=False)
                entries = info.get("entries", [])
                if entries:
                    sec_uid = entries[0].get("uploader_id") or entries[0].get("channel_id")
                    if sec_uid:
                        return f"tiktokuser:{sec_uid}"
        except Exception:
            continue

    logging.warning(f"⚠️ Could not get secUid for @{username}, using web fallback")
    return profile_url


def is_livestream(entry: dict) -> bool:
    if not entry:
        return False

    if entry.get("is_live") or entry.get("live_status") in ("is_live", "is_upcoming", "post_live"):
        return True

    url = entry.get("url", "") or entry.get("webpage_url", "") or ""
    if "/live/" in url.lower():
        return True

    title = (entry.get("title") or "").lower()
    return any(kw in title for kw in LIVESTREAM_KEYWORDS)


def filter_videos(entries: list) -> list:
    if not entries:
        return []
    return [e for e in entries if e and not is_livestream(e)]


def get_latest_video_subprocess(username: str, cookies_file: str) -> list:
    cmd = [
        "yt-dlp",
        "--cookies", cookies_file,
        "--skip-download",
        "--dump-json",
        "--flat-playlist",
        "--playlist-items", f"1-{PLAYLIST_LIMIT}",
        "--extractor-args", "tiktok:skip=api",
        f"https://www.tiktok.com/@{username}"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp subprocess failed: {result.stderr[:200]}")

    entries = []
    for line in result.stdout.strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return filter_videos(entries)


def build_ydl_opts(target: str, cookies: str) -> dict:
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "playlistend": PLAYLIST_LIMIT,
        "playlist_items": f"1-{PLAYLIST_LIMIT}",
        "no_warnings": True,
    }

    if target.startswith("tiktokuser:"):
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "web"}}
    else:
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "api"}}

    if cookies:
        ydl_opts["cookies"] = cookies

    return ydl_opts


def find_latest_video(entries: list):
    valid = [e for e in entries if e.get("timestamp") and not e.get("is_pinned")]
    if valid:
        return max(valid, key=lambda e: e["timestamp"])
    return None


def get_latest_video_url(username: str):
    target = resolve_tiktok_target(username)
    cookies = get_cookies_file()
    ydl_opts = build_ydl_opts(target, cookies)

    last_error = "No video found"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target, download=False)
            entries = filter_videos(info.get("entries", []))

        latest = find_latest_video(entries)
        if latest:
            video_url = f"https://www.tiktok.com/@{username}/video/{latest['id']}"
            return video_url, latest.get("title", ""), False

    except Exception as e:
        last_error = str(e)
        logging.warning(f"⚠️ Primary method failed: {last_error[:100]}, trying subprocess...")

    if cookies:
        try:
            entries = get_latest_video_subprocess(username, cookies)
            latest = find_latest_video(entries)
            if latest:
                video_url = f"https://www.tiktok.com/@{username}/video/{latest['id']}"
                return video_url, latest.get("title", ""), True
        except Exception as e:
            last_error = str(e)
            logging.error(f"❌ Subprocess also failed: {last_error[:100]}")

    raise RuntimeError(f"Could not get video for @{username}: {last_error[:150]}")


def download_audio_subprocess(video_url: str, video_id: str, cookies_file: str) -> str:
    os.makedirs(AUDIO_DIR, exist_ok=True)
    output_template = os.path.join(AUDIO_DIR, f"{video_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--cookies", cookies_file,
        "-f", "bestaudio/best",
        "-x", "--audio-format", "mp3",
        "--audio-quality", "192K",
        "-o", output_template,
        video_url
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"Download failed: {result.stderr[:200]}")

    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")


def download_audio(video_url: str, video_id: str, use_subprocess: bool = False) -> str:
    os.makedirs(AUDIO_DIR, exist_ok=True)
    cookies = get_cookies_file()

    if use_subprocess:
        if not cookies:
            raise RuntimeError("Cookies required for subprocess download")
        return download_audio_subprocess(video_url, video_id, cookies)

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
    }

    if cookies:
        ydl_opts["cookies"] = cookies

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")


def is_auth_error(error_str: str) -> bool:
    return any(kw in error_str.lower() for kw in AUTH_ERROR_KEYWORDS)


def random_delay(min_sec: int = DELAY_MIN, max_sec: int = DELAY_MAX):
    delay = random.randint(min_sec, max_sec)
    logging.info(f"⏳ Waiting {delay}s...")
    time.sleep(delay)


def process_single_account(username: str, name: str) -> tuple:
    global auth_error_count

    username = username.replace("@", "")
    logging.info(f"\n🎵 Processing: {name} (@{username})")
    random_delay()

    try:
        video_url, title, used_subprocess = get_latest_video_url(username)

        if not db.validate_yt_post(title, video_url):
            logging.info("⏭️ Already exists, skipping")
            return "skipped", "Already exists"

        video_id_db = f"t_{username}_{int(time.time())}"
        audio_path = download_audio(video_url, video_id_db, use_subprocess=used_subprocess)
        db.insert_yt_post(video_id_db, title, video_url, audio_path)

        logging.info(f"✅ Success: {audio_path}")
        auth_error_count = 0
        return "success", title[:50]

    except Exception as e:
        return handle_error(username, name, e)


def handle_error(username: str, name: str, error: Exception) -> tuple:
    global auth_error_count

    error_str = str(error)
    logging.error(f"❌ Error for {username}: {error_str[:100]}")
    traceback.print_exc()

    if is_auth_error(error_str):
        auth_error_count += 1
        logging.warning(f"⚠️ Auth error #{auth_error_count}")

        if try_refresh_cookies():
            retry_result = retry_account(username, name)
            if retry_result:
                return retry_result

    if "Unable to extract secondary user ID" in error_str:
        return "skipped", "Possibly livestream/private"
    elif "429" in error_str or "Too Many Requests" in error_str:
        wait = random.randint(RATE_LIMIT_DELAY_MIN, RATE_LIMIT_DELAY_MAX)
        logging.warning(f"⚠️ Rate limited, waiting {wait // 60} minutes...")
        time.sleep(wait)
        return "failed", "Rate limited"

    return "failed", error_str[:80]


def retry_account(username: str, name: str) -> tuple:
    logging.info(f"🔄 Retrying @{username} after cookie refresh...")
    time.sleep(random.randint(10, 20))

    try:
        video_url, title, used_subprocess = get_latest_video_url(username)

        if not db.validate_yt_post(title, video_url):
            return "skipped", "Already exists"

        video_id_db = f"t_{username}_{int(time.time())}"
        audio_path = download_audio(video_url, video_id_db, use_subprocess=used_subprocess)
        db.insert_yt_post(video_id_db, title, video_url, audio_path)

        logging.info(f"✅ Retry successful: {audio_path}")
        return "success", title[:50]

    except Exception as e:
        logging.error(f"❌ Retry also failed: {str(e)[:100]}")
        return None


def log_summary(success_list: list, skipped_list: list, failed_list: list):
    logging.info("\n" + "=" * 60)
    logging.info("📊 CRAWL SUMMARY")
    logging.info("=" * 60)

    logging.info(f"\n✅ SUCCESS: {len(success_list)}")
    for u, n, t in success_list:
        logging.info(f"  - @{u} ({n}): {t}")

    logging.info(f"\n⏭️ SKIPPED: {len(skipped_list)}")
    for u, n, reason in skipped_list:
        logging.info(f"  - @{u} ({n}): {reason}")

    logging.info(f"\n❌ FAILED: {len(failed_list)}")
    for u, n, err in failed_list:
        logging.info(f"  - @{u} ({n}): {err}")


def main():
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT tt_link, tt_name FROM tt_group")
        groups = cur.fetchall()
    conn.close()

    success_list, failed_list, skipped_list = [], [], []

    for username, name in groups:
        status, detail = process_single_account(username, name)

        if status == "success":
            success_list.append((username, name, detail))
        elif status == "skipped":
            skipped_list.append((username, name, detail))
        else:
            failed_list.append((username, name, detail))

        random_delay(DELAY_MIN, DELAY_MAX + 30)

    log_summary(success_list, skipped_list, failed_list)


def create_trigger(scheduler_cfg: dict):
    schedule_type = scheduler_cfg.get("type", "interval")
    settings = scheduler_cfg.get("settings", {})

    if schedule_type == "interval":
        cfg = settings.get("interval", {})
        return IntervalTrigger(
            hours=cfg.get("hours", 0),
            minutes=cfg.get("minutes", 0),
            seconds=cfg.get("seconds", 0) or 3600
        )
    elif schedule_type == "cron":
        cfg = settings.get("cron", {})
        return CronTrigger(
            hour=cfg.get("hour", "*"),
            minute=cfg.get("minute", "0"),
            day_of_week=cfg.get("day_of_week", "*")
        )
    elif schedule_type == "date":
        cfg = settings.get("date", {})
        return DateTrigger(run_date=cfg.get("run_date"))

    raise ValueError(f"Unknown schedule type: {schedule_type}")


def run_scheduler():
    config = load_config()
    scheduler_cfg = config["scheduler"]

    if not scheduler_cfg.get("enabled", True):
        logging.info("Scheduler disabled")
        return

    scheduler = BlockingScheduler(timezone=scheduler_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
    trigger = create_trigger(scheduler_cfg)
    scheduler.add_job(main, trigger, id="tiktok_downloader", replace_existing=True)

    if scheduler_cfg.get("run_on_startup", False):
        logging.info("🚀 Running on startup...")
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
