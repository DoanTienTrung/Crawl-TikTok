import os
import sys
import json
import time
import random
import subprocess
import yt_dlp
import logging
import traceback
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from db import db_adapter as db

# Import cookie refresher
try:
    from cookie_refresher import auto_refresh_if_needed, PLAYWRIGHT_AVAILABLE
    COOKIE_REFRESH_ENABLED = PLAYWRIGHT_AVAILABLE
except ImportError:
    COOKIE_REFRESH_ENABLED = False
    logging.warning("Cookie refresher không khả dụng")

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
COOKIES_FILE = os.path.join(COOKIES_DIR, "tiktok_refreshed.txt")  # Cookies từ Playwright
CONFIG_FILE = "scheduler_config.json"

# Đếm số lỗi auth liên tiếp để trigger refresh cookies
AUTH_ERROR_COUNT = 0
AUTH_ERROR_THRESHOLD = 1  # Refresh ngay khi gặp lỗi auth

def get_cookies_file():
    """Lấy file cookies, tự động refresh nếu chưa có"""
    if os.path.exists(COOKIES_FILE):
        return COOKIES_FILE

    # Chưa có cookies → thử refresh
    if COOKIE_REFRESH_ENABLED:
        logging.info("🔄 Chưa có cookies, đang refresh bằng Playwright...")
        try:
            auto_refresh_if_needed(force=True)
            if os.path.exists(COOKIES_FILE):
                return COOKIES_FILE
        except Exception as e:
            logging.error(f"❌ Refresh cookies thất bại: {e}")

    logging.warning("⚠️ Không có cookies, crawl có thể fail!")
    return None

def try_refresh_cookies():
    """Thử refresh cookies nếu có Playwright"""
    global AUTH_ERROR_COUNT
    if not COOKIE_REFRESH_ENABLED:
        logging.warning("⚠️ Không thể refresh cookies (Playwright chưa cài)")
        return False

    try:
        logging.info("🔄 Đang refresh cookies bằng Playwright...")
        new_cookies = auto_refresh_if_needed(force=True)
        if new_cookies and os.path.exists(COOKIES_FILE):
            AUTH_ERROR_COUNT = 0
            logging.info("✅ Refresh cookies thành công!")
            return True
    except Exception as e:
        logging.error(f"❌ Refresh cookies thất bại: {e}")
    return False

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================= RESOLVE TARGET =================
def resolve_tiktok_target(username: str) -> str:
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

        cookies = get_cookies_file()
        if cookies:
            ydl_opts["cookies"] = cookies

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
                        return f"tiktokuser:{sec_uid}"
        except Exception:
            continue

    # ❌ Không lấy được → fallback WEB
    logging.warning(f"⚠️ @{username} không lấy được secUid → fallback WEB")
    return profile_url

# ================= HELPER: Check if entry is livestream =================
def is_livestream(entry: dict) -> bool:
    """Kiểm tra entry có phải livestream không"""
    if not entry:
        return False

    # Check các field phổ biến của livestream
    if entry.get("is_live") is True:
        return True
    if entry.get("live_status") in ("is_live", "is_upcoming", "post_live"):
        return True
    if entry.get("_type") == "live":
        return True

    # Check URL chứa /live/
    url = entry.get("url", "") or entry.get("webpage_url", "") or ""
    if "/live/" in url.lower():
        return True

    # Check title chứa keyword livestream
    title = (entry.get("title") or "").lower()
    if any(kw in title for kw in ["livestream", "live stream", "đang live", "live now"]):
        return True

    return False

def filter_non_livestream(entries: list) -> list:
    """Lọc bỏ livestream, chỉ giữ video thường"""
    if not entries:
        return []

    filtered = []
    for entry in entries:
        if entry and not is_livestream(entry):
            filtered.append(entry)
        elif entry and is_livestream(entry):
            logging.info(f"  ⏭️ Skip livestream: {entry.get('title', 'N/A')[:50]}")

    return filtered

# ================= GET LATEST VIDEO (via subprocess for auth) =================
def get_latest_video_url_subprocess(username: str, cookies_file: str):
    """Dùng subprocess gọi yt-dlp command line (hoạt động với cookies)"""
    target = f"https://www.tiktok.com/@{username}"

    cmd = [
        "yt-dlp",
        "--cookies", cookies_file,
        "--skip-download",
        "--dump-json",
        "--flat-playlist",
        "--playlist-items", "1-10",
        "--extractor-args", "tiktok:skip=api",
        target
    ]

    logging.info(f"  → Subprocess: yt-dlp với cookies {os.path.basename(cookies_file)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp subprocess failed: {result.stderr[:200]}")

    # Parse JSON lines (mỗi video 1 line)
    entries = []
    for line in result.stdout.strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    # Filter bỏ livestream
    entries = filter_non_livestream(entries)
    return entries

# ================= GET LATEST VIDEO =================
def get_latest_video_url(username: str):
    # Thử cách bình thường trước (Python library)
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

    cookies = get_cookies_file()
    if target.startswith("tiktokuser:"):
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "web"}}
        logging.info("  → Dùng TikTok API")
    else:
        ydl_opts["extractor_args"] = {"tiktok": {"skip": "api"}}
        logging.info("  → Dùng WEB")

    if cookies:
        ydl_opts["cookies"] = cookies

    last_error = "Không tìm thấy video"  # Default error
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target, download=False)
            entries = info.get("entries", [])

        if entries:
            # Filter bỏ livestream
            entries = filter_non_livestream(entries)

            valid = [e for e in entries if e.get("timestamp") and not e.get("is_pinned")]
            if valid:
                latest = max(valid, key=lambda e: e["timestamp"])
                return f"https://www.tiktok.com/@{username}/video/{latest['id']}", latest.get("title", ""), False

    except Exception as e:
        last_error = str(e)  # Lưu lại error gốc
        error_str = last_error.lower()
        auth_keywords = ["private", "login", "sign in", "log in", "comfortable", "embedding disabled"]
        if any(kw in error_str for kw in auth_keywords):
            logging.warning(f"⚠️ Cần auth, thử fallback subprocess...")
        else:
            logging.warning(f"⚠️ Lỗi: {last_error[:100]}, thử fallback subprocess...")

    # FALLBACK: Thử subprocess với cookies
    cookies = get_cookies_file()
    if cookies:
        logging.info(f"🔄 Fallback: subprocess với cookies cho @{username}")
        try:
            entries = get_latest_video_url_subprocess(username, cookies)
            if entries:
                valid = [e for e in entries if e.get("timestamp") and not e.get("is_pinned")]
                if valid:
                    latest = max(valid, key=lambda e: e["timestamp"])
                    return f"https://www.tiktok.com/@{username}/video/{latest['id']}", latest.get("title", ""), True
        except Exception as e2:
            last_error = str(e2)  # Cập nhật error từ subprocess
            logging.error(f"❌ Subprocess cũng fail: {last_error[:100]}")

    # Preserve error gốc để AUTH_ERROR_COUNT hoạt động đúng
    raise RuntimeError(f"Không lấy được video cho @{username}: {last_error[:150]}")




def download_audio_subprocess(video_url: str, video_id: str, cookies_file: str):
    """Download audio bằng subprocess (hoạt động với auth accounts)"""
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

    logging.info(f"  → Download subprocess với {os.path.basename(cookies_file)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        raise RuntimeError(f"Download subprocess failed: {result.stderr[:200]}")

    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")


def download_audio(video_url: str, video_id: str, use_subprocess: bool = False):
    os.makedirs(AUDIO_DIR, exist_ok=True)
    cookies = get_cookies_file()

    # Nếu cần subprocess (auth account)
    if use_subprocess:
        if cookies:
            return download_audio_subprocess(video_url, video_id, cookies)
        else:
            raise RuntimeError("Cần cookies để download auth account!")

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
    }

    if cookies:
        ydl_opts["cookies"] = cookies

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    logging.info(f"✅ Download thành công")
    return os.path.join(AUDIO_DIR, f"{video_id}.mp3")

def main():
    global AUTH_ERROR_COUNT
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT tt_link, tt_name FROM tt_group")
        groups = cur.fetchall()
    conn.close()

    success_list = []
    failed_list = []
    skipped_list = []

    # Kiểm tra cookies khi bắt đầu
    if not get_cookies_file():
        logging.warning("⚠️ Không có cookies → crawl có thể fail!")

    for username, name in groups:
        username = username.replace("@", "")
        start = time.time()

        logging.info(f"\n🎵 Xử lý: {name}")
        logging.info(f"⏳ Đợi random 40-50s để tránh rate limit...")
        time.sleep(random.randint(40, 50))

        try:
            # get_latest_video_url trả về (url, title, used_subprocess)
            # used_subprocess = True nếu phải dùng fallback subprocess (account cần auth)
            video_url, title, used_subprocess = get_latest_video_url(username)

            if not db.validate_yt_post(title, video_url):
                logging.info("⏭️ Đã tồn tại, bỏ qua")
                skipped_list.append((username, name, "Đã tồn tại"))
                time.sleep(random.randint(40, 50))
                continue

            video_id_db = f"t_{username}_{int(time.time())}"
            # Dùng subprocess nếu đã phải fallback ở bước lấy video list
            audio_path = download_audio(video_url, video_id_db, use_subprocess=used_subprocess)

            db.insert_yt_post(video_id_db, title, video_url, audio_path)

            elapsed = time.time() - start
            logging.info(f"✅ Thành công: {audio_path} ({elapsed:.1f}s)")
            success_list.append((username, name, title[:50]))
            AUTH_ERROR_COUNT = 0  # Reset counter khi thành công
            logging.info(f"⏳ Đợi random 40-50s trước khi tiếp tục...")
            time.sleep(random.randint(40, 50))

        except Exception as e:
            elapsed = time.time() - start
            error_str = str(e)
            logging.error(f"❌ Lỗi chi tiết cho {username}: {repr(e)} - {error_str} ({elapsed:.1f}s)")
            traceback.print_exc(file=sys.stdout)
            logging.error(traceback.format_exc())

            # Detect lỗi auth để trigger refresh cookies
            auth_keywords = ["private", "login", "sign in", "auth", "embedding disabled", "comfortable"]
            is_auth_error = any(kw in error_str.lower() for kw in auth_keywords)

            if is_auth_error:
                AUTH_ERROR_COUNT += 1
                logging.warning(f"⚠️ Lỗi auth #{AUTH_ERROR_COUNT}/{AUTH_ERROR_THRESHOLD}")

                if AUTH_ERROR_COUNT >= AUTH_ERROR_THRESHOLD:
                    logging.info("🔄 Đạt ngưỡng lỗi auth, thử refresh cookies...")
                    if try_refresh_cookies():
                        logging.info("✅ Đã refresh cookies, tiếp tục crawl...")
                    else:
                        logging.warning("⚠️ Không refresh được, tiếp tục với cookies cũ...")

            if "Unable to extract secondary user ID" in error_str:
                logging.warning(f"⚠️ Bỏ qua kênh {username}: User có thể đang livestream, profile private hoặc bị block")
                skipped_list.append((username, name, "Có thể đang livestream/private"))
            elif "429" in error_str or "Too Many Requests" in error_str:
                wait = random.randint(300, 900)
                logging.warning(f"⚠️ Rate limit toàn cục, đợi {wait//60} phút...")
                time.sleep(wait)
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