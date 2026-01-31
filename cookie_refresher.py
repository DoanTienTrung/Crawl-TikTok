import os
import time
import logging
from datetime import datetime

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

COOKIES_DIR = "cookies"
BROWSER_STATE_DIR = "browser_state"
DEFAULT_COOKIES_FILE = "tiktok_refreshed.txt"
STATE_FILE = "tiktok_state.json"

LOGIN_TIMEOUT = 120
HEADLESS_TIMEOUT = 60
COOKIE_EXPIRY_THRESHOLD = 0.5

AUTH_COOKIE_NAMES = ["sessionid", "sid_tt", "uid_tt"]


def ensure_dirs():
    os.makedirs(COOKIES_DIR, exist_ok=True)
    os.makedirs(BROWSER_STATE_DIR, exist_ok=True)


def cookies_to_netscape(cookies: list) -> str:
    lines = ["# Netscape HTTP Cookie File", ""]

    for cookie in cookies:
        if "tiktok" not in cookie.get("domain", "").lower():
            continue

        domain = cookie.get("domain", "")
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = cookie.get("path", "/")
        secure = "TRUE" if cookie.get("secure", False) else "FALSE"
        expiry = int(cookie.get("expires", 0))
        if expiry <= 0:
            expiry = int(time.time()) + 86400 * 365
        name = cookie.get("name", "")
        value = cookie.get("value", "")

        lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{name}\t{value}")

    return "\n".join(lines)


def is_logged_in_by_cookies(cookies: list) -> bool:
    tiktok_cookies = [c for c in cookies if "tiktok" in c.get("domain", "")]
    auth_cookies = [c for c in tiktok_cookies if c.get("name") in AUTH_COOKIE_NAMES]
    return len(auth_cookies) > 0


def wait_for_auth_cookies(context, timeout: int) -> bool:
    start_time = time.time()

    while time.time() - start_time < timeout:
        if is_logged_in_by_cookies(context.cookies()):
            return True

        remaining = int(timeout - (time.time() - start_time))
        if remaining > 0 and remaining % 10 == 0:
            logging.info(f"⏳ Waiting for login... {remaining}s remaining")

        time.sleep(2)

    return False


def save_cookies(context, state_path: str) -> str:
    cookies = context.cookies()
    logging.info(f"Got {len(cookies)} cookies")

    context.storage_state(path=state_path)

    netscape_content = cookies_to_netscape(cookies)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(COOKIES_DIR, f"tiktok_{timestamp}.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(netscape_content)

    default_file = os.path.join(COOKIES_DIR, DEFAULT_COOKIES_FILE)
    with open(default_file, "w", encoding="utf-8") as f:
        f.write(netscape_content)

    logging.info(f"Cookies saved: {output_file}")
    return default_file


def refresh_cookies_playwright(headless: bool = False, timeout: int = LOGIN_TIMEOUT, force_login: bool = False) -> str:
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright not installed")

    ensure_dirs()
    state_path = os.path.join(BROWSER_STATE_DIR, STATE_FILE)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )

        context_args = {"viewport": {"width": 1280, "height": 720}}
        if not force_login and os.path.exists(state_path):
            context_args["storage_state"] = state_path
            logging.info("Using saved session")

        context = browser.new_context(**context_args)
        page = context.new_page()

        try:
            page.goto("https://www.tiktok.com/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)

            if force_login:
                logging.info(f"Please login manually within {timeout}s...")
                try:
                    login_btn = page.locator('[data-e2e="top-login-button"]')
                    if login_btn.count() > 0:
                        login_btn.first.click()
                except Exception:
                    pass

                if not wait_for_auth_cookies(context, timeout):
                    raise RuntimeError("Login timeout - no auth cookies found")
            else:
                if not is_logged_in_by_cookies(context.cookies()):
                    logging.info(f"Please login within {timeout}s...")
                    if not wait_for_auth_cookies(context, timeout):
                        raise RuntimeError("Login timeout")

            logging.info("Login successful - auth cookies found")
            time.sleep(2)

            return save_cookies(context, state_path)

        finally:
            context.close()
            browser.close()


def check_cookies_valid(cookies_file: str) -> bool:
    if not os.path.exists(cookies_file):
        return False

    try:
        with open(cookies_file, "r", encoding="utf-8") as f:
            content = f.read()

        current_time = time.time()
        expired_count = 0
        total_count = 0

        for line in content.split("\n"):
            if line.startswith("#") or not line.strip():
                continue

            parts = line.split("\t")
            if len(parts) >= 7:
                total_count += 1
                if int(parts[4]) < current_time:
                    expired_count += 1

        if total_count == 0:
            return False

        return (expired_count / total_count) <= COOKIE_EXPIRY_THRESHOLD

    except Exception:
        return False


def auto_refresh_if_needed(force: bool = False) -> str:
    default_file = os.path.join(COOKIES_DIR, DEFAULT_COOKIES_FILE)

    if not force and check_cookies_valid(default_file):
        logging.info("Cookies still valid")
        return default_file

    logging.info("Cookies need refresh...")

    state_path = os.path.join(BROWSER_STATE_DIR, STATE_FILE)
    if os.path.exists(state_path):
        try:
            return refresh_cookies_playwright(headless=True, timeout=HEADLESS_TIMEOUT)
        except Exception as e:
            logging.warning(f"Headless refresh failed: {e}")

    return refresh_cookies_playwright(headless=False, timeout=LOGIN_TIMEOUT)


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright required: pip install playwright && playwright install chromium")
        sys.exit(1)

    if "--login" in sys.argv:
        refresh_cookies_playwright(headless=False, timeout=300, force_login=True)
    else:
        auto_refresh_if_needed(force="--force" in sys.argv)
