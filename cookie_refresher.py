"""
Cookie Refresher - Tự động refresh TikTok cookies bằng Playwright
Khi cookies hết hạn hoặc bị rate limit, module này sẽ:
1. Mở browser với session đã lưu
2. Truy cập TikTok để refresh session
3. Export cookies mới ra file .txt
"""

import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright chưa được cài. Chạy: pip install playwright && playwright install chromium")

COOKIES_DIR = "cookies"
BROWSER_STATE_DIR = "browser_state"
DEFAULT_COOKIES_FILE = "tiktok_refreshed.txt"


def ensure_dirs():
    """Tạo các thư mục cần thiết"""
    os.makedirs(COOKIES_DIR, exist_ok=True)
    os.makedirs(BROWSER_STATE_DIR, exist_ok=True)


def cookies_to_netscape(cookies: list, domain: str = ".tiktok.com") -> str:
    """Chuyển cookies từ Playwright format sang Netscape format (cho yt-dlp)"""
    lines = ["# Netscape HTTP Cookie File", "# https://curl.haxx.se/rfc/cookie_spec.html", ""]

    for cookie in cookies:
        # Chỉ lấy cookies của TikTok
        cookie_domain = cookie.get("domain", "")
        if "tiktok" not in cookie_domain.lower():
            continue

        # Netscape format: domain, flag, path, secure, expiry, name, value
        domain = cookie_domain
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = cookie.get("path", "/")
        secure = "TRUE" if cookie.get("secure", False) else "FALSE"
        expiry = int(cookie.get("expires", 0))
        if expiry == -1 or expiry == 0:
            expiry = int(time.time()) + 86400 * 365  # 1 năm
        name = cookie.get("name", "")
        value = cookie.get("value", "")

        lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{name}\t{value}")

    return "\n".join(lines)


def refresh_cookies_playwright(headless: bool = False, timeout: int = 120) -> str:
    """
    Refresh cookies bằng Playwright

    Args:
        headless: True = chạy ngầm, False = hiện browser (cần khi login lần đầu)
        timeout: Thời gian chờ tối đa (giây)

    Returns:
        Path đến file cookies mới
    """
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright chưa được cài đặt. Chạy: pip install playwright && playwright install chromium")

    ensure_dirs()
    state_path = os.path.join(BROWSER_STATE_DIR, "tiktok_state.json")

    logging.info(f"🔄 Bắt đầu refresh cookies (headless={headless})...")

    with sync_playwright() as p:
        # Launch browser với state đã lưu (nếu có)
        browser_args = {
            "headless": headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
        }

        browser = p.chromium.launch(**browser_args)

        # Tạo context với state cũ hoặc mới
        context_args = {
            "viewport": {"width": 1280, "height": 720},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        if os.path.exists(state_path):
            context_args["storage_state"] = state_path
            logging.info("  → Dùng session đã lưu")
        else:
            logging.info("  → Chưa có session, cần login thủ công")
            if headless:
                logging.warning("  ⚠️ Chuyển sang non-headless để login")
                browser.close()
                browser = p.chromium.launch(headless=False, args=browser_args["args"])

        context = browser.new_context(**context_args)
        page = context.new_page()

        try:
            # Truy cập TikTok
            logging.info("  → Đang truy cập TikTok...")
            page.goto("https://www.tiktok.com/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)

            # Check đã login chưa
            logged_in = False
            try:
                # Thử tìm avatar hoặc profile icon (đã login)
                # TikTok có nhiều selector khác nhau tùy version
                login_selectors = [
                    '[data-e2e="profile-icon"]',
                    '[data-e2e="nav-profile"]',
                    'a[href*="/profile"]',
                    '[class*="avatar"]',
                    'img[class*="Avatar"]',
                    '[data-e2e="upload-icon"]',  # Nút upload chỉ hiện khi đã login
                ]
                for selector in login_selectors:
                    try:
                        el = page.locator(selector)
                        if el.count() > 0:
                            logged_in = True
                            logging.info(f"  ✓ Đã đăng nhập (detect: {selector})")
                            break
                    except:
                        continue

                if not logged_in:
                    # Fallback: check URL hoặc cookies
                    cookies = context.cookies()
                    tiktok_cookies = [c for c in cookies if "tiktok" in c.get("domain", "")]
                    auth_cookies = [c for c in tiktok_cookies if c.get("name") in ["sessionid", "sid_tt", "uid_tt"]]
                    if auth_cookies:
                        logged_in = True
                        logging.info("  ✓ Đã đăng nhập (detect qua cookies)")

            except Exception as e:
                logging.debug(f"Check login error: {e}")

            if not logged_in:
                logging.info("  → Chưa đăng nhập, cần login thủ công...")

            if not logged_in:
                # Mở popup login
                try:
                    login_btn = page.locator('[data-e2e="top-login-button"]')
                    if login_btn.count() > 0:
                        login_btn.first.click()
                        logging.info("  → Đã mở popup login")
                except Exception:
                    pass

                logging.info(f"  ⏳ Vui lòng đăng nhập trong {timeout}s...")
                logging.info("  💡 Tip: Dùng QR code hoặc phone number để login nhanh")

                # Chờ user login
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        # Check nhiều selector
                        for selector in ['[data-e2e="profile-icon"]', '[data-e2e="upload-icon"]', 'a[href*="/profile"]']:
                            try:
                                el = page.locator(selector)
                                if el.count() > 0:
                                    logged_in = True
                                    break
                            except:
                                continue

                        # Fallback check cookies
                        if not logged_in:
                            cookies = context.cookies()
                            auth_cookies = [c for c in cookies if c.get("name") in ["sessionid", "sid_tt"]]
                            if auth_cookies:
                                logged_in = True

                        if logged_in:
                            logging.info("  ✓ Đăng nhập thành công!")
                            break

                        time.sleep(2)
                        remaining = int(timeout - (time.time() - start_time))
                        if remaining > 0 and remaining % 10 == 0:
                            logging.info(f"  ⏳ Còn {remaining}s để đăng nhập...")
                    except Exception:
                        time.sleep(2)

                if not logged_in:
                    raise RuntimeError("Hết thời gian chờ đăng nhập")

            # Đợi thêm để cookies ổn định
            time.sleep(2)

            # Refresh session bằng cách scroll/interact nhẹ thay vì navigate
            logging.info("  → Đang refresh session...")
            try:
                page.mouse.wheel(0, 300)
                time.sleep(1)
            except:
                pass

            # Lấy cookies
            cookies = context.cookies()
            logging.info(f"  → Lấy được {len(cookies)} cookies")

            # Lưu state cho lần sau
            context.storage_state(path=state_path)
            logging.info(f"  → Đã lưu session vào {state_path}")

            # Export ra file Netscape
            netscape_content = cookies_to_netscape(cookies)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(COOKIES_DIR, f"tiktok_{timestamp}.txt")

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(netscape_content)

            # Cũng ghi đè file mặc định
            default_file = os.path.join(COOKIES_DIR, DEFAULT_COOKIES_FILE)
            with open(default_file, "w", encoding="utf-8") as f:
                f.write(netscape_content)

            logging.info(f"  ✅ Đã export cookies: {output_file}")
            logging.info(f"  ✅ Đã cập nhật: {default_file}")

            return default_file

        finally:
            context.close()
            browser.close()


def check_cookies_valid(cookies_file: str) -> bool:
    """Kiểm tra cookies có còn hạn không (dựa trên expiry)"""
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
                expiry = int(parts[4])
                if expiry < current_time:
                    expired_count += 1

        if total_count == 0:
            return False

        # Nếu > 50% cookies hết hạn → cần refresh
        expired_ratio = expired_count / total_count
        if expired_ratio > 0.5:
            logging.warning(f"⚠️ {expired_count}/{total_count} cookies đã hết hạn")
            return False

        return True

    except Exception as e:
        logging.error(f"Lỗi check cookies: {e}")
        return False


def auto_refresh_if_needed(force: bool = False) -> str:
    """
    Tự động refresh cookies nếu cần

    Args:
        force: True = bắt buộc refresh, False = chỉ refresh nếu hết hạn

    Returns:
        Path đến file cookies (mới hoặc cũ)
    """
    default_file = os.path.join(COOKIES_DIR, DEFAULT_COOKIES_FILE)

    if not force and check_cookies_valid(default_file):
        logging.info("✓ Cookies vẫn còn hạn")
        return default_file

    logging.info("🔄 Cookies cần refresh...")

    # Thử headless trước (nếu đã có session)
    state_path = os.path.join(BROWSER_STATE_DIR, "tiktok_state.json")
    if os.path.exists(state_path):
        try:
            return refresh_cookies_playwright(headless=True, timeout=60)
        except Exception as e:
            logging.warning(f"Headless refresh fail: {e}, thử non-headless...")

    # Fallback: mở browser để user login
    return refresh_cookies_playwright(headless=False, timeout=120)


# CLI interface
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if not PLAYWRIGHT_AVAILABLE:
        print("❌ Cần cài Playwright:")
        print("   pip install playwright")
        print("   playwright install chromium")
        sys.exit(1)

    force = "--force" in sys.argv
    headless = "--headless" in sys.argv

    if "--login" in sys.argv:
        # Mode login thủ công
        print("🔐 Mode login - mở browser để đăng nhập TikTok...")
        refresh_cookies_playwright(headless=False, timeout=300)
    else:
        # Auto refresh
        auto_refresh_if_needed(force=force)
