import yt_dlp
import os

COOKIES_FILE = "cookies_loi.txt"

def test_cookies():
    """Test xem cookies có load được và TikTok có truy cập được không"""

    # 1. Kiểm tra file cookies tồn tại
    if not os.path.exists(COOKIES_FILE):
        print(f"❌ File {COOKIES_FILE} không tồn tại!")
        return False

    # 2. Kiểm tra nội dung cookies
    with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
        if not content.strip():
            print(f"❌ File {COOKIES_FILE} rỗng!")
            return False

        lines = content.strip().split('\n')
        print(f"✅ File cookies có {len(lines)} dòng")

        # Kiểm tra format Netscape
        if not any('tiktok.com' in line for line in lines):
            print("⚠️ Cookies có vẻ không chứa domain tiktok.com")

    # 3. Test với yt-dlp
    print("\n🧪 Test 1: Truy cập profile TikTok với cookies...")
    test_url = "https://www.tiktok.com/@tiktokvn"

    ydl_opts = {
        "quiet": False,
        "extract_flat": True,
        "skip_download": True,
        "cookies": COOKIES_FILE,
        "verbose": True,  # Bật verbose để xem chi tiết
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(test_url, download=False)
            print(f"\n✅ Thành công! Lấy được {len(info.get('entries', []))} entries")
            return True
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        return False

def test_without_cookies():
    """Test không dùng cookies"""
    print("\n🧪 Test 2: Truy cập TikTok KHÔNG dùng cookies...")
    test_url = "https://www.tiktok.com/@tiktokvn"

    ydl_opts = {
        "quiet": False,
        "extract_flat": True,
        "skip_download": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(test_url, download=False)
            print(f"\n✅ Không cần cookies cũng chạy được!")
            return True
    except Exception as e:
        print(f"\n❌ Không cookies cũng bị lỗi: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("KIỂM TRA COOKIES & KẾT NỐI TIKTOK")
    print("=" * 60)

    result1 = test_cookies()
    result2 = test_without_cookies()

    print("\n" + "=" * 60)
    print("KẾT LUẬN:")
    print("=" * 60)

    if result1:
        print("✅ Cookies hoạt động tốt")
    else:
        print("❌ Cookies có vấn đề hoặc TikTok chặn yt-dlp")

    if not result1 and not result2:
        print("\n⚠️ TikTok đã CHẶN CỨNG yt-dlp!")
        print("Giải pháp:")
        print("  1. Đợi vài giờ/ngày để TikTok bỏ chặn IP")
        print("  2. Đổi IP (VPN, đổi mạng)")
        print("  3. Chuyển sang Playwright/Selenium")
        print("  4. Dùng TikTok API không chính thức")
