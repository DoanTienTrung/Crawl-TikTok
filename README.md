# TikTok Audio Downloader

Tool tự động crawl và tải audio từ các kênh TikTok, hỗ trợ scheduler chạy định kỳ và tự động refresh cookies.

## Tính năng

- Tải audio từ video TikTok mới nhất của các kênh được theo dõi
- Tự động refresh cookies bằng Playwright khi session hết hạn
- Scheduler linh hoạt (interval, cron, date)
- Lọc bỏ livestream, chỉ tải video thường
- Rate limit handling với random delay
- Fallback subprocess khi API không hoạt động
- Lưu trữ vào PostgreSQL để tránh tải trùng

## Cấu trúc thư mục

```
Crawl_Tiktok/
├── tiktok_audio_downloader.py  # Script chính
├── cookie_refresher.py         # Module refresh cookies tự động
├── scheduler_config.json       # Cấu hình scheduler
├── requirements.txt            # Dependencies
├── db/
│   └── db_adapter.py           # Database adapter (PostgreSQL)
├── cookies/                    # Lưu cookies TikTok
├── browser_state/              # Lưu session Playwright
└── downloads/
    └── audio/                  # Audio đã tải
```

## Yêu cầu

- Python 3.8+
- PostgreSQL
- FFmpeg (để convert audio)
- Chromium (cho Playwright)

## Cài đặt

1. **Clone repository**
   ```bash
   git clone <repo-url>
   cd Crawl_Tiktok
   ```

2. **Tạo virtual environment**
   ```bash
   python -m venv venv

   # Windows
   venv\Scripts\activate

   # Linux/Mac
   source venv/bin/activate
   ```

3. **Cài đặt dependencies**
   ```bash
   pip install -r requirements.txt
   pip install playwright apscheduler python-dotenv
   playwright install chromium
   ```

4. **Cài đặt FFmpeg**
   - Windows: Tải từ [ffmpeg.org](https://ffmpeg.org/download.html) và thêm vào PATH
   - Linux: `sudo apt install ffmpeg`
   - Mac: `brew install ffmpeg`

5. **Cấu hình PostgreSQL**

   Tạo database và tables:
   ```sql
   CREATE DATABASE tiktok_crawler;

   -- Bảng danh sách kênh TikTok cần crawl
   CREATE TABLE tt_group (
       id SERIAL PRIMARY KEY,
       tt_link VARCHAR(255) NOT NULL,  -- username TikTok (không có @)
       tt_name VARCHAR(255)            -- tên hiển thị
   );

   -- Bảng lưu video đã tải
   CREATE TABLE yt_post (
       id SERIAL PRIMARY KEY,
       video_id VARCHAR(255),
       title TEXT,
       url VARCHAR(500) UNIQUE,
       audio_path VARCHAR(500),
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

6. **Cập nhật config database**

   Chỉnh sửa `db/db_adapter.py`:
   ```python
   DB_CONFIG = {
       "host": "localhost",
       "dbname": "tiktok_crawler",
       "user": "your_username",
       "password": "your_password",
       "port": 5432
   }
   ```

## Sử dụng

### Login TikTok lần đầu

Chạy cookie refresher để đăng nhập TikTok:
```bash
python cookie_refresher.py --login
```
Browser sẽ mở ra, bạn đăng nhập TikTok (QR code hoặc phone number). Session sẽ được lưu tự động.

### Thêm kênh cần crawl

```sql
INSERT INTO tt_group (tt_link, tt_name) VALUES
('username1', 'Tên kênh 1'),
('username2', 'Tên kênh 2');
```

### Chạy một lần

```bash
python tiktok_audio_downloader.py --once
```

### Chạy với scheduler

```bash
python tiktok_audio_downloader.py
```

## Cấu hình Scheduler

Chỉnh sửa `scheduler_config.json`:

### Chạy theo interval (mặc định)
```json
{
    "scheduler": {
        "enabled": true,
        "type": "interval",
        "settings": {
            "interval": {
                "hours": 1,
                "minutes": 0,
                "seconds": 0
            }
        },
        "run_on_startup": true,
        "timezone": "Asia/Ho_Chi_Minh"
    }
}
```

### Chạy theo cron
```json
{
    "scheduler": {
        "enabled": true,
        "type": "cron",
        "settings": {
            "cron": {
                "hour": "8,14,20",
                "minute": "0",
                "day_of_week": "mon-sun"
            }
        },
        "run_on_startup": false,
        "timezone": "Asia/Ho_Chi_Minh"
    }
}
```

### Chạy một lần vào thời điểm cụ thể
```json
{
    "scheduler": {
        "enabled": true,
        "type": "date",
        "settings": {
            "date": {
                "run_date": "2025-01-24 08:00:00"
            }
        },
        "timezone": "Asia/Ho_Chi_Minh"
    }
}
```

## Xử lý lỗi

### Cookies hết hạn
Tool sẽ tự động refresh cookies khi gặp lỗi auth. Nếu không thành công:
```bash
python cookie_refresher.py --force
```

### Rate limit
Tool tự động đợi 40-80 giây giữa các request. Khi gặp rate limit (429), sẽ đợi 5-15 phút.

### Kênh private/livestream
Các kênh đang livestream hoặc private sẽ được bỏ qua tự động.

## Log

Log được ghi vào `tiktok_crawl.log` và hiển thị trên console.

## License

MIT License
