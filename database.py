import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

def get_db_connection():
    """ایجاد اتصال به دیتابیس PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host='ep-super-star-adez9eza-pooler.c-2.us-east-1.aws.neon.tech',
            database='neondb',
            user='neondb_owner',
            password='npg_2p5PjGHVlsnR',
            port='5432',
            sslmode='require'
        )
        return conn
    except Exception as e:
        logger.error(f"خطا در اتصال به دیتابیس: {e}")
        return None

def create_tables():
    """ایجاد جداول در PostgreSQL"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # جدول تنظیمات
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        chat_id BIGINT PRIMARY KEY,
                        forward_lock BOOLEAN DEFAULT TRUE,
                        auto_delete_time INTEGER DEFAULT 30,
                        allow_uploads BOOLEAN DEFAULT FALSE,
                        force_view_reaction_enabled BOOLEAN DEFAULT FALSE,
                        view_reaction_link TEXT,
                        view_reaction_channel_id TEXT,
                        welcome_message TEXT DEFAULT '👋 خوش آمدید، {user}! 😊',
                        force_join_enabled BOOLEAN DEFAULT TRUE,
                        force_join_link TEXT,
                        force_join_channel_id TEXT
                    )
                ''')
                
                # جدول کاربران
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        banned BOOLEAN DEFAULT FALSE,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        language TEXT DEFAULT 'fa'
                    )
                ''')
                
                # جدول ادمین‌ها
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS bot_admins (
                        user_id BIGINT PRIMARY KEY,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # جدول فایل‌ها
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS files (
                        file_id TEXT PRIMARY KEY,
                        user_id BIGINT,
                        file_type TEXT,
                        message_id BIGINT,
                        chat_id BIGINT,
                        download_count INTEGER DEFAULT 0,
                        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        caption TEXT,
                        original_filename TEXT
                    )
                ''')
                
                # جدول آلبوم‌ها
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS albums (
                        album_id TEXT PRIMARY KEY,
                        user_id BIGINT,
                        message_ids TEXT,
                        chat_id BIGINT,
                        download_count INTEGER DEFAULT 0,
                        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # جدول متون سفارشی
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS custom_texts (
                        key TEXT PRIMARY KEY,
                        text TEXT
                    )
                ''')
                
                # اضافه کردن ادمین اصلی
                admin_id = int(os.getenv('ADMIN_ID', 5959954413))
                cursor.execute('INSERT INTO bot_admins (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING', (admin_id,))
                
            conn.commit()
        logger.info("✅ جداول دیتابیس با موفقیت ایجاد شدند")
    except Exception as e:
        logger.error(f"❌ خطا در ایجاد جداول: {e}")

# توابع کاربران
def get_user_language(user_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SELECT language FROM users WHERE user_id=%s', (user_id,))
            row = cursor.fetchone()
            return row['language'] if row else 'fa'

def update_user_language(user_id, lang_code):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO users (user_id, language) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET language=%s', 
                         (user_id, lang_code, lang_code))
        conn.commit()

def add_user(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO users (user_id, last_active) VALUES (%s, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO UPDATE SET last_active=CURRENT_TIMESTAMP', 
                         (user_id,))
        conn.commit()

def update_user_activity(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('UPDATE users SET last_active=CURRENT_TIMESTAMP WHERE user_id=%s', (user_id,))
        conn.commit()

def is_user_banned(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT banned FROM users WHERE user_id=%s', (user_id,))
            row = cursor.fetchone()
            return row[0] if row else False

def ban_user(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('UPDATE users SET banned=TRUE WHERE user_id=%s', (user_id,))
        conn.commit()

def unban_user(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('UPDATE users SET banned=FALSE WHERE user_id=%s', (user_id,))
        conn.commit()

# توابع ادمین‌ها
def is_admin(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT user_id FROM bot_admins WHERE user_id=%s', (user_id,))
            return cursor.fetchone() is not None

def get_all_admins():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT user_id FROM bot_admins')
            return [row[0] for row in cursor.fetchall()]

def add_admin(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO bot_admins (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING', (user_id,))
        conn.commit()

def remove_admin(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM bot_admins WHERE user_id=%s', (user_id,))
        conn.commit()

def get_admin_count():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM bot_admins')
            return cursor.fetchone()[0]

# توابع تنظیمات
def get_settings(chat_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SELECT * FROM settings WHERE chat_id=%s', (chat_id,))
            return cursor.fetchone()

def create_or_get_settings(chat_id):
    settings = get_settings(chat_id)
    if not settings:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('INSERT INTO settings (chat_id, allow_uploads, force_join_enabled) VALUES (%s, %s, %s)', 
                             (chat_id, False, True))
            conn.commit()
        return get_settings(chat_id)
    return settings

def update_setting(chat_id, key, value):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'UPDATE settings SET {key}=%s WHERE chat_id=%s', (value, chat_id))
        conn.commit()

# توابع متون سفارشی
def get_custom_text(key, lang_code='fa'):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT text FROM custom_texts WHERE key=%s', (key,))
            row = cursor.fetchone()
            return row[0] if row else None

def update_custom_text(key, text):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO custom_texts (key, text) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET text=%s', 
                         (key, text, text))
        conn.commit()

# توابع فایل‌ها
def save_file_info(file_id, user_id, file_type, message_id, chat_id, caption=None, original_filename=None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO files (file_id, user_id, file_type, message_id, chat_id, caption, original_filename) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (file_id, user_id, file_type, message_id, chat_id, caption, original_filename))
        conn.commit()

def get_file_info(file_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SELECT * FROM files WHERE file_id=%s', (file_id,))
            return cursor.fetchone()

def update_file_download_count(file_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('UPDATE files SET download_count = download_count + 1 WHERE file_id=%s', (file_id,))
        conn.commit()

def delete_file_info(file_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM files WHERE file_id=%s', (file_id,))
        conn.commit()

def search_files(query):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            search_query = f"%{query}%"
            cursor.execute('''
                SELECT file_id, caption, original_filename, upload_date 
                FROM files 
                WHERE file_id LIKE %s OR caption LIKE %s OR original_filename LIKE %s 
                ORDER BY upload_date DESC LIMIT 20
            ''', (search_query, search_query, search_query))
            return cursor.fetchall()

def get_total_files():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM files')
            return cursor.fetchone()[0]

# توابع آلبوم‌ها
def save_album_info(album_id, user_id, message_ids, chat_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO albums (album_id, user_id, message_ids, chat_id) 
                VALUES (%s, %s, %s, %s)
            ''', (album_id, user_id, message_ids, chat_id))
        conn.commit()

def get_album_info(album_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SELECT * FROM albums WHERE album_id=%s', (album_id,))
            return cursor.fetchone()

def update_album_download_count(album_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('UPDATE albums SET download_count = download_count + 1 WHERE album_id=%s', (album_id,))
        conn.commit()

def delete_album_info(album_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM albums WHERE album_id=%s', (album_id,))
        conn.commit()

def get_total_albums():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM albums')
            return cursor.fetchone()[0]

# توابع آمار
def get_all_users():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT user_id FROM users')
            return [row[0] for row in cursor.fetchall()]

def get_total_users():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM users')
            return cursor.fetchone()[0]

def get_active_users_today():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users WHERE DATE(last_active)=%s', (today,))
            return cursor.fetchone()[0]

def get_new_users_count(days=1):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            past_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) FROM users WHERE join_date >= %s', (past_date,))
            return cursor.fetchone()[0]
