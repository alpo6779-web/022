import os
import logging
from datetime import datetime, timedelta
import random
import string
import urllib.parse as urlparse

logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://neondb_owner:npg_2p5PjGHVlsnR@ep-super-star-adez9eza-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require'

class PostgresDB:
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """اتصال به PostgreSQL با pg8000"""
        try:
            if DATABASE_URL:
                result = urlparse.urlparse(DATABASE_URL)
                
                database = result.path[1:]
                user = result.username
                password = result.password
                host = result.hostname
                port = result.port
                
                import pg8000
                self.conn = pg8000.connect(
                    user=user,
                    password=password,
                    host=host,
                    database=database,
                    port=port
                )
                logger.info("✅ Connected to PostgreSQL Neon successfully")
            else:
                logger.error("❌ DATABASE_URL not found")
        except Exception as e:
            logger.error(f"❌ Error connecting to PostgreSQL: {e}")
    
    def execute(self, query, params=None, fetchone=False, fetchall=False):
        """اجرای کوئری و بازگشت نتیجه"""
        try:
            if not self.conn:
                self._connect()
            
            cursor = self.conn.cursor()
            cursor.execute(query, params or ())
            
            if fetchone:
                result = cursor.fetchone()
            elif fetchall:
                result = cursor.fetchall()
            else:
                result = None
                self.conn.commit()
            
            cursor.close()
            return result
            
        except Exception as e:
            logger.error(f"❌ Query error: {e} - Query: {query}")
            if self.conn:
                self.conn.rollback()
            return None

pg_db = PostgresDB()

def get_db_connection():
    return pg_db.conn

def create_tables():
    try:
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                chat_id BIGINT PRIMARY KEY,
                forward_lock INTEGER DEFAULT 1,
                auto_delete_time INTEGER DEFAULT 30,
                allow_uploads INTEGER DEFAULT 0,
                force_view_reaction_enabled INTEGER DEFAULT 0,
                view_reaction_link TEXT,
                view_reaction_channel_id TEXT,
                welcome_message TEXT DEFAULT '👋 خوش آمدید، {user}! 😊',
                force_join_enabled INTEGER DEFAULT 1,
                force_join_link TEXT,
                force_join_channel_id TEXT
            )
        ''')
        
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                banned INTEGER DEFAULT 0,
                last_active TEXT,
                join_date TEXT DEFAULT CURRENT_TIMESTAMP,
                language TEXT DEFAULT 'fa'
            )
        ''')
        
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id BIGINT PRIMARY KEY,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                user_id BIGINT,
                file_type TEXT,
                message_id BIGINT,
                chat_id BIGINT,
                download_count INTEGER DEFAULT 0,
                upload_date TEXT DEFAULT CURRENT_TIMESTAMP,
                caption TEXT,
                original_filename TEXT
            )
        ''')
        
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS albums (
                album_id TEXT PRIMARY KEY,
                user_id BIGINT,
                message_ids TEXT,
                chat_id BIGINT,
                download_count INTEGER DEFAULT 0,
                upload_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        pg_db.execute('''
            CREATE TABLE IF NOT EXISTS custom_texts (
                key TEXT PRIMARY KEY,
                text TEXT
            )
        ''')
        
        admin_id = int(os.environ.get('ADMIN_ID', 123456789))
        pg_db.execute(
            'INSERT INTO bot_admins (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING', 
            (admin_id,)
        )
        
        logger.info("✅ همه جداول ایجاد شدند")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطا در ایجاد جداول: {e}")
        return False

def get_user_language(user_id):
    result = pg_db.execute(
        'SELECT language FROM users WHERE user_id=%s', 
        (user_id,), 
        fetchone=True
    )
    return result[0] if result else 'fa'

def update_user_language(user_id, lang_code):
    pg_db.execute(
        'INSERT INTO users (user_id, language, last_active) VALUES (%s, %s, %s) '
        'ON CONFLICT (user_id) DO UPDATE SET language=%s, last_active=%s',
        (user_id, lang_code, datetime.now().strftime('%Y-%m-%d'), lang_code, datetime.now().strftime('%Y-%m-%d'))
    )

def get_settings(chat_id):
    result = pg_db.execute(
        'SELECT chat_id, forward_lock, auto_delete_time, allow_uploads, force_view_reaction_enabled, '
        'view_reaction_link, view_reaction_channel_id, welcome_message, force_join_enabled, '
        'force_join_link, force_join_channel_id FROM settings WHERE chat_id=%s', 
        (chat_id,), 
        fetchone=True
    )
    if result:
        keys = ['chat_id', 'forward_lock', 'auto_delete_time', 'allow_uploads', 'force_view_reaction_enabled', 
                'view_reaction_link', 'view_reaction_channel_id', 'welcome_message', 'force_join_enabled', 
                'force_join_link', 'force_join_channel_id']
        return dict(zip(keys, result))
    return None

def create_or_get_settings(chat_id):
    settings = get_settings(chat_id)
    if not settings:
        pg_db.execute(
            'INSERT INTO settings (chat_id, allow_uploads, force_join_enabled) VALUES (%s, %s, %s)', 
            (chat_id, 0, 1)
        )
        return get_settings(chat_id)
    return settings

def update_setting(chat_id, key, value):
    pg_db.execute(
        f'UPDATE settings SET {key}=%s WHERE chat_id=%s', 
        (value, chat_id)
    )

def get_custom_text(key, lang_code='fa'):
    result = pg_db.execute(
        'SELECT text FROM custom_texts WHERE key=%s', 
        (key,), 
        fetchone=True
    )
    if result:
        return result[0]
    
    from aplod import LANGUAGES
    return LANGUAGES.get(lang_code, LANGUAGES['fa']).get(key, f"متن پیش‌فرض برای {key}")

def update_custom_text(key, text):
    pg_db.execute(
        'INSERT INTO custom_texts (key, text) VALUES (%s, %s) '
        'ON CONFLICT (key) DO UPDATE SET text=%s',
        (key, text, text)
    )

def add_user(user_id):
    pg_db.execute(
        'INSERT INTO users (user_id, last_active) VALUES (%s, %s) '
        'ON CONFLICT (user_id) DO UPDATE SET last_active=%s',
        (user_id, datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d'))
    )

def is_admin(user_id):
    result = pg_db.execute(
        'SELECT user_id FROM bot_admins WHERE user_id=%s', 
        (user_id,), 
        fetchone=True
    )
    return result is not None

def get_all_admins():
    result = pg_db.execute(
        'SELECT user_id FROM bot_admins', 
        fetchall=True
    )
    return [row[0] for row in result] if result else []

def add_admin(user_id):
    pg_db.execute(
        'INSERT INTO bot_admins (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING', 
        (user_id,)
    )

def remove_admin(user_id):
    pg_db.execute(
        'DELETE FROM bot_admins WHERE user_id=%s', 
        (user_id,)
    )

def get_all_users():
    result = pg_db.execute(
        'SELECT user_id FROM users', 
        fetchall=True
    )
    return [row[0] for row in result] if result else []

def get_admin_count():
    result = pg_db.execute('SELECT COUNT(*) FROM bot_admins', fetchone=True)
    return result[0] if result else 0

def get_total_users():
    result = pg_db.execute('SELECT COUNT(*) FROM users', fetchone=True)
    return result[0] if result else 0

def get_active_users_today():
    today = datetime.now().strftime('%Y-%m-%d')
    result = pg_db.execute(
        'SELECT COUNT(DISTINCT user_id) FROM users WHERE last_active=%s', 
        (today,), 
        fetchone=True
    )
    return result[0] if result else 0

def update_user_activity(user_id):
    pg_db.execute(
        'UPDATE users SET last_active=%s WHERE user_id=%s', 
        (datetime.now().strftime('%Y-%m-%d'), user_id)
    )

def get_new_users_count(days=1):
    past_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    result = pg_db.execute(
        'SELECT COUNT(*) FROM users WHERE join_date >= %s', 
        (past_date,), 
        fetchone=True
    )
    return result[0] if result else 0

def get_total_files():
    result = pg_db.execute('SELECT COUNT(*) FROM files', fetchone=True)
    return result[0] if result else 0

def get_total_albums():
    result = pg_db.execute('SELECT COUNT(*) FROM albums', fetchone=True)
    return result[0] if result else 0

def get_file_info(file_id):
    result = pg_db.execute(
        'SELECT file_type, message_id, chat_id, download_count, caption, original_filename FROM files WHERE file_id=%s', 
        (file_id,), 
        fetchone=True
    )
    if result:
        keys = ['file_type', 'message_id', 'chat_id', 'download_count', 'caption', 'original_filename']
        return dict(zip(keys, result))
    return None

def update_file_download_count(file_id):
    pg_db.execute(
        'UPDATE files SET download_count = download_count + 1 WHERE file_id=%s', 
        (file_id,)
    )

def save_file_info(file_id, user_id, file_type, message_id, chat_id, caption=None, original_filename=None):
    pg_db.execute(
        'INSERT INTO files (file_id, user_id, file_type, message_id, chat_id, caption, original_filename) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (file_id, user_id, file_type, message_id, chat_id, caption, original_filename)
    )

def delete_file_info(file_id):
    pg_db.execute('DELETE FROM files WHERE file_id=%s', (file_id,))

def get_album_info(album_id):
    result = pg_db.execute(
        'SELECT user_id, message_ids, chat_id, download_count FROM albums WHERE album_id=%s', 
        (album_id,), 
        fetchone=True
    )
    if result:
        keys = ['user_id', 'message_ids', 'chat_id', 'download_count']
        return dict(zip(keys, result))
    return None

def update_album_download_count(album_id):
    pg_db.execute(
        'UPDATE albums SET download_count = download_count + 1 WHERE album_id=%s', 
        (album_id,)
    )

def save_album_info(album_id, user_id, message_ids, chat_id):
    pg_db.execute(
        'INSERT INTO albums (album_id, user_id, message_ids, chat_id) VALUES (%s, %s, %s, %s)',
        (album_id, user_id, message_ids, chat_id)
    )

def delete_album_info(album_id):
    pg_db.execute('DELETE FROM albums WHERE album_id=%s', (album_id,))

def is_user_banned(user_id):
    result = pg_db.execute(
        'SELECT banned FROM users WHERE user_id=%s', 
        (user_id,), 
        fetchone=True
    )
    return result and result[0] == 1

def ban_user(user_id):
    pg_db.execute('UPDATE users SET banned=1 WHERE user_id=%s', (user_id,))

def unban_user(user_id):
    pg_db.execute('UPDATE users SET banned=0 WHERE user_id=%s', (user_id,))

def search_files(query):
    search_query = f"%{query}%"
    result = pg_db.execute(
        'SELECT file_id, caption, original_filename, upload_date FROM files '
        'WHERE file_id LIKE %s OR caption LIKE %s OR original_filename LIKE %s '
        'ORDER BY upload_date DESC LIMIT 20',
        (search_query, search_query, search_query),
        fetchall=True
    )
    return result if result else []

def generate_unique_id(length=16):
    import random
    import string
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def get_channel_id_from_link(link):
    import telebot
    link = link.strip()
    if 't.me/' in link:
        if 't.me/c/' in link:
            try:
                parts = link.split('/c/')
                if len(parts) > 1:
                    channel_id_num = parts[1].split('/')[0]
                    return int(f"-100{channel_id_num}")
            except (IndexError, ValueError) as e:
                logging.error(f"Error extracting channel ID from link (private): {link} - {e}")
                return None
        else:
            try:
                channel_handle = link.split('/')[-1]
                if channel_handle.startswith('@'):
                    return channel_handle
                else:
                    return f"@{channel_handle}"
            except IndexError as e:
                logging.error(f"Invalid link format: {link} - {e}")
                return None
    elif link.startswith('@'):
        return link
    logging.warning(f"Invalid or unsupported link format provided for channel: {link}")
    return None

def is_user_member(user_id, channel_id):
    import telebot
    if not channel_id:
        return False
    try:
        from aplod import bot
        status = bot.get_chat_member(channel_id, user_id).status
        return status in ['member', 'administrator', 'creator']
    except telebot.apihelper.ApiTelegramException as e:
        logging.error(f"خطا در بررسی عضویت کاربر {user_id} در کانال {channel_id}: {e}")
        if "chat not found" in str(e).lower() or "bad request: chat not found" in str(e).lower():
            logging.warning(f"The specified channel/group '{channel_id}' was not found.")
        elif "user not in chat" in str(e).lower():
            return False
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while checking membership for {user_id} in {channel_id}: {e}")
        return False
