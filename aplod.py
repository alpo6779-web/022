import telebot
from telebot import types
import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import random
import string
import os
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی
load_dotenv()

# --- تنظیمات اولیه ---
TOKEN = os.getenv('TELEGRAM_TOKEN', '8342748520:AAHaLxjLBY4tZGD1nYDcu_PJDbc34zFB4Xs')
ADMIN_ID = int(os.getenv('ADMIN_ID', 5959954413))
MAX_MESSAGE_LENGTH = 4096

# ایمپورت دیتابیس جدید PostgreSQL
from database import (
    get_db_connection, create_tables, get_user_language, update_user_language,
    add_user, update_user_activity, is_user_banned, ban_user, unban_user,
    is_admin, get_all_admins, add_admin, remove_admin, get_admin_count,
    get_settings, create_or_get_settings, update_setting,
    get_custom_text, update_custom_text,
    save_file_info, get_file_info, update_file_download_count, delete_file_info,
    search_files, get_total_files,
    save_album_info, get_album_info, update_album_download_count, delete_album_info, get_total_albums,
    get_all_users, get_total_users, get_active_users_today, get_new_users_count
)

# --- تنظیمات لاگ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- راه‌اندازی ربات و دیتابیس ---
bot = telebot.TeleBot(TOKEN, parse_mode='Markdown')
scheduler = BackgroundScheduler()
scheduler.start()
user_states = {}  # مدیریت وضعیت‌های کاربران
album_upload_data = {}  # ذخیره فایل‌های آلبوم

# --- پشتیبانی از زبان‌ها ---
LANGUAGES = {
    'fa': {
        'welcome_admin': 'به پنل مدیریت ربات خوش آمدید 👨‍💻',
        'welcome_user': '👋 سلام! به ربات قدرتمند ما خوش آمدید! 😊',
        'file_found': '✅ فایل با موفقیت پیدا شد! 🗂️',
        'file_not_found': '❌ فایل مورد نظر یافت نشد. شاید حذف شده باشد یا آیدی اشتباه است. 😕',
        'upload_album_success': '✅ فایل‌ها با موفقیت آپلود و لینک گروهی ایجاد شد! 🔗',
        'upload_link_single': '🔗 لینک دانلود شما:\n`https://t.me/{bot_username}?start={file_id}`\n\nاین فایل ظرف {seconds} ثانیه دیگر غیر فعال خواهد شد.\n\nبرای دریافت مجدد، از دکمه زیر استفاده کنید:',
        'upload_album_link': '🔗 لینک دانلود آلبوم شما:\n`https://t.me/{bot_username}?start={album_id}`\n\nاین آلبوم ظرف {seconds} ثانیه دیگر غیر فعال خواهد شد.\n\nبرای دریافت مجدد، از دکمه زیر استفاده کنید:',
        'album_upload_add': 'فایل اضافه شد. تعداد فایل‌های آپلود شده: {current}/{total}',
        'no_valid_id': '❌ لطفاً یک آیدی عددی معتبر وارد کنید! 🔢',
        'cancel': '✅ عملیات لغو شد!',
        'settings_menu': '⚙️ منوی تنظیمات پیشرفته ربات',
        'admin_panel': 'به پنل مدیریت خوش آمدید.',
        'user_menu': 'منوی اصلی کاربران',
        'back_to_main_menu': '🔙 بازگشت به منوی اصلی',
        'back_to_settings_menu': '🔙 بازگشت به تنظیمات',
        'back_to_text_menu': '🔙 بازگشت به منوی ویرایش متون',
        'back_to_user_management': '🔙 بازگشت به مدیریت کاربران',
        'back_to_admin_management': '🔙 بازگشت به مدیریت ادمین',
        'back_to_view_reaction_menu': '🔙 بازگشت به تنظیمات قفل مشاهده/واکنش',
        'back_to_file_menu': '🔙 بازگشت به مدیریت فایل‌ها',
        'back_to_album_upload': '🔙 بازگشت به آپلود گروهی',
        'btn_change_text': '✍️ ویرایش متون ربات',
        'btn_auto_delete_individual': '⏳ تنظیمات حذف خودکار فایل',
        'btn_add_admin': '👨‍💼 مدیریت ادمین‌ها',
        'btn_view_reaction_lock': '👁️‍🗨️ قفل مشاهده/واکنش',
        'btn_forward_lock': '🚫 قفل فوروارد رسانه',
        'btn_back': '🔙 برگشت',
        'btn_manage_users': '👥 مدیریت کاربران',
        'btn_view_files': '🗃️ مدیریت فایل‌ها',
        'btn_remove_admin': '🗑️ حذف ادمین',
        'btn_file_search': '🔍 جستجوی فایل',
        'btn_report_problem': '📩 گزارش مشکل',
        'btn_album_upload': '📂 آپلود گروهی فایل',
        'btn_broadcast': '📣 پیام‌رسان همگانی',
        'btn_bot_info': '🤖 درباره ربات',
        'btn_change_language': '🌍 تغییر زبان',
        'btn_force_join': '➕ جوین اجباری',
        'btn_upload_file': '⬆️ آپلود فایل جدید',
        'btn_redownload_file': '🔄 دریافت مجدد فایل',
        'auto_delete_prompt': '⏳ زمان حذف خودکار فعلی: {current_time} ثانیه\nلطفاً عددی بین 5 تا 180 (ثانیه) را به عنوان زمان حذف خودکار وارد کنید.\nاگر می‌خواهید فایل بطور خودکار حذف نشود، عدد 0 را ارسال کنید.',
        'auto_delete_success': '✅ زمان حذف خودکار فایل‌ها روی {seconds} ثانیه تنظیم شد!',
        'change_text_prompt': '✍️ متن کدام بخش را می‌خواهید ویرایش کنید؟',
        'change_text_menu_start': 'متن شروع ربات',
        'change_text_menu_file_not_found': 'متن فایل ناموجود',
        'change_text_menu_auto_delete': 'متن حذف خودکار',
        'change_text_menu_view_reaction': 'متن قفل مشاهده/واکنش',
        'change_text_menu_prompt_new': '📝 لطفاً متن جدید را وارد کنید:',
        'change_text_success': '✅ متن `{key}` با موفقیت تغییر کرد.',
        'forward_lock_on': '🚫 قفل فوروارد فایل‌های آپلود شده *فعال* شد. ☀️',
        'forward_lock_off': '🚫 قفل فوروارد فایل‌های آپلود شده *غیرفعال* شد. 🌙',
        'admin_list': '📋 *لیست ادمین‌های ربات*:\n{list}',
        'no_admins': '❌ هیچ کس ادمین ربات نیست!',
        'add_admin_prompt': '👨‍💼 لطفاً آیدی عددی ادمین جدید را وارد کنید.',
        'add_admin_success': '🎉 کاربر با آیدی `{user_id}` به لیست ادمین‌ها اضافه شد!',
        'remove_admin_prompt': '🗑️ لطفاً آیدی عددی ادمینی که می‌خواهید حذف کنید را وارد کنید.',
        'remove_admin_success': '✅ ادمین با آیدی `{user_id}` با موفقیت حذف شد.',
        'cannot_remove_main_admin': '❌ نمی‌توان ادمین اصلی ربات را حذف کرد! 👑',
        'not_an_admin': '❌ این آیدی متعلق به یک ادمین نیست!',
        'view_reaction_settings_menu': '👁️‍🗨️ تنظیمات قفل مشاهده/واکنش',
        'view_reaction_not_set': '👁️‍🗨️ قفل مشاهده/واکنش تنظیم نشده است. جهت تنظیم، روی دکمه *تنظیم لینک* کلیک کنید 👇',
        'view_reaction_menu_toggle': '💡 تغییر وضعیت قفل',
        'view_reaction_menu_set_link': '🔗 تنظیم لینک قفل',
        'view_reaction_on': '✅ قفل مشاهده/واکنش *فعال* شد. ☀️',
        'view_reaction_off': '❌ قفل مشاهده/واکنش *غیرفعال* شد. 🌙',
        'view_reaction_link_prompt': '🔗 لطفاً لینک کانال یا گروه مورد نظر برای قفل مشاهده/واکنش را ارسال کنید (مثلاً: `https://t.me/yourchannel` یا `https://t.me/yourgroup`).\n\n**نکته:** ربات باید در کانال/گروه ادمین باشد تا بتواند عضویت کاربران را بررسی کند.',
        'view_reaction_link_success': '✅ لینک قفل مشاهده/واکنش با موفقیت تنظیم شد.',
        'view_reaction_required': '❌ برای دسترسی به این محتوا، لطفاً ابتدا در کانال/گروه زیر عضو شوید:\n',
        'view_reaction_not_member_error': '❌ شما هنوز عضو کانال/گروه مورد نظر نیستید!',
        'stats': '📊 *آمار کلی ربات*:\n\n- تعداد کل کاربران: {total_users} نفر 👥\n- تعداد کاربران فعال (امروز): {active_users} نفر ({active_percent}%)\n- آخرین به‌روزرسانی آمار: {last_update}\n- تعداد کاربران جدید امروز: {new_users_today} نفر\n- 7 روز گذشته: {new_users_week} نفر\n- 31 روز گذشته: {new_users_month} نفر\n- پینگ ربات: {ping}ms 🏓',
        'stats_details': '⏰ *جزئیات تنظیمات*:\n\n- تعداد ادمین‌ها: {admins} نفر 👨‍💼\n- زمان حذف خودکار فایل‌ها: {auto_delete_time} ثانیه ⏳\n- قفل فوروارد فایل‌های آپلود شده: {forward_lock_status}\n- قفل مشاهده/واکنش: {view_reaction_status} {view_reaction_link_status}\n- تعداد فایل‌های آپلود شده: {uploaded_files} 📁\n- تعداد آلبوم‌های آپلود شده: {uploaded_albums} 📸\n- جوین اجباری: {force_join_status} {force_join_link_status}',
        'broadcast_prompt': '📣 لطفاً پیام خود را برای ارسال همگانی به کاربران وارد کنید.',
        'broadcast_success': '✅ پیام شما با موفقیت برای {count} کاربر ارسال شد!',
        'support_prompt': '📩 لطفاً پیام خود را برای ارسال به ادمین‌های پشتیبانی وارد کنید:',
        'support_sent': '✅ پیام شما با موفقیت برای ادمین‌ها ارسال شد! در اسرع وقت پاسخ داده خواهد شد.',
        'banned': '🚫 شما از استفاده از ربات مسدود شده‌اید. لطفاً با ادمین تماس بگیرید.',
        'user_management_menu': '👥 منوی مدیریت کاربران ربات',
        'ban_user_prompt': '🚫 لطفاً آیدی عددی کاربری که می‌خواهید بن کنید را وارد کنید.',
        'unban_user_prompt': '✅ لطفاً آیدی عددی کاربری که می‌خواهید آن‌بن کنید را وارد کنید.',
        'user_banned': '🚫 کاربر با آیدی `{user_id}` با موفقیت بن شد.',
        'user_unbanned': '✅ کاربر با آیدی `{user_id}` با موفقیت آن‌بن شد.',
        'user_already_banned': '❌ این کاربر قبلاً بن شده است.',
        'user_not_banned': '❌ این کاربر بن نشده است.',
        'view_files_menu': '🗃️ منوی مدیریت و مشاهده فایل‌ها',
        'file_list': '📂 *20 فایل اخیر آپلود شده*:\n{list}\n\nبرای مشاهده جزئیات یا حذف، آیدی فایل را انتخاب کنید.',
        'no_files': '❌ هیچ فایلی تا کنون آپلود نشده است.',
        'delete_file_prompt': '🗑️ لطفاً آیدی فایل را برای حذف وارد کنید:',
        'file_deleted_success': '✅ فایل با آیدی `{file_id}` با موفقیت حذف شد.',
        'file_not_found_to_delete': '❌ فایلی با این آیدی یافت نشد.',
        'admin_management_menu': '👨‍💼 منوی مدیریت ادمین‌های ربات',
        'admin_added_successfully': '✅ ادمین جدید با موفقیت اضافه شد.',
        'admin_removed_successfully': '✅ ادمین با موفقیت حذف شد.',
        'user_is_not_admin': '❌ کاربر مورد نظر ادمین نیست.',
        'file_search_prompt': '🔍 لطفاً کلمه کلیدی یا بخشی از آیدی فایل را برای جستجو وارد کنید:',
        'no_search_results': '❌ نتیجه‌ای برای جستجوی شما یافت نشد.',
        'search_results': '🔍 *نتایج جستجو*:\n{results}',
        'report_sent_to_admin': 'پیام گزارش مشکل از کاربر `{user_id}`: \n\n`{message_text}`',
        'album_upload_start_prompt': '📸 لطفاً فایل‌های آلبومی خود را به صورت یکجا ارسال کنید. پس از اتمام، روی دکمه "پایان آپلود آلبوم" بزنید.',
        'album_upload_done_button': 'پایان آپلود آلبوم',
        'album_upload_cancel_button': 'لغو آپلود آلبوم',
        'album_upload_info': 'فایل‌های آلبومی خود را بفرستید. حداقل ۲ و حداکثر ۱۰ فایل.',
        'album_upload_limit_reached': '⚠️ حداکثر ۱۰ فایل برای آلبوم آپلود شده است. لطفاً "پایان آپلود آلبوم" را بزنید.',
        'album_upload_min_files': '❌ حداقل ۲ فایل برای ایجاد آلبوم نیاز است.',
        'album_upload_canceled': '✅ عملیات آپلود آلبوم لغو شد.',
        'album_file_saved': 'فایل‌های آلبوم با موفقیت ذخیره شدند.',
        'choose_language': 'لطفاً زبان مورد نظر خود را انتخاب کنید:',
        'language_changed': '✅ زبان ربات به {language_name} تغییر یافت.',
        'force_join_menu': '➕ منوی تنظیمات جوین اجباری',
        'force_join_toggle': '💡 تغییر وضعیت جوین اجباری',
        'force_join_set_link': '🔗 تنظیم لینک کانال/گروه',
        'force_join_on': '✅ جوین اجباری *فعال* شد. ☀️',
        'force_join_off': '❌ جوین اجباری *غیرفعال* شد. 🌙',
        'force_join_link_prompt': '🔗 لطفاً لینک کانال یا گروه مورد نظر برای جوین اجباری را ارسال کنید (مثلاً: `https://t.me/yourchannel` یا `https://t.me/yourgroup` یا `@yourchannel`).\n\n**نکته:** ربات باید در کانال/گروه ادمین باشد و دسترسی‌های لازم (مثل افزودن عضو برای گروه‌های خصوصی) را داشته باشد.',
        'force_join_link_success': '✅ لینک جوین اجباری با موفقیت تنظیم شد.',
        'not_a_member': '❌ برای استفاده از ربات، لطفاً ابتدا در کانال/گروه زیر عضو شوید:\n{link}',
        'channel_id_error': '❌ لینک نامعتبر است. لطفاً لینک معتبر کانال یا گروه (مثل `https://t.me/yourchannel` یا `@yourchannel`) را وارد کنید.',
        'bot_not_admin_in_channel': '❌ ربات در کانال/گروه {channel_title} ادمین نیست یا دسترسی لازم را ندارد. لطفاً ربات را ادمین کنید و دسترسی‌های لازم (مثل افزودن عضو) را بدهید.',
    },
    'en': {
        'welcome_admin': 'Welcome to the bot admin panel 👨‍💻',
        'welcome_user': '👋 Hello! Welcome to our powerful bot! 😊',
        'file_not_found': '❌ File not found. It may have been deleted or the ID is incorrect. 😕',
        'btn_redownload_file': '🔄 Redownload File',
        'force_join_link_prompt': '🔗 Please send the link of the channel or group for force join (e.g., `https://t.me/yourchannel` or `@yourchannel`).\n\n**Note:** The bot must be an admin in the channel/group with necessary permissions (e.g., invite users for private groups).',
        'force_join_link_success': '✅ Force join link set successfully.',
        'not_a_member': '❌ To use the bot, please join the following channel/group first:\n{link}',
        'channel_id_error': '❌ Invalid link. Please enter a valid channel/group link (e.g., `https://t.me/yourchannel` or `@yourchannel`).',
        'bot_not_admin_in_channel': '❌ The bot is not an admin in the channel/group {channel_title} or lacks necessary permissions. Please make the bot an admin with required permissions (e.g., invite users).',
        'album_upload_add': 'File added. Uploaded files: {current}/{total}',
        'album_upload_min_files': '❌ At least 2 files are required to create an album.',
        'album_upload_limit_reached': '⚠️ Maximum 10 files uploaded for the album. Please press "Finish Album Upload".',
        'album_upload_canceled': '✅ Album upload operation cancelled.',
        'album_file_saved': 'Album files saved successfully.',
        'album_upload_start_prompt': '📸 Please send your album files at once. When finished, press "Finish Album Upload".',
        'album_upload_done_button': 'Finish Album Upload',
        'album_upload_cancel_button': 'Cancel Album Upload',
    }
}

def generate_unique_id(length=16):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def get_channel_id_from_link(link):
    link = link.strip()
    if 't.me/' in link:
        if 't.me/c/' in link:
            try:
                # For private channel links like https://t.me/c/1234567890/123
                parts = link.split('/c/')
                if len(parts) > 1:
                    channel_id_num = parts[1].split('/')[0]
                    # Telegram channel IDs are negative, starting with -100
                    return int(f"-100{channel_id_num}") # Convert to int
            except (IndexError, ValueError) as e:
                logger.error(f"Error extracting channel ID from link (private): {link} - {e}")
                return None
        else:
            try:
                # For public channel links like https://t.me/yourchannel or t.me/yourchannel
                channel_handle = link.split('/')[-1]
                if channel_handle.startswith('@'):
                    return channel_handle
                else:
                    return f"@{channel_handle}"
            except IndexError as e:
                logger.error(f"Invalid link format: {link} - {e}")
                return None
    elif link.startswith('@'):
        return link
    logger.warning(f"Invalid or unsupported link format provided for channel: {link}")
    return None

def is_user_member(user_id, channel_id):
    if not channel_id:
        return False
    try:
        status = bot.get_chat_member(channel_id, user_id).status
        return status in ['member', 'administrator', 'creator']
    except telebot.apihelper.ApiTelegramException as e:
        logger.error(f"خطا در بررسی عضویت کاربر {user_id} در کانال {channel_id}: {e}")
        if "chat not found" in str(e).lower() or "bad request: chat not found" in str(e).lower():
            logger.warning(f"The specified channel/group '{channel_id}' was not found. Consider updating force join settings.")
        elif "user not in chat" in str(e).lower():
            return False
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking membership for {user_id} in {channel_id}: {e}")
        return False

# --- توابع کیبورد ---
def show_admin_main_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['btn_upload_file']),
        types.KeyboardButton(LANGUAGES[lang]['btn_album_upload']),
        types.KeyboardButton(LANGUAGES[lang]['btn_broadcast']),
        types.KeyboardButton(LANGUAGES[lang]['btn_bot_info']),
        types.KeyboardButton(LANGUAGES[lang]['btn_change_language']),
        types.KeyboardButton(LANGUAGES[lang]['btn_report_problem']),
        types.KeyboardButton(LANGUAGES[lang]['btn_file_search']),
        types.KeyboardButton(LANGUAGES[lang]['btn_manage_users']),
        types.KeyboardButton(LANGUAGES[lang]['btn_view_files']),
        types.KeyboardButton(LANGUAGES[lang]['btn_add_admin']),
        types.KeyboardButton(LANGUAGES[lang]['settings_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['admin_panel'], reply_markup=markup)

def show_user_main_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['btn_bot_info']),
        types.KeyboardButton(LANGUAGES[lang]['btn_report_problem']),
        types.KeyboardButton(LANGUAGES[lang]['btn_change_language']),
        types.KeyboardButton(LANGUAGES[lang]['btn_file_search'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['user_menu'], reply_markup=markup)

def show_settings_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['btn_forward_lock']),
        types.KeyboardButton(LANGUAGES[lang]['btn_view_reaction_lock']),
        types.KeyboardButton(LANGUAGES[lang]['btn_auto_delete_individual']),
        types.KeyboardButton(LANGUAGES[lang]['btn_change_text']),
        types.KeyboardButton(LANGUAGES[lang]['btn_force_join']),
        types.KeyboardButton(LANGUAGES[lang]['back_to_main_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['settings_menu'], reply_markup=markup)

def show_change_text_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['change_text_menu_start']),
        types.KeyboardButton(LANGUAGES[lang]['change_text_menu_file_not_found']),
        types.KeyboardButton(LANGUAGES[lang]['change_text_menu_auto_delete']),
        types.KeyboardButton(LANGUAGES[lang]['change_text_menu_view_reaction']),
        types.KeyboardButton(LANGUAGES[lang]['back_to_settings_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['change_text_prompt'], reply_markup=markup)

def show_view_reaction_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['view_reaction_menu_toggle']),
        types.KeyboardButton(LANGUAGES[lang]['view_reaction_menu_set_link']),
        types.KeyboardButton(LANGUAGES[lang]['back_to_settings_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_settings_menu'], reply_markup=markup)

def show_force_join_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['force_join_toggle']),
        types.KeyboardButton(LANGUAGES[lang]['force_join_set_link']),
        types.KeyboardButton(LANGUAGES[lang]['back_to_settings_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['force_join_menu'], reply_markup=markup)

def show_user_management_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton('🚫 بن کردن کاربر'),
        types.KeyboardButton('✅ آن‌بن کردن کاربر'),
        types.KeyboardButton(LANGUAGES[lang]['back_to_main_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['user_management_menu'], reply_markup=markup)

def show_admin_management_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton('🧑‍💻 افزودن ادمین'),
        types.KeyboardButton('🗑️ حذف ادمین'),
        types.KeyboardButton(LANGUAGES[lang]['back_to_main_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['admin_management_menu'], reply_markup=markup)

def show_file_management_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(
        types.KeyboardButton('📂 مشاهده لیست فایل‌ها'),
        types.KeyboardButton('🗑️ حذف فایل'),
        types.KeyboardButton(LANGUAGES[lang]['back_to_main_menu'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['view_files_menu'], reply_markup=markup)

def show_album_upload_menu(chat_id, lang):
    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
    markup.add(
        types.KeyboardButton(LANGUAGES[lang]['album_upload_done_button']),
        types.KeyboardButton(LANGUAGES[lang]['album_upload_cancel_button'])
    )
    bot.send_message(chat_id, LANGUAGES[lang]['album_upload_start_prompt'], reply_markup=markup)

def show_language_selection_menu(chat_id, lang):
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("فارسی 🇮🇷", callback_data="set_lang_fa"),
        types.InlineKeyboardButton("English 🇬🇧", callback_data="set_lang_en")
    )
    bot.send_message(chat_id, LANGUAGES[lang]['choose_language'], reply_markup=markup)

# --- هندلرهای مبتنی بر وضعیت ---
def set_auto_delete_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    settings = get_settings(chat_id)
    bot.send_message(chat_id, LANGUAGES[lang]['auto_delete_prompt'].format(current_time=settings['auto_delete_time']))
    bot.register_next_step_handler(message, set_auto_delete_step2)

def set_auto_delete_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    try:
        time_in_seconds = int(message.text)
        if 0 <= time_in_seconds <= 180:
            update_setting(chat_id, 'auto_delete_time', time_in_seconds)
            bot.send_message(chat_id, LANGUAGES[lang]['auto_delete_success'].format(seconds=time_in_seconds))
        else:
            bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    except ValueError:
        bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    show_settings_menu(chat_id, lang)

def change_text_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    show_change_text_menu(chat_id, lang)

def change_text_step2(message, db_key):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)

    bot.send_message(chat_id, LANGUAGES[lang]['change_text_menu_prompt_new'])
    user_states[chat_id] = {'state': 'awaiting_new_text_for_key', 'key': db_key}
    bot.register_next_step_handler(message, change_text_step3)

def change_text_step3(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)

    state_data = user_states.get(chat_id)
    if state_data and state_data['state'] == 'awaiting_new_text_for_key':
        key = state_data['key']
        new_text = message.text
        update_custom_text(key, new_text)
        bot.send_message(chat_id, LANGUAGES[lang]['change_text_success'].format(key=key))
    else:
        bot.send_message(chat_id, "❌ خطایی در تغییر متن رخ داد. لطفاً دوباره تلاش کنید.")

    if chat_id in user_states:
        del user_states[chat_id]
    show_change_text_menu(chat_id, lang)

def add_admin_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['add_admin_prompt'])
    bot.register_next_step_handler(message, add_admin_step2)

def add_admin_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    try:
        target_id = int(message.text)
        if not is_admin(target_id):
            add_admin(target_id)
            bot.send_message(chat_id, LANGUAGES[lang]['add_admin_success'].format(user_id=target_id))
        else:
            bot.send_message(chat_id, "❌ این کاربر قبلاً ادمین بوده است.")
    except ValueError:
        bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    show_admin_management_menu(chat_id, lang)

def remove_admin_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['remove_admin_prompt'])
    bot.register_next_step_handler(message, remove_admin_step2)

def remove_admin_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    try:
        target_id = int(message.text)
        if not is_admin(target_id):
            bot.send_message(chat_id, LANGUAGES[lang]['user_is_not_admin'])
        elif target_id == ADMIN_ID:
            bot.send_message(chat_id, LANGUAGES[lang]['cannot_remove_main_admin'])
        else:
            remove_admin(target_id)
            bot.send_message(chat_id, LANGUAGES[lang]['remove_admin_success'].format(user_id=target_id))
    except ValueError:
        bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    show_admin_management_menu(chat_id, lang)

def delete_file_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['delete_file_prompt'])
    bot.register_next_step_handler(message, delete_file_step2)

def delete_file_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    file_id = message.text
    file_info = get_file_info(file_id)
    if file_info:
        delete_file_info(file_id)
        bot.send_message(chat_id, LANGUAGES[lang]['file_deleted_success'].format(file_id=file_id))
        try:
            bot.delete_message(file_info['chat_id'], file_info['message_id'])
        except Exception as e:
            logger.warning(f"خطا در حذف پیام فایل {file_id}: {e}")
    else:
        bot.send_message(chat_id, LANGUAGES[lang]['file_not_found_to_delete'])
    show_file_management_menu(chat_id, lang)

def file_search_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['file_search_prompt'])
    bot.register_next_step_handler(message, file_search_step2)

def file_search_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    query = message.text
    results = search_files(query)
    if results:
        formatted_results = []
        for file_id, caption, original_filename, upload_date in results:
            item_text = f"ID: `{file_id}`\n"
            if original_filename:
                item_text += f"نام فایل: `{original_filename}`\n"
            if caption:
                item_text += f"کپشن: `{caption}`\n"
            item_text += f"تاریخ آپلود: {upload_date}\n"
            formatted_results.append(item_text)
        bot.send_message(chat_id, LANGUAGES[lang]['search_results'].format(results='\n---\n'.join(formatted_results)), parse_mode='Markdown')
    else:
        bot.send_message(chat_id, LANGUAGES[lang]['no_search_results'])
    if is_admin(user_id):
        show_admin_main_menu(chat_id, lang)
    else:
        show_user_main_menu(chat_id, lang)

def set_force_join_link_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['force_join_link_prompt'])
    bot.register_next_step_handler(message, set_force_join_link_step2)

def set_force_join_link_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)

    link = message.text.strip()
    channel_id = get_channel_id_from_link(link)

    if not channel_id:
        bot.send_message(chat_id, LANGUAGES[lang]['channel_id_error'])
        show_force_join_menu(chat_id, lang)
        return

    try:
        bot_member = bot.get_chat_member(channel_id, bot.get_me().id)
        if bot_member.status not in ['administrator', 'creator']:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
            show_force_join_menu(chat_id, lang)
            return

        if bot_member.status == 'administrator' and not bot_member.can_invite_users:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
            show_force_join_menu(chat_id, lang)
            return

        update_setting(chat_id, 'force_join_link', link)
        update_setting(chat_id, 'force_join_channel_id', str(channel_id))
        bot.send_message(chat_id, LANGUAGES[lang]['force_join_link_success'])
    except telebot.apihelper.ApiTelegramException as e:
        error_message = str(e).lower()
        logger.error(f"Error checking bot status in channel {channel_id}: {e}")
        if any(err in error_message for err in ["chat not found", "user not in chat", "have no rights", "invite request sent"]):
            bot.send_message(chat_id, LANGUAGES[lang]['channel_id_error'])
        else:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
    except Exception as e:
        logger.error(f"Unexpected error setting force join link: {e}")
        bot.send_message(chat_id, "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید.")

    show_force_join_menu(chat_id, lang)

def set_view_reaction_link_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_link_prompt'])
    bot.register_next_step_handler(message, set_view_reaction_link_step2)

def set_view_reaction_link_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)

    link = message.text.strip()
    channel_id = get_channel_id_from_link(link)

    if not channel_id:
        bot.send_message(chat_id, LANGUAGES[lang]['channel_id_error'])
        show_view_reaction_menu(chat_id, lang)
        return

    try:
        bot_member = bot.get_chat_member(channel_id, bot.get_me().id)
        if bot_member.status not in ['administrator', 'creator']:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
            show_view_reaction_menu(chat_id, lang)
            return

        if bot_member.status == 'administrator' and not bot_member.can_invite_users:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
            show_view_reaction_menu(chat_id, lang)
            return

        update_setting(chat_id, 'view_reaction_link', link)
        update_setting(chat_id, 'view_reaction_channel_id', str(channel_id))
        bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_link_success'])
    except telebot.apihelper.ApiTelegramException as e:
        error_message = str(e).lower()
        logger.error(f"خطا در بررسی وضعیت ربات در کانال {channel_id}: {e}")
        if any(err in error_message for err in ["chat not found", "user not in chat", "have no rights"]):
            bot.send_message(chat_id, LANGUAGES[lang]['channel_id_error'])
        else:
            bot.send_message(chat_id, LANGUAGES[lang]['bot_not_admin_in_channel'].format(channel_title=channel_id))
    except Exception as e:
        logger.error(f"خطای غیرمنتظره در تنظیم لینک قفل مشاهده/واکنش: {e}")
        bot.send_message(chat_id, "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید.")

    show_view_reaction_menu(chat_id, lang)

def broadcast_message_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['broadcast_prompt'])
    bot.register_next_step_handler(message, broadcast_message_step2)

def broadcast_message_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    all_users = get_all_users()
    sent_count = 0
    for uid in all_users:
        try:
            bot.send_message(uid, message.text)
            sent_count += 1
        except Exception as e:
            logger.error(f"خطا در ارسال پیام همگانی به کاربر {uid}: {e}")
    bot.send_message(chat_id, LANGUAGES[lang]['broadcast_success'].format(count=sent_count))
    show_admin_main_menu(chat_id, lang)

def ban_user_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['ban_user_prompt'])
    bot.register_next_step_handler(message, ban_user_step2)

def ban_user_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    try:
        target_id = int(message.text)
        if is_user_banned(target_id):
            bot.send_message(chat_id, LANGUAGES[lang]['user_already_banned'])
        else:
            ban_user(target_id)
            bot.send_message(chat_id, LANGUAGES[lang]['user_banned'].format(user_id=target_id))
    except ValueError:
        bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    show_user_management_menu(chat_id, lang)

def unban_user_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['unban_user_prompt'])
    bot.register_next_step_handler(message, unban_user_step2)

def unban_user_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    try:
        target_id = int(message.text)
        if not is_user_banned(target_id):
            bot.send_message(chat_id, LANGUAGES[lang]['user_not_banned'])
        else:
            unban_user(target_id)
            bot.send_message(chat_id, LANGUAGES[lang]['user_unbanned'].format(user_id=target_id))
    except ValueError:
        bot.send_message(chat_id, LANGUAGES[lang]['no_valid_id'])
    show_user_management_menu(chat_id, lang)

def support_message_step1(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    bot.send_message(chat_id, LANGUAGES[lang]['support_prompt'])
    bot.register_next_step_handler(message, support_message_step2)

def support_message_step2(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    admin_ids = get_all_admins()
    if admin_ids:
        report_message = LANGUAGES[lang]['report_sent_to_admin'].format(user_id=user_id, message_text=message.text)
        for admin_id in admin_ids:
            try:
                bot.send_message(admin_id, report_message)
            except Exception as e:
                logger.error(f"خطا در ارسال پیام پشتیبانی به ادمین {admin_id}: {e}")
        bot.send_message(chat_id, LANGUAGES[lang]['support_sent'])
    else:
        bot.send_message(chat_id, "❌ ادمینی برای دریافت پیام شما پیدا نشد.")
    show_user_main_menu(chat_id, lang)

# --- هندلرهای اصلی ---
@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        create_tables()
        chat_id = message.chat.id
        user_id = message.from_user.id
        add_user(user_id)

        if is_user_banned(user_id):
            bot.send_message(chat_id, LANGUAGES['fa']['banned'])
            return

        lang = get_user_language(user_id)
        settings = create_or_get_settings(ADMIN_ID)

        # Force Join Check
        if settings['force_join_enabled'] and settings['force_join_link'] and settings['force_join_channel_id']:
            if not is_user_member(user_id, settings['force_join_channel_id']):
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(text="عضویت در کانال/گروه", url=settings['force_join_link']))
                bot.send_message(chat_id, LANGUAGES[lang]['not_a_member'].format(link=settings['force_join_link']), reply_markup=markup)
                return

        if message.text.startswith('/start '):
            param = message.text.split(' ')[1]

            # View/Reaction Lock Check
            if settings['force_view_reaction_enabled'] and settings['view_reaction_link'] and settings['view_reaction_channel_id']:
                if not is_user_member(user_id, settings['view_reaction_channel_id']):
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(text="مشاهده/واکنش", url=settings['view_reaction_link']))
                    bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_required'] + settings['view_reaction_link'], reply_markup=markup)
                    bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_not_member_error'])
                    return

            file_info = get_file_info(param)
            if file_info:
                try:
                    bot.copy_message(chat_id, file_info['chat_id'], file_info['message_id'], disable_notification=True)
                    update_file_download_count(param)
                    seconds_text = str(settings['auto_delete_time']) if settings['auto_delete_time'] > 0 else "نامشخص"

                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(text=LANGUAGES[lang]['btn_redownload_file'], url=f"https://t.me/{bot.get_me().username}?start={param}"))

                    bot.send_message(chat_id, LANGUAGES[lang]['upload_link_single'].format(bot_username=bot.get_me().username, file_id=param, seconds=seconds_text), reply_markup=markup)

                    if settings['auto_delete_time'] > 0:
                        scheduler.add_job(
                            bot.delete_message,
                            'date',
                            run_date=datetime.now() + timedelta(seconds=settings['auto_delete_time']),
                            args=[chat_id, message.message_id + 1]
                        )
                except Exception as e:
                    logger.error(f"خطا در ارسال فایل: {e}")
                    bot.send_message(chat_id, LANGUAGES[lang]['file_not_found'])
                return

            album_info = get_album_info(param)
            if album_info:
                try:
                    message_ids = [int(mid) for mid in album_info['message_ids'].split(',')]
                    for msg_id in message_ids:
                        bot.copy_message(chat_id, album_info['chat_id'], msg_id, disable_notification=True)
                        time.sleep(0.1)
                    update_album_download_count(param)
                    seconds_text = str(settings['auto_delete_time']) if settings['auto_delete_time'] > 0 else "نامشخص"

                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(text=LANGUAGES[lang]['btn_redownload_file'], url=f"https://t.me/{bot.get_me().username}?start={param}"))

                    bot.send_message(chat_id, LANGUAGES[lang]['upload_album_link'].format(bot_username=bot.get_me().username, album_id=param, seconds=seconds_text), reply_markup=markup)

                    if settings['auto_delete_time'] > 0:
                        pass
                except Exception as e:
                    logger.error(f"خطا در ارسال آلبوم: {e}")
                    bot.send_message(chat_id, LANGUAGES[lang]['file_not_found'])
                return

        if is_admin(user_id):
            bot.send_message(chat_id, LANGUAGES[lang]['welcome_admin'])
            show_admin_main_menu(chat_id, lang)
        else:
            bot.send_message(chat_id, LANGUAGES[lang]['welcome_user'])
            show_user_main_menu(chat_id, lang)
    except Exception as e:
        logger.error(f"خطا در دستور استارت: {e}")
        bot.send_message(message.chat.id, "❌ خطایی رخ داد!")

@bot.message_handler(content_types=['new_chat_members'])
def welcome_new_member(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    welcome_message = get_custom_text('welcome_message', lang)
    if welcome_message is None:
        welcome_message = LANGUAGES[lang]['welcome_user']
    for new_member in message.new_chat_members:
        if new_member.id == bot.get_me().id:
            logger.info(f"Bot joined chat {chat_id} ({message.chat.title})")
            continue
        username = new_member.first_name or "کاربر"
        bot.send_message(chat_id, welcome_message.format(user=username))

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio'])
def handle_file_upload(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    settings = create_or_get_settings(ADMIN_ID)
    lang = get_user_language(user_id)

    if is_user_banned(user_id):
        bot.send_message(chat_id, LANGUAGES[lang]['banned'])
        return

    # Check Force Join
    if settings['force_join_enabled'] and settings['force_join_link'] and settings['force_join_channel_id']:
        if not is_user_member(user_id, settings['force_join_channel_id']):
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(text="عضویت در کانال/گروه", url=settings['force_join_link']))
            bot.send_message(chat_id, LANGUAGES[lang]['not_a_member'].format(link=settings['force_join_link']), reply_markup=markup)
            return

    if not is_admin(user_id):
        bot.send_message(chat_id, "❌ شما اجازه آپلود فایل ندارید!")
        return

    # Handle album files
    if user_states.get(chat_id) == 'awaiting_album_files':
        if len(album_upload_data.get(chat_id, [])) >= 10:
            bot.send_message(chat_id, LANGUAGES[lang]['album_upload_limit_reached'])
            return
        album_upload_data.setdefault(chat_id, []).append(str(message.message_id))
        bot.send_message(chat_id, LANGUAGES[lang]['album_upload_add'].format(current=len(album_upload_data[chat_id]), total=10))
        return

    # Handle single file uploads
    file_id = generate_unique_id()
    file_type = message.content_type
    original_filename = None
    caption = message.caption

    if file_type == 'photo':
        file_info = message.photo[-1]
        original_filename = f"photo_{file_info.file_id}.jpg"
    elif file_type == 'video':
        file_info = message.video
        original_filename = getattr(message.video, 'file_name', f"video_{file_info.file_id}.mp4")
    elif file_type == 'document':
        file_info = message.document
        original_filename = message.document.file_name
    elif file_type == 'audio':
        file_info = message.audio
        original_filename = getattr(message.audio, 'file_name', f"audio_{file_info.file_id}.mp3")
    else:
        bot.send_message(chat_id, "فایل ارسالی پشتیبانی نمی‌شود. لطفا عکس، ویدئو، سند یا صدا ارسال کنید.")
        return

    save_file_info(file_id, user_id, file_type, message.message_id, chat_id, caption, original_filename)

    bot_username = bot.get_me().username
    seconds_text = str(settings['auto_delete_time']) if settings['auto_delete_time'] > 0 else "نامشخص"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(text=LANGUAGES[lang]['btn_redownload_file'], url=f"https://t.me/{bot_username}?start={file_id}"))

    bot.send_message(
        chat_id,
        LANGUAGES[lang]['upload_link_single'].format(bot_username=bot_username, file_id=file_id, seconds=seconds_text),
        reply_markup=markup
    )

    if settings['auto_delete_time'] > 0:
        scheduler.add_job(
            bot.delete_message,
            'date',
            run_date=datetime.now() + timedelta(seconds=settings['auto_delete_time']),
            args=[chat_id, message.message_id]
        )

# --- Command Handlers for Menu Buttons ---
@bot.message_handler(func=lambda message: True)
def handle_menu_buttons(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    settings = create_or_get_settings(ADMIN_ID)

    if is_user_banned(user_id):
        bot.send_message(chat_id, LANGUAGES[lang]['banned'])
        return

    # Force Join Check
    if settings['force_join_enabled'] and settings['force_join_link'] and settings['force_join_channel_id']:
        if not is_user_member(user_id, settings['force_join_channel_id']):
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(text="عضویت در کانال/گروه", url=settings['force_join_link']))
            bot.send_message(chat_id, LANGUAGES[lang]['not_a_member'].format(link=settings['force_join_link']), reply_markup=markup)
            return

    update_user_activity(user_id)

    # Handle state-based inputs
    if chat_id in user_states:
        state_data = user_states.get(chat_id)
        if state_data == 'awaiting_album_files':
            if message.text == LANGUAGES[lang]['album_upload_done_button']:
                if len(album_upload_data.get(chat_id, [])) >= 2:
                    album_id = generate_unique_id()
                    message_ids = ",".join(map(str, album_upload_data[chat_id]))
                    save_album_info(album_id, user_id, message_ids, chat_id)

                    bot_username = bot.get_me().username
                    seconds_text = str(settings['auto_delete_time']) if settings['auto_delete_time'] > 0 else "نامشخص"

                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(text=LANGUAGES[lang]['btn_redownload_file'], url=f"https://t.me/{bot_username}?start={album_id}"))

                    bot.send_message(chat_id, LANGUAGES[lang]['upload_album_link'].format(
                        bot_username=bot_username, album_id=album_id, seconds=seconds_text
                    ), reply_markup=markup)
                    bot.send_message(chat_id, LANGUAGES[lang]['album_file_saved'])
                    del album_upload_data[chat_id]
                    del user_states[chat_id]
                    show_admin_main_menu(chat_id, lang)
                else:
                    bot.send_message(chat_id, LANGUAGES[lang]['album_upload_min_files'])
                return
            elif message.text == LANGUAGES[lang]['album_upload_cancel_button']:
                if chat_id in album_upload_data:
                    del album_upload_data[chat_id]
                if chat_id in user_states:
                    del user_states[chat_id]
                bot.send_message(chat_id, LANGUAGES[lang]['album_upload_canceled'])
                show_admin_main_menu(chat_id, lang)
                return

    # Handle main menu navigation
    if message.text == LANGUAGES[lang]['back_to_main_menu']:
        if chat_id in user_states: del user_states[chat_id]
        if is_admin(user_id): show_admin_main_menu(chat_id, lang)
        else: show_user_main_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_settings_menu']:
        if chat_id in user_states: del user_states[chat_id]
        show_settings_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_text_menu']:
        if chat_id in user_states: del user_states[chat_id]
        show_change_text_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_user_management']:
        if chat_id in user_states: del user_states[chat_id]
        show_user_management_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_admin_management']:
        if chat_id in user_states: del user_states[chat_id]
        show_admin_management_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_view_reaction_menu']:
        if chat_id in user_states: del user_states[chat_id]
        show_view_reaction_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_file_menu']:
        if chat_id in user_states: del user_states[chat_id]
        show_file_management_menu(chat_id, lang)
        return
    elif message.text == LANGUAGES[lang]['back_to_album_upload']:
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in album_upload_data: del album_upload_data[chat_id]
        show_admin_main_menu(chat_id, lang)
        return

    # User-specific commands
    elif message.text == LANGUAGES[lang]['btn_bot_info']:
        bot.send_message(chat_id, "🤖 اطلاعات ربات: این ربات برای مدیریت فایل‌های شما و ارائه خدمات بهینه توسط تیم ما توسعه یافته است.")
    elif message.text == LANGUAGES[lang]['btn_report_problem']:
        support_message_step1(message)
    elif message.text == LANGUAGES[lang]['btn_change_language']:
        show_language_selection_menu(chat_id, lang)
    elif message.text == LANGUAGES[lang]['btn_file_search']:
        file_search_step1(message)

    # Admin-specific commands
    elif is_admin(user_id):
        if message.text == LANGUAGES[lang]['settings_menu']:
            show_settings_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_upload_file']:
            bot.send_message(chat_id, "لطفاً فایل خود (عکس، ویدئو، سند یا صدا) را ارسال کنید.")
            user_states[chat_id] = 'awaiting_file_upload'
        elif message.text == LANGUAGES[lang]['btn_album_upload']:
            show_album_upload_menu(chat_id, lang)
            user_states[chat_id] = 'awaiting_album_files'
        elif message.text == LANGUAGES[lang]['btn_broadcast']:
            broadcast_message_step1(message)
        elif message.text == LANGUAGES[lang]['btn_manage_users']:
            show_user_management_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_view_files']:
            show_file_management_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_add_admin']:
            show_admin_management_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_auto_delete_individual']:
            set_auto_delete_step1(message)
        elif message.text == LANGUAGES[lang]['btn_change_text']:
            change_text_step1(message)
        elif message.text == LANGUAGES[lang]['btn_forward_lock']:
            new_status = not settings['forward_lock']
            update_setting(ADMIN_ID, 'forward_lock', new_status)
            bot.send_message(chat_id, LANGUAGES[lang]['forward_lock_on'] if new_status else LANGUAGES[lang]['forward_lock_off'])
            show_settings_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_view_reaction_lock']:
            show_view_reaction_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['btn_force_join']:
            show_force_join_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['force_join_toggle']:
            new_status = not settings['force_join_enabled']
            update_setting(ADMIN_ID, 'force_join_enabled', new_status)
            bot.send_message(chat_id, LANGUAGES[lang]['force_join_on'] if new_status else LANGUAGES[lang]['force_join_off'])
            show_force_join_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['force_join_set_link']:
            set_force_join_link_step1(message)
        elif message.text == LANGUAGES[lang]['view_reaction_menu_toggle']:
            new_status = not settings['force_view_reaction_enabled']
            update_setting(ADMIN_ID, 'force_view_reaction_enabled', new_status)
            bot.send_message(chat_id, LANGUAGES[lang]['view_reaction_on'] if new_status else LANGUAGES[lang]['view_reaction_off'])
            show_view_reaction_menu(chat_id, lang)
        elif message.text == LANGUAGES[lang]['view_reaction_menu_set_link']:
            set_view_reaction_link_step1(message)
        elif message.text == '🚫 بن کردن کاربر':
            ban_user_step1(message)
        elif message.text == '✅ آن‌بن کردن کاربر':
            unban_user_step1(message)
        elif message.text == '🧑‍💻 افزودن ادمین':
            add_admin_step1(message)
        elif message.text == '🗑️ حذف ادمین':
            remove_admin_step1(message)
        elif message.text == '📂 مشاهده لیست فایل‌ها':
            results = search_files('')
            if results:
                file_list = []
                for file_id, caption, original_filename, upload_date in results:
                    item_text = f"ID: `{file_id}`\n"
                    if original_filename:
                        item_text += f"نام فایل: `{original_filename}`\n"
                    if caption:
                        item_text += f"کپشن: `{caption}`\n"
                    item_text += f"تاریخ آپلود: {upload_date}\n"
                    file_list.append(item_text)
                bot.send_message(chat_id, LANGUAGES[lang]['file_list'].format(list='\n---\n'.join(file_list)), parse_mode='Markdown')
            else:
                bot.send_message(chat_id, LANGUAGES[lang]['no_files'])
            show_file_management_menu(chat_id, lang)
        elif message.text == '🗑️ حذف فایل':
            delete_file_step1(message)

        # Specific text change handlers
        elif message.text == LANGUAGES[lang]['change_text_menu_start']:
            change_text_step2(message, 'welcome_message')
        elif message.text == LANGUAGES[lang]['change_text_menu_file_not_found']:
            change_text_step2(message, 'file_not_found')
        elif message.text == LANGUAGES[lang]['change_text_menu_auto_delete']:
            change_text_step2(message, 'auto_delete_prompt')
        elif message.text == LANGUAGES[lang]['change_text_menu_view_reaction']:
            change_text_step2(message, 'view_reaction_settings_menu')
        else:
            show_admin_main_menu(chat_id, lang)
    else:
        show_user_main_menu(chat_id, lang)

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_lang_'))
def set_language(call):
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    lang_code = call.data.split('_')[-1]
    lang_name = 'فارسی' if lang_code == 'fa' else 'English'
    update_user_language(user_id, lang_code)
    bot.edit_message_text(
        LANGUAGES[lang_code]['language_changed'].format(language_name=lang_name),
        chat_id,
        call.message.message_id
    )
    if is_admin(user_id):
        show_admin_main_menu(chat_id, lang_code)
    else:
        show_user_main_menu(chat_id, lang_code)

# --- راه‌اندازی ربات ---
if __name__ == '__main__':
    create_tables()
    logger.info("ربات با موفقیت راه‌اندازی شد.")
    bot.infinity_polling()
