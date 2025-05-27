import os
import logging
import platform
import secrets
import string
import sqlite3
import warnings
import shutil
from datetime import datetime
from typing import Dict, Optional, List, Union
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    CallbackContext,
    Filters
)
from telegram.error import (
    TelegramError,
    Unauthorized,
    BadRequest,
    TimedOut,
    NetworkError
)
from database import init_db, DialogStatus, get_db
from collections import deque
from functools import lru_cache
import weakref
import textwrap
from telegram.ext import ContextTypes
import psutil
import time

# Filter urllib3 warnings
warnings.filterwarnings('ignore', message='python-telegram-bot is using upstream urllib3')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('support_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "")
ADMIN_CHAT_ID: int = int(os.getenv("ADMIN_CHAT_ID", "0"))
LEVEL2_ADMIN_ID: int = int(os.getenv("LEVEL2_ADMIN_ID", "0"))

# Type hints for global state
admin_tags: Dict[int, str] = {}
admin_levels: Dict[int, int] = {LEVEL2_ADMIN_ID: 2}
active_dialogs: Dict[int, int] = {}
one_time_passwords: Dict[str, datetime] = {}
admin_active_status: Dict[int, bool] = {}

# Constants
DIALOG_TIMEOUT = 3600  # 1 hour in seconds
MAX_MESSAGE_LENGTH = 4096
PASSWORD_LENGTH = 8
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'support_bot.db'))
BACKUP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'backups'))

# Ensure backup directory exists
os.makedirs(BACKUP_DIR, exist_ok=True)

# Message queue for handling high load
message_queue = deque(maxlen=1000)

# Beautiful message templates
WELCOME_BANNER = """
╔══════════════════════╗
║   Support Bot v2.0                          ║
╚══════════════════════╝

👋 Добро пожаловать в систему поддержки!
"""

ADMIN_PANEL_BANNER = """
╔═══════════════════════╗
║    Панель админа                           ║
╚═══════════════════════╝
"""

ERROR_BANNER = """
⚠️ Ошибка:
{}
"""

SUCCESS_BANNER = """
✅ Успешно:
{}
"""

# Add after other cache definitions
stats_cache = {}
STATS_CACHE_TTL = 5  # 5 seconds

class Cache:
    """Simple cache implementation with TTL"""
    def __init__(self, ttl: int = 3600):
        self._cache = {}
        self._ttl = ttl
        
    def get(self, key: str) -> Optional[any]:
        """Get value from cache if not expired"""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if (datetime.now() - timestamp).total_seconds() < self._ttl:
                return value
            del self._cache[key]
        return None
        
    def set(self, key: str, value: any):
        """Set value in cache with current timestamp"""
        self._cache[key] = (value, datetime.now())
        
    def clear(self):
        """Clear expired cache entries"""
        now = datetime.now()
        expired = [
            k for k, (_, t) in self._cache.items()
            if (now - t).total_seconds() >= self._ttl
        ]
        for k in expired:
            del self._cache[k]

# Initialize caches
user_cache = Cache(ttl=3600)  # 1 hour TTL
admin_cache = Cache(ttl=3600)  # 1 hour TTL

class MessageFormatter:
    @staticmethod
    def format_message(text: str, max_width: int = 40) -> str:
        """Format message with proper wrapping and emoji"""
        return textwrap.fill(text, width=max_width)
    
    @staticmethod
    def format_admin_message(admin_tag: str, message: str) -> str:
        """Format admin message with beautiful styling"""
        return f"""
╭──── #{admin_tag} ────╮
│ {message}
╰─────────────────╯
"""
    
    @staticmethod
    def format_client_message(client_name: str, message: str) -> str:
        """Format client message with beautiful styling"""
        return f"""
╭──── {client_name} ────╮
│ {message}
╰─────────────────╯
"""
    
    @staticmethod
    def format_stats(stats: Dict[str, Union[int, str]]) -> str:
        """Format statistics with beautiful styling"""
        return f"""
╔════ Статистика ════╗
║ Повідомлень: {stats['messages']:>6} ║
║ Діалогів:    {stats['dialogs']:>6} ║
║ Адмінів:     {stats['admins']:>6} ║
╚═══════════════════╝
"""

msg_formatter = MessageFormatter()

# Message queue for handling high load
message_queue = deque(maxlen=1000)

# Cache for frequently accessed data
@lru_cache(maxsize=100)
def get_user_display_name(user) -> str:
    """Get user's display name with caching"""
    cache_key = f"user_name_{getattr(user, 'id', 0)}"
    cached_name = user_cache.get(cache_key)
    if cached_name:
        return cached_name
        
    name = _get_user_display_name(user)
    user_cache.set(cache_key, name)
    return name

def _get_user_display_name(user) -> str:
    """Internal function to get user's display name"""
    if user is None:
        return "Unknown User"
    
    if hasattr(user, 'full_name'):
        full_name = user.full_name
        username = user.username
    else:
        full_name = user.title if hasattr(user, 'title') else (user.first_name or "Unknown")
        username = user.username if hasattr(user, 'username') else None
    
    return f"{full_name} (@{username})" if username else full_name

class CustomMessageHandler:
    def __init__(self):
        self._cache = weakref.WeakValueDictionary()
        
    async def process_message(self, update: Update, context: CallbackContext):
        """Process message with caching and queueing"""
        message = update.message.text
        user = update.effective_user
        user_id = user.id
        
        # Check cache first
        cache_key = f"{user_id}:{message}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Add to queue for processing
        message_queue.append((update, context))
        
        # Process message
        result = await self._handle_message(update, context)
        
        # Cache result
        self._cache[cache_key] = result
        return result
        
    async def _handle_message(self, update: Update, context: CallbackContext):
        """Internal message handling logic"""
        user = update.effective_user
        user_id = user.id
        message = update.message.text
        user_display_name = get_user_display_name(user)
        chat_id = update.effective_chat.id  # Use effective_chat.id instead of user.id

        if user_id in admin_levels:
            # Handle admin messages
            if user_id in active_dialogs:
                client_id = active_dialogs[user_id]
                tag = admin_tags.get(user_id, "Admin")
                response = f"💬 Ответ #{tag} 😊\n{message}"
                msg_sent = safe_send_message(context.bot, client_id, response)
                if msg_sent:
                    save_message_to_history(user_id, client_id, message)
                    logger.info(f"Админ {user_display_name} ответил клиенту {client_id}")
                else:
                    safe_send_message(
                        context.bot,
                        chat_id,
                        "❌ Ошибка при отправке сообщения клиенту!"
                    )
        else:
            # Check if message starts with hashtag
            if message.startswith('#'):
                requested_tag = message[1:].lower()
                selected_admin = None
                
                # Find admin by tag
                for admin_id, tag in admin_tags.items():
                    if tag.lower() == requested_tag:
                        if not admin_active_status.get(admin_id, True):
                            safe_send_message(
                                context.bot,
                                chat_id,
                                f"❌ Администратор #{tag} сейчас не на месте. Пожалуйста, выберите другого администратора или подождите."
                            )
                            return
                        selected_admin = admin_id
                        break
                
                if selected_admin:
                    if selected_admin in active_dialogs:
                        safe_send_message(
                            context.bot,
                            chat_id,
                            f"⏳ Администратор #{requested_tag} сейчас занят другим диалогом.\n"
                            "Вы можете:\n"
                            "1. Подождать, пока он освободится\n"
                            "2. Выбрать другого администратора командой /admins\n"
                            "3. Отправить сообщение, и первый свободный администратор вам ответит"
                        )
                    else:
                        active_dialogs[selected_admin] = chat_id
                        save_state()
                        safe_send_message(
                            context.bot,
                            chat_id,
                            f"✅ Вы выбрали администратора #{requested_tag}. Можете начинать диалог!"
                        )
                        
                        admin_msg = f"👋 Клиент {user_display_name} выбрал вас как администратора!"
                        safe_send_message(context.bot, selected_admin, admin_msg)
                        logger.info(f"Клиент {user_display_name} выбрал админа #{requested_tag}")
                    return
                else:
                    safe_send_message(
                        context.bot,
                        chat_id,
                        "❌ Администратор не найден. Используйте /admins чтобы увидеть список администраторов."
                    )
                    return
            
            # Handle regular messages
            admin_id = None
            for admin, client in active_dialogs.items():
                if client == chat_id:
                    admin_id = admin
                    break

            if admin_id:
                safe_send_message(
                    context.bot,
                    admin_id,
                    f"📨 Сообщение от {user_display_name}:\n\n{message}"
                )
                save_message_to_history(chat_id, admin_id, message)
                logger.info(f"Сообщение от {user_display_name} отправлено админу {admin_id}")
            else:
                keyboard = [[InlineKeyboardButton("📩 Взять клиента", callback_data=f"take_client_{chat_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                safe_send_message(
                    context.bot,
                    ADMIN_CHAT_ID,
                    f"📩 Новое сообщение от {user_display_name}:\n\n{message}",
                    reply_markup=reply_markup
                )
                logger.info(f"Новое сообщение от {user_display_name}")
                
                safe_send_message(
                    context.bot,
                    chat_id,
                    "✅ Ваше сообщение получено! Ожидайте ответа администратора.\n\n💡 Вы также можете выбрать конкретного администратора командой /admins"
                )

message_handler = CustomMessageHandler()

def handle_message(update: Update, context: CallbackContext):
    """Handle incoming messages"""
    try:
        user = update.effective_user
        user_id = user.id
        message = update.message.text
        user_display_name = get_user_display_name(user)
        chat_id = update.effective_chat.id  # Use effective_chat.id instead of user.id

        if user_id in admin_levels:
            # Handle admin messages
            if user_id in active_dialogs:
                client_id = active_dialogs[user_id]
                tag = admin_tags.get(user_id, "Admin")
                response = f"💬 Ответ #{tag} 😊\n{message}"
                msg_sent = safe_send_message(context.bot, client_id, response)
                if msg_sent:
                    save_message_to_history(user_id, client_id, message)
                    logger.info(f"Админ {user_display_name} ответил клиенту {client_id}")
                else:
                    safe_send_message(
                        context.bot,
                        chat_id,
                        "❌ Ошибка при отправке сообщения клиенту!"
                    )
        else:
            # Check if message starts with hashtag
            if message.startswith('#'):
                requested_tag = message[1:].lower()
                selected_admin = None
                
                # Find admin by tag
                for admin_id, tag in admin_tags.items():
                    if tag.lower() == requested_tag:
                        if not admin_active_status.get(admin_id, True):
                            safe_send_message(
                                context.bot,
                                chat_id,
                                f"❌ Администратор #{tag} сейчас не на месте. Пожалуйста, выберите другого администратора или подождите."
                            )
                            return
                        selected_admin = admin_id
                        break
                
                if selected_admin:
                    if selected_admin in active_dialogs:
                        safe_send_message(
                            context.bot,
                            chat_id,
                            f"⏳ Администратор #{requested_tag} сейчас занят другим диалогом.\n"
                            "Вы можете:\n"
                            "1. Подождать, пока он освободится\n"
                            "2. Выбрать другого администратора командой /admins\n"
                            "3. Отправить сообщение, и первый свободный администратор вам ответит"
                        )
                    else:
                        active_dialogs[selected_admin] = chat_id
                        save_state()
                        safe_send_message(
                            context.bot,
                            chat_id,
                            f"✅ Вы выбрали администратора #{requested_tag}. Можете начинать диалог!"
                        )
                        
                        admin_msg = f"👋 Клиент {user_display_name} выбрал вас как администратора!"
                        safe_send_message(context.bot, selected_admin, admin_msg)
                        logger.info(f"Клиент {user_display_name} выбрал админа #{requested_tag}")
                    return
                else:
                    safe_send_message(
                        context.bot,
                        chat_id,
                        "❌ Администратор не найден. Используйте /admins чтобы увидеть список администраторов."
                    )
                    return
            
            # Handle regular messages
            admin_id = None
            for admin, client in active_dialogs.items():
                if client == chat_id:
                    admin_id = admin
                    break

            if admin_id:
                safe_send_message(
                    context.bot,
                    admin_id,
                    f"📨 Сообщение от {user_display_name}:\n\n{message}"
                )
                save_message_to_history(chat_id, admin_id, message)
                logger.info(f"Сообщение от {user_display_name} отправлено админу {admin_id}")
            else:
                keyboard = [[InlineKeyboardButton("📩 Взять клиента", callback_data=f"take_client_{chat_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                safe_send_message(
                    context.bot,
                    ADMIN_CHAT_ID,
                    f"📩 Новое сообщение от {user_display_name}:\n\n{message}",
                    reply_markup=reply_markup
                )
                logger.info(f"Новое сообщение от {user_display_name}")
                
                safe_send_message(
                    context.bot,
                    chat_id,
                    "✅ Ваше сообщение получено! Ожидайте ответа администратора.\n\n💡 Вы также можете выбрать конкретного администратора командой /admins"
                )
    except Exception as e:
        logger.error(f"Error in handle_message: {e}")
        if update and update.effective_chat:
            safe_send_message(
                context.bot,
                update.effective_chat.id,
                "❌ Произошла ошибка при обработке сообщения. Попробуйте позже."
            )

class DatabaseConnection:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._connection = None
        
        # Create backup directory if it doesn't exist
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            
        # Create backup on initialization
        self._create_backup()

    def _create_backup(self):
        """Create a backup of the database"""
        if os.path.exists(self.db_path):
            backup_name = f'support_bot_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
            backup_path = os.path.join(BACKUP_DIR, backup_name)
            try:
                shutil.copy2(self.db_path, backup_path)
                # Keep only last 5 backups
                backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.db')])
                for old_backup in backups[:-5]:
                    os.remove(os.path.join(BACKUP_DIR, old_backup))
                logger.info(f"Created database backup: {backup_name}")
            except Exception as e:
                logger.error(f"Failed to create database backup: {e}")

    def get_connection(self) -> sqlite3.Connection:
        """Get database connection with automatic reconnection"""
        try:
            if self._connection is None:
                self._connection = sqlite3.connect(self.db_path)
                self._connection.row_factory = sqlite3.Row
                # Enable foreign keys
                self._connection.execute('PRAGMA foreign_keys = ON')
                # Enable WAL mode for better concurrency
                self._connection.execute('PRAGMA journal_mode = WAL')
            # Test the connection
            self._connection.execute('SELECT 1')
            return self._connection
        except (sqlite3.Error, Exception) as e:
            logger.error(f"Database connection error: {e}")
            # Try to close and reconnect
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
            self._connection = None
            # Try to restore from backup if database is corrupted
            if isinstance(e, sqlite3.DatabaseError):
                self._restore_from_backup()
            raise

    def _restore_from_backup(self):
        """Attempt to restore database from latest backup"""
        try:
            backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.db')])
            if backups:
                latest_backup = os.path.join(BACKUP_DIR, backups[-1])
                shutil.copy2(latest_backup, self.db_path)
                logger.info(f"Restored database from backup: {backups[-1]}")
        except Exception as e:
            logger.error(f"Failed to restore database from backup: {e}")

    def close(self):
        """Safely close the database connection"""
        if self._connection:
            try:
                self._connection.commit()
                self._connection.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self._connection = None

db = DatabaseConnection()

def save_message_to_history(sender_id: int, receiver_id: int, message: str):
    """Save message to dialog history"""
    conn = sqlite3.connect('support_bot.db')
    c = conn.cursor()
    try:
        c.execute(
            'INSERT INTO dialog_history (sender_id, receiver_id, message) VALUES (?, ?, ?)',
            (sender_id, receiver_id, message)
        )
        conn.commit()
    finally:
        c.close()
        conn.close()

def init_database():
    """Initialize database with all necessary tables"""
    conn = None
    try:
        # Create backup of existing database if it exists
        if os.path.exists(DB_PATH):
            db._create_backup()
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Enable foreign keys and WAL mode
        c.execute('PRAGMA foreign_keys = ON')
        c.execute('PRAGMA journal_mode = WAL')
        
        # First, rename existing admin_status table if it exists
        c.execute('''SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='admin_status' ''')
        if c.fetchone():
            c.execute('ALTER TABLE admin_status RENAME TO admin_status_old')
            
        # Create tables with better constraints
        c.execute('''CREATE TABLE IF NOT EXISTS admin_tags
                     (admin_id INTEGER PRIMARY KEY,
                      tag TEXT UNIQUE NOT NULL)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS admin_levels
                     (admin_id INTEGER PRIMARY KEY,
                      level INTEGER NOT NULL CHECK (level IN (1, 2)),
                      FOREIGN KEY (admin_id) REFERENCES admin_tags(admin_id))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS active_dialogs
                     (admin_id INTEGER PRIMARY KEY,
                      client_id INTEGER NOT NULL,
                      start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                      FOREIGN KEY (admin_id) REFERENCES admin_tags(admin_id))''')
        
        # Create new admin_status table
        c.execute('''CREATE TABLE IF NOT EXISTS admin_status
                     (admin_id INTEGER PRIMARY KEY,
                      is_active BOOLEAN NOT NULL DEFAULT 1,
                      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                      FOREIGN KEY (admin_id) REFERENCES admin_tags(admin_id))''')
        
        # Migrate data from old table if it exists
        c.execute('''SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='admin_status_old' ''')
        if c.fetchone():
            c.execute('''INSERT OR REPLACE INTO admin_status (admin_id, is_active)
                        SELECT admin_id, is_active FROM admin_status_old''')
            c.execute('DROP TABLE admin_status_old')
        
        c.execute('''CREATE TABLE IF NOT EXISTS dialog_history
                     (message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                      sender_id INTEGER NOT NULL,
                      receiver_id INTEGER NOT NULL,
                      message TEXT NOT NULL,
                      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        
        # Add indexes for better performance
        c.execute('CREATE INDEX IF NOT EXISTS idx_dialog_history_sender ON dialog_history(sender_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dialog_history_receiver ON dialog_history(receiver_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dialog_history_timestamp ON dialog_history(timestamp)')
        
        # Initialize level 2 admin if not exists
        if LEVEL2_ADMIN_ID:
            c.execute('INSERT OR IGNORE INTO admin_tags VALUES (?, ?)', 
                     (LEVEL2_ADMIN_ID, 'admin'))
            c.execute('INSERT OR IGNORE INTO admin_levels VALUES (?, ?)', 
                     (LEVEL2_ADMIN_ID, 2))
            # Fixed insert statement for admin_status
            c.execute('INSERT OR IGNORE INTO admin_status (admin_id, is_active) VALUES (?, ?)', 
                     (LEVEL2_ADMIN_ID, True))
        
        # Load saved state with error handling
        admin_tags.clear()
        admin_levels.clear()
        active_dialogs.clear()
        admin_active_status.clear()
        
        try:
            # Load admin tags
            c.execute('SELECT * FROM admin_tags')
            for row in c.fetchall():
                admin_tags[row[0]] = row[1]
            
            # Load admin levels
            c.execute('SELECT * FROM admin_levels')
            for row in c.fetchall():
                admin_levels[row[0]] = row[1]
            
            # Load active dialogs
            c.execute('SELECT * FROM active_dialogs')
            for row in c.fetchall():
                active_dialogs[row[0]] = row[1]
            
            # Load admin statuses
            c.execute('SELECT admin_id, is_active FROM admin_status')
            for row in c.fetchall():
                admin_active_status[row[0]] = bool(row[1])
            
            logger.info("Successfully loaded state from database")
            
        except Exception as e:
            logger.error(f"Error loading state from database: {e}")
            # Try to restore from backup
            db._restore_from_backup()
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        if conn:
            conn.rollback()
        # Try to restore from backup
        db._restore_from_backup()
        raise
    finally:
        if conn:
            conn.close()

def save_state():
    """Save current state to database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Begin transaction
        c.execute('BEGIN TRANSACTION')
        
        # Save admin tags
        c.execute('DELETE FROM admin_tags')
        for admin_id, tag in admin_tags.items():
            c.execute('INSERT INTO admin_tags VALUES (?, ?)', (admin_id, tag))
        
        # Save admin levels
        c.execute('DELETE FROM admin_levels')
        for admin_id, level in admin_levels.items():
            c.execute('INSERT INTO admin_levels VALUES (?, ?)', (admin_id, level))
        
        # Save active dialogs
        c.execute('DELETE FROM active_dialogs')
        for admin_id, client_id in active_dialogs.items():
            c.execute('''INSERT INTO active_dialogs 
                        (admin_id, client_id, start_time) 
                        VALUES (?, ?, CURRENT_TIMESTAMP)''', 
                     (admin_id, client_id))
        
        # Save admin statuses
        c.execute('DELETE FROM admin_status')
        for admin_id, is_active in admin_active_status.items():
            c.execute('''INSERT INTO admin_status 
                        (admin_id, is_active, last_updated) 
                        VALUES (?, ?, CURRENT_TIMESTAMP)''', 
                     (admin_id, is_active))
        
        # Commit transaction
        conn.commit()
        logger.info("Successfully saved state to database")
        
    except Exception as e:
        # If any error occurs, rollback the transaction
        conn.rollback()
        logger.error(f"Error saving state to database: {e}")
        
        # Try to create a backup before potential recovery
        db._create_backup()
        
        try:
            # Attempt recovery by saving without timestamps
            c.execute('BEGIN TRANSACTION')
            
            # Re-save admin tags
            c.execute('DELETE FROM admin_tags')
            for admin_id, tag in admin_tags.items():
                c.execute('INSERT INTO admin_tags VALUES (?, ?)', (admin_id, tag))
            
            # Re-save admin levels
            c.execute('DELETE FROM admin_levels')
            for admin_id, level in admin_levels.items():
                c.execute('INSERT INTO admin_levels VALUES (?, ?)', (admin_id, level))
            
            # Re-save active dialogs (simplified)
            c.execute('DELETE FROM active_dialogs')
            for admin_id, client_id in active_dialogs.items():
                c.execute('INSERT INTO active_dialogs (admin_id, client_id) VALUES (?, ?)', 
                         (admin_id, client_id))
            
            # Re-save admin statuses (simplified)
            c.execute('DELETE FROM admin_status')
            for admin_id, is_active in admin_active_status.items():
                c.execute('INSERT INTO admin_status (admin_id, is_active) VALUES (?, ?)', 
                         (admin_id, is_active))
            
            conn.commit()
            logger.info("Successfully recovered and saved state to database")
            
        except Exception as recovery_error:
            conn.rollback()
            logger.error(f"Failed to recover state: {recovery_error}")
            # If recovery fails, try to restore from backup
            db._restore_from_backup()
        
    finally:
        conn.close()

def set_tag_command(update: Update, context: CallbackContext):
    """Handle /set_tag command"""
    args = context.args
    if len(args) != 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Использование: /set_tag <пароль> <тег>"
        )
        return

    password, tag = args
    user = update.effective_user
    user_id = user.id
    user_display_name = get_user_display_name(user)

    # Check one-time password
    if password in one_time_passwords:
        # Check if password is not expired (24 hours)
        if (datetime.now() - one_time_passwords[password]).total_seconds() <= 86400:
            admin_tags[user_id] = tag
            admin_levels[user_id] = 1
            admin_active_status[user_id] = True  # Initialize admin as active
            del one_time_passwords[password]  # Remove used password
            
            # Save to database with proper column specification
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            try:
                c.execute('INSERT OR REPLACE INTO admin_tags VALUES (?, ?)', 
                         (user_id, tag))
                c.execute('INSERT OR REPLACE INTO admin_levels VALUES (?, ?)', 
                         (user_id, 1))
                c.execute('''INSERT OR REPLACE INTO admin_status 
                           (admin_id, is_active, last_updated) 
                           VALUES (?, ?, CURRENT_TIMESTAMP)''', 
                         (user_id, True))
                conn.commit()
                
                logger.info(f"New admin initialized: {user_display_name} (ID: {user_id}), tag={tag}, active=True")
                
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"✅ Тег установлен: {tag}\nТеперь вы администратор 1-го уровня."
                )
                logger.info(f"Установлен тег {tag} для админа {user_display_name} (ID: {user_id})")
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in set_tag_command: {e}")
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Произошла ошибка при сохранении данных. Пожалуйста, попробуйте позже."
                )
            finally:
                conn.close()
        else:
            del one_time_passwords[password]  # Remove expired password
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Пароль истек! Запросите новый пароль у администратора 2-го уровня."
            )
    else:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Неверный пароль!"
        )
        logger.warning(f"Попытка установки тега с неверным паролем от пользователя {user_display_name} (ID: {user_id})")

def generate_one_time_password(length: int = PASSWORD_LENGTH) -> str:
    """Generate a random one-time password"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def safe_send_message(bot, chat_id, text, reply_markup=None):
    """Safely send a message with error handling"""
    try:
        return bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except TelegramError as e:
        error_type = type(e).__name__
        if error_type == 'ChatMigrated':
            # Get new chat id from the error message
            new_chat_id = e.migrate_to_chat_id if hasattr(e, 'migrate_to_chat_id') else None
            if not new_chat_id and isinstance(e.message, str):
                # Try to extract chat id from error message
                import re
                match = re.search(r'New chat id: (-?\d+)', e.message)
                if match:
                    new_chat_id = int(match.group(1))
            
            if new_chat_id:
                logger.info(f"Чат {chat_id} был перемещен в {new_chat_id}")
                
                # Update active_dialogs if needed
                for admin_id, old_chat_id in list(active_dialogs.items()):
                    if old_chat_id == chat_id:
                        active_dialogs[admin_id] = new_chat_id
                        save_state()
                        break
                
                # Update database
                conn = sqlite3.connect('support_bot.db')
                c = conn.cursor()
                try:
                    # Update sender_id
                    c.execute(
                        'UPDATE dialog_history SET sender_id = ? WHERE sender_id = ?',
                        (new_chat_id, chat_id)
                    )
                    # Update receiver_id
                    c.execute(
                        'UPDATE dialog_history SET receiver_id = ? WHERE receiver_id = ?',
                        (new_chat_id, chat_id)
                    )
                    conn.commit()
                except Exception as db_error:
                    logger.error(f"Error updating chat ID in database: {db_error}")
                    conn.rollback()
                finally:
                    conn.close()
                
                # Try sending to new chat_id
                try:
                    return bot.send_message(
                        chat_id=new_chat_id,
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                except Exception as new_error:
                    logger.error(f"Error sending to migrated chat: {new_error}")
                    return None
            else:
                logger.error(f"Could not extract new chat ID from error: {e}")
                return None
        elif isinstance(e, Unauthorized):
            logger.error(f"Бот был заблокирован пользователем {chat_id}")
            # Remove from active dialogs if blocked
            for admin_id, client_id in list(active_dialogs.items()):
                if client_id == chat_id:
                    del active_dialogs[admin_id]
                    save_state()
                    break
        elif isinstance(e, BadRequest):
            logger.error(f"Неверный запрос при отправке сообщения {chat_id}: {e}")
        elif isinstance(e, TimedOut):
            logger.error(f"Таймаут при отправке сообщения {chat_id}: {e}")
        elif isinstance(e, NetworkError):
            logger.error(f"Ошибка сети при отправке сообщения {chat_id}: {e}")
        else:
            logger.error(f"Ошибка {error_type} при отправке сообщения {chat_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке сообщения {chat_id}: {e}")
        return None

def start_command(update: Update, context: CallbackContext):
    """Handle /start command with beautiful formatting"""
    try:
        user = update.effective_user
        user_display_name = get_user_display_name(user)
        
        welcome_message = f"{WELCOME_BANNER}\n\nРады видеть вас, {user_display_name}! 😊\n\n"
        welcome_message += "🔹 Для связи с администратором просто напишите сообщение\n"
        welcome_message += "🔹 Для выбора конкретного админа используйте /admins\n"
        welcome_message += "🔹 Для просмотра всех команд используйте /help"
        
        context.bot.send_message(chat_id=update.effective_chat.id, text=welcome_message)
        logger.info(f"Пользователь {user_display_name} начал работу с ботом")
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Произошла ошибка при обработке команды. Попробуйте позже."
        )

def help_command(update: Update, context: CallbackContext):
    """Show help message with available commands"""
    user = update.effective_user
    user_id = user.id
    
    # Basic commands for all users
    help_text = f"""
╔═══ Доступные команды ═══╗
║ 🔹 /start - Начать работу
║ 🔹 /help - Это меню
║ 🔹 /admins - Список админов
║ 🔹 /feedback - Оставить отзыв
"""
    
    # Admin commands
    if user_id in admin_levels:
        admin_level = admin_levels[user_id]
        help_text += f"""
║ 👨‍💼 Команды админа:
║ 🔸 /admin - Панель админа
║ 🔸 /stats - Статистика
"""
        
        if admin_level == 2:
            help_text += f"""
║ ⭐ Команды админа 2 уровня:
║ 🔸 /monitor - Мониторинг
║ 🔸 /promote - Повысить админа
║ 🔸 /broadcast - Рассылка
"""
    
    help_text += "╚════════════════════╝"
    context.bot.send_message(chat_id=update.effective_chat.id, text=help_text)

def admin_command(update: Update, context: CallbackContext):
    """Handle /admin command"""
    user = update.effective_user
    user_id = user.id
    user_display_name = get_user_display_name(user)
    if user_id not in admin_levels:
        context.bot.send_message(chat_id=update.effective_chat.id, text="❌ У вас нет доступа к панели администратора.")
        return

    update_admin_panel(update, context)
    logger.info(f"Админ {user_display_name} (ID: {user_id}) открыл админ-панель")

def list_admins_command(update: Update, context: CallbackContext):
    """Handle /admins command - show list of available admins"""
    all_admins = []
    available_admins_exist = False

    for admin_id in admin_levels.keys():
        try:
            tag = admin_tags.get(admin_id, "Admin")
            is_active = admin_active_status.get(admin_id, True)
            
            if admin_id in active_dialogs:
                status = "🔴"
            elif not is_active:
                status = "⚫"
            else:
                status = "🟢"
                available_admins_exist = True
                
            all_admins.append(f"{status} #{tag}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки админа {admin_id}: {e}")
            continue

    if all_admins:
        message = "📋 Список администраторов:\n\n" + "\n".join(all_admins)
        if available_admins_exist:
            message += "\n\n💡 Чтобы выбрать администратора, напишите его хештег (например: #support)"
        else:
            message += "\n\n😔 В данный момент все администраторы заняты или неактивны. Попробуйте позже!"
    else:
        message = "😔 К сожалению, сейчас нет доступных администраторов. Попробуйте позже!"

    context.bot.send_message(chat_id=update.effective_chat.id, text=message)

def get_admin_transfer_keyboard(admin_id):
    """Create keyboard for admin transfer"""
    keyboard = []
    row = []
    count = 0
    
    for target_admin_id, tag in admin_tags.items():
        if target_admin_id != admin_id:  # Don't show current admin
            try:
                admin = bot.get_chat(target_admin_id)
                admin_name = get_user_display_name(admin)
                status = "🔴" if target_admin_id in active_dialogs else "🟢"
                button = InlineKeyboardButton(
                    f"{status} #{tag} - {admin_name}",
                    callback_data=f"transfer_{target_admin_id}"
                )
                row.append(button)
                count += 1
                
                if count % 2 == 0:  # Two buttons per row
                    keyboard.append(row)
                    row = []
            except:
                continue
    
    if row:  # Add remaining buttons
        keyboard.append(row)
    
    # Add cancel button
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_transfer")])
    
    return InlineKeyboardMarkup(keyboard)

def button_callback(update: Update, context: CallbackContext):
    """Handle button callbacks"""
    query = update.callback_query
    query.answer()
    
    data = query.data
    user = query.from_user
    user_id = user.id
    user_display_name = get_user_display_name(user)

    if data == "generate_password" and admin_levels.get(user_id) == 2:
        password = generate_one_time_password()
        one_time_passwords[password] = datetime.now()
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🔑 Сгенерирован одноразовый пароль для нового администратора:\n`{password}`\n\n"
            "Передайте этот пароль новому администратору. Он действителен в течение 24 часов.\n"
            "Администратор должен использовать команду:\n"
            f"`/set_tag {password} желаемый_тег`"
        )
        logger.info(f"Админ {user_display_name} сгенерировал одноразовый пароль")
        return

    elif data == "promote_info" and admin_levels.get(user_id) == 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⭐ Как повысить администратора до 2-го ранга:\n\n"
            "1. Узнайте ID администратора, которого хотите повысить\n"
            "2. Используйте команду:\n"
            "`/promote <user_id>`\n\n"
            "Например: `/promote 123456789`\n\n"
            "❗ Повышать можно только существующих администраторов 1-го ранга"
        )
        return

    elif data.startswith("take_client_"):
        client_id = int(data.split("_")[2])
        try:
            client = context.bot.get_chat(client_id)
            client_display_name = get_user_display_name(client)
        except:
            client_display_name = f"Клиент {client_id}"

        if client_id in active_dialogs.values():
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text="❌ Клиент уже занят другим администратором!"
            )
            return

        active_dialogs[user_id] = client_id
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text=f"✅ Вы взяли клиента {client_display_name}\n"
            "Теперь вы можете отвечать на сообщения этого клиента."
        )
        
        context.bot.send_message(
            chat_id=client_id,
            text="👋 Администратор подключился к диалогу! Можете продолжать общение."
        )
        
        logger.info(f"Админ {user_display_name} взял клиента {client_display_name}")

    elif data == "close_dialog":
        if user_id in active_dialogs:
            client_id = active_dialogs[user_id]
            try:
                client = context.bot.get_chat(client_id)
                client_display_name = get_user_display_name(client)
            except:
                client_display_name = f"Клиент {client_id}"

            keyboard = [[InlineKeyboardButton("✅ Подтвердить завершение", callback_data="confirm_close")]]
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text=f"❓ Вы уверены, что хотите завершить диалог с {client_display_name}?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text="❌ У вас нет активных диалогов!"
            )

    elif data == "confirm_close":
        if user_id in active_dialogs:
            client_id = active_dialogs[user_id]
            try:
                client = context.bot.get_chat(client_id)
                client_display_name = get_user_display_name(client)
            except:
                client_display_name = f"Клиент {client_id}"

            del active_dialogs[user_id]
            context.bot.send_message(
                chat_id=client_id,
                text="🙏 Диалог завершен. Спасибо за обращение! 😊"
            )
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text=f"✅ Диалог с {client_display_name} завершен!"
            )
            logger.info(f"Админ {user_display_name} завершил диалог с {client_display_name}")

    elif data == "monitor" and admin_levels.get(user_id) == 2:
        monitor_command(update, context)
        return

    elif data == "stats":
        if user_id in admin_levels:
            try:
                stats_text = get_statistics(context)
                current_text = query.message.text
                if current_text != f"📊 Статистика бота:\n\n{stats_text}":
                    context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=query.message.message_id,
                        text=f"📊 Статистика бота:\n\n{stats_text}",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔄 Обновить", callback_data="stats")
                        ]])
                    )
                else:
                    # Якщо дані не змінились, просто показуємо повідомлення
                    query.answer("Статистика актуальна")
            except Exception as e:
                logger.error(f"Error updating stats: {e}")
                query.answer("Не удалось обновить статистику")
        else:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text="❌ У вас нет доступа к статистике!"
            )
        return

    elif data == "transfer_client":
        if user_id not in active_dialogs:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=query.message.message_id,
                text="❌ У вас нет активных диалогов для передачи!"
            )
            return
            
        keyboard = get_admin_transfer_keyboard(user_id)
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text="👥 Выберите администратора, которому хотите передать клиента:",
            reply_markup=keyboard
        )
        return

    elif data.startswith("transfer_"):
        target_admin_id = int(data.split("_")[1])
        transfer_client(update, context, target_admin_id)
        return

    elif data == "cancel_transfer":
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text="❌ Передача клиента отменена"
        )
        return

    elif data == "toggle_status":
        toggle_activity_status(update, context)
        update_admin_panel(update, context)
        return

def transfer_client(update: Update, context: CallbackContext, target_admin_id: int):
    """Transfer client to another admin"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_display_name = get_user_display_name(user)
    
    if user_id not in active_dialogs:
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text="❌ У вас больше нет активного диалога для передачи!"
        )
        return
        
    client_id = active_dialogs[user_id]
    
    # Check if target admin exists and is not busy
    if target_admin_id in active_dialogs:
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text="❌ Выбранный администратор уже занят другим диалогом!\n"
                 "Попробуйте выбрать другого администратора:"
        )
        keyboard = get_admin_transfer_keyboard(user_id)
        context.bot.edit_message_reply_markup(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            reply_markup=keyboard
        )
        return
        
    try:
        # Get admin names
        target_admin = context.bot.get_chat(target_admin_id)
        target_admin_name = get_user_display_name(target_admin)
        target_admin_tag = admin_tags.get(target_admin_id, "Admin")
        
        client = context.bot.get_chat(client_id)
        client_name = get_user_display_name(client)
        
        # Transfer dialog
        del active_dialogs[user_id]
        active_dialogs[target_admin_id] = client_id
        
        # Save state after transfer
        save_state()
        
        # Notify all parties
        context.bot.send_message(
            chat_id=client_id,
            text=f"👋 Ваш диалог был передан администратору #{target_admin_tag}"
        )
        
        context.bot.send_message(
            chat_id=target_admin_id,
            text=f"👋 Вам был передан клиент {client_name} от администратора {user_display_name}"
        )
        
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text=f"✅ Клиент {client_name} успешно передан администратору #{target_admin_tag}"
        )
        
        logger.info(f"Админ {user_display_name} передал клиента {client_name} админу #{target_admin_tag}")
        
    except Exception as e:
        logger.error(f"Error transferring client: {e}")
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=query.message.message_id,
            text="❌ Произошла ошибка при передаче клиента!"
        )
        return

def broadcast_command(update: Update, context: CallbackContext):
    """Send broadcast message to all users (admin level 2 only)"""
    user = update.effective_user
    user_id = user.id
    
    if admin_levels.get(user_id) != 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=ERROR_BANNER.format("Эта команда доступна только админам 2 уровня!")
        )
        return
    
    if not context.args:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="ℹ️ Использование: /broadcast <сообщение>"
        )
        return
    
    message = ' '.join(context.args)
    
    # Get all unique users from dialog history
    conn = sqlite3.connect('support_bot.db')
    c = conn.cursor()
    try:
        c.execute('''
            SELECT DISTINCT sender_id FROM dialog_history 
            WHERE sender_id NOT IN (SELECT admin_id FROM admin_levels)
        ''')
        users = c.fetchall()
    finally:
        c.close()
        conn.close()
    
    success_count = 0
    fail_count = 0
    
    broadcast_msg = f"""
📢 Оголошение от администрации:

{message}
"""
    
    for user_row in users:
        try:
            context.bot.send_message(chat_id=user_row[0], text=broadcast_msg)
            success_count += 1
            time.sleep(0.1)  # Prevent flooding
        except Exception as e:
            logger.error(f"Error sending broadcast to {user_row[0]}: {e}")
            fail_count += 1
    
    result_msg = f"""
📊 Результаты рассылки:
✅ Успешно: {success_count}
❌ Ошибок: {fail_count}
"""
    context.bot.send_message(chat_id=update.effective_chat.id, text=result_msg)

def promote_admin_command(update: Update, context: CallbackContext):
    """Handle /promote command to upgrade admin level"""
    user = update.effective_user
    user_id = user.id
    user_display_name = get_user_display_name(user)
    
    # Check if command user is level 2 admin
    if admin_levels.get(user_id) != 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Только администраторы 2-го ранга могут повышать других администраторов!"
        )
        return

    # Check command arguments
    args = context.args
    if len(args) != 1:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Использование: /promote <user_id>"
        )
        return

    try:
        target_id = int(args[0])
        target_chat = context.bot.get_chat(target_id)
        target_name = get_user_display_name(target_chat)
    except ValueError:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Неверный формат ID пользователя!"
        )
        return
    except Exception as e:
        logger.error(f"Error getting target user info: {e}")
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Не удалось получить информацию о пользователе!"
        )
        return

    # Check if target is already an admin
    if target_id not in admin_levels:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Указанный пользователь не является администратором!"
        )
        return

    # Check if target is already level 2
    if admin_levels[target_id] == 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Этот администратор уже имеет 2-й ранг!"
        )
        return

    # Promote admin to level 2
    admin_levels[target_id] = 2
    admin_active_status[target_id] = True  # Ensure promoted admin is active
    save_state()  # Save changes
    
    # Notify both admins
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"✅ Администратор {target_name} (ID: {target_id}) повышен до 2-го ранга!"
    )
    
    try:
        context.bot.send_message(
            chat_id=target_id,
            text="🎉 Поздравляем! Вы были повышены до администратора 2-го ранга!\n"
                 "Теперь вам доступны дополнительные функции:\n"
                 "- Мониторинг всех диалогов\n"
                 "- Генерация паролей для новых администраторов\n"
                 "- Повышение других администраторов"
        )
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление администратору {target_name} (ID: {target_id}): {e}")

    logger.info(f"Админ {user_display_name} (ID: {user_id}) повысил админа {target_name} (ID: {target_id}) до 2-го ранга")

def monitor_command(update: Update, context: CallbackContext):
    """Handle /monitor command for level 2 admins"""
    user_id = update.effective_user.id
    if admin_levels.get(user_id) != 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Эта команда доступна только администраторам 2-го уровня!"
        )
        return

    conn = sqlite3.connect('support_bot.db')
    c = conn.cursor()
    
    # Get active dialogs with admin names
    active_dialog_info = []
    for admin_id, client_id in active_dialogs.items():
        admin_tag = admin_tags.get(admin_id, "Unknown")
        try:
            admin = context.bot.get_chat(admin_id)
            admin_name = get_user_display_name(admin)
            client = context.bot.get_chat(client_id)
            client_name = get_user_display_name(client)
            
            # Get last 3 messages from this dialog
            c.execute('''
                SELECT sender_id, message, timestamp 
                FROM dialog_history 
                WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?)
                ORDER BY timestamp DESC LIMIT 3
            ''', (admin_id, client_id, client_id, admin_id))
            
            messages = c.fetchall()
            dialog_preview = "\n".join([
                f"{'👤' if msg[0] == client_id else '👨‍💼'} {msg[1][:50]}..." 
                for msg in messages
            ])
            
            active_dialog_info.append(
                f"👨‍💼 Админ: #{admin_tag} - {admin_name}\n"
                f"👤 Клиент: {client_name}\n"
                f"💬 Последние сообщения:\n{dialog_preview}\n"
                f"➖➖➖➖➖➖➖➖➖➖"
            )
        except:
            continue
    
    if active_dialog_info:
        message = "📊 Активные диалоги:\n\n" + "\n\n".join(active_dialog_info)
    else:
        message = "📊 Сейчас нет активных диалогов"
    
    conn.close()
    context.bot.send_message(chat_id=update.effective_chat.id, text=message)

def get_statistics(context: CallbackContext) -> str:
    """Get detailed statistics about bot usage with caching"""
    current_time = time.time()  # Fix: Use time.time() instead of time()
    
    # Check cache
    if 'stats' in stats_cache:
        cached_stats, cache_time = stats_cache['stats']
        if current_time - cache_time < STATS_CACHE_TTL:
            return cached_stats
    
    conn = sqlite3.connect(DB_PATH)  # Use DB_PATH constant
    c = conn.cursor()
    
    try:
        stats = []
        
        # Get total number of messages
        c.execute('SELECT COUNT(*) FROM dialog_history')
        total_messages = c.fetchone()[0]
        stats.append(f"📨 Всего сообщений: {total_messages}")
        
        # Get number of active dialogs
        active_dialog_count = len(active_dialogs)
        stats.append(f"💬 Активных диалогов: {active_dialog_count}")
        
        # Get statistics for each admin
        stats.append("\n👨‍💼 Статистика администраторов:")
        for admin_id, tag in admin_tags.items():
            try:
                # Get admin info
                admin = context.bot.get_chat(admin_id)
                admin_name = get_user_display_name(admin)
                
                # Get total messages sent by this admin
                c.execute('SELECT COUNT(*) FROM dialog_history WHERE sender_id = ?', (admin_id,))
                messages_sent = c.fetchone()[0]
                
                # Get total dialogs participated
                c.execute('SELECT COUNT(DISTINCT receiver_id) FROM dialog_history WHERE sender_id = ?', (admin_id,))
                total_dialogs = c.fetchone()[0]
                
                # Get average response time
                c.execute('''
                    SELECT AVG(response_time) FROM (
                        SELECT 
                            h1.timestamp as msg_time,
                            MIN(h2.timestamp) as response_time,
                            (JULIANDAY(MIN(h2.timestamp)) - JULIANDAY(h1.timestamp)) * 24 * 60 as response_time_minutes
                        FROM dialog_history h1
                        LEFT JOIN dialog_history h2 ON h2.sender_id = ? 
                            AND h2.receiver_id = h1.sender_id
                            AND h2.timestamp > h1.timestamp
                        WHERE h1.receiver_id = ?
                        GROUP BY h1.message_id
                    ) WHERE response_time IS NOT NULL
                ''', (admin_id, admin_id))
                avg_response_time = c.fetchone()[0]
                
                # Get current status
                status = "🔴 занят" if admin_id in active_dialogs else "🟢 свободен"
                
                admin_stats = [
                    f"#{tag} - {admin_name}",
                    f"├ Статус: {status}",
                    f"├ Отправлено сообщений: {messages_sent}",
                    f"├ Всего диалогов: {total_dialogs}",
                    f"└ Среднее время ответа: {int(avg_response_time) if avg_response_time else 0} мин."
                ]
                stats.extend(admin_stats)
            except Exception as e:
                logger.error(f"Error getting stats for admin {admin_id}: {e}")
                continue
        
        # Get today's statistics
        today = datetime.now().strftime('%Y-%m-%d')
        c.execute('''
            SELECT COUNT(*) FROM dialog_history 
            WHERE date(timestamp) = date('now')
        ''')
        today_messages = c.fetchone()[0]
        stats.append(f"\n📅 Сообщений за сегодня: {today_messages}")
        
        # Get busiest hour
        c.execute('''
            SELECT 
                strftime('%H', timestamp) as hour,
                COUNT(*) as message_count
            FROM dialog_history
            GROUP BY hour
            ORDER BY message_count DESC
            LIMIT 1
        ''')
        busiest_hour = c.fetchone()
        if busiest_hour:
            stats.append(f"⏰ Самый активный час: {busiest_hour[0]}:00 ({busiest_hour[1]} сообщений)")
        
        result = "\n".join(stats)
        
        # Update cache
        stats_cache['stats'] = (result, current_time)
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return "❌ Ошибка при получении статистики"
    finally:
        conn.close()

def stats_command(update: Update, context: CallbackContext):
    """Handle /stats command"""
    user = update.effective_user
    user_id = user.id
    user_display_name = get_user_display_name(user)
    
    if user_id not in admin_levels:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Эта команда доступна только администраторам!"
        )
        logger.warning(f"Попытка доступа к статистике от пользователя {user_display_name} (ID: {user_id})")
        return

    stats_text = get_statistics(context)
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"📊 Статистика бота:\n\n{stats_text}"
    )
    logger.info(f"Админ {user_display_name} (ID: {user_id}) запросил статистику")

def toggle_activity_status(update: Update, context: CallbackContext):
    """Toggle admin's activity status"""
    user = update.effective_user
    user_id = user.id
    user_display_name = get_user_display_name(user)
    
    if user_id not in admin_levels:
        return
        
    # Toggle status
    current_status = admin_active_status.get(user_id, True)
    new_status = not current_status
    admin_active_status[user_id] = new_status
    
    logger.info(f"Админ {user_display_name} (ID: {user_id}) изменил статус:")
    logger.info(f"- Старый статус: {'активный' if current_status else 'неактивный'}")
    logger.info(f"- Новый статус: {'активный' if new_status else 'неактивный'}")
    logger.info(f"- Текущие статусы админов: {admin_active_status}")
    
    save_state()

def update_admin_panel(update: Update, context: CallbackContext):
    """Update admin panel with current status"""
    user_id = update.effective_user.id
    is_active = admin_active_status.get(user_id, True)
    status_text = "🟢 Активный" if is_active else "🔴 Неактивный"
    
    keyboard = [
        [InlineKeyboardButton(status_text, callback_data="toggle_status")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("🚪 Завершить диалог", callback_data="close_dialog")],
        [InlineKeyboardButton("🔄 Передать клиента", callback_data="transfer_client")]
    ]
    
    if admin_levels.get(user_id) == 2:
        keyboard.extend([
            [InlineKeyboardButton("🔍 Мониторинг диалогов", callback_data="monitor")],
            [InlineKeyboardButton("🔑 Сгенерировать пароль для админа", callback_data="generate_password")],
            [InlineKeyboardButton("⭐ Инструкция по повышению админа", callback_data="promote_info")]
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
            text=f"⚙️ Панель администратора\nВаш статус: {status_text}",
            reply_markup=reply_markup
        )
    else:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⚙️ Панель администратора\nВаш статус: {status_text}",
            reply_markup=reply_markup
        )

def feedback_command(update: Update, context: CallbackContext):
    """Handle user feedback"""
    user = update.effective_user
    user_display_name = get_user_display_name(user)
    
    if not context.args:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="ℹ️ Использование: /feedback <ваш отзыв>"
        )
        return
    
    feedback = ' '.join(context.args)
    
    # Save feedback to database
    conn = sqlite3.connect('support_bot.db')
    c = conn.cursor()
    try:
        c.execute(
            'INSERT INTO feedback (user_id, feedback, timestamp) VALUES (?, ?, datetime("now"))',
            (user.id, feedback)
        )
        conn.commit()
    finally:
        c.close()
        conn.close()
    
    # Notify admins
    admin_msg = f"""
🔔 Новый отзыв от пользователя:
👤 {user_display_name}
💬 {feedback}
"""
    context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_msg)
    
    # Thank the user
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🙏 Спасибо за ваш отзыв! Мы обязательно его рассмотрим."
    )

def cleanup_tasks():
    """Periodic cleanup of caches and temporary data"""
    while True:
        try:
            # Clear message queue if too large
            if len(message_queue) > 900:
                message_queue.clear()
            
            # Clear expired passwords
            current_time = datetime.now()
            expired_passwords = [
                pwd for pwd, time in one_time_passwords.items()
                if (current_time - time).total_seconds() > 86400
            ]
            for pwd in expired_passwords:
                del one_time_passwords[pwd]
            
            # Clear caches
            user_cache.clear()
            admin_cache.clear()
            
            # Clear function cache
            get_user_display_name.cache_clear()
            
            # Force garbage collection
            import gc
            gc.collect()
            
            time.sleep(3600)  # Run every hour
            
        except Exception as e:
            logger.error(f"Error in cleanup_tasks: {e}")
            time.sleep(3600)

def optimize_database():
    """Optimize database periodically"""
    while True:
        try:
            # First connection for deletion
            conn = sqlite3.connect('support_bot.db')
            c = conn.cursor()
            try:
                # Remove old messages
                c.execute(
                    'DELETE FROM dialog_history WHERE timestamp < datetime("now", "-30 days")'
                )
                conn.commit()
            finally:
                c.close()
                conn.close()

            # Second connection for VACUUM
            conn = sqlite3.connect('support_bot.db')
            try:
                conn.execute('VACUUM')
                conn.commit()
            finally:
                conn.close()
                
            time.sleep(86400)  # Run daily
            
        except Exception as e:
            logger.error(f"Error in optimize_database: {e}")
            time.sleep(3600)

def cleanup_old_dialogs():
    """Cleanup old dialogs periodically"""
    while True:
        try:
            conn = sqlite3.connect('support_bot.db')
            c = conn.cursor()
            try:
                # Remove dialogs older than DIALOG_TIMEOUT
                c.execute(
                    'DELETE FROM dialog_history WHERE timestamp < datetime("now", ?)',
                    (f'-{DIALOG_TIMEOUT} seconds',)
                )
                conn.commit()
            finally:
                c.close()
                conn.close()
            
            time.sleep(3600)  # Run cleanup every hour
            
        except Exception as e:
            logger.error(f"Error in cleanup_old_dialogs: {e}")
            time.sleep(3600)

def main():
    """Main function to run the bot"""
    try:
        # Check for running instances
        current_process = psutil.Process()
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if proc.info['name'] == 'python.exe' and proc.pid != current_process.pid:
                try:
                    cmdline = proc.info['cmdline']
                    if cmdline and 'support_bot.py' in cmdline[-1]:
                        logger.error("Another instance of the bot is already running!")
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

        if not BOT_TOKEN:
            logger.error("Не установлен токен бота!")
            return

        # Initialize database
        init_database()
        
        # Create updater and application
        updater = Updater(BOT_TOKEN, use_context=True, workers=8)
        dispatcher = updater.dispatcher

        # Register handlers with run_async parameter
        dispatcher.add_handler(CommandHandler("start", start_command, run_async=True))
        dispatcher.add_handler(CommandHandler("help", help_command, run_async=True))
        dispatcher.add_handler(CommandHandler("admin", admin_command, run_async=True))
        dispatcher.add_handler(CommandHandler("set_tag", set_tag_command, run_async=True))
        dispatcher.add_handler(CommandHandler("promote", promote_admin_command, run_async=True))
        dispatcher.add_handler(CommandHandler("admins", list_admins_command, run_async=True))
        dispatcher.add_handler(CommandHandler("monitor", monitor_command, run_async=True))
        dispatcher.add_handler(CommandHandler("stats", stats_command, run_async=True))
        dispatcher.add_handler(CommandHandler("feedback", feedback_command, run_async=True))
        dispatcher.add_handler(CommandHandler("broadcast", broadcast_command, run_async=True))
        
        # Message handler with custom filters
        message_handler = CustomMessageHandler()
        dispatcher.add_handler(MessageHandler(
            Filters.text & ~Filters.command & Filters.chat_type.private,
            handle_message,
            run_async=True
        ))
        
        # Callback query handler
        dispatcher.add_handler(CallbackQueryHandler(button_callback, run_async=True))

        # Add error handler
        def error_handler(update, context):
            """Log Errors caused by Updates."""
            logger.error(f'Update "{update}" caused error "{context.error}"')
            
        dispatcher.add_error_handler(error_handler)

        # Start background tasks in threads
        import threading
        threading.Thread(target=cleanup_tasks, daemon=True).start()
        threading.Thread(target=cleanup_old_dialogs, daemon=True).start()
        threading.Thread(target=optimize_database, daemon=True).start()

        # Start the bot
        logger.info("Запуск бота с оптимизированными настройками...")
        updater.start_polling()
        updater.idle()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        save_state()
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Неожиданная ошибка: {e}")
        raise 