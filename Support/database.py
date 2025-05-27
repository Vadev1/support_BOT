import aiosqlite
import logging
from enum import Enum
from datetime import datetime
from contextlib import asynccontextmanager

# Configure logging
logger = logging.getLogger(__name__)

class DialogStatus(Enum):
    OPEN = "open"
    ASSIGNED = "assigned"
    CLOSED = "closed"

@asynccontextmanager
async def get_db():
    """Get database connection using context manager"""
    db = await aiosqlite.connect("support_bot.db")
    try:
        yield db
    finally:
        await db.close()

async def init_db():
    """Initialize database tables"""
    async with get_db() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                tag TEXT NOT NULL,
                level INTEGER NOT NULL,
                password TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS dialogues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                admin_id INTEGER,
                status TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                FOREIGN KEY (admin_id) REFERENCES admins(user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dialogue_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                FOREIGN KEY (dialogue_id) REFERENCES dialogues(id)
            )
        """)

        await db.commit()
        logger.info("Database tables created successfully")

async def create_dialogue(client_id: int):
    """Create a new dialogue"""
    async with get_db() as db:
        cursor = await db.execute(
            """
            INSERT INTO dialogues (client_id, status, start_time)
            VALUES (?, ?, ?)
            """,
            (client_id, DialogStatus.OPEN.value, datetime.now())
        )
        dialogue_id = cursor.lastrowid
        await db.commit()
        return dialogue_id

async def assign_dialogue(dialogue_id: int, admin_id: int):
    """Assign dialogue to admin"""
    async with get_db() as db:
        await db.execute(
            """
            UPDATE dialogues
            SET admin_id = ?, status = ?
            WHERE id = ?
            """,
            (admin_id, DialogStatus.ASSIGNED.value, dialogue_id)
        )
        await db.commit()

async def close_dialogue(dialogue_id: int):
    """Close dialogue"""
    async with get_db() as db:
        await db.execute(
            """
            UPDATE dialogues
            SET status = ?, end_time = ?
            WHERE id = ?
            """,
            (DialogStatus.CLOSED.value, datetime.now(), dialogue_id)
        )
        await db.commit()

async def add_message(dialogue_id: int, user_id: int, message: str):
    """Add message to dialogue"""
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO messages (dialogue_id, user_id, message, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (dialogue_id, user_id, message, datetime.now())
        )
        await db.commit()

async def get_admin_stats(admin_id: int):
    """Get admin statistics"""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*) as total_dialogues,
                   COUNT(CASE WHEN status = ? THEN 1 END) as active_dialogues,
                   COUNT(CASE WHEN status = ? THEN 1 END) as closed_dialogues
            FROM dialogues
            WHERE admin_id = ?
            """,
            (DialogStatus.ASSIGNED.value, DialogStatus.CLOSED.value, admin_id)
        )
        stats = await cursor.fetchone()
        return {
            "total_dialogues": stats[0],
            "active_dialogues": stats[1],
            "closed_dialogues": stats[2]
        }

async def get_dialogue_history(dialogue_id: int):
    """Get dialogue message history"""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT user_id, message, timestamp
            FROM messages
            WHERE dialogue_id = ?
            ORDER BY timestamp ASC
            """,
            (dialogue_id,)
        )
        return await cursor.fetchall()

async def get_active_dialogues():
    """Get all active dialogues"""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT d.id, d.client_id, d.admin_id, d.start_time, a.tag
            FROM dialogues d
            LEFT JOIN admins a ON d.admin_id = a.user_id
            WHERE d.status = ?
            ORDER BY d.start_time DESC
            """,
            (DialogStatus.ASSIGNED.value,)
        )
        return await cursor.fetchall()

async def get_admin_clients(admin_id: int):
    """Get list of clients for admin"""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT DISTINCT client_id
            FROM dialogues
            WHERE admin_id = ?
            ORDER BY start_time DESC
            """,
            (admin_id,)
        )
        return [row[0] for row in await cursor.fetchall()] 