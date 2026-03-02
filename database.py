"""
database.py — асинхронный SQLite-слой через aiosqlite.
Таблицы: users, sessions, events (лог ошибок/действий)
"""

import aiosqlite
import time
from datetime import datetime
from typing import Optional

DB_PATH = "manager.db"

# ══════════════════════════════════════════════════════
#   ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            uid          INTEGER PRIMARY KEY,
            username     TEXT,
            full_name    TEXT,
            joined_at    REAL    NOT NULL,
            last_seen    REAL    NOT NULL,
            is_admin     INTEGER NOT NULL DEFAULT 0,
            is_banned    INTEGER NOT NULL DEFAULT 0,
            sub_checked  INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sessions (
            phone        TEXT    PRIMARY KEY,
            owner_uid    INTEGER NOT NULL REFERENCES users(uid),
            session_str  TEXT    NOT NULL,
            added_at     REAL    NOT NULL,
            last_ok      REAL,
            is_alive     INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           REAL    NOT NULL,
            uid          INTEGER,
            event_type   TEXT    NOT NULL,
            detail       TEXT
        );
        """)
        await db.commit()


# ══════════════════════════════════════════════════════
#   USERS
# ══════════════════════════════════════════════════════
async def upsert_user(uid: int, username: Optional[str], full_name: Optional[str]):
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (uid, username, full_name, joined_at, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                username  = excluded.username,
                full_name = excluded.full_name,
                last_seen = excluded.last_seen
        """, (uid, username, full_name, now, now))
        await db.commit()


async def touch_user(uid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET last_seen=? WHERE uid=?", (time.time(), uid))
        await db.commit()


async def get_user(uid: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE uid=?", (uid,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_all_users() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users ORDER BY joined_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def set_admin(uid: int, flag: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_admin=? WHERE uid=?", (int(flag), uid))
        await db.commit()


async def set_banned(uid: int, flag: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned=? WHERE uid=?", (int(flag), uid))
        await db.commit()


async def set_sub_checked(uid: int, flag: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET sub_checked=? WHERE uid=?", (int(flag), uid))
        await db.commit()


async def count_users() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def count_active_users(since_hours: int = 24) -> int:
    cutoff = time.time() - since_hours * 3600
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE last_seen>=?", (cutoff,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


# ══════════════════════════════════════════════════════
#   SESSIONS
# ══════════════════════════════════════════════════════
async def add_session(phone: str, owner_uid: int, session_str: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO sessions (phone, owner_uid, session_str, added_at, last_ok, is_alive)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(phone) DO UPDATE SET
                session_str = excluded.session_str,
                last_ok     = excluded.last_ok,
                is_alive    = 1
        """, (phone, owner_uid, session_str, time.time(), time.time()))
        await db.commit()


async def remove_session(phone: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sessions WHERE phone=?", (phone,))
        await db.commit()


async def get_sessions_by_owner(uid: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM sessions WHERE owner_uid=? ORDER BY added_at DESC", (uid,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_all_sessions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT s.*, u.username, u.full_name
            FROM sessions s
            LEFT JOIN users u ON u.uid = s.owner_uid
            ORDER BY s.added_at DESC
        """) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def set_session_alive(phone: str, alive: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        extra = ", last_ok=?" if alive else ""
        args  = (int(alive), phone) if not alive else (int(alive), time.time(), phone)
        await db.execute(
            f"UPDATE sessions SET is_alive=?{extra} WHERE phone=?", args
        )
        await db.commit()


async def count_sessions(alive_only: bool = False) -> int:
    q = "SELECT COUNT(*) FROM sessions" + (" WHERE is_alive=1" if alive_only else "")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(q) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


# ══════════════════════════════════════════════════════
#   EVENTS
# ══════════════════════════════════════════════════════
async def log_event(event_type: str, detail: str = "", uid: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO events (ts, uid, event_type, detail) VALUES (?,?,?,?)",
            (time.time(), uid, event_type, detail[:4000])
        )
        await db.commit()


async def get_recent_events(limit: int = 50, event_type: Optional[str] = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if event_type:
            q   = "SELECT * FROM events WHERE event_type=? ORDER BY ts DESC LIMIT ?"
            args = (event_type, limit)
        else:
            q   = "SELECT * FROM events ORDER BY ts DESC LIMIT ?"
            args = (limit,)
        async with db.execute(q, args) as cur:
            return [dict(r) for r in await cur.fetchall()]


def ts_fmt(ts: Optional[float]) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
