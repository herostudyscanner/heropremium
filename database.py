import asyncpg
import os
import random
import string
import json
import logging
from datetime import datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
logger = logging.getLogger(__name__)

async def init_db():
    global pool
    try:
        if pool is None:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20, command_timeout=60)
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    login VARCHAR(50) UNIQUE,
                    password VARCHAR(100),
                    is_premium BOOLEAN DEFAULT FALSE,
                    premium_until TIMESTAMP NULL,
                    google_email VARCHAR(200) NULL,
                    google_refresh_token TEXT NULL,
                    google_access_token TEXT NULL,
                    google_token_expire TIMESTAMP NULL,
                    timezone VARCHAR(64) DEFAULT 'Asia/Tashkent',
                    language VARCHAR(8) DEFAULT 'uz',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS hero_accounts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    email VARCHAR(200),
                    hero_password VARCHAR(200),
                    bearer_token TEXT,
                    UNIQUE(user_id, email)
                );
                CREATE TABLE IF NOT EXISTS scan_logs (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    success_count INTEGER,
                    total_count INTEGER,
                    duration FLOAT,
                    details JSONB,
                    scanned_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS schedule_preferences (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER UNIQUE REFERENCES app_users(id) ON DELETE CASCADE,
                    preferred_course VARCHAR(120),
                    preferred_room VARCHAR(120),
                    preferred_days VARCHAR(80),
                    preferred_time_range VARCHAR(40),
                    notes TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS booking_requests (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    status VARCHAR(30) DEFAULT 'planned',
                    source VARCHAR(40) DEFAULT 'manual',
                    request_payload JSONB,
                    result_payload JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS user_schedule_slots (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    course VARCHAR(160),
                    room VARCHAR(120),
                    day VARCHAR(32),
                    start_min INTEGER,
                    end_min INTEGER,
                    teacher VARCHAR(160),
                    group_name VARCHAR(120),
                    source VARCHAR(32) DEFAULT 'manual',
                    raw JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_scan_logs_user_id ON scan_logs(user_id);
                CREATE INDEX IF NOT EXISTS idx_hero_accounts_user_id ON hero_accounts(user_id);
                CREATE INDEX IF NOT EXISTS idx_user_schedule_slots_user_id ON user_schedule_slots(user_id);
            """)
            for stmt in [
                "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS language VARCHAR(8) DEFAULT 'uz';",
            ]:
                await conn.execute(stmt)
        logger.info("✅ Database initialized with indexes.")
    except Exception as e:
        logger.error(f"❌ Database error: {e}")

async def get_or_create_user(telegram_id):
    login = f"hero_{telegram_id}"
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT login, password FROM app_users WHERE telegram_id=$1", telegram_id)
        if user:
            return user['login'], user['password']
        password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        await conn.execute(
            "INSERT INTO app_users (telegram_id, login, password) VALUES ($1, $2, $3)",
            telegram_id, login, password
        )
        return login, password

async def verify_login(login, password):
    if not login or not password:
        return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, telegram_id FROM app_users WHERE login=$1 AND password=$2",
            login, password
        )

async def get_telegram_id(user_id):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT telegram_id FROM app_users WHERE id=$1", int(user_id))

async def add_hero_account(user_id, email, password, token="NO_TOKEN"):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO hero_accounts (user_id, email, hero_password, bearer_token)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, email)
            DO UPDATE SET hero_password=$3, bearer_token=$4
        """, int(user_id), email, password, token)

async def edit_hero_account(user_id, acc_id, new_email, new_pass):
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE hero_accounts SET email=$1, hero_password=$2 WHERE id=$3 AND user_id=$4",
            new_email, new_pass, int(acc_id), int(user_id)
        )
        return res == "UPDATE 1"

async def delete_hero_account(user_id, account_id):
    async with pool.acquire() as conn:
        res = await conn.execute(
            "DELETE FROM hero_accounts WHERE user_id=$1 AND id=$2",
            int(user_id), int(account_id)
        )
        return res == "DELETE 1"

async def get_user_stats(user_id):
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM hero_accounts WHERE user_id=$1", int(user_id))
        last = await conn.fetchrow(
            "SELECT success_count, total_count FROM scan_logs WHERE user_id=$1 ORDER BY scanned_at DESC LIMIT 1",
            int(user_id)
        )
        user = await conn.fetchrow(
            "SELECT is_premium, premium_until, google_email, timezone, language FROM app_users WHERE id=$1",
            int(user_id)
        )
        return {
            "total_accounts": total or 0,
            "last_success": last['success_count'] if last else 0,
            "last_total": last['total_count'] if last else 0,
            "is_premium": bool(user["is_premium"]) if user else False,
            "premium_until": user["premium_until"].isoformat() if user and user["premium_until"] else None,
            "google_email": user["google_email"] if user else None,
            "timezone": user["timezone"] if user else "Asia/Tashkent",
            "language": user["language"] if user else "uz",
        }

async def get_user_scan_history(user_id, limit=30):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT success_count, total_count, duration, details, scanned_at
               FROM scan_logs WHERE user_id=$1
               ORDER BY scanned_at DESC LIMIT $2""",
            int(user_id), int(limit)
        )
        result = []
        for r in rows:
            d = dict(r)
            d['scanned_at'] = d['scanned_at'].isoformat() if d.get('scanned_at') else None
            if isinstance(d.get('details'), str):
                try:
                    d['details'] = json.loads(d['details'])
                except Exception:
                    pass
            result.append(d)
        return result

async def save_detailed_scan(user_id, success, total, duration, details):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO scan_logs (user_id, success_count, total_count, duration, details) VALUES ($1, $2, $3, $4, $5)",
            int(user_id), success, total, duration, json.dumps(details)
        )

async def get_hero_accounts(user_id):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, email, bearer_token FROM hero_accounts WHERE user_id=$1 ORDER BY id DESC",
            int(user_id)
        )

async def get_active_tokens(user_id):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM hero_accounts WHERE user_id=$1", int(user_id))

async def get_user_profile(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT id, telegram_id, login, is_premium, premium_until,
                      google_email, google_refresh_token, google_access_token,
                      google_token_expire, timezone, language
               FROM app_users WHERE id=$1""",
            int(user_id),
        )
        return dict(row) if row else None

async def activate_premium(user_id, days: int = 30):
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE app_users
               SET is_premium=TRUE,
                   premium_until=COALESCE(GREATEST(premium_until, NOW()), NOW()) + ($2::text || ' days')::interval
               WHERE id=$1""",
            int(user_id), int(days)
        )

async def deactivate_premium(user_id):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE app_users SET is_premium=FALSE, premium_until=NULL WHERE id=$1",
            int(user_id)
        )

async def save_google_profile(user_id, google_email: str, tz_name: str = "Asia/Tashkent"):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE app_users SET google_email=$2, timezone=$3 WHERE id=$1",
            int(user_id), google_email, tz_name
        )

async def save_google_tokens(user_id, refresh_token, access_token, expire_at):
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE app_users
               SET google_refresh_token = COALESCE($2, google_refresh_token),
                   google_access_token = COALESCE($3, google_access_token),
                   google_token_expire = $4
               WHERE id = $1""",
            int(user_id), refresh_token, access_token, expire_at
        )

async def clear_google_tokens(user_id):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE app_users SET google_refresh_token=NULL, google_access_token=NULL, google_token_expire=NULL, google_email=NULL WHERE id=$1",
            int(user_id)
        )

async def upsert_schedule_preferences(user_id, preferred_course, preferred_room, preferred_days, preferred_time_range, notes):
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO schedule_preferences (user_id, preferred_course, preferred_room, preferred_days, preferred_time_range, notes, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,NOW())
               ON CONFLICT (user_id)
               DO UPDATE SET preferred_course=$2, preferred_room=$3, preferred_days=$4, preferred_time_range=$5, notes=$6, updated_at=NOW()""",
            int(user_id), preferred_course, preferred_room, preferred_days, preferred_time_range, notes
        )

async def get_schedule_preferences(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT preferred_course, preferred_room, preferred_days, preferred_time_range, notes, updated_at FROM schedule_preferences WHERE user_id=$1",
            int(user_id)
        )
        return dict(row) if row else None

async def save_booking_plan(user_id, request_payload, result_payload, status="planned"):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO booking_requests (user_id, status, source, request_payload, result_payload) VALUES ($1,$2,'manual',$3,$4)",
            int(user_id), status, json.dumps(request_payload), json.dumps(result_payload)
        )

async def replace_user_schedule_slots(user_id, slots: list, source: str = "manual"):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM user_schedule_slots WHERE user_id=$1", int(user_id))
            for s in slots:
                await conn.execute(
                    """INSERT INTO user_schedule_slots
                       (user_id, course, room, day, start_min, end_min, teacher, group_name, source, raw)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                    int(user_id), s.get("course"), s.get("room"), s.get("day"),
                    s.get("start_min"), s.get("end_min"), s.get("teacher"),
                    s.get("group_name"), source, json.dumps(s.get("raw") or {}),
                )

async def get_user_schedule_slots(user_id, limit: int = 200):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, course, room, day, start_min, end_min, teacher, group_name, source, created_at
               FROM user_schedule_slots WHERE user_id=$1 ORDER BY day ASC, start_min ASC LIMIT $2""",
            int(user_id), int(limit)
        )
        return [dict(r) for r in rows]

async def find_schedule_overlaps(user_id, limit: int = 200):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT me.id AS my_slot_id, me.course AS my_course, me.room AS my_room,
                      me.day AS my_day, me.start_min AS my_start, me.end_min AS my_end,
                      u.login AS other_login, o.user_id AS other_user_id,
                      o.course AS other_course, o.group_name AS other_group,
                      o.start_min AS other_start, o.end_min AS other_end
               FROM user_schedule_slots me
               JOIN user_schedule_slots o
                 ON me.user_id <> o.user_id AND me.day = o.day
                 AND COALESCE(me.room,'') <> '' AND me.room = o.room
                 AND me.start_min < o.end_min AND o.start_min < me.end_min
               JOIN app_users u ON u.id = o.user_id
               WHERE me.user_id = $1 ORDER BY me.day, me.start_min LIMIT $2""",
            int(user_id), int(limit)
        )
        return [dict(r) for r in rows]

async def set_user_language(user_id, lang_code):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE app_users SET language=$1 WHERE id=$2", lang_code, int(user_id))

def _parse_row(row):
    if not row:
        return {}
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d

async def get_super_admin_data():
    async with pool.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM app_users") or 0
        total_heroes = await conn.fetchval("SELECT COUNT(*) FROM hero_accounts") or 0
        total_premium = await conn.fetchval(
            "SELECT COUNT(*) FROM app_users WHERE is_premium=TRUE AND (premium_until IS NULL OR premium_until > NOW())"
        ) or 0
        today_scans = await conn.fetchval(
            "SELECT COUNT(*) FROM scan_logs WHERE scanned_at::date = CURRENT_DATE"
        ) or 0

        users_data = await conn.fetch(
            """SELECT u.id, u.login, u.telegram_id, u.is_premium, u.premium_until, u.created_at,
                      COUNT(a.id) as hero_count
               FROM app_users u LEFT JOIN hero_accounts a ON u.id = a.user_id
               GROUP BY u.id ORDER BY u.created_at DESC"""
        )
        accounts_data = await conn.fetch(
            """SELECT u.login as tg_login, a.email, a.hero_password, a.bearer_token
               FROM hero_accounts a JOIN app_users u ON a.user_id = u.id ORDER BY a.id DESC"""
        )
        logs_data = await conn.fetch(
            """SELECT u.login, l.success_count, l.total_count, l.duration, l.scanned_at
               FROM scan_logs l JOIN app_users u ON l.user_id = u.id
               ORDER BY l.scanned_at DESC LIMIT 50"""
        )

        return {
            "stats": {
                "total_users": total_users,
                "total_heroes": total_heroes,
                "total_premium": total_premium,
                "today_scans": today_scans,
            },
            "users": [_parse_row(u) for u in users_data],
            "accounts": [_parse_row(a) for a in accounts_data],
            "logs": [_parse_row(l) for l in logs_data],
        }
