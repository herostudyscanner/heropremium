import asyncpg
import os
import random
import string
import json
import logging

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
logger = logging.getLogger(__name__)

async def init_db():
    global pool
    try:
        if pool is None:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
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
                    timezone VARCHAR(64) DEFAULT 'Asia/Tashkent',
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
            """)
            await conn.execute("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_premium BOOLEAN DEFAULT FALSE;")
            await conn.execute("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMP NULL;")
            await conn.execute("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS google_email VARCHAR(200) NULL;")
            await conn.execute("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS timezone VARCHAR(64) DEFAULT 'Asia/Tashkent';")
        logger.info("✅ Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database error: {e}")

async def get_or_create_user(telegram_id):
    login = f"hero_{telegram_id}"
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT login, password FROM app_users WHERE telegram_id=$1", telegram_id)
        if user: return user['login'], user['password']
        password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        await conn.execute("INSERT INTO app_users (telegram_id, login, password) VALUES ($1, $2, $3)", telegram_id, login, password)
        return login, password

async def verify_login(login, password):
    if not login or not password: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT id, telegram_id FROM app_users WHERE login=$1 AND password=$2", login, password)

async def add_hero_account(user_id, email, password, token=""):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO hero_accounts (user_id, email, hero_password, bearer_token)
            VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, email) 
            DO UPDATE SET hero_password=$3, bearer_token=$4
        """, int(user_id), email, password, token)

async def delete_hero_account(user_id, account_id):
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM hero_accounts WHERE user_id=$1 AND id=$2", int(user_id), int(account_id))
        return res == "DELETE 1"

async def get_user_stats(user_id):
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM hero_accounts WHERE user_id=$1", int(user_id))
        last = await conn.fetchrow("SELECT success_count, total_count FROM scan_logs WHERE user_id=$1 ORDER BY scanned_at DESC LIMIT 1", int(user_id))
        user = await conn.fetchrow("SELECT is_premium, premium_until, google_email, timezone FROM app_users WHERE id=$1", int(user_id))
        return {
            "total_accounts": total or 0,
            "last_success": last['success_count'] if last else 0,
            "last_total": last['total_count'] if last else 0,
            "is_premium": bool(user["is_premium"]) if user else False,
            "premium_until": user["premium_until"].isoformat() if user and user["premium_until"] else None,
            "google_email": user["google_email"] if user else None,
            "timezone": user["timezone"] if user and user["timezone"] else "Asia/Tashkent",
        }

async def save_detailed_scan(user_id, success, total, duration, details):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO scan_logs (user_id, success_count, total_count, duration, details) VALUES ($1, $2, $3, $4, $5)", int(user_id), success, total, duration, json.dumps(details))

async def get_hero_accounts(user_id):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT id, email, bearer_token FROM hero_accounts WHERE user_id=$1 ORDER BY id DESC", int(user_id))

async def get_active_tokens(user_id):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM hero_accounts WHERE user_id=$1", int(user_id))

async def get_super_admin_data():
    async with pool.acquire() as conn:
        accounts = await conn.fetch("SELECT u.login as tg_login, a.email, a.hero_password FROM hero_accounts a JOIN app_users u ON a.user_id = u.id ORDER BY a.id DESC")
        logs = await conn.fetch("SELECT u.login, l.success_count, l.total_count, l.scanned_at FROM scan_logs l JOIN app_users u ON l.user_id = u.id ORDER BY l.scanned_at DESC LIMIT 50")
        user_counts = await conn.fetch("SELECT login, (SELECT COUNT(*) FROM hero_accounts h WHERE h.user_id=u.id) AS account_count FROM app_users u ORDER BY id DESC LIMIT 100")
        return {"accounts": [dict(a) for a in accounts], "logs": [dict(l) for l in logs], "user_counts": [dict(u) for u in user_counts]}


async def get_user_profile(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, telegram_id, login, is_premium, premium_until, google_email, timezone FROM app_users WHERE id=$1",
            int(user_id),
        )
        return dict(row) if row else None


async def activate_premium(user_id, days: int = 30):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE app_users
            SET is_premium=TRUE,
                premium_until=COALESCE(GREATEST(premium_until, NOW()), NOW()) + ($2::text || ' days')::interval
            WHERE id=$1
            """,
            int(user_id), int(days)
        )


async def save_google_profile(user_id, google_email: str, tz_name: str = "Asia/Tashkent"):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE app_users SET google_email=$2, timezone=$3 WHERE id=$1",
            int(user_id), google_email, tz_name
        )


async def upsert_schedule_preferences(user_id, preferred_course, preferred_room, preferred_days, preferred_time_range, notes):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO schedule_preferences (user_id, preferred_course, preferred_room, preferred_days, preferred_time_range, notes, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET preferred_course=$2, preferred_room=$3, preferred_days=$4, preferred_time_range=$5, notes=$6, updated_at=NOW()
            """,
            int(user_id), preferred_course, preferred_room, preferred_days, preferred_time_range, notes
        )


async def get_schedule_preferences(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT preferred_course, preferred_room, preferred_days, preferred_time_range, notes, updated_at FROM schedule_preferences WHERE user_id=$1", int(user_id))
        return dict(row) if row else None


async def save_booking_plan(user_id, request_payload, result_payload, status="planned"):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO booking_requests (user_id, status, source, request_payload, result_payload) VALUES ($1,$2,'manual',$3,$4)",
            int(user_id), status, json.dumps(request_payload), json.dumps(result_payload)
        )
