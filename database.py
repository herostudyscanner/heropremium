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
            """)
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
        return {"total_accounts": total or 0, "last_success": last['success_count'] if last else 0, "last_total": last['total_count'] if last else 0}

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
        return {"accounts": [dict(a) for a in accounts], "logs": [dict(l) for l in logs]}
