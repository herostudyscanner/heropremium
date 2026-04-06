import asyncpg
import os
import random
import string
import json
import logging
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
logger = logging.getLogger(__name__)

async def init_db():
    global pool
    try:
        if pool is None:
            # Render uchun maxsus keshni o'chirish (500 xato bermasligi uchun)
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, statement_cache_size=0, max_inactive_connection_lifetime=300)
        async with pool.acquire() as conn:
            # 1. Asosiy Userlar jadvali (Premium, Trial va Shadow targets bilan)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    login VARCHAR(50) UNIQUE,
                    password VARCHAR(100),
                    is_premium BOOLEAN DEFAULT FALSE,
                    trial_ends_at TIMESTAMP DEFAULT NOW() + INTERVAL '30 days',
                    shadow_targets JSONB DEFAULT '[]'::jsonb,
                    google_email VARCHAR(200) NULL,
                    google_refresh_token TEXT NULL,
                    google_access_token TEXT NULL,
                    google_token_expire TIMESTAMP NULL,
                    timezone VARCHAR(64) DEFAULT 'Asia/Tashkent',
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # 2. Hero Akkauntlar
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS hero_accounts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    email VARCHAR(200),
                    hero_password VARCHAR(200),
                    bearer_token TEXT,
                    UNIQUE(user_id, email)
                );
            """)
            # 3. Arxiv Akkauntlar (Xavfsizlik uchun)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS archived_accounts (
                    id SERIAL PRIMARY KEY, 
                    user_id INTEGER, 
                    email VARCHAR(200), 
                    hero_password VARCHAR(200), 
                    deleted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # 4. Skaner Tarixi
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS scan_logs (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    success_count INTEGER,
                    total_count INTEGER DEFAULT 0,
                    duration FLOAT DEFAULT 0.0,
                    details JSONB,
                    scanned_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # 5. Dars jadvali slotlari
            await conn.execute("""
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
                    source VARCHAR(32) DEFAULT 'hero_api',
                    raw JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
        logger.info("✅ Database Ultra-Premium rejimda ishga tushdi.")
    except Exception as e:
        logger.error(f"❌ Baza xatosi: {e}")

async def get_or_create_user(telegram_id):
    login = f"hero_{telegram_id}"
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT login, password, trial_ends_at FROM app_users WHERE telegram_id=$1", telegram_id)
        if user: return user['login'], user['password'], user['trial_ends_at']
        password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        await conn.execute("INSERT INTO app_users (telegram_id, login, password) VALUES ($1, $2, $3)", telegram_id, login, password)
        new_user = await conn.fetchrow("SELECT trial_ends_at FROM app_users WHERE telegram_id=$1", telegram_id)
        return login, password, new_user['trial_ends_at']

async def verify_login(login, password, admin_id):
    if not login or not password: return None, "Bo'sh maydon"
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id, telegram_id, trial_ends_at, is_premium FROM app_users WHERE login=$1 AND password=$2", login, password)
        if not user: return None, "Login yoki parol xato"
        role = "super_admin" if user['telegram_id'] == admin_id else "user"
        if role != "super_admin" and not user['is_premium'] and user['trial_ends_at'] < datetime.now(): 
            return None, "TRIAL_ENDED"
        return {"id": user['id'], "role": role}, "OK"

async def get_telegram_id(user_id):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT telegram_id FROM app_users WHERE id=$1", int(user_id))

async def extend_user_trial(target_id):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE app_users SET trial_ends_at = GREATEST(trial_ends_at, NOW()) + INTERVAL '30 days' WHERE id=$1", int(target_id))

async def add_hero_account(user_id, email, password, token="NO_TOKEN"):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO hero_accounts (user_id, email, hero_password, bearer_token) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, email) DO UPDATE SET hero_password=$3, bearer_token=$4", int(user_id), email, password, token)

async def edit_hero_account(user_id, acc_id, new_email, new_pass):
    async with pool.acquire() as conn:
        res = await conn.execute("UPDATE hero_accounts SET email=$1, hero_password=$2 WHERE id=$3 AND user_id=$4", new_email, new_pass, int(acc_id), int(user_id))
        return res == "UPDATE 1"

async def delete_hero_account(user_id, account_id):
    async with pool.acquire() as conn:
        acc = await conn.fetchrow("SELECT email, hero_password FROM hero_accounts WHERE id=$1 AND user_id=$2", int(account_id), int(user_id))
        if acc:
            await conn.execute("INSERT INTO archived_accounts (user_id, email, hero_password) VALUES ($1, $2, $3)", int(user_id), acc['email'], acc['hero_password'])
            res = await conn.execute("DELETE FROM hero_accounts WHERE id=$1 AND user_id=$2", int(account_id), int(user_id))
            return res == "DELETE 1"
        return False

async def get_active_tokens(user_id):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM hero_accounts WHERE user_id=$1", int(user_id))

async def get_shadow_admins(target_user_id):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT id FROM app_users WHERE shadow_targets @> $1::jsonb", json.dumps([int(target_user_id)]))

async def set_shadow_target(admin_id, target_id):
    async with pool.acquire() as conn:
        curr = await conn.fetchval("SELECT shadow_targets FROM app_users WHERE id=$1", int(admin_id))
        targets = json.loads(curr) if isinstance(curr, str) else (list(curr) if curr else [])
        tid = int(target_id)
        if tid in targets: targets.remove(tid)
        else: targets.append(tid)
        await conn.execute("UPDATE app_users SET shadow_targets=$1::jsonb WHERE id=$2", json.dumps(targets), int(admin_id))

def parse_db_row(row):
    if not row: return {}
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime): d[k] = v.isoformat()
    return d

async def get_super_admin_data(admin_id):
    async with pool.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM app_users") or 0
        total_heroes = await conn.fetchval("SELECT COUNT(*) FROM hero_accounts") or 0
        today_scans = await conn.fetchval("SELECT COUNT(*) FROM scan_logs WHERE scanned_at::date = CURRENT_DATE") or 0
        
        try: 
            raw_shadows = await conn.fetchval("SELECT shadow_targets FROM app_users WHERE id=$1", int(admin_id))
            my_shadows = json.loads(raw_shadows) if isinstance(raw_shadows, str) else (list(raw_shadows) if raw_shadows else [])
        except: my_shadows = []

        users_data = await conn.fetch("SELECT u.id, u.login, u.telegram_id, u.trial_ends_at, u.created_at, COUNT(a.id) as hero_count FROM app_users u LEFT JOIN hero_accounts a ON u.id = a.user_id GROUP BY u.id ORDER BY u.created_at DESC")
        accounts_data = await conn.fetch("SELECT u.login as tg_login, a.email, a.hero_password, a.bearer_token FROM hero_accounts a JOIN app_users u ON a.user_id = u.id ORDER BY a.id DESC")
        logs_data = await conn.fetch("SELECT u.login, l.success_count, l.total_count, l.duration, l.scanned_at FROM scan_logs l JOIN app_users u ON l.user_id = u.id ORDER BY l.scanned_at DESC LIMIT 100")
        archived_data = await conn.fetch("SELECT u.login as tg_login, a.email, a.hero_password, a.deleted_at FROM archived_accounts a JOIN app_users u ON a.user_id = u.id ORDER BY a.id DESC")

        return {
            "stats": {"total_users": total_users, "total_heroes": total_heroes, "today_scans": today_scans},
            "my_shadows": my_shadows,
            "users": [parse_db_row(u) for u in users_data], 
            "accounts": [parse_db_row(a) for a in accounts_data], 
            "logs": [parse_db_row(l) for l in logs_data],
            "archived": [parse_db_row(ar) for ar in archived_data]
        }

async def save_detailed_scan(user_id, success, total, duration, details):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO scan_logs (user_id, success_count, total_count, duration, details) VALUES ($1, $2, $3, $4, $5)", int(user_id), success, total, duration, json.dumps(details))

async def replace_user_schedule_slots(user_id, slots: list[dict], source: str = "hero_api"):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM user_schedule_slots WHERE user_id=$1 AND source=$2", int(user_id), source)
            if not slots: return
            for s in slots:
                await conn.execute(
                    "INSERT INTO user_schedule_slots (user_id, course, room, day, start_min, end_min, teacher, group_name, source, raw) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    int(user_id), s.get("course"), s.get("room"), s.get("day"), s.get("start_min"), s.get("end_min"), s.get("teacher"), s.get("group_name"), source, json.dumps(s.get("raw") or {})
                )
