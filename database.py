import asyncpg
import os
import logging
import random
import string
import json
from datetime import datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
logger = logging.getLogger(__name__)

async def init_db():
    global pool
    try:
        if pool is None:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        async with pool.acquire() as conn:
            # SQL so'rovlari (Sharhlarsiz toza holatda)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    id SERIAL PRIMARY KEY, 
                    telegram_id BIGINT UNIQUE, 
                    login VARCHAR(50) UNIQUE,
                    password VARCHAR(100), 
                    trial_ends_at TIMESTAMP,
                    is_premium BOOLEAN DEFAULT FALSE,
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
                    scanned_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS global_schedule (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES app_users(id) ON DELETE CASCADE,
                    room_number VARCHAR(50),
                    subject_name VARCHAR(200),
                    start_time TIME,
                    end_time TIME,
                    date DATE DEFAULT CURRENT_DATE
                );
            """)
            logger.info("✅ Baza muvaffaqiyatli ishga tushdi")
    except Exception as e:
        logger.error(f"❌ DB Xatosi: {e}")

async def get_or_create_user(telegram_id: int):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT login, password, trial_ends_at FROM app_users WHERE telegram_id = $1", telegram_id)
        if user:
            return user['login'], user['password'], user['trial_ends_at']
        
        login = "hero_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
        password = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        ends_at = datetime.now() + timedelta(days=30)
        
        await conn.execute(
            "INSERT INTO app_users (telegram_id, login, password, trial_ends_at) VALUES ($1, $2, $3, $4)",
            telegram_id, login, password, ends_at
        )
        return login, password, ends_at

async def set_premium(telegram_id: int):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE app_users SET is_premium = TRUE WHERE telegram_id = $1", telegram_id)

async def check_premium(login: str):
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT is_premium FROM app_users WHERE login = $1", login)
        return val if val else False

# Panel uchun ma'lumotlarni olish (Agar main.py da kerak bo'lsa)
async def get_user_data(login: str):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM app_users WHERE login = $1", login)

async def get_user_accounts(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM hero_accounts WHERE user_id = $1", user_id)
