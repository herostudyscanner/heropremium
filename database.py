import asyncpg
import os
import logging
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
logger = logging.getLogger(__name__)

async def init_db():
    global pool
    try:
        if pool is None:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        async with pool.acquire() as conn:
            # Asosiy foydalanuvchilar (is_premium qo'shildi)
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
                # Arvoh davomat uchun Dars jadvallari bazasi
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
            logger.info("✅ Baza muvaffaqiyatli ishga tushdi (Premium va Schedule qo'shilgan)")
    except Exception as e:
        logger.error(f"❌ DB Xatosi: {e}")

# ... (Sizning oldingi get_or_create_user va boshqa funksiyalaringiz shu yerda turadi)
# Pastga bitta yangi funksiya qo'shamiz:

async def set_premium(telegram_id: int):
    """To'lov o'tgach foydalanuvchini Premium qilish"""
    async with pool.acquire() as conn:
        await conn.execute("UPDATE app_users SET is_premium = TRUE WHERE telegram_id = $1", telegram_id)

async def check_premium(login: str):
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT is_premium FROM app_users WHERE login = $1", login)
        return val if val else False
