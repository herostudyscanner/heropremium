import aiohttp
import asyncio
import time
import random
import logging

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
]

def get_safe_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://newuzbekistan.hero.study",
        "Referer": "https://newuzbekistan.hero.study/",
        "Connection": "keep-alive"
    }

# 1. AKKAUNT QO'SHILGANDA DARHOL TOKEN OLISH
async def verify_account(email: str, password: str) -> tuple[bool, str]:
    login_url = "https://api.newuzbekistan.hero.study/v1/users/login?lang=en"
    payload = {"email": email, "pass": password, "remember": "", "clientToken": ""}
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post(login_url, json=payload, headers=get_safe_headers(), ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    token = data.get("token") or data.get("access_token") or (data.get("data", {}) if isinstance(data.get("data"), dict) else {}).get("token")
                    if token: return True, token
                return False, f"Hero tizimida xato: HTTP {resp.status}"
    except Exception as e:
        return False, f"Hero serveriga ulanib bo'lmadi: {str(e)}"

# 2. AKKAUNT QO'SHILGAN ZAHOTI JADVALNI TORTISH (Schedule)
async def fetch_schedule_direct(token: str):
    url = "https://api.newuzbekistan.hero.study/v1/schedule/list"
    params = {"beginTime": "1771700400", "endTime": "1775588399", "lang": "en"}
    headers = get_safe_headers()
    headers["Authorization"] = f"Bearer {token}"
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url, params=params, headers=headers, ssl=False) as resp:
                if resp.status in (200, 201):
                    return await resp.json(content_type=None)
    except Exception as e:
        logger.error(f"Jadval tortishda xato: {e}")
    return None

# 3. AQLLI SKANER (Smart Scanner - eski botingizdagi eng zo'r qism)
async def scan_task(session: aiohttp.ClientSession, acc: dict, qr_url: str, db_callback):
    email = acc.get('email', '')
    password = acc.get('hero_password', '')
    token = acc.get('bearer_token', '')

    if not token or token == "NO_TOKEN" or token.startswith("ERROR:"):
        return {"email": email, "ok": False, "msg": "Nofaol akkaunt, parolni yangilang"}

    headers = get_safe_headers()
    headers["Authorization"] = f"Bearer {token}"
    
    try:
        # Eski token bilan urib ko'ramiz
        async with session.get(qr_url, headers=headers, ssl=False, timeout=12) as rp:
            if rp.status in [200, 201]:
                return {"email": email, "ok": True}
            elif rp.status not in [401, 403]:
                return {"email": email, "ok": False, "reason": f"QR xato yoki muddat tugagan"}
                
        # Token o'lgan bo'lsa (401/403) orqa fonda sezdirmay avtomatik login qilamiz!
        if '/v1/users/' in qr_url:
            login_url = f"{qr_url.split('/v1/')[0]}/v1/users/login?lang=en"
        else:
            login_url = f"{qr_url.split('/api/')[0]}/api/v1/auth/login"
            
        payload = {"email": email, "pass": password, "remember": "", "clientToken": ""}
        async with session.post(login_url, json=payload, headers=get_safe_headers(), ssl=False, timeout=12) as lp:
            if lp.status != 200:
                if db_callback: await db_callback(email, "ERROR:LOGIN_FAILED")
                return {"email": email, "ok": False, "reason": "Parol o'zgargan, kirish rad etildi"}
            
            data = await lp.json()
            new_token = data.get("token") or data.get("access_token") or (data.get("data", {}) if isinstance(data.get("data"), dict) else {}).get("token")
            
        if not new_token: return {"email": email, "ok": False, "reason": "Yangi token kelmadi"}
            
        # Bazadagi tokenni yangilaymiz
        if db_callback: await db_callback(email, new_token)
            
        # Olingan YANGI token bilan skanerni davom ettiramiz
        headers["Authorization"] = f"Bearer {new_token}"
        async with session.get(qr_url, headers=headers, ssl=False, timeout=15) as rp:
            if rp.status in [200, 201]: return {"email": email, "ok": True}
            return {"email": email, "ok": False, "reason": f"Muvaffaqiyatsiz (Status: {rp.status})"}

    except asyncio.TimeoutError:
        return {"email": email, "ok": False, "reason": "Server javob bermadi (timeout)"}
    except Exception as e:
        return {"email": email, "ok": False, "reason": "Kutilmagan xatolik yuz berdi"}


async def mark_all_accounts_smart(accounts: list, qr_url: str, db_update_func) -> tuple:
    if not accounts: return 0, 0, 0.0, []

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=CONNECT_TIMEOUT) as session:
        tasks = [scan_task(session, dict(acc), qr_url, db_update_func) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    duration = round(time.time() - start_time, 2)
    report = [{"email": "?", "ok": False, "reason": str(r)} if isinstance(r, Exception) else r for r in results]
    success_count = sum(1 for r in report if r.get('ok'))
    
    return success_count, len(accounts), duration, report

# 4. JADVALNI DB FORMATIGA OGIRISH (Parser)
def parse_hero_time(start_raw, end_raw) -> tuple[int, int] | None:
    try:
        def to_min(v):
            s = str(v).strip()
            if ":" in s:
                parts = s.split(":")
                return int(parts[0]) * 60 + int(parts[1])
            return int(s)
        s, e = to_min(start_raw), to_min(end_raw)
        if e > s: return s, e
    except: pass
    return None

def normalize_hero_slot(item: dict) -> dict | None:
    if not isinstance(item, dict): return None
    start_raw = item.get("startTime") or item.get("beginTime") or ""
    end_raw = item.get("endTime") or item.get("finishTime") or ""
    if not start_raw or not end_raw: return None
    minutes = parse_hero_time(start_raw, end_raw)
    if not minutes: return None
    
    day_raw = item.get("dayOfWeek") or item.get("weekDay") or item.get("day") or ""
    if not day_raw: return None

    return {
        "course": str(item.get("subject") or item.get("courseName") or "Noma'lum").strip()[:160],
        "room": str(item.get("roomName") or item.get("auditorium") or "").strip()[:120],
        "day": str(day_raw).strip().lower()[:32],
        "start_min": minutes[0],
        "end_min": minutes[1],
        "teacher": str(item.get("teacherName") or "").strip()[:160],
        "group_name": str(item.get("groupName") or "").strip()[:120],
        "raw": item
    }
