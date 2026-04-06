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

# YANGA QO'SHILDI: Akkaunt bazaga qo'shilayotganda tokenni darhol olish uchun
async def verify_account(email: str, password: str) -> tuple[bool, str]:
    login_url = "https://api.newuzbekistan.hero.study/v1/users/login?lang=en"
    payload = {"email": email, "pass": password, "remember": "", "clientToken": ""}
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post(login_url, json=payload, headers=get_safe_headers(), ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    token = data.get("token") or data.get("access_token") or (data.get("data", {}) if isinstance(data.get("data"), dict) else {}).get("token")
                    if token:
                        return True, token
                return False, f"Hero tizimida xato. Status: {resp.status}"
    except Exception as e:
        return False, f"Hero serveriga ulanib bo'lmadi: {str(e)}"

# ESKI BOTDAGI AQLLI SKANER (Bor tokenni ishlatadi, xato bo'lsa yangilaydi)
async def scan_task(session: aiohttp.ClientSession, acc: dict, qr_url: str, db_callback):
    email = acc.get('email', '')
    password = acc.get('hero_password', '')
    token = acc.get('bearer_token', '')

    if not token or token == "NO_TOKEN" or token.startswith("ERROR:"):
        return {"email": email, "ok": False, "msg": "Nofaol akkaunt, iltimos parolni yangilang"}

    # 1-QADAM: To'g'ridan to'g'ri bor token bilan urib ko'rish (Tezlik uchun)
    headers = get_safe_headers()
    headers["Authorization"] = f"Bearer {token}"
    
    try:
        async with session.get(qr_url, headers=headers, ssl=False, timeout=12) as rp:
            if rp.status in [200, 201]:
                return {"email": email, "ok": True}
            elif rp.status not in [401, 403]:
                # Token ishladi, lekin QR kod eskirgan/noto'g'ri bo'lsa tokenni o'chirmaymiz
                return {"email": email, "ok": False, "reason": f"QR xato: {rp.status}"}
                
        # Faqat 401 yoki 403 (Token o'lgan) bo'lsagina pastga o'tib yangi token olamiz
        if '/v1/users/' in qr_url:
            login_url = f"{qr_url.split('/v1/')[0]}/v1/users/login?lang=en"
        else:
            login_url = f"{qr_url.split('/api/')[0]}/api/v1/auth/login"
            
        login_headers = get_safe_headers()
        payload = {"email": email, "pass": password, "remember": "", "clientToken": ""}
        
        async with session.post(login_url, json=payload, headers=login_headers, ssl=False, timeout=12) as lp:
            if lp.status != 200:
                if db_callback: await db_callback(email, "ERROR:LOGIN_FAILED")
                return {"email": email, "ok": False, "reason": "Parol o'zgargan"}
            
            data = await lp.json()
            new_token = data.get("token") or data.get("access_token") or (data.get("data", {}) if isinstance(data.get("data"), dict) else {}).get("token")
            
        if not new_token:
            return {"email": email, "ok": False, "reason": "Token kelmadi"}
            
        # Bazada tokenni yangilaymiz
        if db_callback:
            await db_callback(email, new_token)
            
        # Yangi token bilan qayta urib ko'ramiz
        headers["Authorization"] = f"Bearer {new_token}"
        async with session.get(qr_url, headers=headers, ssl=False, timeout=15) as rp:
            if rp.status in [200, 201]:
                return {"email": email, "ok": True}
            return {"email": email, "ok": False, "reason": f"scan_{rp.status}"}

    except asyncio.TimeoutError:
        return {"email": email, "ok": False, "reason": "timeout"}
    except Exception as e:
        return {"email": email, "ok": False, "reason": "unexpected error"}


async def mark_all_accounts_smart(accounts: list, qr_url: str, db_update_func) -> tuple:
    if not accounts:
        return 0, 0, 0.0, []

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=CONNECT_TIMEOUT) as session:
        tasks = [scan_task(session, dict(acc), qr_url, db_update_func) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    duration = round(time.time() - start_time, 2)
    report = [{"email": "?", "ok": False, "reason": str(r)} if isinstance(r, Exception) else r for r in results]
    success_count = sum(1 for r in report if r.get('ok'))
    
    return success_count, len(accounts), duration, report
