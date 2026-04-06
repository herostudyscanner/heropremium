import aiohttp
import asyncio
import time
import logging

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)

async def scan_task(session: aiohttp.ClientSession, acc: dict, qr_url: str, db_callback):
    email = acc.get('email', '')
    password = acc.get('hero_password', '')

    try:
        # Resolve login URL from QR URL pattern
        if '/v1/users/' in qr_url:
            base_url = qr_url.split('/v1/')[0]
            login_url = f"{base_url}/v1/users/login?lang=en"
        elif '/api/' in qr_url:
            base_url = qr_url.split('/api/')[0]
            login_url = f"{base_url}/api/v1/auth/login"
        else:
            logger.warning(f"Unknown URL pattern: {qr_url}")
            return {"email": email, "ok": False, "reason": "unknown_url"}

        # Step 1: Login → get token
        async with session.post(
            login_url,
            json={"email": email, "password": password},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as lp:
            if lp.status != 200:
                logger.debug(f"Login failed [{email}]: HTTP {lp.status}")
                return {"email": email, "ok": False, "reason": f"login_{lp.status}"}
            try:
                data = await lp.json(content_type=None)
            except Exception:
                return {"email": email, "ok": False, "reason": "json_parse_error"}

        # Extract token from various response shapes
        token = (
            data.get("token")
            or data.get("access_token")
            or (data.get("data") or {}).get("token")
            or (data.get("data") or {}).get("access_token")
        )
        if not token:
            return {"email": email, "ok": False, "reason": "no_token"}

        # Step 2: Hit QR URL with token
        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "HeroScanner/PRO-2.0",
            "Accept": "application/json",
        }
        async with session.get(
            qr_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=12)
        ) as rp:
            if rp.status in (200, 201):
                if db_callback:
                    try:
                        await db_callback(email, token)
                    except Exception as cb_err:
                        logger.error(f"DB callback error [{email}]: {cb_err}")
                return {"email": email, "ok": True}
            return {"email": email, "ok": False, "reason": f"scan_{rp.status}"}

    except asyncio.TimeoutError:
        return {"email": email, "ok": False, "reason": "timeout"}
    except aiohttp.ClientError as ce:
        logger.warning(f"Network error [{email}]: {ce}")
        return {"email": email, "ok": False, "reason": "network_error"}
    except Exception as e:
        logger.error(f"Unexpected error [{email}]: {e}", exc_info=True)
        return {"email": email, "ok": False, "reason": "unexpected"}


async def mark_all_accounts_smart(accounts: list, qr_url: str, db_update_func) -> tuple:
    """
    Returns: (success_count, total_count, duration_seconds, report_list)
    """
    if not accounts:
        return 0, 0, 0.0, []

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=CONNECT_TIMEOUT) as session:
        tasks = [scan_task(session, dict(acc), qr_url, db_update_func) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    duration = round(time.time() - start_time, 2)

    report = []
    for r in results:
        if isinstance(r, Exception):
            report.append({"email": "?", "ok": False, "reason": str(r)})
        else:
            report.append(r)

    success_count = sum(1 for r in report if r.get('ok'))
    return success_count, len(accounts), duration, report
