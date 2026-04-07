import aiohttp
import asyncio
import time
import logging

logger = logging.getLogger(__name__)

# ─── TIMEOUTS ─────────────────────────────────────────────────────────────────
_TIMEOUT_CONNECT = aiohttp.ClientTimeout(total=20, connect=8)
_TIMEOUT_LOGIN   = aiohttp.ClientTimeout(total=15)
_TIMEOUT_SCAN    = aiohttp.ClientTimeout(total=15)

# ─── LOGIN URL RESOLVER ───────────────────────────────────────────────────────
def _resolve_login_url(qr_url: str) -> str | None:
    """QR URL dan login endpointini aniqlaydi."""
    if "/v1/users/" in qr_url or "/v1/" in qr_url:
        base = qr_url.split("/v1/")[0]
        return f"{base}/v1/users/login?lang=en"
    if "/api/" in qr_url:
        base = qr_url.split("/api/")[0]
        return f"{base}/api/v1/auth/login"
    return None

# ─── TOKEN EXTRACTOR ─────────────────────────────────────────────────────────
def _extract_token(data: dict) -> str | None:
    """Turli xil API javob formatlaridan token oladi."""
    return (
        data.get("token") or
        data.get("access_token") or
        (data.get("data") or {}).get("token") or
        (data.get("data") or {}).get("access_token") or
        (data.get("result") or {}).get("token")
    )

# ─── SINGLE ACCOUNT SCANNER ──────────────────────────────────────────────────
async def scan_task(
    session: aiohttp.ClientSession,
    acc: dict,
    qr_url: str,
    db_callback,
    retries: int = 2,
) -> dict:
    email     = acc.get("email", "")
    password  = acc.get("hero_password", "")
    cached_tk = acc.get("bearer_token", "")

    login_url = _resolve_login_url(qr_url)
    if not login_url:
        logger.warning(f"URL pattern noma'lum: {qr_url}")
        return {"email": email, "ok": False, "reason": "unknown_url"}

    # ── 1. Avval cached tokenni sinab ko'rish ────────────────────────────────
    if cached_tk and cached_tk not in ("NO_TOKEN", "", None):
        result = await _try_scan(session, qr_url, cached_tk)
        if result is True:
            return {"email": email, "ok": True, "source": "cached_token"}
        # 401/403 → token eskirgan, yangi olamiz
        if result not in ("timeout", "network"):
            logger.debug(f"Cached token yaroqsiz [{email}], qayta login qilinadi")

    # ── 2. Yangi token olish (retry bilan) ───────────────────────────────────
    token = None
    last_err = "unknown"
    for attempt in range(retries + 1):
        if attempt > 0:
            await asyncio.sleep(0.5 * attempt)   # progressive backoff

        try:
            # Hero Study API "pass" kalitini talab qiladi!
            payload = {
                "email": email,
                "pass": password,
                "remember": "",
                "clientToken": "",
            }
            async with session.post(
                login_url, json=payload, timeout=_TIMEOUT_LOGIN
            ) as lp:
                if lp.status == 429:
                    last_err = "rate_limited"
                    await asyncio.sleep(1)
                    continue
                if lp.status != 200:
                    last_err = f"login_{lp.status}"
                    logger.debug(f"Login failed [{email}]: HTTP {lp.status}")
                    break
                try:
                    data = await lp.json(content_type=None)
                except Exception:
                    last_err = "json_parse_error"
                    break

            token = _extract_token(data)
            if token:
                break
            last_err = "no_token"

        except asyncio.TimeoutError:
            last_err = "timeout"
        except aiohttp.ClientError as ce:
            last_err = "network_error"
            logger.warning(f"Network error [{email}] attempt {attempt}: {ce}")

    if not token:
        return {"email": email, "ok": False, "reason": last_err}

    # ── 3. QR endpoint ga so'rov ─────────────────────────────────────────────
    scan_result = await _try_scan(session, qr_url, token)
    if scan_result is True:
        if db_callback:
            try:
                await db_callback(email, token)
            except Exception as cb_err:
                logger.error(f"DB callback error [{email}]: {cb_err}")
        return {"email": email, "ok": True, "source": "fresh_token"}

    if scan_result in ("timeout", "network"):
        return {"email": email, "ok": False, "reason": scan_result}
    return {"email": email, "ok": False, "reason": f"scan_{scan_result}"}


async def _try_scan(
    session: aiohttp.ClientSession,
    qr_url: str,
    token: str,
) -> bool | str:
    """
    QR URL ni token bilan ura bosadi.
    True     → muvaffaqiyatli
    int      → HTTP status kodi (muvaffaqiyatsiz)
    'timeout'/'network' → tarmoq xatosi
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent":    "HeroScanner/PRO-4.0",
        "Accept":        "application/json",
    }
    try:
        async with session.get(
            qr_url, headers=headers, timeout=_TIMEOUT_SCAN
        ) as rp:
            if rp.status in (200, 201):
                return True
            return rp.status
    except asyncio.TimeoutError:
        return "timeout"
    except aiohttp.ClientError:
        return "network"


# ─── BULK SCANNER ─────────────────────────────────────────────────────────────
async def mark_all_accounts_smart(
    accounts: list,
    qr_url: str,
    db_update_func,
    concurrency: int = 20,
) -> tuple[int, int, float, list]:
    """
    Barcha akkauntlarni parallel skanerlaydi.
    Returns: (success_count, total, duration_sec, report_list)
    """
    if not accounts:
        return 0, 0, 0.0, []

    connector  = aiohttp.TCPConnector(limit=concurrency + 5, ssl=False)
    semaphore  = asyncio.Semaphore(concurrency)
    start_time = time.time()

    async def _bounded(acc):
        async with semaphore:
            return await scan_task(
                session, dict(acc), qr_url, db_update_func
            )

    async with aiohttp.ClientSession(
        connector=connector, timeout=_TIMEOUT_CONNECT
    ) as session:
        results = await asyncio.gather(
            *[_bounded(acc) for acc in accounts],
            return_exceptions=True,
        )

    duration = round(time.time() - start_time, 2)

    report = []
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Gather exception: {r}")
            report.append({"email": "?", "ok": False, "reason": str(r)})
        else:
            report.append(r)

    success_count = sum(1 for r in report if r.get("ok"))
    logger.info(
        f"Scan done: {success_count}/{len(accounts)} muvaffaqiyatli, "
        f"{duration}s"
    )
    return success_count, len(accounts), duration, report


# ─── STANDALONE TOKEN CHECKER ────────────────────────────────────────────────
async def verify_credentials(email: str, password: str) -> tuple[bool, str]:
    """
    Bitta akkauntning login/parolini tekshiradi va token qaytaradi.
    Returns: (is_valid, token_or_error_msg)
    """
    login_url = "https://api.newuzbekistan.hero.study/v1/users/login?lang=en"
    payload = {
        "email": email,
        "pass": password,      # ← Hero Study "pass" kalitini talab qiladi
        "remember": "",
        "clientToken": "",
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.post(
                login_url, json=payload, ssl=False
            ) as resp:
                if resp.status == 200:
                    data  = await resp.json(content_type=None)
                    token = _extract_token(data)
                    if token:
                        return True, token
                    return False, "Token topilmadi (API javobi noto'g'ri format)"
                if resp.status == 401:
                    return False, "Login yoki parol noto'g'ri"
                if resp.status == 429:
                    return False, "Hero serverida rate limit – biroz kuting"
                return False, f"Hero server xatosi: HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, "Hero serveriga ulanish vaqti tugadi"
    except aiohttp.ClientError as e:
        return False, f"Tarmoq xatosi: {e}"
    except Exception as e:
        logger.error(f"verify_credentials kutilmagan xato: {e}", exc_info=True)
        return False, "Kutilmagan xato"
