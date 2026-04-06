import asyncio
import os
import time
import logging
import re
from urllib.parse import urlencode
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from aiohttp import web
import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command
import jwt
import database as db
import hero_api as api

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN   = os.getenv("BOT_TOKEN")
ADMIN_ID    = int(os.getenv("ADMIN_ID", 0))
raw_url     = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBAPP_URL  = f"https://{raw_url.replace('https://','').replace('http://','')}" if raw_url else ""
PORT        = int(os.getenv("PORT", 8080))
JWT_SECRET  = os.getenv("JWT_SECRET", "change-me-in-production-use-random-string")
JWT_EXPIRE_HOURS = 24
PREMIUM_ACTIVATION_CODE = os.getenv("PREMIUM_ACTIVATION_CODE", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "")
GOOGLE_SCOPES = "openid email profile https://www.googleapis.com/auth/calendar.readonly"

bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp  = Dispatcher()

# ─── JWT HELPERS ──────────────────────────────────────────────────────────────

def create_token(user_id: int, telegram_id: int, role: str) -> str:
    payload = {
        "user_id": user_id,
        "telegram_id": telegram_id,
        "role": role,
        "exp": datetime.now(tz=timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

_rate_store: dict[str, list[float]] = defaultdict(list)

def is_rate_limited(key: str, max_req: int = 5, window_sec: int = 60) -> bool:
    now = time.time()
    _rate_store[key] = [t for t in _rate_store[key] if now - t < window_sec]
    if len(_rate_store[key]) >= max_req:
        return True
    _rate_store[key].append(now)
    return False

# ─── MIDDLEWARE ───────────────────────────────────────────────────────────────

PUBLIC_PATHS = {"/", "/health", "/api/auth/login", "/api/google/oauth/callback"}

@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in PUBLIC_PATHS or request.method == "OPTIONS":
        return await handler(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return web.json_response({"status": "error", "message": "Unauthorized"}, status=401)

    payload = decode_token(auth_header[7:])
    if not payload:
        return web.json_response({"status": "error", "message": "Token yaroqsiz yoki muddati tugagan"}, status=401)

    request["user"] = payload
    return await handler(request)

@web.middleware
async def security_headers_middleware(request: web.Request, handler):
    resp = await handler(request)
    resp.headers["X-Content-Type-Options"]  = "nosniff"
    resp.headers["X-Frame-Options"]          = "DENY"
    resp.headers["X-XSS-Protection"]         = "1; mode=block"
    resp.headers["Referrer-Policy"]           = "strict-origin-when-cross-origin"
    resp.headers["Cache-Control"]             = "no-store"
    return resp

# ─── VALIDATION HELPERS ───────────────────────────────────────────────────────

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

def validate_email(email: str) -> bool:
    return bool(email and EMAIL_RE.match(email) and len(email) <= 200)

def validate_url(url: str) -> bool:
    return bool(url and url.startswith("http") and len(url) <= 2000)

def get_client_ip(request: web.Request) -> str:
    return request.headers.get("X-Forwarded-For", request.remote or "unknown").split(",")[0].strip()


def is_user_premium(profile: dict | None) -> bool:
    if not profile:
        return False
    if not profile.get("is_premium"):
        return False
    until = profile.get("premium_until")
    if not until:
        return True
    if isinstance(until, str):
        try:
            until = datetime.fromisoformat(until)
        except ValueError:
            return False
    return until > datetime.now()


def parse_time_to_minutes(value: str) -> tuple[int, int] | None:
    if not value or "-" not in value:
        return None
    try:
        start, end = value.split("-", 1)
        sh, sm = [int(x) for x in start.strip().split(":", 1)]
        eh, em = [int(x) for x in end.strip().split(":", 1)]
        return sh * 60 + sm, eh * 60 + em
    except Exception:
        return None


def schedule_matches_pref(slot: dict, pref: dict) -> bool:
    course = (pref.get("preferred_course") or "").strip().lower()
    room = (pref.get("preferred_room") or "").strip().lower()
    days = {d.strip().lower() for d in (pref.get("preferred_days") or "").split(",") if d.strip()}
    tr = parse_time_to_minutes(pref.get("preferred_time_range") or "")

    slot_course = str(slot.get("course", "")).lower()
    slot_room = str(slot.get("room", "")).lower()
    slot_day = str(slot.get("day", "")).lower()
    slot_time = str(slot.get("time", ""))
    slot_minutes = parse_time_to_minutes(slot_time)

    if course and course not in slot_course:
        return False
    if room and room not in slot_room:
        return False
    if days and slot_day not in days:
        return False
    if tr and slot_minutes:
        start, end = tr
        s2, e2 = slot_minutes
        if s2 < start or e2 > end:
            return False
    return True


def normalize_schedule_item(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    time_text = str(raw.get("time", "")).strip()
    minutes = parse_time_to_minutes(time_text)
    if not minutes:
        return None
    start_min, end_min = minutes
    day = str(raw.get("day", "")).strip().lower()
    if not day:
        return None
    return {
        "course": str(raw.get("course", "")).strip()[:160],
        "room": str(raw.get("room", "")).strip()[:120],
        "day": day[:32],
        "start_min": start_min,
        "end_min": end_min,
        "teacher": str(raw.get("teacher", "")).strip()[:160],
        "group_name": str(raw.get("group", raw.get("group_name", ""))).strip()[:120],
        "raw": raw,
    }


def build_google_oauth_state(user_id: int, telegram_id: int) -> str:
    payload = {
        "uid": int(user_id),
        "tg": int(telegram_id),
        "exp": datetime.now(tz=timezone.utc) + timedelta(minutes=15),
        "iat": datetime.now(tz=timezone.utc),
        "kind": "google_oauth_state",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def parse_google_oauth_state(state: str) -> dict | None:
    try:
        data = jwt.decode(state, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        return None
    if data.get("kind") != "google_oauth_state":
        return None
    return data


def iso_to_minute_fields(dt_iso: str) -> tuple[str, int, int] | None:
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00")).astimezone()
        day = dt.strftime("%A").lower()
        start_min = dt.hour * 60 + dt.minute
        return day, start_min, dt.second
    except Exception:
        return None


async def refresh_google_access_token(profile: dict) -> tuple[str | None, str | None]:
    refresh_token = profile.get("google_refresh_token")
    if not refresh_token:
        return None, "Google refresh token topilmadi"
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
        return None, "Google OAuth env sozlanmagan"

    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post("https://oauth2.googleapis.com/token", data=payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    return None, data.get("error_description") or data.get("error") or "Google token refresh xatoligi"
                access_token = data.get("access_token")
                expires_in = int(data.get("expires_in", 3600))
                expire_at = datetime.now() + timedelta(seconds=max(120, expires_in - 30))
                await db.save_google_tokens(profile["id"], None, access_token, expire_at)
                return access_token, None
    except Exception as e:
        return None, str(e)

# ─── ROUTE HANDLERS ───────────────────────────────────────────────────────────

async def auth_login(request: web.Request):
    ip = get_client_ip(request)
    if is_rate_limited(f"login:{ip}", max_req=10, window_sec=60):
        return web.json_response({"status": "error", "message": "Juda ko'p urinish. 1 daqiqa kuting."}, status=429)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    login    = str(data.get("login", "")).strip()[:50]
    password = str(data.get("password", "")).strip()[:100]

    if not login or not password:
        return web.json_response({"status": "error", "message": "Maydonlar bo'sh"}, status=400)

    user = await db.verify_login(login, password)
    if not user:
        # Same rate key to prevent timing attacks
        if is_rate_limited(f"login_fail:{ip}", max_req=5, window_sec=120):
            return web.json_response({"status": "error", "message": "Akkaunt vaqtincha bloklandi"}, status=429)
        return web.json_response({"status": "error", "message": "Login yoki parol xato"}, status=401)

    role  = "super_admin" if user["telegram_id"] == ADMIN_ID else "user"
    token = create_token(user["id"], user["telegram_id"], role)
    profile = await db.get_user_profile(user["id"])
    return web.json_response({
        "status": "success",
        "token": token,
        "user_id": user["id"],
        "role": role,
        "premium": is_user_premium(profile),
    })


async def get_users_list(request: web.Request):
    user = request["user"]
    rows = await db.get_hero_accounts(user["user_id"])
    return web.json_response({"status": "success", "users": [dict(r) for r in rows]})


async def add_user(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    email    = str(data.get("email", "")).strip()
    password = str(data.get("password", "")).strip()[:200]

    if not validate_email(email):
        return web.json_response({"status": "error", "message": "Email manzil noto'g'ri"}, status=400)
    if not password:
        return web.json_response({"status": "error", "message": "Parol kiritilmagan"}, status=400)

    await db.add_hero_account(user["user_id"], email, password, "NO_TOKEN")
    return web.json_response({"status": "success"})


async def delete_user(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
        account_id = int(data.get("account_id", 0))
    except Exception:
        return web.json_response({"status": "error", "message": "Noto'g'ri so'rov"}, status=400)

    if account_id <= 0:
        return web.json_response({"status": "error", "message": "ID xato"}, status=400)

    success = await db.delete_hero_account(user["user_id"], account_id)
    if success:
        return web.json_response({"status": "success"})
    return web.json_response({"status": "error", "message": "Akkaunt topilmadi"}, status=404)


async def get_stats(request: web.Request):
    user = request["user"]
    stats = await db.get_user_stats(user["user_id"])
    return web.json_response({"status": "success", "data": stats})


async def get_premium_status(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    premium = is_user_premium(profile)
    return web.json_response({
        "status": "success",
        "data": {
            "is_premium": premium,
            "premium_until": profile.get("premium_until").isoformat() if profile and profile.get("premium_until") else None,
            "google_email": profile.get("google_email") if profile else None,
            "timezone": profile.get("timezone") if profile else "Asia/Tashkent",
        }
    })


async def activate_premium(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    code = str(data.get("code", "")).strip()
    days = int(data.get("days", 30) or 30)
    if days <= 0 or days > 365:
        days = 30

    # Super admin can activate directly without code.
    if user.get("role") != "super_admin":
        if not PREMIUM_ACTIVATION_CODE:
            return web.json_response({"status": "error", "message": "Premium kod serverda sozlanmagan"}, status=500)
        if code != PREMIUM_ACTIVATION_CODE:
            return web.json_response({"status": "error", "message": "Premium kod noto'g'ri"}, status=403)

    await db.activate_premium(user["user_id"], days=days)
    return web.json_response({"status": "success", "message": f"Premium {days} kunga yoqildi"})


async def get_schedule_preferences(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    pref = await db.get_schedule_preferences(user["user_id"])
    data = pref or {}
    data["google_email"] = profile.get("google_email") if profile else None
    data["timezone"] = profile.get("timezone") if profile else "Asia/Tashkent"
    return web.json_response({"status": "success", "data": data})


async def save_schedule_preferences(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    google_email = str(data.get("google_email", "")).strip()
    tz_name = str(data.get("timezone", "Asia/Tashkent")).strip()[:64] or "Asia/Tashkent"
    preferred_course = str(data.get("preferred_course", "")).strip()[:120]
    preferred_room = str(data.get("preferred_room", "")).strip()[:120]
    preferred_days = str(data.get("preferred_days", "")).strip()[:80]
    preferred_time_range = str(data.get("preferred_time_range", "")).strip()[:40]
    notes = str(data.get("notes", "")).strip()[:1200]

    if google_email and not validate_email(google_email):
        return web.json_response({"status": "error", "message": "Google email noto'g'ri"}, status=400)

    await db.save_google_profile(user["user_id"], google_email, tz_name)
    await db.upsert_schedule_preferences(
        user["user_id"], preferred_course, preferred_room, preferred_days, preferred_time_range, notes
    )
    return web.json_response({"status": "success", "message": "Premium jadval sozlamalari saqlandi"})


async def build_schedule_plan(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    pref = await db.get_schedule_preferences(user["user_id"]) or {}
    schedule = data.get("schedule", [])
    if not isinstance(schedule, list):
        return web.json_response({"status": "error", "message": "schedule massiv bo'lishi kerak"}, status=400)

    # Basic planner: find free slots that match user preferences.
    candidates = []
    for slot in schedule:
        if not isinstance(slot, dict):
            continue
        status = str(slot.get("status", "")).lower()
        if status and status not in {"free", "available", "open"}:
            continue
        if schedule_matches_pref(slot, pref):
            candidates.append({
                "course": slot.get("course"),
                "room": slot.get("room"),
                "day": slot.get("day"),
                "time": slot.get("time"),
                "reason": "matched_preferences"
            })
        if len(candidates) >= 5:
            break

    result = {
        "total_input": len(schedule),
        "suggested": candidates,
        "message": "Mos bo'sh slotlar topildi" if candidates else "Mos slot topilmadi, preferensiyani kengaytiring",
        "next_step": "Topilgan slotni tanlab booking API ga yuborish mumkin"
    }
    await db.save_booking_plan(user["user_id"], {"schedule_count": len(schedule), "pref": pref}, result, status="planned")
    return web.json_response({"status": "success", "data": result})


async def verify_google_token(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)
    access_token = str(data.get("access_token", "")).strip()
    if len(access_token) < 20:
        return web.json_response({"status": "error", "message": "Google access token noto'g'ri"}, status=400)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"access_token": access_token},
            ) as resp:
                payload = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.json_response({"status": "error", "message": "Google token yaroqsiz", "details": payload}, status=400)
                email = str(payload.get("email", "")).strip()
                if email and validate_email(email):
                    await db.save_google_profile(user["user_id"], email, profile.get("timezone") or "Asia/Tashkent")
                return web.json_response({
                    "status": "success",
                    "data": {
                        "email": email,
                        "scope": payload.get("scope"),
                        "expires_in": payload.get("expires_in"),
                        "aud": payload.get("aud"),
                    }
                })
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Google token tekshirishda xatolik: {e}"}, status=500)


async def start_google_oauth(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    if not GOOGLE_CLIENT_ID or not GOOGLE_REDIRECT_URI:
        return web.json_response({"status": "error", "message": "GOOGLE_CLIENT_ID yoki GOOGLE_REDIRECT_URI sozlanmagan"}, status=500)
    state = build_google_oauth_state(user["user_id"], user["telegram_id"])
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return web.json_response({"status": "success", "data": {"auth_url": url}})


async def google_oauth_callback(request: web.Request):
    code = str(request.query.get("code", "")).strip()
    state = str(request.query.get("state", "")).strip()
    if not code or not state:
        return web.Response(text="Google OAuth callback xato: code/state topilmadi", status=400)
    parsed = parse_google_oauth_state(state)
    if not parsed:
        return web.Response(text="Google OAuth state yaroqsiz", status=400)
    user_id = int(parsed["uid"])
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
        return web.Response(text="Google OAuth env sozlanmagan", status=500)

    payload = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.post("https://oauth2.googleapis.com/token", data=payload) as resp:
                token_data = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.Response(text=f"Google token olishda xatolik: {token_data}", status=400)
                access_token = token_data.get("access_token")
                refresh_token = token_data.get("refresh_token")
                expires_in = int(token_data.get("expires_in", 3600))
                expire_at = datetime.now() + timedelta(seconds=max(120, expires_in - 30))

            email = ""
            if access_token:
                async with session.get(
                    "https://openidconnect.googleapis.com/v1/userinfo",
                    headers={"Authorization": f"Bearer {access_token}"}
                ) as ui:
                    ui_data = await ui.json(content_type=None)
                    if ui.status == 200:
                        email = str(ui_data.get("email", "")).strip()
            await db.save_google_tokens(user_id, refresh_token, access_token, expire_at)
            if email and validate_email(email):
                await db.save_google_profile(user_id, email, "Asia/Tashkent")

        return web.Response(
            text=(
                "<html><body style='font-family:Arial;padding:24px'>"
                "<h2>Google ulandi ✅</h2>"
                "<p>Mini appga qayting va <b>Schedule import</b> tugmasini bosing.</p>"
                "</body></html>"
            ),
            content_type="text/html"
        )
    except Exception as e:
        return web.Response(text=f"Google callback xatoligi: {e}", status=500)


HERO_SCHEDULE_URL = "https://api.newuzbekistan.hero.study/v1/schedule/list"
HERO_SCHEDULE_PARAMS = {
    "beginTime": "1771700400",
    "endTime":   "1775588399",
    "lang":      "en",
}

# ─── HERO SCHEDULE FIELD NORMALIZER ───────────────────────────────────────────

def _parse_hero_time(start_raw, end_raw) -> tuple[int, int] | None:
    """Convert various Hero API time formats to (start_min, end_min)."""
    try:
        def to_min(v) -> int:
            if isinstance(v, int):
                # Unix timestamp → minutes since midnight
                dt = datetime.fromtimestamp(v)
                return dt.hour * 60 + dt.minute
            s = str(v).strip()
            if ":" in s:
                parts = s.split(":")
                return int(parts[0]) * 60 + int(parts[1])
            return int(s)
        s = to_min(start_raw)
        e = to_min(end_raw)
        if e > s:
            return s, e
    except Exception:
        pass
    return None


def _normalize_hero_slot(item: dict) -> dict | None:
    """Map Hero API schedule item → internal slot dict."""
    if not isinstance(item, dict):
        return None

    # ── time ──
    start_raw = (
        item.get("startTime") or item.get("start_time") or item.get("start") or
        item.get("beginTime") or item.get("begin_time") or item.get("timeStart") or
        item.get("lessonStart") or ""
    )
    end_raw = (
        item.get("endTime") or item.get("end_time") or item.get("end") or
        item.get("finishTime") or item.get("finish_time") or item.get("timeEnd") or
        item.get("lessonEnd") or ""
    )
    # Fallback: combined "HH:MM-HH:MM" field
    if not start_raw or not end_raw:
        combined = str(item.get("time", "") or item.get("lessonTime", "") or "").strip()
        if "-" in combined:
            parts = combined.split("-", 1)
            start_raw, end_raw = parts[0].strip(), parts[1].strip()

    minutes = _parse_hero_time(start_raw, end_raw)
    if not minutes:
        return None
    start_min, end_min = minutes

    # ── day ──
    day_raw = (
        item.get("day") or item.get("weekDay") or item.get("week_day") or
        item.get("dayOfWeek") or item.get("day_of_week") or ""
    )
    day = str(day_raw).strip().lower()
    if not day:
        # Try to parse from a date field
        date_raw = (
            item.get("date") or item.get("lessonDate") or item.get("lesson_date") or ""
        )
        if date_raw:
            try:
                if isinstance(date_raw, int):
                    day = datetime.fromtimestamp(date_raw).strftime("%A").lower()
                else:
                    day = datetime.fromisoformat(str(date_raw).replace("Z", "+00:00")).strftime("%A").lower()
            except Exception:
                pass
    if not day:
        return None

    # ── other fields ──
    course = str(
        item.get("subject") or item.get("course") or item.get("courseName") or
        item.get("discipline") or item.get("name") or item.get("title") or ""
    ).strip()[:160]

    room = str(
        item.get("room") or item.get("roomName") or item.get("auditorium") or
        item.get("audience") or item.get("classroom") or item.get("cabinet") or ""
    ).strip()[:120]

    teacher = str(
        item.get("teacher") or item.get("teacherName") or item.get("lecturer") or
        item.get("instructor") or ""
    ).strip()[:160]

    group_name = str(
        item.get("group") or item.get("groupName") or item.get("group_name") or
        item.get("groupCode") or ""
    ).strip()[:120]

    return {
        "course":     course or "—",
        "room":       room,
        "day":        day[:32],
        "start_min":  start_min,
        "end_min":    end_min,
        "teacher":    teacher,
        "group_name": group_name,
        "raw":        item,
    }


def _extract_hero_items(data: dict | list) -> list:
    """Handle various top-level response shapes."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "list", "items", "schedule", "lessons", "result", "records"):
            val = data.get(key)
            if isinstance(val, list):
                return val
        # Sometimes nested under "data.list"
        nested = data.get("data") or {}
        if isinstance(nested, dict):
            for key in ("list", "items", "schedule", "lessons"):
                val = nested.get(key)
                if isinstance(val, list):
                    return val
    return []


async def fetch_hero_schedule(request: web.Request):
    """
    Hero API dan dars jadvalini tortib olib DB ga saqlaydi.
    Foydalanuvchi hero_accounts jadvalidagi birinchi faol token ishlatiladi.
    """
    user = request["user"]

    # ── token olish ──
    accounts = await db.get_active_tokens(user["user_id"])
    if not accounts:
        return web.json_response(
            {"status": "error", "message": "Hero akkaunt topilmadi. Avval akkaunt qo'shing."},
            status=400,
        )

    bearer_token = None
    used_email = None
    for acc in accounts:
        t = acc.get("bearer_token") or ""
        if len(t) > 10:
            bearer_token = t
            used_email = acc.get("email", "")
            break

    if not bearer_token:
        return web.json_response(
            {"status": "error", "message": "Faol Bearer token topilmadi. Avval scan qiling."},
            status=400,
        )

    # ── Hero API ga so'rov ──
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json",
        "User-Agent": "HeroPremium/2.0",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(
                HERO_SCHEDULE_URL,
                params=HERO_SCHEDULE_PARAMS,
                headers=headers,
                ssl=False,
            ) as resp:
                if resp.status == 401:
                    return web.json_response(
                        {"status": "error", "message": "Token eskirgan. Qayta scan qiling."},
                        status=401,
                    )
                if resp.status not in (200, 201):
                    raw_text = await resp.text()
                    logger.warning(f"Hero schedule HTTP {resp.status}: {raw_text[:300]}")
                    return web.json_response(
                        {"status": "error", "message": f"Hero API xatoligi: HTTP {resp.status}"},
                        status=502,
                    )
                data = await resp.json(content_type=None)
    except asyncio.TimeoutError:
        return web.json_response({"status": "error", "message": "Hero API javob bermadi (timeout)"}, status=504)
    except Exception as e:
        logger.error(f"fetch_hero_schedule network error: {e}", exc_info=True)
        return web.json_response({"status": "error", "message": f"Tarmoq xatoligi: {e}"}, status=502)

    # ── parse ──
    items = _extract_hero_items(data)
    if not items:
        return web.json_response(
            {"status": "error", "message": "Hero API dan jadval topilmadi yoki format noto'g'ri"},
            status=400,
        )

    slots = []
    for item in items:
        normalized = _normalize_hero_slot(item)
        if normalized:
            slots.append(normalized)

    if not slots:
        return web.json_response(
            {"status": "error", "message": f"Jami {len(items)} ta item keldi, lekin normalize qilib bo'lmadi. Format o'zgacha bo'lishi mumkin."},
            status=400,
        )

    # ── DB ga saqlash ──
    await db.replace_user_schedule_slots(user["user_id"], slots, source="hero_api")

    logger.info(f"[fetch_hero_schedule] user_id={user['user_id']} email={used_email} → {len(slots)} slots saved")
    return web.json_response({
        "status":  "success",
        "message": f"Hero jadvalidan {len(slots)} ta slot import qilindi",
        "count":   len(slots),
        "email":   used_email,
    })


async def fetch_google_schedule(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    access_token = profile.get("google_access_token")
    expire_at = profile.get("google_token_expire")
    if not access_token or (expire_at and expire_at <= datetime.now()):
        access_token, err = await refresh_google_access_token(profile)
        if err:
            return web.json_response({"status": "error", "message": f"Google token yangilab bo'lmadi: {err}"}, status=400)

    try:
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        max_iso = (datetime.utcnow() + timedelta(days=14)).replace(microsecond=0).isoformat() + "Z"
        url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
        params = {"timeMin": now_iso, "timeMax": max_iso, "singleEvents": "true", "orderBy": "startTime", "maxResults": "250"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, params=params, headers={"Authorization": f"Bearer {access_token}"}) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.json_response({"status": "error", "message": f"Calendar olishda xatolik: {data}"}, status=400)

        items = data.get("items", []) or []
        slots = []
        for ev in items:
            start_dt = (ev.get("start") or {}).get("dateTime")
            end_dt = (ev.get("end") or {}).get("dateTime")
            if not start_dt or not end_dt:
                continue
            s = iso_to_minute_fields(start_dt)
            e = iso_to_minute_fields(end_dt)
            if not s or not e:
                continue
            day, start_min, _ = s
            _, end_min, _ = e
            if end_min <= start_min:
                continue
            slots.append({
                "course": str(ev.get("summary", "Untitled")).strip()[:160],
                "room": str(ev.get("location", "")).strip()[:120],
                "day": day,
                "start_min": start_min,
                "end_min": end_min,
                "teacher": "",
                "group_name": "",
                "raw": ev,
            })

        await db.replace_user_schedule_slots(user["user_id"], slots, source="google_calendar")
        return web.json_response({"status": "success", "message": f"Google Calendar'dan {len(slots)} ta slot import qilindi", "count": len(slots)})
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Google schedule import xatoligi: {e}"}, status=500)


async def upload_schedule(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)
    schedule = data.get("schedule", [])
    source = str(data.get("source", "manual")).strip()[:32] or "manual"
    if not isinstance(schedule, list):
        return web.json_response({"status": "error", "message": "schedule massiv bo'lishi kerak"}, status=400)

    normalized = []
    for item in schedule:
        parsed = normalize_schedule_item(item)
        if parsed:
            normalized.append(parsed)
    if not normalized:
        return web.json_response({"status": "error", "message": "Yaroqli schedule item topilmadi"}, status=400)

    await db.replace_user_schedule_slots(user["user_id"], normalized, source=source)
    return web.json_response({"status": "success", "message": f"{len(normalized)} ta slot saqlandi"})


async def get_my_schedule(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    rows = await db.get_user_schedule_slots(user["user_id"], limit=300)
    return web.json_response({"status": "success", "data": rows})


async def get_schedule_overlaps(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    rows = await db.find_schedule_overlaps(user["user_id"], limit=300)
    return web.json_response({"status": "success", "data": rows, "count": len(rows)})


async def get_history(request: web.Request):
    user = request["user"]
    logs = await db.get_user_scan_history(user["user_id"], limit=20)
    # Convert datetime to ISO string for JSON serialization
    for log in logs:
        if "scanned_at" in log and log["scanned_at"]:
            log["scanned_at"] = log["scanned_at"].isoformat()
    return web.json_response({"status": "success", "logs": logs})


async def do_scan(request: web.Request):
    user     = request["user"]
    user_id  = user["user_id"]
    ip       = get_client_ip(request)

    if is_rate_limited(f"scan:{user_id}", max_req=10, window_sec=60):
        return web.json_response({"status": "error", "message": "Juda tez skanlamoqdasiz"}, status=429)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    qr_url = str(data.get("url", "")).strip()
    if not validate_url(qr_url):
        return web.json_response({"status": "error", "message": "URL noto'g'ri"}, status=400)

    accounts = await db.get_active_tokens(user_id)
    if not accounts:
        return web.json_response({"status": "error", "message": "Hali akkaunt qo'shilmagan"}, status=400)

    async def update_token(email: str, token: str):
        await db.add_hero_account(user_id, email, "", token)

    success, total, duration, report = await api.mark_all_accounts_smart(
        list(accounts), qr_url, update_token
    )
    await db.save_detailed_scan(user_id, success, total, duration, report)

    return web.json_response({
        "status":   "success",
        "success":  success,
        "total":    total,
        "duration": duration,
        "report":   report,
    })


async def get_admin_all_data(request: web.Request):
    user = request["user"]
    if user.get("role") != "super_admin":
        return web.json_response({"status": "error", "message": "Ruxsat yo'q"}, status=403)

    data = await db.get_super_admin_data()
    # Passwords are already excluded from DB query — safe to return
    for log in data.get("logs", []):
        if "scanned_at" in log and log["scanned_at"]:
            log["scanned_at"] = log["scanned_at"].isoformat()
    return web.json_response({"status": "success", "data": data})


async def index(request: web.Request):
    if os.path.exists("scanner.html"):
        return web.FileResponse("scanner.html")
    return web.Response(text="404 Not Found", status=404)


async def health_check(request: web.Request):
    return web.Response(text="OK", status=200)

# ─── TELEGRAM BOT ─────────────────────────────────────────────────────────────

if bot:
    @dp.message(Command("start"))
    async def cmd_start(m: Message):
        text = (
            "🛡 *Hero Scanner PRO · Premium*\n\n"
            "⚡ Davomat skaneri, tarix, premium scheduler va admin panel bir joyda.\n"
            "🔐 Kirish ma'lumotlari: /login\n"
        )
        if WEBAPP_URL:
            kb = [[InlineKeyboardButton(text="🚀 Mini Appni Ochish", web_app=WebAppInfo(url=WEBAPP_URL))]]
            text += f"\n🌐 WebApp manzil: `{WEBAPP_URL}`"
            await m.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")
        else:
            text += "\n❗ WEBAPP_URL sozlanmagan. Admin server env ga to'g'ri URL kiritsin."
            await m.answer(text, parse_mode="Markdown")

    @dp.message(Command("login"))
    async def cmd_login(m: Message):
        login, plain_password = await db.get_or_create_user(m.from_user.id)
        await m.answer(
            f"🔐 *Kirish ma'lumotlaringiz (Hero Scanner PRO):*\n\n"
            f"👤 Login: `{login}`\n"
            f"🔑 Parol: `{plain_password}`\n\n"
            f"⚠️ Xavfsizlik uchun xabarni saqlamang va begonalarga yubormang.\n"
            f"📱 Mini Appga qaytib shu ma'lumot bilan login qiling.",
            parse_mode="Markdown"
        )

# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    app = web.Application(middlewares=[security_headers_middleware, auth_middleware])

    app.router.add_get("/",                    index)
    app.router.add_get("/health",              health_check)
    app.router.add_post("/api/auth/login",     auth_login)
    app.router.add_get("/api/users",           get_users_list)
    app.router.add_post("/api/users/add",      add_user)
    app.router.add_post("/api/users/delete",   delete_user)
    app.router.add_get("/api/stats",           get_stats)
    app.router.add_get("/api/premium/status",  get_premium_status)
    app.router.add_post("/api/premium/activate", activate_premium)
    app.router.add_post("/api/google/verify_token", verify_google_token)
    app.router.add_get("/api/google/oauth/start", start_google_oauth)
    app.router.add_get("/api/google/oauth/callback", google_oauth_callback)
    app.router.add_post("/api/hero/schedule/fetch",   fetch_hero_schedule)
    app.router.add_post("/api/google/schedule/fetch", fetch_google_schedule)
    app.router.add_get("/api/schedule/preferences", get_schedule_preferences)
    app.router.add_post("/api/schedule/preferences", save_schedule_preferences)
    app.router.add_post("/api/schedule/plan", build_schedule_plan)
    app.router.add_post("/api/schedule/upload", upload_schedule)
    app.router.add_get("/api/schedule/my", get_my_schedule)
    app.router.add_get("/api/schedule/overlaps", get_schedule_overlaps)
    app.router.add_get("/api/history",         get_history)
    app.router.add_post("/api/scan",           do_scan)
    app.router.add_get("/api/admin/all_data",  get_admin_all_data)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info(f"✅ Server started on port {PORT}")

    await db.init_db()

    if bot:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    else:
        logger.warning("⚠️ BOT_TOKEN not set — running without Telegram bot")
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
