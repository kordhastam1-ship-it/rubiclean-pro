import asyncio
import logging
import secrets
import time

from typing import Dict, Any

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Header,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from rubpy import Client
from rubpy.crypto import Crypto


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rubika_cleaner")

app = FastAPI(title="Rubika Cleaner API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kordhastam1-ship-it.github.io"
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=[
        "Content-Type",
        "X-Session-Token",
    ],
)

# ورودهای موقت
pending: Dict[str, dict] = {}

# نشست کاربران
sessions: Dict[str, Client] = {}

# محدودیت درخواست‌ها
attempts: Dict[str, list] = {}

# عملیات ترک کانال
jobs: Dict[str, dict] = {}
job_tasks: Dict[str, asyncio.Task] = {}
channel_cache: Dict[str, list] = {}

# عملیات ترک گپ
group_jobs: Dict[str, dict] = {}
group_job_tasks: Dict[str, asyncio.Task] = {}
group_cache: Dict[str, list] = {}


class PhoneRequest(BaseModel):
    phone: str = Field(min_length=10, max_length=16)


class CodeRequest(BaseModel):
    token: str = Field(min_length=20, max_length=200)
    code: str = Field(min_length=4, max_length=8)


def get_value(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)

    return getattr(obj, key, default)


def to_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value

    if hasattr(value, "to_dict"):
        try:
            result = value.to_dict
            if callable(result):
                result = result()

            if isinstance(result, dict):
                return result
        except Exception:
            pass

    if hasattr(value, "model_dump"):
        try:
            result = value.model_dump()

            if isinstance(result, dict):
                return result
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        try:
            return vars(value)
        except Exception:
            pass

    return {}


def clean_phone(phone: str) -> str:
    phone = (
        phone.strip()
        .replace(" ", "")
        .replace("-", "")
        .replace("+", "")
    )

    if not phone.isdigit():
        raise HTTPException(400, "شماره تلفن معتبر نیست.")

    if phone.startswith("0"):
        phone = "98" + phone[1:]

    if not phone.startswith("98") or len(phone) != 12:
        raise HTTPException(
            400,
            "فعلاً فقط شماره‌های معتبر ایران پشتیبانی می‌شوند."
        )

    return phone


def check_rate_limit(request: Request):
    ip = request.client.host if request.client else "unknown"
    now = time.time()

    recent = [
        t for t in attempts.get(ip, [])
        if now - t < 600
    ]

    if len(recent) >= 5:
        raise HTTPException(
            429,
            "تعداد درخواست‌ها زیاد است. ده دقیقه بعد دوباره تلاش کن."
        )

    recent.append(now)
    attempts[ip] = recent


def get_session(session_token: str | None) -> Client:
    if not session_token:
        raise HTTPException(
            401,
            "نشست معتبر نیست. ابتدا وارد شو."
        )

    client = sessions.get(session_token)

    if client is None:
        raise HTTPException(
            401,
            "نشست پیدا نشد یا منقضی شده است. دوباره وارد شو."
        )

    return client


def get_chat_info(chat: Any) -> dict:
    """
    استخراج اطلاعات گفت‌وگو از پاسخ Rubpy.
    """

    chat_data = to_dict(chat)

    obj = (
        chat_data.get("abs_object")
        or get_value(chat, "abs_object")
        or {}
    )

    obj_data = to_dict(obj)

    guid = (
        chat_data.get("object_guid")
        or get_value(chat, "object_guid")
        or obj_data.get("object_guid")
        or ""
    )

    title = (
        obj_data.get("title")
        or chat_data.get("title")
        or get_value(chat, "title")
        or "بدون نام"
    )

    chat_type = (
        obj_data.get("type")
        or chat_data.get("type")
        or get_value(chat, "type")
        or ""
    )

    return {
        "guid": guid,
        "title": str(title),
        "type": chat_type,
    }


async def fetch_all_chats(client: Client) -> list:
    """
    دریافت صفحه‌به‌صفحه گفت‌وگوها.
    """

    all_chats = []
    seen_pages = set()
    seen_guids = set()
    start_id = None

    for _ in range(100):
        result = await client.get_chats(start_id=start_id)
        data = to_dict(result)

        chats = (
            data.get("chats")
            or get_value(result, "chats")
            or []
        )

        if not isinstance(chats, list):
            chats = []

        for chat in chats:
            info = get_chat_info(chat)
            guid = info["guid"]

            if not isinstance(guid, str) or not guid:
                continue

            if guid in seen_guids:
                continue

            seen_guids.add(guid)
            all_chats.append(info)

        next_id = (
            data.get("next_start_id")
            or get_value(result, "next_start_id")
        )

        has_continue = (
            data.get("has_continue")
            if "has_continue" in data
            else get_value(result, "has_continue", False)
        )

        if not has_continue or not next_id:
            break

        if next_id in seen_pages:
            break

        seen_pages.add(next_id)
        start_id = next_id

    return all_chats


async def load_channels(
    token: str,
    client: Client,
) -> list:

    if token in channel_cache:
        return channel_cache[token]

    chats = await fetch_all_chats(client)

    channels = [
        {
            "guid": chat["guid"],
            "title": chat["title"],
        }
        for chat in chats
        if chat["guid"].startswith("c0")
    ]

    channel_cache[token] = channels
    return channels


async def load_groups(
    token: str,
    client: Client,
) -> list:

    if token in group_cache:
        return group_cache[token]

    chats = await fetch_all_chats(client)

    groups = [
        {
            "guid": chat["guid"],
            "title": chat["title"],
        }
        for chat in chats
        if (
            chat["guid"].startswith("g0")
            and chat["type"] == "Group"
        )
    ]

    group_cache[token] = groups
    return groups


def operation_running(token: str) -> bool:
    channel_running = jobs.get(token, {}).get("running", False)
    group_running = group_jobs.get(token, {}).get("running", False)

    return channel_running or group_running


@app.get("/")
async def home():
    return {
        "status": "ok",
        "message": "سرویس مدیریت کانال‌ها و گپ‌های روبیکا فعال است.",
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/api/auth/send-code")
async def send_code(data: PhoneRequest, request: Request):
    check_rate_limit(request)
    phone = clean_phone(data.phone)

    token = secrets.token_urlsafe(32)
    public_key, private_key = Crypto.create_keys()

    client = Client(
        name="rc_" + secrets.token_hex(12),
        private_key=private_key,
        display_welcome=False,
    )

    try:
        await client.connect()

        result = await client.send_code(
            phone_number=phone,
            send_type="SMS",
        )

        status = get_value(result, "status")

        if status == "SendPassKey":
            await client.disconnect()

            raise HTTPException(
                400,
                "این حساب به مرحله تأیید دیگری نیاز دارد."
            )

        if status != "OK":
            await client.disconnect()

            raise HTTPException(
                400,
                "روبیکا کد ورود را نپذیرفت. دوباره تلاش کن."
            )

        phone_code_hash = get_value(result, "phone_code_hash")

        if not phone_code_hash:
            await client.disconnect()

            raise HTTPException(
                502,
                "پاسخ روبیکا فاقد اطلاعات لازم برای تأیید است."
            )

        pending[token] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": phone_code_hash,
            "public_key": public_key,
            "private_key": private_key,
            "expires": time.time() + 300,
            "verify_attempts": 0,
        }

        return {
            "success": True,
            "token": token,
            "message": "کد ورود ارسال شد.",
        }

    except HTTPException:
        raise

    except Exception:
        logger.exception("ارسال کد ورود ناموفق بود.")

        try:
            await client.disconnect()
        except Exception:
            pass

        raise HTTPException(
            502,
            "ارتباط با روبیکا برقرار نشد. گزارش سرور را بررسی کن."
        )


@app.post("/api/auth/verify-code")
async def verify_code(data: CodeRequest):
    item = pending.get(data.token)

    if item is None:
        raise HTTPException(
            401,
            "درخواست ورود پیدا نشد. دوباره کد بگیر."
        )

    if time.time() > item["expires"]:
        pending.pop(data.token, None)

        try:
            await item["client"].disconnect()
        except Exception:
            pass

        raise HTTPException(
            401,
            "زمان درخواست تمام شده است. دوباره کد بگیر."
        )

    item["verify_attempts"] += 1

    if item["verify_attempts"] > 5:
        pending.pop(data.token, None)

        try:
            await item["client"].disconnect()
        except Exception:
            pass

        raise HTTPException(
            429,
            "تعداد تلاش‌ها بیش از حد مجاز است. دوباره کد بگیر."
        )

    client = item["client"]

    try:
        result = await client.sign_in(
            phone_code=data.code,
            phone_number=item["phone"],
            phone_code_hash=item["phone_code_hash"],
            public_key=item["public_key"],
        )

        status = get_value(result, "status")

        if status != "OK":
            raise HTTPException(
                400,
                "کد تأیید درست نیست یا اعتبار آن تمام شده است."
            )

        auth = get_value(result, "auth")
        user = get_value(result, "user")

        if not auth or user is None:
            raise HTTPException(
                502,
                "پاسخ ورود روبیکا ناقص است."
            )

        auth = Crypto.decrypt_RSA_OAEP(
            item["private_key"],
            auth,
        )

        client.key = Crypto.passphrase(auth)
        client.auth = auth
        client.decode_auth = Crypto.decode_auth(client.auth)

        from Crypto.PublicKey import RSA
        from Crypto.Signature import pkcs1_15

        private_key = item["private_key"]

        if isinstance(private_key, str):
            private_key_bytes = private_key.encode()
        else:
            private_key_bytes = private_key

        client.import_key = pkcs1_15.new(
            RSA.import_key(private_key_bytes)
        )

        user_guid = get_value(user, "user_guid")
        user_phone = get_value(user, "phone") or item["phone"]

        if not user_guid:
            raise HTTPException(
                502,
                "شناسه کاربر در پاسخ روبیکا وجود ندارد."
            )

        client.session.insert(
            auth=client.auth,
            guid=user_guid,
            user_agent=client.user_agent,
            phone_number=user_phone,
            private_key=item["private_key"],
        )

        await client.register_device(device_model=client.name)

        sessions[data.token] = client
        pending.pop(data.token, None)

        return {
            "success": True,
            "message": "ورود موفق بود.",
        }

    except HTTPException:
        raise

    except Exception:
        logger.exception("خطای فنی هنگام تکمیل ورود رخ داد.")

        raise HTTPException(
            502,
            "ورود کامل نشد. گزارش خطا را در Render بررسی کن."
        )


@app.get("/api/channels/status")
async def channels_status(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    client = get_session(x_session_token)
    token = x_session_token

    try:
        channels = await load_channels(token, client)
        job = jobs.get(token, {})

        return {
            "connected": True,
            "channels_loaded": True,
            "total_channels": len(channels),
            "completed": job.get("completed", 0),
            "failed": job.get("failed", 0),
            "progress": job.get("progress", 0),
            "running": job.get("running", False),
            "stopped": job.get("stopped", False),
            "message": "فهرست کانال‌ها دریافت شد.",
        }

    except Exception:
        logger.exception("دریافت فهرست کانال‌ها ناموفق بود.")

        raise HTTPException(
            502,
            "دریافت کانال‌ها ناموفق بود."
        )


@app.get("/api/groups/status")
async def groups_status(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    client = get_session(x_session_token)
    token = x_session_token

    try:
        groups = await load_groups(token, client)
        job = group_jobs.get(token, {})

        return {
            "connected": True,
            "groups_loaded": True,
            "total_groups": len(groups),
            "completed": job.get("completed", 0),
            "failed": job.get("failed", 0),
            "progress": job.get("progress", 0),
            "current": job.get("current", 0),
            "running": job.get("running", False),
            "stopped": job.get("stopped", False),
            "message": job.get(
                "message",
                "فهرست گپ‌ها دریافت شد.",
            ),
        }

    except Exception:
        logger.exception("دریافت فهرست گپ‌ها ناموفق بود.")

        raise HTTPException(
            502,
            "دریافت گپ‌ها ناموفق بود."
        )


async def leave_worker(token: str, client: Client, channels: list):
    job = jobs[token]
    total = len(channels)

    try:
        if total == 0:
            job["progress"] = 100
            job["message"] = "کانالی برای ترک وجود ندارد."
            return

        for index, channel in enumerate(channels):
            if job["stop_event"].is_set():
                job["stopped"] = True
                job["message"] = "عملیات به درخواست کاربر متوقف شد."
                break

            try:
                await client.builder(
                    name="joinChannelAction",
                    input={
                        "channel_guid": channel["guid"],
                        "action": "Leave",
                    },
                    dict=True,
                )
                job["completed"] += 1

            except Exception:
                job["failed"] += 1
                logger.warning("ترک یک کانال ناموفق بود.")

            job["current"] = index + 1
            job["progress"] = int((index + 1) * 100 / total)

            await asyncio.sleep(0.5)

        if not job["stopped"]:
            job["progress"] = 100
            job["message"] = "عملیات ترک کانال‌ها پایان یافت."

    except Exception:
        logger.exception("خطا در عملیات ترک کانال‌ها.")
        job["message"] = "عملیات با خطای فنی متوقف شد."

    finally:
        job["running"] = False
        channel_cache.pop(token, None)


async def leave_group_worker(token: str, client: Client, groups: list):
    job = group_jobs[token]
    total = len(groups)

    try:
        if total == 0:
            job["progress"] = 100
            job["message"] = "گپی برای ترک وجود ندارد."
            return

        for index, group in enumerate(groups):
            if job["stop_event"].is_set():
                job["stopped"] = True
                job["message"] = "عملیات به درخواست کاربر متوقف شد."
                break

            try:
                # همان متدی که در سورس محلی شما استفاده شده است.
                await client.leave_group(group["guid"])
                job["completed"] += 1

            except Exception:
                job["failed"] += 1
                logger.warning("ترک یک گپ ناموفق بود.")

            job["current"] = index + 1
            job["progress"] = int((index + 1) * 100 / total)

            await asyncio.sleep(0.5)

        if not job["stopped"]:
            job["progress"] = 100
            job["message"] = "عملیات ترک گپ‌ها پایان یافت."

    except Exception:
        logger.exception("خطا در عملیات ترک گپ‌ها.")
        job["message"] = "عملیات با خطای فنی متوقف شد."

    finally:
        job["running"] = False
        group_cache.pop(token, None)


@app.post("/api/channels/leave/start")
async def start_leave(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    client = get_session(x_session_token)
    token = x_session_token

    if operation_running(token):
        raise HTTPException(
            409,
            "یک عملیات دیگر در حال اجراست."
        )

    try:
        channels = await load_channels(token, client)

    except Exception:
        logger.exception("آماده‌سازی فهرست کانال‌ها ناموفق بود.")
        raise HTTPException(502, "فهرست کانال‌ها دریافت نشد.")

    if not channels:
        return {
            "success": True,
            "running": False,
            "total_channels": 0,
            "message": "کانالی برای ترک وجود ندارد.",
        }

    jobs[token] = {
        "running": True,
        "stopped": False,
        "completed": 0,
        "failed": 0,
        "current": 0,
        "progress": 0,
        "total": len(channels),
        "message": "عملیات ترک کانال‌ها آغاز شد.",
        "stop_event": asyncio.Event(),
    }

    job_tasks[token] = asyncio.create_task(
        leave_worker(token, client, channels)
    )

    return {
        "success": True,
        "running": True,
        "total_channels": len(channels),
        "message": "عملیات ترک کانال‌ها آغاز شد.",
    }


@app.get("/api/channels/leave/status")
async def leave_status(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    get_session(x_session_token)
    job = jobs.get(x_session_token)

    if not job:
        return {
            "running": False,
            "stopped": False,
            "total": 0,
            "completed": 0,
            "failed": 0,
            "current": 0,
            "progress": 0,
            "message": "هنوز عملیاتی شروع نشده است.",
        }

    return {
        "running": job["running"],
        "stopped": job["stopped"],
        "total": job["total"],
        "completed": job["completed"],
        "failed": job["failed"],
        "current": job["current"],
        "progress": job["progress"],
        "message": job["message"],
    }


@app.post("/api/channels/leave/stop")
async def stop_leave(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    get_session(x_session_token)
    job = jobs.get(x_session_token)

    if not job or not job.get("running"):
        return {
            "success": True,
            "running": False,
            "message": "عملیات فعالی برای توقف وجود ندارد.",
        }

    job["stop_event"].set()

    return {
        "success": True,
        "message": "درخواست توقف ثبت شد.",
    }


@app.post("/api/groups/leave/start")
async def start_group_leave(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    client = get_session(x_session_token)
    token = x_session_token

    if operation_running(token):
        raise HTTPException(
            409,
            "یک عملیات دیگر در حال اجراست."
        )

    try:
        groups = await load_groups(token, client)

    except Exception:
        logger.exception("آماده‌سازی فهرست گپ‌ها ناموفق بود.")
        raise HTTPException(502, "فهرست گپ‌ها دریافت نشد.")

    if not groups:
        return {
            "success": True,
            "running": False,
            "total_groups": 0,
            "message": "گپی برای ترک وجود ندارد.",
        }

    group_jobs[token] = {
        "running": True,
        "stopped": False,
        "completed": 0,
        "failed": 0,
        "current": 0,
        "progress": 0,
        "total": len(groups),
        "message": "عملیات ترک گپ‌ها آغاز شد.",
        "stop_event": asyncio.Event(),
    }

    group_job_tasks[token] = asyncio.create_task(
        leave_group_worker(token, client, groups)
    )

    return {
        "success": True,
        "running": True,
        "total_groups": len(groups),
        "message": "عملیات ترک گپ‌ها آغاز شد.",
    }


@app.get("/api/groups/leave/status")
async def group_leave_status(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    get_session(x_session_token)
    job = group_jobs.get(x_session_token)

    if not job:
        return {
            "running": False,
            "stopped": False,
            "total": 0,
            "completed": 0,
            "failed": 0,
            "current": 0,
            "progress": 0,
            "message": "هنوز عملیاتی شروع نشده است.",
        }

    return {
        "running": job["running"],
        "stopped": job["stopped"],
        "total": job["total"],
        "completed": job["completed"],
        "failed": job["failed"],
        "current": job["current"],
        "progress": job["progress"],
        "message": job["message"],
    }


@app.post("/api/groups/leave/stop")
async def stop_group_leave(
    x_session_token: str | None = Header(
        default=None,
        alias="X-Session-Token",
    ),
):
    get_session(x_session_token)
    job = group_jobs.get(x_session_token)

    if not job or not job.get("running"):
        return {
            "success": True,
            "running": False,
            "message": "عملیات فعالی برای توقف وجود ندارد.",
        }

    job["stop_event"].set()

    return {
        "success": True,
        "message": "درخواست توقف ثبت شد.",
}
