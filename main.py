import os
import hmac
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import date as date_cls
from urllib.parse import unquote

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

import db
from products import (
    PRODUCTS, QUIZ_QUESTIONS, get_quiz_result, get_follow_up_question,
    get_smart_result,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    from articles_data import ARTICLES
    await db.seed_articles(ARTICLES)
    if WEBHOOK_URL:
        await _set_webhook()
    yield


app = FastAPI(lifespan=lifespan)


# ── Webhook ──────────────────────────────────────────────────────────────────

async def _set_webhook():
    import aiohttp
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    async with aiohttp.ClientSession() as session:
        await session.post(url, json={"url": f"{WEBHOOK_URL}/tg-webhook"})


@app.post("/tg-webhook")
async def tg_webhook(request: Request):
    from bot import dp, bot
    from aiogram.types import Update
    data = await request.json()
    update = Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}


# ── Telegram Web App auth ─────────────────────────────────────────────────────

def verify_tg_init_data(init_data: str) -> dict | None:
    """Проверяет подпись Telegram initData. Возвращает dict с данными или None."""
    try:
        parsed = dict(pair.split("=", 1) for pair in unquote(init_data).split("&"))
        check_hash = parsed.pop("hash", None)
        if not check_hash:
            return None

        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected, check_hash):
            return None

        user_data = json.loads(parsed.get("user", "{}"))
        return user_data
    except Exception:
        return None


def get_tg_user(request: Request) -> dict:
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing Telegram auth")
    user = verify_tg_init_data(init_data)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid Telegram auth")
    return user


# ── Pydantic models ───────────────────────────────────────────────────────────

class QuizAnswerIn(BaseModel):
    goal: str
    detail: str | None = None

class SmartQuizIn(BaseModel):
    gender: str = "female"
    age: str = "26-35"
    goal: str = "energy"
    symptoms: list[str] = []
    stress: int = 1
    sleep_q: int = 4
    diet: str = "balanced"
    women_health: str | None = None

class WellnessIn(BaseModel):
    energy: int
    sleep_q: int
    mood: int

class ChatIn(BaseModel):
    message: str = ""

class TrackerAddIn(BaseModel):
    product_id: str

class TrackerLogIn(BaseModel):
    product_id: str


# ── API: пользователь ─────────────────────────────────────────────────────────

@app.post("/api/user/init")
async def user_init(request: Request):
    tg = get_tg_user(request)
    body = await request.json()
    referred_by = body.get("ref")
    user = await db.get_or_create_user(
        user_id=tg["id"],
        username=tg.get("username", ""),
        first_name=tg.get("first_name", ""),
        referred_by=referred_by,
    )
    ref_count = await db.get_referral_count(user["ref_code"])
    return {**user, "ref_count": ref_count}


# ── API: квиз ─────────────────────────────────────────────────────────────────

@app.get("/api/quiz/start")
async def quiz_start():
    return QUIZ_QUESTIONS["goal"]


@app.post("/api/quiz/followup")
async def quiz_followup(body: QuizAnswerIn):
    q = get_follow_up_question(body.goal)
    return q or {"done": True}


@app.post("/api/quiz/result")
async def quiz_result(body: QuizAnswerIn):
    products = get_quiz_result(body.goal, body.detail)
    return {"products": products}


@app.post("/api/quiz/smart-result")
async def quiz_smart_result(body: SmartQuizIn):
    return get_smart_result(body.model_dump())


# ── API: дом ─────────────────────────────────────────────────────────────────

@app.get("/api/home")
async def home_data(request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    products    = await db.get_tracker_products(tg["id"])
    today_logs  = await db.get_today_logs(tg["id"])
    wellness    = await db.get_wellness_today(tg["id"], today)
    course_days = await db.get_course_days(tg["id"])
    streak      = await db.get_global_streak(tg["id"])
    user        = await db.get_user(tg["id"])
    ref_count   = await db.get_referral_count(user["ref_code"]) if user else 0
    products_out = [{**p, "taken_today": p["product_id"] in today_logs} for p in products]
    return {
        "products": products_out,
        "wellness_today": wellness,
        "course_days": course_days,
        "global_streak": streak,
        "ref_count": ref_count,
    }


# ── API: самочувствие ────────────────────────────────────────────────────────

@app.post("/api/wellness/log")
async def wellness_log(body: WellnessIn, request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    await db.log_wellness(tg["id"], today, body.energy, body.sleep_q, body.mood)
    return {"ok": True}


@app.get("/api/wellness")
async def wellness_get(request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    today_log = await db.get_wellness_today(tg["id"], today)
    history   = await db.get_wellness_history(tg["id"], 14)
    return {"today": today_log, "history": history}


# ── API: статьи ──────────────────────────────────────────────────────────────

@app.get("/api/articles")
async def articles_random():
    return {"articles": await db.get_random_articles(4)}


# ── API: ИИ-нутрициолог (заглушка) ───────────────────────────────────────────

@app.post("/api/ai/chat")
async def ai_chat(body: ChatIn, request: Request):
    get_tg_user(request)
    return {
        "message": "Я пока в разработке — скоро смогу отвечать на любые вопросы о нутриентах. "
                   "А пока пройди персональный анализ: он уже учитывает твои симптомы, питание и образ жизни.",
        "stub": True,
    }


# ── API: трекер ───────────────────────────────────────────────────────────────

@app.get("/api/tracker")
async def tracker_get(request: Request):
    tg = get_tg_user(request)
    products = await db.get_tracker_products(tg["id"])
    today_logs = await db.get_today_logs(tg["id"])
    result = []
    for p in products:
        streak = await db.get_streak(tg["id"], p["product_id"])
        result.append({**p, "streak": streak, "taken_today": p["product_id"] in today_logs})
    return {"products": result}


@app.post("/api/tracker/add")
async def tracker_add(body: TrackerAddIn, request: Request):
    tg = get_tg_user(request)
    if body.product_id not in PRODUCTS:
        raise HTTPException(status_code=400, detail="Unknown product")
    p = PRODUCTS[body.product_id]
    await db.add_tracker_product(tg["id"], body.product_id, p["name"])
    return {"ok": True}


@app.post("/api/tracker/log")
async def tracker_log(body: TrackerLogIn, request: Request):
    tg = get_tg_user(request)
    ok = await db.log_intake(tg["id"], body.product_id)
    streak = await db.get_streak(tg["id"], body.product_id)
    return {"ok": ok, "streak": streak, "already_taken": not ok}


@app.delete("/api/tracker/{product_id}")
async def tracker_remove(product_id: str, request: Request):
    tg = get_tg_user(request)
    await db.remove_tracker_product(tg["id"], product_id)
    return {"ok": True}


# ── API: рефералка ────────────────────────────────────────────────────────────

@app.get("/api/referral")
async def referral_info(request: Request):
    tg = get_tg_user(request)
    user = await db.get_user(tg["id"])
    if not user:
        raise HTTPException(status_code=404)
    count = await db.get_referral_count(user["ref_code"])
    levels = [
        {"count": 3,  "reward": "Промокод −15% на WB / Ozon"},
        {"count": 7,  "reward": "Промокод −25%"},
        {"count": 15, "reward": "Бесплатный продукт на выбор"},
    ]
    next_level = next((l for l in levels if l["count"] > count), None)
    return {
        "ref_code": user["ref_code"],
        "ref_count": count,
        "levels": levels,
        "next_level": next_level,
    }


# ── API: розыгрыш ─────────────────────────────────────────────────────────────

@app.get("/api/giveaway")
async def giveaway_info(request: Request):
    tg = get_tg_user(request)
    count = await db.get_giveaway_count()
    participating = await db.is_in_giveaway(tg["id"])
    return {"total_participants": count, "participating": participating}


@app.post("/api/giveaway/join")
async def giveaway_join(request: Request):
    tg = get_tg_user(request)
    await db.get_or_create_user(tg["id"], tg.get("username", ""), tg.get("first_name", ""))
    ok = await db.join_giveaway(tg["id"])
    count = await db.get_giveaway_count()
    return {"ok": ok, "already_joined": not ok, "total_participants": count}


# ── Static: Mini App ──────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="web"), name="static")


@app.get("/")
async def root():
    return FileResponse("web/index.html")
