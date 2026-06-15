import os
import re
import hmac
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import date as date_cls, timedelta, datetime
from urllib.parse import unquote

from apscheduler.schedulers.asyncio import AsyncIOScheduler

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

import aiohttp

load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL  = os.getenv("WEBHOOK_URL", "")
GROQ_KEY = os.getenv("GROQ_API_KEY", "")

# per-user chat history: { user_id: [ {role, content}, ... ] }
AI_SESSIONS: dict[int, list] = {}


async def ask_groq(user_id: int, message: str, system_prompt: str) -> str:
    history = AI_SESSIONS.get(user_id, [])

    messages = (
        [{"role": "system", "content": system_prompt}]
        + history
        + [{"role": "user", "content": message}]
    )

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": messages,
        "max_tokens": 800,
        "temperature": 0.7,
    }

    headers = {
        "Authorization": f"Bearer {GROQ_KEY}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload, headers=headers
        ) as resp:
            data = await resp.json()

    if "error" in data:
        print("GROQ ERROR:", data["error"])
        return "Не смог получить ответ, попробуй ещё раз."

    try:
        reply = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        print("GROQ UNEXPECTED:", json.dumps(data, ensure_ascii=False)[:300])
        return "Не смог получить ответ, попробуй ещё раз."

    # сохраняем историю (не больше 10 обменов = 20 сообщений)
    history.append({"role": "user",      "content": message})
    history.append({"role": "assistant", "content": reply})
    AI_SESSIONS[user_id] = history[-20:]

    return reply

# ── Club constants ────────────────────────────────────────────────────────────

SPIN_PRIZES = [
    {"idx": 0, "label": "+25 баллов",   "points": 25,  "promo": None,       "weight": 28},
    {"idx": 1, "label": "+50 баллов",   "points": 50,  "promo": None,       "weight": 24},
    {"idx": 2, "label": "+100 баллов",  "points": 100, "promo": None,       "weight": 18},
    {"idx": 3, "label": "Промо −10%",   "points": 50,  "promo": "BIOLAR10", "weight": 12},
    {"idx": 4, "label": "+50 баллов",   "points": 50,  "promo": None,       "weight": 10},
    {"idx": 5, "label": "+200 баллов",  "points": 200, "promo": None,       "weight": 5},
    {"idx": 6, "label": "+25 баллов",   "points": 25,  "promo": None,       "weight": 2},
    {"idx": 7, "label": "Джекпот!",     "points": 500, "promo": None,       "weight": 1},
]

CHALLENGES = [
    {"id": "first_product", "title": "Первый шаг",       "desc": "Добавь продукт в курс",         "reward": 100,  "total": 1},
    {"id": "week_streak",   "title": "Неделя подряд",    "desc": "7 дней подряд принимай курс",   "reward": 200,  "total": 7},
    {"id": "month_streak",  "title": "Месяц подряд",     "desc": "30 дней подряд принимай курс",  "reward": 500,  "total": 30},
    {"id": "diary_30",      "title": "Дневник: 30 дней", "desc": "Заполни дневник самочувствия",  "reward": 300,  "total": 30},
    {"id": "ref_3",         "title": "Посол бренда",     "desc": "Пригласи 3 друзей",             "reward": 400,  "total": 3},
]

LEVELS = [
    {"name": "Bronze",   "idx": 0, "min": 0,    "max": 499,  "color_from": "#b87333", "color_to": "#d4943f"},
    {"name": "Silver",   "idx": 1, "min": 500,  "max": 1999, "color_from": "#8888a0", "color_to": "#a8a8c0"},
    {"name": "Gold",     "idx": 2, "min": 2000, "max": 4999, "color_from": "#c4965a", "color_to": "#e8b060"},
    {"name": "Platinum", "idx": 3, "min": 5000, "max": None, "color_from": "#5080a0", "color_to": "#8090b8"},
]


def get_level_info(total_points: int) -> dict:
    for i, lvl in enumerate(LEVELS):
        if lvl["max"] is None or total_points <= lvl["max"]:
            next_lvl = LEVELS[i + 1] if i + 1 < len(LEVELS) else None
            if next_lvl:
                pct = min(100, round((total_points - lvl["min"]) / (next_lvl["min"] - lvl["min"]) * 100))
            else:
                pct = 100
            return {
                "name": lvl["name"], "idx": lvl["idx"],
                "color_from": lvl["color_from"], "color_to": lvl["color_to"],
                "next": next_lvl["name"] if next_lvl else None,
                "next_threshold": next_lvl["min"] if next_lvl else None,
                "progress_pct": pct,
            }
    return {"name": "Platinum", "idx": 3, "color_from": "#5080a0", "color_to": "#8090b8",
            "next": None, "next_threshold": None, "progress_pct": 100}


scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    from articles_data import ARTICLES
    await db.seed_articles(ARTICLES)
    if WEBHOOK_URL:
        await _set_webhook()
    scheduler.add_job(send_daily_reminders, "interval", minutes=1)
    scheduler.add_job(send_weekly_summaries, "cron", day_of_week="sun", hour=18, minute=0)
    scheduler.start()
    yield
    scheduler.shutdown()


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
    await db.award_points(tg["id"], 10)
    return {"ok": True}


@app.get("/api/wellness")
async def wellness_get(request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    today_log = await db.get_wellness_today(tg["id"], today)
    history   = await db.get_wellness_history(tg["id"], 30)
    return {"today": today_log, "history": history}


# ── API: питание / фото ──────────────────────────────────────────────────────

class FoodAnalyzeIn(BaseModel):
    image_b64: str

class FoodSaveIn(BaseModel):
    food_name: str
    calories: int
    protein: float
    fat: float
    carbs: float

class FoodDeleteIn(BaseModel):
    log_id: int


@app.post("/api/diary/food/analyze")
async def food_analyze(body: FoodAnalyzeIn, request: Request):
    get_tg_user(request)
    if not GROQ_KEY:
        raise HTTPException(status_code=503, detail="AI not configured")

    payload = {
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{body.image_b64}"},
                },
                {
                    "type": "text",
                    "text": (
                        "Посмотри на фото еды. Определи блюдо и дай приблизительное КБЖУ на эту порцию. "
                        'Ответь ТОЛЬКО в формате JSON, без каких-либо других слов: '
                        '{"food": "название блюда на русском", "calories": 350, "protein": 20.5, "fat": 12.3, "carbs": 40.1}'
                    ),
                },
            ],
        }],
        "max_tokens": 256,
        "temperature": 0.1,
    }

    headers = {"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload, headers=headers,
        ) as resp:
            data = await resp.json()

    print("FOOD GROQ RAW:", json.dumps(data, ensure_ascii=False)[:600])

    if "error" in data:
        print("FOOD AI ERROR:", data["error"])
        raise HTTPException(status_code=500, detail=str(data["error"].get("message", "AI error")))

    try:
        text = data["choices"][0]["message"]["content"]
        print("FOOD AI TEXT:", text)
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        result = json.loads(match.group() if match else text)
        return {
            "food":     result.get("food", "Блюдо"),
            "calories": int(float(result.get("calories", 0))),
            "protein":  round(float(result.get("protein", 0)), 1),
            "fat":      round(float(result.get("fat", 0)), 1),
            "carbs":    round(float(result.get("carbs", 0)), 1),
        }
    except Exception as e:
        print("FOOD PARSE ERROR:", e, "| text:", text if "text" in dir() else "N/A")
        raise HTTPException(status_code=500, detail="Не удалось распознать блюдо")


@app.get("/api/diary/food")
async def food_get(request: Request, date: str = None):
    tg = get_tg_user(request)
    d = date if date else date_cls.today().isoformat()
    return {"logs": await db.get_food_logs(tg["id"], d)}


@app.get("/api/diary/food/month")
async def food_month(request: Request, month: str = None):
    tg = get_tg_user(request)
    m = month if month else date_cls.today().strftime("%Y-%m")
    days = await db.get_food_month(tg["id"], m)
    return {"days": days}


@app.post("/api/diary/food/save")
async def food_save(body: FoodSaveIn, request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    log_id = await db.log_food(tg["id"], today, body.food_name, body.calories, body.protein, body.fat, body.carbs)
    return {"ok": True, "id": log_id}


@app.post("/api/diary/food/delete")
async def food_delete(body: FoodDeleteIn, request: Request):
    tg = get_tg_user(request)
    await db.delete_food_log(tg["id"], body.log_id)
    return {"ok": True}


# ── API: вода ─────────────────────────────────────────────────────────────────

class WaterSetIn(BaseModel):
    glasses: int

@app.get("/api/diary/water")
async def water_get(request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    return {"glasses": await db.get_water(tg["id"], today)}

@app.post("/api/diary/water/set")
async def water_set(body: WaterSetIn, request: Request):
    tg = get_tg_user(request)
    today = date_cls.today().isoformat()
    g = max(0, min(body.glasses, 20))
    await db.set_water(tg["id"], today, g)
    return {"ok": True, "glasses": g}


# ── API: цели КБЖУ ────────────────────────────────────────────────────────────

class GoalsIn(BaseModel):
    calories: int = 2000
    protein: float = 80.0
    fat: float = 70.0
    carbs: float = 250.0

@app.get("/api/diary/goals")
async def goals_get(request: Request):
    tg = get_tg_user(request)
    return await db.get_goals(tg["id"])

@app.post("/api/diary/goals")
async def goals_set(body: GoalsIn, request: Request):
    tg = get_tg_user(request)
    await db.set_goals(tg["id"], body.calories, body.protein, body.fat, body.carbs)
    return {"ok": True}


# ── API: анализ питания за неделю ─────────────────────────────────────────────

@app.get("/api/diary/food/week-analysis")
async def food_week_analysis(request: Request):
    tg = get_tg_user(request)
    if not GROQ_KEY:
        raise HTTPException(status_code=503, detail="AI not configured")
    week = await db.get_food_week(tg["id"])
    if not week:
        return {"analysis": "За последние 7 дней нет записей о питании. Сфотографируй блюда через камеру — и я дам анализ."}

    lines = []
    for day in reversed(week):
        total_cal = sum(l["calories"] for l in day["logs"])
        total_p   = sum(l["protein"]  for l in day["logs"])
        total_f   = sum(l["fat"]      for l in day["logs"])
        total_c   = sum(l["carbs"]    for l in day["logs"])
        foods = ", ".join(l["food_name"] for l in day["logs"])
        lines.append(f"{day['date']}: {foods} — {total_cal} ккал, Б{total_p:.0f}/Ж{total_f:.0f}/У{total_c:.0f}г")

    analysis = await ask_groq(
        tg["id"],
        f"Мой рацион за последние 7 дней:\n" + "\n".join(lines) +
        "\n\nДай анализ питания: что хорошо, что стоит улучшить, 1-2 конкретных совета. 5-7 предложений, конкретно и без воды.",
        "Ты AI-нутрициолог Biolar. Анализируй честно и конкретно."
    )
    return {"analysis": analysis}


# ── API: напоминания ──────────────────────────────────────────────────────────

class ReminderIn(BaseModel):
    reminder_time: str | None = None
    enabled: bool = True

@app.get("/api/settings/reminder")
async def reminder_get(request: Request):
    tg = get_tg_user(request)
    return await db.get_reminder(tg["id"])

@app.post("/api/settings/reminder")
async def reminder_set(body: ReminderIn, request: Request):
    tg = get_tg_user(request)
    await db.set_reminder(tg["id"], body.reminder_time, body.enabled)
    return {"ok": True}


# ── Планировщик ───────────────────────────────────────────────────────────────

async def _send_tg_message(chat_id: int, text: str):
    payload = {
        "chat_id": chat_id, "text": text, "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": [[{
            "text": "🌿 Открыть Biolar", "web_app": {"url": WEBHOOK_URL}
        }]]} if WEBHOOK_URL else {},
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)


async def call_groq_once(message: str, system_prompt: str) -> str:
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": message},
        ],
        "max_tokens": 400, "temperature": 0.7,
    }
    headers = {"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers
            ) as resp:
                data = await resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"call_groq_once error: {e}")
        return ""


async def send_daily_reminders():
    if not BOT_TOKEN or not WEBHOOK_URL:
        return
    now = datetime.now().strftime("%H:%M")
    users = await db.get_users_with_reminders(now)
    for u in users:
        try:
            await _send_tg_message(
                u["chat_id"],
                "⏰ *Напоминание Biolar*\n\nВремя принять добавки и отметить в приложении 🌿"
            )
        except Exception as e:
            print(f"Reminder send error uid={u['user_id']}: {e}")


async def send_weekly_summaries():
    if not BOT_TOKEN or not GROQ_KEY:
        return
    users = await db.get_users_for_weekly_summary()
    for u in users:
        try:
            history = await db.get_wellness_history(u["user_id"], days=7)
            if not history:
                continue
            streak = await db.get_global_streak(u["user_id"])
            avg_charge = sum(round((h["energy"] + h["sleep_q"] + h["mood"]) / 3, 1) for h in history) / len(history)
            week_food = await db.get_food_week(u["user_id"])
            food_line = ""
            if week_food:
                avg_cal = sum(sum(l["calories"] for l in d["logs"]) for d in week_food) / len(week_food)
                food_line = f"Питание: {len(week_food)}/7 дней, средние ккал {avg_cal:.0f}."

            summary = await call_groq_once(
                f"Итоги недели пользователя:\n"
                f"Средний заряд: {avg_charge:.1f}/5, стрик: {streak} дней. {food_line}\n"
                "Напиши итог недели (3-4 предложения): что хорошо, один совет на следующую неделю. Тепло и мотивирующе.",
                "Ты AI-нутрициолог Biolar. Пиши кратко и поддерживающе."
            )
            if not summary:
                continue
            name = u["first_name"] or "друг"
            await _send_tg_message(
                u["chat_id"],
                f"🌿 *Итоги недели, {name}!*\n\n{summary}\n\nПродолжай — ты молодец 💪"
            )
        except Exception as e:
            print(f"Weekly summary error uid={u['user_id']}: {e}")


# ── API: статьи ──────────────────────────────────────────────────────────────

@app.get("/api/articles")
async def articles_random():
    return {"articles": await db.get_random_articles(4)}


# ── API: ИИ-нутрициолог (заглушка) ───────────────────────────────────────────

@app.post("/api/ai/chat")
async def ai_chat(body: ChatIn, request: Request):
    tg = get_tg_user(request)

    if not body.message.strip():
        raise HTTPException(status_code=400, detail="Empty message")

    if not GROQ_KEY:
        return {"message": "AI-нутрициолог скоро будет доступен."}

    # контекст пользователя для персонализации
    products  = await db.get_tracker_products(tg["id"])
    history   = await db.get_wellness_history(tg["id"], 7)
    course_days = await db.get_course_days(tg["id"])

    prod_names = ", ".join(p["product_name"] for p in products) if products else "ничего не добавлено"

    avg_charge = ""
    if history:
        charges = [round((e["energy"] + e["sleep_q"] + e["mood"]) / 3, 1) for e in history]
        avg_charge = f"{sum(charges)/len(charges):.1f}/5"

    system = f"""Ты — персональный нутрициолог Biolar Organics. Говоришь по-русски, тепло и конкретно — как знакомый эксперт.

Твоя область знаний: питание, нутриенты, витамины, минералы, гормоны, анализы крови, синдромы и заболевания связанные с нутритивным статусом (СПКЯ, гипотиреоз, анемия, инсулинорезистентность и т.д.), образ жизни и его влияние на здоровье.

Как отвечаешь:
— Понимаешь русские медицинские аббревиатуры (СПКЯ, ИР, ТТГ, ЖДА, АМГ, ФСГ и др.). Если аббревиатура незнакома — уточни у пользователя что имеется в виду.
— Даёшь конкретный, практичный совет. Без лишних оговорок "проконсультируйтесь с врачом" — только если ситуация реально требует врача.
— Если вопрос связан с курсом пользователя — упоминаешь: "Ты уже {course_days} дней на курсе, как раз сейчас..."
— Если вопрос неточный — задаёшь один уточняющий вопрос.
— Длина ответа по ситуации: простой — коротко, сложный — подробнее.
— Пишешь живо, без канцелярита.

Продукты Biolar Organics:
Iron OptiFerrol, Прогестерон Контроль, Витамин С Lipovit, Витамин С Factor C, IMMULAR Activator, IMMULAR Booster, IMMULAR Control, Витамин Д3 4000 МЕ, Витамин Д3 2000 МЕ + 7 элементов, Липосомальный Д3+К2, Нейро Комплекс, Кальций 7 Essential, Гормон Баланс, Хром 4 Essential, Магний 4 Elements, Витамин С Faster C, ThyroSel, Витамин С Pecto C, Цинк 2 Elements, Thyroid Support.
Рекомендуй только эти продукты когда уместно, не выдумывай других.

Данные пользователя:
— Принимает: {prod_names}
— Дней в курсе: {course_days}
{f"— Средний заряд за неделю: {avg_charge}" if avg_charge else ""}

Если вопрос совсем не про здоровье — мягко возвращай к теме."""

    reply = await ask_groq(tg["id"], body.message, system)
    return {"message": reply}


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
    if ok:
        await db.award_points(tg["id"], 20)
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


# ── API: Клуб ────────────────────────────────────────────────────────────────

class ChallengeClaimIn(BaseModel):
    challenge_id: str

@app.get("/api/club/data")
async def club_data(request: Request):
    tg = get_tg_user(request)
    user = await db.get_user(tg["id"])
    if not user:
        raise HTTPException(status_code=404)

    pts_data    = await db.get_user_points(tg["id"])
    total_pts   = pts_data["total_points"]
    spin_count  = pts_data["spin_count"]
    spin_avail  = await db.can_spin(tg["id"])
    level       = get_level_info(total_pts)
    leaderboard = await db.get_leaderboard(10)
    claimed     = set(await db.get_claimed_challenges(tg["id"]))
    ref_count   = await db.get_referral_count(user["ref_code"])
    streak      = await db.get_global_streak(tg["id"])
    course_days = await db.get_course_days(tg["id"])
    wellness_tot = await db.get_wellness_total(tg["id"])
    products    = await db.get_tracker_products(tg["id"])
    giveaway_cnt = await db.get_giveaway_count()
    is_giveaway = await db.is_in_giveaway(tg["id"])

    def challenge_progress(cid: str) -> tuple[int, int]:
        if cid == "first_product": return (1 if products else 0, 1)
        if cid == "week_streak":   return (min(streak, 7), 7)
        if cid == "month_streak":  return (min(streak, 30), 30)
        if cid == "diary_30":      return (min(wellness_tot, 30), 30)
        if cid == "ref_3":         return (min(ref_count, 3), 3)
        return (0, 1)

    challenges_out = []
    for c in CHALLENGES:
        prog, tot = challenge_progress(c["id"])
        challenges_out.append({
            **c,
            "progress": prog,
            "met": prog >= tot,
            "claimed": c["id"] in claimed,
        })

    return {
        "total_points": total_pts,
        "level": level,
        "can_spin": spin_avail,
        "spin_count": spin_count,
        "challenges": challenges_out,
        "leaderboard": leaderboard,
        "ref_code": user["ref_code"],
        "ref_count": ref_count,
        "giveaway_total": giveaway_cnt,
        "giveaway_participating": is_giveaway,
        "achievements_data": {
            "streak": streak,
            "course_days": course_days,
            "ref_count": ref_count,
            "has_products": bool(products),
        },
    }


@app.post("/api/club/spin")
async def club_spin(request: Request):
    import random as _random
    tg = get_tg_user(request)
    if not await db.can_spin(tg["id"]):
        return {"already_spun": True}
    weights = [p["weight"] for p in SPIN_PRIZES]
    prize = _random.choices(SPIN_PRIZES, weights=weights, k=1)[0]
    ok = await db.do_spin(tg["id"], prize["points"])
    if not ok:
        return {"already_spun": True}
    new_total = (await db.get_user_points(tg["id"]))["total_points"]
    return {
        "already_spun": False,
        "segment": prize["idx"],
        "prize": {"label": prize["label"], "points": prize["points"], "promo": prize["promo"]},
        "new_total": new_total,
    }


@app.post("/api/club/challenge/claim")
async def challenge_claim(body: ChallengeClaimIn, request: Request):
    tg = get_tg_user(request)
    c = next((x for x in CHALLENGES if x["id"] == body.challenge_id), None)
    if not c:
        raise HTTPException(status_code=400, detail="Unknown challenge")
    ok = await db.claim_challenge(tg["id"], body.challenge_id)
    if not ok:
        return {"ok": False, "already_claimed": True}
    new_total = await db.award_points(tg["id"], c["reward"])
    return {"ok": True, "reward": c["reward"], "new_total": new_total}


# ── Static: Mini App ──────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="web"), name="static")


@app.get("/")
async def root():
    return FileResponse("web/index.html")
