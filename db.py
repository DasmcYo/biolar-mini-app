import asyncio
import aiohttp
import os
from typing import Any

TURSO_URL   = os.getenv("TURSO_URL", "")   # libsql://dbname-org.turso.io
TURSO_TOKEN = os.getenv("TURSO_TOKEN", "")
DB_PATH     = os.path.join(os.path.dirname(__file__), "biolar.db")


# ── Turso HTTP API ─────────────────────────────────────────────────────────────

def _http_url() -> str:
    return TURSO_URL.replace("libsql://", "https://", 1)


def _to_arg(v: Any) -> dict:
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}


def _from_cell(cell: dict) -> Any:
    t = cell.get("type", "null")
    if t == "null" or cell.get("value") is None:
        return None
    if t == "integer":
        return int(cell["value"])
    if t in ("real", "float"):
        return float(cell["value"])
    return cell["value"]


async def _pipeline(stmts: list[dict]) -> list[dict]:
    requests = [{"type": "execute", "stmt": s} for s in stmts]
    requests.append({"type": "close"})
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_http_url()}/v2/pipeline",
                json={"requests": requests},
                headers={"Authorization": f"Bearer {TURSO_TOKEN}"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                status = resp.status
                text = await resp.text()
        print(f"TURSO [{status}]: {text[:300]}")
        import json as _json
        data = _json.loads(text)
    except Exception as e:
        print(f"TURSO HTTP ERROR: {type(e).__name__}: {e}")
        raise
    results = []
    for res in data["results"][:-1]:
        if res["type"] == "error":
            print(f"TURSO SQL ERROR: {res['error']}")
            raise Exception(res["error"]["message"])
        results.append(res["response"]["result"])
    return results


async def _q(sql: str, args: list = None) -> dict:
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [_to_arg(a) for a in args]
    if TURSO_URL and TURSO_TOKEN:
        results = await _pipeline([stmt])
        return results[0]
    return await _local(sql, args)


async def _local(sql: str, args: list = None) -> dict:
    import aiosqlite

    def _cell(v):
        if v is None:
            return {"type": "null"}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "real", "value": str(v)}
        return {"type": "text", "value": str(v)}

    async with aiosqlite.connect(DB_PATH) as db:
        try:
            async with db.execute(sql, args or []) as cur:
                cols = [d[0] for d in (cur.description or [])]
                rows_raw = await cur.fetchall()
                lastrowid = cur.lastrowid
            await db.commit()
        except Exception:
            await db.rollback()
            raise

    return {
        "cols": [{"name": c} for c in cols],
        "rows": [[_cell(v) for v in row] for row in rows_raw],
        "last_insert_rowid": str(lastrowid) if lastrowid else None,
        "affected_row_count": 0,
    }


def _dicts(result: dict) -> list[dict]:
    cols = [c["name"] for c in result["cols"]]
    return [dict(zip(cols, [_from_cell(cell) for cell in row])) for row in result["rows"]]


def _dict(result: dict) -> dict | None:
    rows = _dicts(result)
    return rows[0] if rows else None


def _lastrow(result: dict) -> int | None:
    v = result.get("last_insert_rowid")
    return int(v) if v else None


# ── Инициализация БД ──────────────────────────────────────────────────────────

SCHEMA = """
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        first_name  TEXT,
        ref_code    TEXT UNIQUE,
        referred_by TEXT,
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS tracker_products (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id      INTEGER NOT NULL,
        product_id   TEXT NOT NULL,
        product_name TEXT NOT NULL,
        started_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        active       INTEGER DEFAULT 1,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS tracker_logs (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        product_id TEXT NOT NULL,
        logged_at  DATE NOT NULL,
        UNIQUE(user_id, product_id, logged_at),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS tracker_dose_counts (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        product_id TEXT NOT NULL,
        date       DATE NOT NULL,
        count      INTEGER DEFAULT 0,
        UNIQUE(user_id, product_id, date),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS giveaway_participants (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id       INTEGER UNIQUE NOT NULL,
        extra_chances INTEGER DEFAULT 0,
        joined_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS wellness_logs (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id  INTEGER NOT NULL,
        date     TEXT NOT NULL,
        energy   INTEGER NOT NULL,
        sleep_q  INTEGER NOT NULL,
        mood     INTEGER NOT NULL,
        UNIQUE(user_id, date),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS articles (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        tag     TEXT NOT NULL,
        title   TEXT NOT NULL,
        preview TEXT NOT NULL,
        body    TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS user_points (
        user_id        INTEGER PRIMARY KEY,
        total_points   INTEGER DEFAULT 0,
        last_spin_date TEXT,
        spin_count     INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS claimed_challenges (
        user_id      INTEGER NOT NULL,
        challenge_id TEXT NOT NULL,
        claimed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, challenge_id)
    );
    CREATE TABLE IF NOT EXISTS food_logs (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id   INTEGER NOT NULL,
        date      TEXT NOT NULL,
        food_name TEXT NOT NULL,
        calories  INTEGER DEFAULT 0,
        protein   REAL DEFAULT 0,
        fat       REAL DEFAULT 0,
        carbs     REAL DEFAULT 0,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS water_logs (
        user_id INTEGER NOT NULL,
        date    TEXT NOT NULL,
        glasses INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, date)
    );
    CREATE TABLE IF NOT EXISTS user_goals (
        user_id  INTEGER PRIMARY KEY,
        calories INTEGER DEFAULT 2000,
        protein  REAL DEFAULT 80.0,
        fat      REAL DEFAULT 70.0,
        carbs    REAL DEFAULT 250.0
    );
    CREATE TABLE IF NOT EXISTS user_reminders (
        user_id       INTEGER PRIMARY KEY,
        reminder_time TEXT DEFAULT NULL,
        enabled       INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS user_chat_ids (
        user_id INTEGER PRIMARY KEY,
        chat_id INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ai_messages (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        first_name TEXT,
        role       TEXT NOT NULL,
        content    TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""


async def init_db():
    stmts = [{"sql": s.strip()} for s in SCHEMA.split(";") if s.strip()]
    if TURSO_URL and TURSO_TOKEN:
        await _pipeline(stmts)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            for stmt in stmts:
                await db.execute(stmt["sql"])
            await db.commit()
    # Migration: add session_token column if not exists
    try:
        await _q("ALTER TABLE users ADD COLUMN session_token TEXT")
    except Exception:
        pass
    # Migration: add water_goal_ml to user_goals
    try:
        await _q("ALTER TABLE user_goals ADD COLUMN water_goal_ml INTEGER DEFAULT 2000")
    except Exception:
        pass
    # Migration: add timezone to users
    try:
        await _q("ALTER TABLE users ADD COLUMN timezone TEXT DEFAULT 'Europe/Moscow'")
    except Exception:
        pass


# ── Users ─────────────────────────────────────────────────────────────────────

async def get_or_create_user(user_id: int, username: str, first_name: str, referred_by: str = None) -> dict:
    import random, string, secrets

    row = _dict(await _q("SELECT * FROM users WHERE user_id = ?", [user_id]))
    if row:
        if not row.get("session_token"):
            token = secrets.token_urlsafe(32)
            await _q("UPDATE users SET session_token = ? WHERE user_id = ?", [token, user_id])
            row["session_token"] = token
        return row

    ref_code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    session_token = secrets.token_urlsafe(32)
    try:
        await _q(
            "INSERT INTO users (user_id, username, first_name, ref_code, referred_by, session_token) VALUES (?, ?, ?, ?, ?, ?)",
            [user_id, username, first_name, ref_code, referred_by, session_token],
        )
        if referred_by:
            await _q(
                "UPDATE giveaway_participants SET extra_chances = extra_chances + 1 "
                "WHERE user_id = (SELECT user_id FROM users WHERE ref_code = ?)",
                [referred_by],
            )
    except Exception as e:
        if "UNIQUE" not in str(e).upper():
            raise

    return _dict(await _q("SELECT * FROM users WHERE user_id = ?", [user_id]))


async def get_user(user_id: int) -> dict | None:
    return _dict(await _q("SELECT * FROM users WHERE user_id = ?", [user_id]))


async def get_user_by_session_token(token: str) -> dict | None:
    return _dict(await _q("SELECT * FROM users WHERE session_token = ?", [token]))


# ── Tracker ───────────────────────────────────────────────────────────────────

async def add_tracker_product(user_id: int, product_id: str, product_name: str):
    await _q(
        "INSERT OR IGNORE INTO tracker_products (user_id, product_id, product_name) VALUES (?, ?, ?)",
        [user_id, product_id, product_name],
    )


async def get_tracker_products(user_id: int) -> list:
    return _dicts(await _q(
        "SELECT * FROM tracker_products WHERE user_id = ? AND active = 1 ORDER BY started_at",
        [user_id],
    ))


async def remove_tracker_product(user_id: int, product_id: str):
    await _q(
        "UPDATE tracker_products SET active = 0 WHERE user_id = ? AND product_id = ?",
        [user_id, product_id],
    )


async def log_intake(user_id: int, product_id: str) -> bool:
    from datetime import date
    today = date.today().isoformat()
    try:
        await _q(
            "INSERT INTO tracker_logs (user_id, product_id, logged_at) VALUES (?, ?, ?)",
            [user_id, product_id, today],
        )
        return True
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            return False
        raise


async def get_streak(user_id: int, product_id: str) -> int:
    from datetime import date, timedelta
    result = await _q(
        "SELECT logged_at FROM tracker_logs WHERE user_id = ? AND product_id = ? ORDER BY logged_at DESC",
        [user_id, product_id],
    )
    rows = [r["logged_at"] for r in _dicts(result)]
    if not rows:
        return 0
    streak, check = 0, date.today()
    for logged in rows:
        d = date.fromisoformat(logged)
        if d == check or d == check - timedelta(days=1):
            streak += 1
            check = d - timedelta(days=1)
        else:
            break
    return streak


async def get_today_logs(user_id: int) -> list[str]:
    from datetime import date
    today = date.today().isoformat()
    result = await _q(
        "SELECT product_id FROM tracker_logs WHERE user_id = ? AND logged_at = ?",
        [user_id, today],
    )
    return [r["product_id"] for r in _dicts(result)]


async def get_doses_today(user_id: int, product_id: str) -> int:
    from datetime import date
    today = date.today().isoformat()
    result = await _q(
        "SELECT count FROM tracker_dose_counts WHERE user_id=? AND product_id=? AND date=?",
        [user_id, product_id, today],
    )
    rows = _dicts(result)
    return rows[0]["count"] if rows else 0


async def update_dose_count(user_id: int, product_id: str, delta: int) -> int:
    from datetime import date
    today = date.today().isoformat()
    result = await _q(
        "SELECT count FROM tracker_dose_counts WHERE user_id=? AND product_id=? AND date=?",
        [user_id, product_id, today],
    )
    rows = _dicts(result)
    current = rows[0]["count"] if rows else 0
    new_count = max(0, current + delta)
    if rows:
        await _q(
            "UPDATE tracker_dose_counts SET count=? WHERE user_id=? AND product_id=? AND date=?",
            [new_count, user_id, product_id, today],
        )
    else:
        await _q(
            "INSERT INTO tracker_dose_counts (user_id, product_id, date, count) VALUES (?, ?, ?, ?)",
            [user_id, product_id, today, new_count],
        )
    return new_count


async def unlog_intake(user_id: int, product_id: str):
    from datetime import date
    today = date.today().isoformat()
    await _q(
        "DELETE FROM tracker_logs WHERE user_id=? AND product_id=? AND logged_at=?",
        [user_id, product_id, today],
    )


async def get_product_days_taken(user_id: int, product_id: str) -> int:
    result = await _q(
        "SELECT COUNT(DISTINCT logged_at) as cnt FROM tracker_logs WHERE user_id=? AND product_id=?",
        [user_id, product_id],
    )
    return _dicts(result)[0]["cnt"]


# ── Giveaway ──────────────────────────────────────────────────────────────────

async def join_giveaway(user_id: int) -> bool:
    try:
        await _q("INSERT INTO giveaway_participants (user_id) VALUES (?)", [user_id])
        return True
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            return False
        raise


async def get_giveaway_count() -> int:
    result = await _q("SELECT COUNT(*) as cnt FROM giveaway_participants")
    return _dicts(result)[0]["cnt"]


async def is_in_giveaway(user_id: int) -> bool:
    result = await _q(
        "SELECT 1 as found FROM giveaway_participants WHERE user_id = ?", [user_id]
    )
    return len(_dicts(result)) > 0


# ── Referrals ─────────────────────────────────────────────────────────────────

async def get_referral_count(ref_code: str) -> int:
    result = await _q(
        "SELECT COUNT(*) as cnt FROM users WHERE referred_by = ?", [ref_code]
    )
    return _dicts(result)[0]["cnt"]


# ── Wellness ──────────────────────────────────────────────────────────────────

async def log_wellness(user_id: int, date: str, energy: int, sleep_q: int, mood: int):
    await _q(
        """INSERT INTO wellness_logs (user_id, date, energy, sleep_q, mood) VALUES (?,?,?,?,?)
           ON CONFLICT(user_id, date) DO UPDATE SET
           energy=excluded.energy, sleep_q=excluded.sleep_q, mood=excluded.mood""",
        [user_id, date, energy, sleep_q, mood],
    )


async def get_wellness_today(user_id: int, date: str) -> dict | None:
    return _dict(await _q(
        "SELECT energy, sleep_q, mood FROM wellness_logs WHERE user_id=? AND date=?",
        [user_id, date],
    ))


async def get_wellness_history(user_id: int, days: int = 14) -> list[dict]:
    return _dicts(await _q(
        "SELECT date, energy, sleep_q, mood FROM wellness_logs WHERE user_id=? ORDER BY date DESC LIMIT ?",
        [user_id, days],
    ))


async def get_wellness_total(user_id: int) -> int:
    result = await _q(
        "SELECT COUNT(*) as cnt FROM wellness_logs WHERE user_id=?", [user_id]
    )
    return _dicts(result)[0]["cnt"]


# ── Course / Streak ───────────────────────────────────────────────────────────

async def get_course_days(user_id: int) -> int:
    result = await _q(
        "SELECT COUNT(DISTINCT logged_at) as cnt FROM tracker_logs WHERE user_id=?", [user_id]
    )
    return _dicts(result)[0]["cnt"]


async def get_global_streak(user_id: int) -> int:
    from datetime import date, timedelta
    result = await _q(
        "SELECT DISTINCT logged_at FROM tracker_logs WHERE user_id=? ORDER BY logged_at DESC",
        [user_id],
    )
    dates = {r["logged_at"] for r in _dicts(result)}
    streak, day = 0, date.today()
    while day.isoformat() in dates:
        streak += 1
        day -= timedelta(days=1)
    return streak


# ── Points / Club ─────────────────────────────────────────────────────────────

async def award_points(user_id: int, points: int) -> int:
    await _q(
        """INSERT INTO user_points (user_id, total_points) VALUES (?, ?)
           ON CONFLICT(user_id) DO UPDATE SET total_points = total_points + excluded.total_points""",
        [user_id, points],
    )
    result = await _q(
        "SELECT total_points FROM user_points WHERE user_id = ?", [user_id]
    )
    return _dicts(result)[0]["total_points"]


async def get_user_points(user_id: int) -> dict:
    result = await _q(
        "SELECT total_points, last_spin_date, spin_count FROM user_points WHERE user_id = ?",
        [user_id],
    )
    row = _dict(result)
    return row if row else {"total_points": 0, "last_spin_date": None, "spin_count": 0}


async def can_spin(user_id: int) -> bool:
    from datetime import date
    today = date.today().isoformat()
    result = await _q(
        "SELECT last_spin_date FROM user_points WHERE user_id = ?", [user_id]
    )
    row = _dict(result)
    return row is None or row["last_spin_date"] != today


async def do_spin(user_id: int, points: int) -> bool:
    from datetime import date
    today = date.today().isoformat()
    result = await _q(
        "SELECT last_spin_date FROM user_points WHERE user_id = ?", [user_id]
    )
    row = _dict(result)
    if row and row["last_spin_date"] == today:
        return False
    await _q(
        """INSERT INTO user_points (user_id, total_points, last_spin_date, spin_count) VALUES (?,?,?,1)
           ON CONFLICT(user_id) DO UPDATE SET
           total_points   = total_points + ?,
           last_spin_date = ?,
           spin_count     = spin_count + 1""",
        [user_id, points, today, points, today],
    )
    return True


async def get_leaderboard(limit: int = 10) -> list[dict]:
    result = await _q(
        """SELECT u.first_name, u.username, up.total_points
           FROM user_points up
           JOIN users u ON u.user_id = up.user_id
           WHERE up.total_points > 0
           ORDER BY up.total_points DESC LIMIT ?""",
        [limit],
    )
    return [{"rank": i + 1, **r} for i, r in enumerate(_dicts(result))]


async def get_claimed_challenges(user_id: int) -> list[str]:
    result = await _q(
        "SELECT challenge_id FROM claimed_challenges WHERE user_id = ?", [user_id]
    )
    return [r["challenge_id"] for r in _dicts(result)]


async def claim_challenge(user_id: int, challenge_id: str) -> bool:
    try:
        await _q(
            "INSERT INTO claimed_challenges (user_id, challenge_id) VALUES (?,?)",
            [user_id, challenge_id],
        )
        return True
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            return False
        raise


# ── Articles ──────────────────────────────────────────────────────────────────

async def seed_articles(articles: list[dict]):
    result = await _q("SELECT COUNT(*) as cnt FROM articles")
    if _dicts(result)[0]["cnt"] == 0 and articles:
        stmts = [{
            "sql": "INSERT INTO articles (tag, title, preview, body) VALUES (?, ?, ?, ?)",
            "args": [_to_arg(a["tag"]), _to_arg(a["title"]), _to_arg(a["preview"]), _to_arg(a["body"])],
        } for a in articles]
        await _pipeline(stmts)


async def get_random_articles(n: int = 4) -> list[dict]:
    return _dicts(await _q(
        "SELECT id, tag, title, preview, body FROM articles ORDER BY RANDOM() LIMIT ?", [n]
    ))


# ── Food ──────────────────────────────────────────────────────────────────────

async def log_food(user_id: int, date: str, food_name: str,
                   calories: int, protein: float, fat: float, carbs: float) -> int:
    result = await _q(
        "INSERT INTO food_logs (user_id, date, food_name, calories, protein, fat, carbs) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [user_id, date, food_name, calories, protein, fat, carbs],
    )
    return _lastrow(result) or 0


async def get_food_logs(user_id: int, date: str) -> list[dict]:
    return _dicts(await _q(
        "SELECT id, food_name, calories, protein, fat, carbs, logged_at FROM food_logs "
        "WHERE user_id=? AND date=? ORDER BY logged_at",
        [user_id, date],
    ))


async def delete_food_log(user_id: int, log_id: int):
    await _q("DELETE FROM food_logs WHERE id=? AND user_id=?", [log_id, user_id])


async def get_food_month(user_id: int, month: str) -> dict:
    result = await _q(
        "SELECT date, SUM(calories) as cal, COUNT(*) as cnt "
        "FROM food_logs WHERE user_id=? AND date LIKE ? GROUP BY date",
        [user_id, f"{month}%"],
    )
    return {r["date"]: {"calories": r["cal"] or 0, "count": r["cnt"]} for r in _dicts(result)}


async def get_food_week(user_id: int) -> list[dict]:
    from datetime import date, timedelta
    today = date.today()
    result = []
    for i in range(7):
        d = (today - timedelta(days=i)).isoformat()
        logs = await get_food_logs(user_id, d)
        if logs:
            result.append({"date": d, "logs": logs})
    return result


# ── Water ──────────────────────────────────────────────────────────────────────

async def get_water(user_id: int, date: str) -> int:
    result = await _q(
        "SELECT glasses FROM water_logs WHERE user_id=? AND date=?", [user_id, date]
    )
    row = _dict(result)
    return row["glasses"] if row else 0


async def set_water(user_id: int, date: str, glasses: int):
    await _q(
        "INSERT INTO water_logs (user_id, date, glasses) VALUES (?,?,?) "
        "ON CONFLICT(user_id, date) DO UPDATE SET glasses=excluded.glasses",
        [user_id, date, glasses],
    )


# ── Goals ──────────────────────────────────────────────────────────────────────

async def get_goals(user_id: int) -> dict:
    row = _dict(await _q(
        "SELECT calories, protein, fat, carbs, water_goal_ml FROM user_goals WHERE user_id=?", [user_id]
    ))
    if not row:
        return {"calories": 2000, "protein": 80.0, "fat": 70.0, "carbs": 250.0, "water_goal_ml": 2000}
    if row.get("water_goal_ml") is None:
        row["water_goal_ml"] = 2000
    return row


async def set_goals(user_id: int, calories: int, protein: float, fat: float, carbs: float):
    await _q(
        "INSERT INTO user_goals (user_id, calories, protein, fat, carbs) VALUES (?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET calories=excluded.calories, protein=excluded.protein, "
        "fat=excluded.fat, carbs=excluded.carbs",
        [user_id, calories, protein, fat, carbs],
    )


async def set_water_goal(user_id: int, goal_ml: int):
    await _q(
        "INSERT INTO user_goals (user_id, water_goal_ml) VALUES (?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET water_goal_ml=excluded.water_goal_ml",
        [user_id, goal_ml],
    )


# ── Reminders ─────────────────────────────────────────────────────────────────

async def get_reminder(user_id: int) -> dict:
    row = _dict(await _q(
        "SELECT reminder_time, enabled FROM user_reminders WHERE user_id=?", [user_id]
    ))
    if row:
        return {"reminder_time": row["reminder_time"], "enabled": bool(row["enabled"])}
    return {"reminder_time": None, "enabled": False}


async def set_reminder(user_id: int, reminder_time: str | None, enabled: bool):
    await _q(
        "INSERT INTO user_reminders (user_id, reminder_time, enabled) VALUES (?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET reminder_time=excluded.reminder_time, enabled=excluded.enabled",
        [user_id, reminder_time, int(enabled)],
    )


async def get_users_with_reminders(current_time: str) -> list[dict]:
    result = await _q(
        "SELECT r.user_id, c.chat_id FROM user_reminders r "
        "JOIN user_chat_ids c ON c.user_id = r.user_id "
        "WHERE r.enabled=1 AND r.reminder_time=?",
        [current_time],
    )
    return _dicts(result)


# ── Chat IDs ──────────────────────────────────────────────────────────────────

async def save_chat_id(user_id: int, chat_id: int):
    await _q(
        "INSERT INTO user_chat_ids (user_id, chat_id) VALUES (?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id",
        [user_id, chat_id],
    )


async def set_user_timezone(user_id: int, timezone: str):
    await _q(
        "UPDATE users SET timezone=? WHERE user_id=?",
        [timezone, user_id],
    )


async def get_users_without_intake_today(target_hour: int) -> list[dict]:
    from datetime import datetime
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    rows = await _q(
        "SELECT DISTINCT u.user_id, u.first_name, u.timezone, c.chat_id "
        "FROM users u "
        "JOIN user_chat_ids c ON c.user_id = u.user_id "
        "JOIN tracker_products tp ON tp.user_id = u.user_id AND tp.active = 1"
    )
    all_users = _dicts(rows)
    if not all_users:
        return []

    intake_rows = await _q(
        "SELECT DISTINCT user_id, logged_at FROM tracker_logs "
        "WHERE logged_at >= date('now', '-1 day')"
    )
    intake_by_user: dict[int, set] = {}
    for r in _dicts(intake_rows):
        intake_by_user.setdefault(r["user_id"], set()).add(r["logged_at"])

    result = []
    for user in all_users:
        try:
            tz = ZoneInfo(user.get("timezone") or "Europe/Moscow")
        except ZoneInfoNotFoundError:
            tz = ZoneInfo("Europe/Moscow")
        local_now = datetime.now(tz)
        if local_now.hour != target_hour:
            continue
        today_local = local_now.date().isoformat()
        if today_local not in intake_by_user.get(user["user_id"], set()):
            result.append(user)
    return result


async def get_users_for_weekly_summary() -> list[dict]:
    return _dicts(await _q(
        "SELECT u.user_id, u.first_name, c.chat_id FROM users u "
        "JOIN user_chat_ids c ON c.user_id = u.user_id"
    ))


# ── AI Messages (admin log) ───────────────────────────────────────────────────

async def save_ai_message(user_id: int, first_name: str, role: str, content: str):
    await _q(
        "INSERT INTO ai_messages (user_id, first_name, role, content) VALUES (?,?,?,?)",
        [user_id, first_name, role, content],
    )


async def get_ai_history(user_id: int, limit: int = 20) -> list[dict]:
    result = await _q(
        "SELECT role, content FROM ai_messages WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
        [user_id, limit],
    )
    return list(reversed(_dicts(result)))


async def get_all_ai_chats() -> list[dict]:
    return _dicts(await _q(
        "SELECT user_id, first_name, role, content, created_at FROM ai_messages "
        "ORDER BY user_id, created_at"
    ))
