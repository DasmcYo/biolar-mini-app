import asyncio
import os
import threading

TURSO_URL   = os.getenv("TURSO_URL", "")
TURSO_TOKEN = os.getenv("TURSO_TOKEN", "")
DB_PATH     = os.path.join(os.path.dirname(__file__), "biolar.db")

_local = threading.local()


def _conn():
    """Возвращает соединение для текущего потока. При первом вызове создаёт и синхронизирует."""
    if not hasattr(_local, "c"):
        import libsql_experimental as libsql
        if TURSO_URL and TURSO_TOKEN:
            replica = os.path.join(os.path.dirname(__file__), "replica.db")
            _local.c = libsql.connect(replica, url=TURSO_URL, auth_token=TURSO_TOKEN)
            _local.c.sync()
        else:
            _local.c = libsql.connect(DB_PATH)
    return _local.c


def _dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


def _dict(cursor) -> dict | None:
    if not cursor.description:
        return None
    cols = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    return dict(zip(cols, row)) if row else None


async def _run(fn):
    return await asyncio.to_thread(fn)


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
    );
"""


async def init_db():
    def _():
        conn = _conn()
        for stmt in SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        conn.commit()
    await _run(_)


# ── Users ─────────────────────────────────────────────────────────────────────

async def get_or_create_user(user_id: int, username: str, first_name: str, referred_by: str = None) -> dict:
    import random, string

    def _():
        conn = _conn()
        cur = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = _dict(cur)
        if row:
            return row
        ref_code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, ref_code, referred_by) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, first_name, ref_code, referred_by),
        )
        if referred_by:
            conn.execute(
                "UPDATE giveaway_participants SET extra_chances = extra_chances + 1 "
                "WHERE user_id = (SELECT user_id FROM users WHERE ref_code = ?)",
                (referred_by,),
            )
        conn.commit()
        return _dict(conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)))

    return await _run(_)


async def get_user(user_id: int) -> dict | None:
    def _():
        return _dict(_conn().execute("SELECT * FROM users WHERE user_id = ?", (user_id,)))
    return await _run(_)


# ── Tracker ───────────────────────────────────────────────────────────────────

async def add_tracker_product(user_id: int, product_id: str, product_name: str):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT OR IGNORE INTO tracker_products (user_id, product_id, product_name) VALUES (?, ?, ?)",
            (user_id, product_id, product_name),
        )
        conn.commit()
    await _run(_)


async def get_tracker_products(user_id: int) -> list:
    def _():
        return _dicts(_conn().execute(
            "SELECT * FROM tracker_products WHERE user_id = ? AND active = 1 ORDER BY started_at",
            (user_id,),
        ))
    return await _run(_)


async def remove_tracker_product(user_id: int, product_id: str):
    def _():
        conn = _conn()
        conn.execute(
            "UPDATE tracker_products SET active = 0 WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )
        conn.commit()
    await _run(_)


async def log_intake(user_id: int, product_id: str) -> bool:
    from datetime import date
    today = date.today().isoformat()

    def _():
        try:
            conn = _conn()
            conn.execute(
                "INSERT INTO tracker_logs (user_id, product_id, logged_at) VALUES (?, ?, ?)",
                (user_id, product_id, today),
            )
            conn.commit()
            return True
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                return False
            raise
    return await _run(_)


async def get_streak(user_id: int, product_id: str) -> int:
    from datetime import date, timedelta

    def _():
        cur = _conn().execute(
            "SELECT logged_at FROM tracker_logs WHERE user_id = ? AND product_id = ? ORDER BY logged_at DESC",
            (user_id, product_id),
        )
        return [r[0] for r in cur.fetchall()]

    rows = await _run(_)
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

    def _():
        cur = _conn().execute(
            "SELECT product_id FROM tracker_logs WHERE user_id = ? AND logged_at = ?",
            (user_id, today),
        )
        return [r[0] for r in cur.fetchall()]
    return await _run(_)


# ── Giveaway ──────────────────────────────────────────────────────────────────

async def join_giveaway(user_id: int) -> bool:
    def _():
        try:
            conn = _conn()
            conn.execute("INSERT INTO giveaway_participants (user_id) VALUES (?)", (user_id,))
            conn.commit()
            return True
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                return False
            raise
    return await _run(_)


async def get_giveaway_count() -> int:
    def _():
        return _conn().execute("SELECT COUNT(*) FROM giveaway_participants").fetchone()[0]
    return await _run(_)


async def is_in_giveaway(user_id: int) -> bool:
    def _():
        return _conn().execute(
            "SELECT 1 FROM giveaway_participants WHERE user_id = ?", (user_id,)
        ).fetchone() is not None
    return await _run(_)


# ── Referrals ─────────────────────────────────────────────────────────────────

async def get_referral_count(ref_code: str) -> int:
    def _():
        return _conn().execute(
            "SELECT COUNT(*) FROM users WHERE referred_by = ?", (ref_code,)
        ).fetchone()[0]
    return await _run(_)


# ── Wellness ──────────────────────────────────────────────────────────────────

async def log_wellness(user_id: int, date: str, energy: int, sleep_q: int, mood: int):
    def _():
        conn = _conn()
        conn.execute(
            """INSERT INTO wellness_logs (user_id, date, energy, sleep_q, mood) VALUES (?,?,?,?,?)
               ON CONFLICT(user_id, date) DO UPDATE SET
               energy=excluded.energy, sleep_q=excluded.sleep_q, mood=excluded.mood""",
            (user_id, date, energy, sleep_q, mood),
        )
        conn.commit()
    await _run(_)


async def get_wellness_today(user_id: int, date: str) -> dict | None:
    def _():
        return _dict(_conn().execute(
            "SELECT energy, sleep_q, mood FROM wellness_logs WHERE user_id=? AND date=?",
            (user_id, date),
        ))
    return await _run(_)


async def get_wellness_history(user_id: int, days: int = 14) -> list[dict]:
    def _():
        return _dicts(_conn().execute(
            "SELECT date, energy, sleep_q, mood FROM wellness_logs WHERE user_id=? ORDER BY date DESC LIMIT ?",
            (user_id, days),
        ))
    return await _run(_)


async def get_wellness_total(user_id: int) -> int:
    def _():
        return _conn().execute(
            "SELECT COUNT(*) FROM wellness_logs WHERE user_id=?", (user_id,)
        ).fetchone()[0]
    return await _run(_)


# ── Course / Streak ───────────────────────────────────────────────────────────

async def get_course_days(user_id: int) -> int:
    def _():
        return _conn().execute(
            "SELECT COUNT(DISTINCT logged_at) FROM tracker_logs WHERE user_id=?", (user_id,)
        ).fetchone()[0]
    return await _run(_)


async def get_global_streak(user_id: int) -> int:
    from datetime import date, timedelta

    def _():
        cur = _conn().execute(
            "SELECT DISTINCT logged_at FROM tracker_logs WHERE user_id=? ORDER BY logged_at DESC",
            (user_id,),
        )
        return {r[0] for r in cur.fetchall()}

    dates = await _run(_)
    streak, day = 0, date.today()
    while day.isoformat() in dates:
        streak += 1
        day -= timedelta(days=1)
    return streak


# ── Points / Club ─────────────────────────────────────────────────────────────

async def award_points(user_id: int, points: int) -> int:
    def _():
        conn = _conn()
        conn.execute(
            """INSERT INTO user_points (user_id, total_points) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET total_points = total_points + excluded.total_points""",
            (user_id, points),
        )
        conn.commit()
        return conn.execute(
            "SELECT total_points FROM user_points WHERE user_id = ?", (user_id,)
        ).fetchone()[0]
    return await _run(_)


async def get_user_points(user_id: int) -> dict:
    def _():
        row = _conn().execute(
            "SELECT total_points, last_spin_date, spin_count FROM user_points WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return {"total_points": row[0], "last_spin_date": row[1], "spin_count": row[2]} if row \
            else {"total_points": 0, "last_spin_date": None, "spin_count": 0}
    return await _run(_)


async def can_spin(user_id: int) -> bool:
    from datetime import date
    today = date.today().isoformat()

    def _():
        row = _conn().execute(
            "SELECT last_spin_date FROM user_points WHERE user_id = ?", (user_id,)
        ).fetchone()
        return row is None or row[0] != today
    return await _run(_)


async def do_spin(user_id: int, points: int) -> bool:
    from datetime import date
    today = date.today().isoformat()

    def _():
        conn = _conn()
        row = conn.execute(
            "SELECT last_spin_date FROM user_points WHERE user_id = ?", (user_id,)
        ).fetchone()
        if row and row[0] == today:
            return False
        conn.execute(
            """INSERT INTO user_points (user_id, total_points, last_spin_date, spin_count) VALUES (?,?,?,1)
               ON CONFLICT(user_id) DO UPDATE SET
               total_points   = total_points + ?,
               last_spin_date = ?,
               spin_count     = spin_count + 1""",
            (user_id, points, today, points, today),
        )
        conn.commit()
        return True
    return await _run(_)


async def get_leaderboard(limit: int = 10) -> list[dict]:
    def _():
        rows = _dicts(_conn().execute(
            """SELECT u.first_name, u.username, up.total_points
               FROM user_points up
               JOIN users u ON u.user_id = up.user_id
               WHERE up.total_points > 0
               ORDER BY up.total_points DESC LIMIT ?""",
            (limit,),
        ))
        return [{"rank": i + 1, **r} for i, r in enumerate(rows)]
    return await _run(_)


async def get_claimed_challenges(user_id: int) -> list[str]:
    def _():
        return [r[0] for r in _conn().execute(
            "SELECT challenge_id FROM claimed_challenges WHERE user_id = ?", (user_id,)
        ).fetchall()]
    return await _run(_)


async def claim_challenge(user_id: int, challenge_id: str) -> bool:
    def _():
        try:
            conn = _conn()
            conn.execute(
                "INSERT INTO claimed_challenges (user_id, challenge_id) VALUES (?,?)",
                (user_id, challenge_id),
            )
            conn.commit()
            return True
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                return False
            raise
    return await _run(_)


# ── Articles ──────────────────────────────────────────────────────────────────

async def seed_articles(articles: list[dict]):
    def _():
        conn = _conn()
        count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        if count == 0:
            for a in articles:
                conn.execute(
                    "INSERT INTO articles (tag, title, preview, body) VALUES (?, ?, ?, ?)",
                    (a["tag"], a["title"], a["preview"], a["body"]),
                )
            conn.commit()
    await _run(_)


async def get_random_articles(n: int = 4) -> list[dict]:
    def _():
        return _dicts(_conn().execute(
            "SELECT id, tag, title, preview, body FROM articles ORDER BY RANDOM() LIMIT ?", (n,)
        ))
    return await _run(_)


# ── Food ──────────────────────────────────────────────────────────────────────

async def log_food(user_id: int, date: str, food_name: str,
                   calories: int, protein: float, fat: float, carbs: float) -> int:
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO food_logs (user_id, date, food_name, calories, protein, fat, carbs) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, date, food_name, calories, protein, fat, carbs),
        )
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return await _run(_)


async def get_food_logs(user_id: int, date: str) -> list[dict]:
    def _():
        return _dicts(_conn().execute(
            "SELECT id, food_name, calories, protein, fat, carbs FROM food_logs "
            "WHERE user_id=? AND date=? ORDER BY logged_at",
            (user_id, date),
        ))
    return await _run(_)


async def delete_food_log(user_id: int, log_id: int):
    def _():
        conn = _conn()
        conn.execute("DELETE FROM food_logs WHERE id=? AND user_id=?", (log_id, user_id))
        conn.commit()
    await _run(_)


async def get_food_month(user_id: int, month: str) -> dict:
    def _():
        rows = _conn().execute(
            "SELECT date, SUM(calories) as cal, COUNT(*) as cnt "
            "FROM food_logs WHERE user_id=? AND date LIKE ? GROUP BY date",
            (user_id, f"{month}%"),
        ).fetchall()
        return {r[0]: {"calories": r[1] or 0, "count": r[2]} for r in rows}
    return await _run(_)


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
    def _():
        row = _conn().execute(
            "SELECT glasses FROM water_logs WHERE user_id=? AND date=?", (user_id, date)
        ).fetchone()
        return row[0] if row else 0
    return await _run(_)


async def set_water(user_id: int, date: str, glasses: int):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO water_logs (user_id, date, glasses) VALUES (?,?,?) "
            "ON CONFLICT(user_id, date) DO UPDATE SET glasses=excluded.glasses",
            (user_id, date, glasses),
        )
        conn.commit()
    await _run(_)


# ── Goals ──────────────────────────────────────────────────────────────────────

async def get_goals(user_id: int) -> dict:
    def _():
        row = _dict(_conn().execute(
            "SELECT calories, protein, fat, carbs FROM user_goals WHERE user_id=?", (user_id,)
        ))
        return row if row else {"calories": 2000, "protein": 80.0, "fat": 70.0, "carbs": 250.0}
    return await _run(_)


async def set_goals(user_id: int, calories: int, protein: float, fat: float, carbs: float):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO user_goals (user_id, calories, protein, fat, carbs) VALUES (?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET calories=excluded.calories, protein=excluded.protein, "
            "fat=excluded.fat, carbs=excluded.carbs",
            (user_id, calories, protein, fat, carbs),
        )
        conn.commit()
    await _run(_)


# ── Reminders ─────────────────────────────────────────────────────────────────

async def get_reminder(user_id: int) -> dict:
    def _():
        row = _conn().execute(
            "SELECT reminder_time, enabled FROM user_reminders WHERE user_id=?", (user_id,)
        ).fetchone()
        return {"reminder_time": row[0], "enabled": bool(row[1])} if row \
            else {"reminder_time": None, "enabled": False}
    return await _run(_)


async def set_reminder(user_id: int, reminder_time: str | None, enabled: bool):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO user_reminders (user_id, reminder_time, enabled) VALUES (?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET reminder_time=excluded.reminder_time, enabled=excluded.enabled",
            (user_id, reminder_time, int(enabled)),
        )
        conn.commit()
    await _run(_)


async def get_users_with_reminders(current_time: str) -> list[dict]:
    def _():
        rows = _conn().execute(
            "SELECT r.user_id, c.chat_id FROM user_reminders r "
            "JOIN user_chat_ids c ON c.user_id = r.user_id "
            "WHERE r.enabled=1 AND r.reminder_time=?",
            (current_time,),
        ).fetchall()
        return [{"user_id": r[0], "chat_id": r[1]} for r in rows]
    return await _run(_)


# ── Chat IDs ──────────────────────────────────────────────────────────────────

async def save_chat_id(user_id: int, chat_id: int):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO user_chat_ids (user_id, chat_id) VALUES (?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id",
            (user_id, chat_id),
        )
        conn.commit()
    await _run(_)


async def get_users_for_weekly_summary() -> list[dict]:
    def _():
        rows = _conn().execute(
            "SELECT u.user_id, u.first_name, c.chat_id FROM users u "
            "JOIN user_chat_ids c ON c.user_id = u.user_id",
        ).fetchall()
        return [{"user_id": r[0], "first_name": r[1], "chat_id": r[2]} for r in rows]
    return await _run(_)


# ── AI Messages (admin log) ───────────────────────────────────────────────────

async def save_ai_message(user_id: int, first_name: str, role: str, content: str):
    def _():
        conn = _conn()
        conn.execute(
            "INSERT INTO ai_messages (user_id, first_name, role, content) VALUES (?,?,?,?)",
            (user_id, first_name, role, content),
        )
        conn.commit()
    await _run(_)


async def get_ai_history(user_id: int, limit: int = 20) -> list[dict]:
    def _():
        return _dicts(_conn().execute(
            "SELECT role, content FROM ai_messages WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ))
    rows = await _run(_)
    return list(reversed(rows))


async def get_all_ai_chats() -> list[dict]:
    def _():
        conn = _conn()
        try:
            conn.sync()
        except Exception:
            pass
        return _dicts(conn.execute(
            "SELECT user_id, first_name, role, content, created_at FROM ai_messages "
            "ORDER BY user_id, created_at"
        ))
    return await _run(_)
