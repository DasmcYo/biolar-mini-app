import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "biolar.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
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
        """)
        await db.commit()


async def get_or_create_user(user_id: int, username: str, first_name: str, referred_by: str = None) -> dict:
    import random, string
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
        if row:
            return dict(row)

        ref_code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        await db.execute(
            "INSERT INTO users (user_id, username, first_name, ref_code, referred_by) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, first_name, ref_code, referred_by),
        )
        if referred_by:
            await db.execute(
                "UPDATE giveaway_participants SET extra_chances = extra_chances + 1 "
                "WHERE user_id = (SELECT user_id FROM users WHERE ref_code = ?)",
                (referred_by,),
            )
        await db.commit()

        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cur:
            return dict(await cur.fetchone())


async def get_user(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None


async def add_tracker_product(user_id: int, product_id: str, product_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO tracker_products (user_id, product_id, product_name) VALUES (?, ?, ?)",
            (user_id, product_id, product_name),
        )
        await db.commit()


async def get_tracker_products(user_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tracker_products WHERE user_id = ? AND active = 1 ORDER BY started_at",
            (user_id,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def remove_tracker_product(user_id: int, product_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tracker_products SET active = 0 WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )
        await db.commit()


async def log_intake(user_id: int, product_id: str) -> bool:
    """Отметить приём на сегодня. Возвращает True если успешно, False если уже отмечено."""
    from datetime import date
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO tracker_logs (user_id, product_id, logged_at) VALUES (?, ?, ?)",
                (user_id, product_id, today),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def get_streak(user_id: int, product_id: str) -> int:
    """Считает текущую серию дней подряд."""
    from datetime import date, timedelta
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT logged_at FROM tracker_logs WHERE user_id = ? AND product_id = ? ORDER BY logged_at DESC",
            (user_id, product_id),
        ) as cur:
            rows = [r[0] for r in await cur.fetchall()]

    if not rows:
        return 0

    streak = 0
    check = date.today()
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
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT product_id FROM tracker_logs WHERE user_id = ? AND logged_at = ?",
            (user_id, today),
        ) as cur:
            return [r[0] for r in await cur.fetchall()]


async def join_giveaway(user_id: int) -> bool:
    """Участвовать в розыгрыше. Возвращает False если уже участвует."""
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO giveaway_participants (user_id) VALUES (?)", (user_id,)
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def get_giveaway_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM giveaway_participants") as cur:
            return (await cur.fetchone())[0]


async def is_in_giveaway(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM giveaway_participants WHERE user_id = ?", (user_id,)
        ) as cur:
            return (await cur.fetchone()) is not None


async def get_referral_count(ref_code: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by = ?", (ref_code,)
        ) as cur:
            return (await cur.fetchone())[0]
