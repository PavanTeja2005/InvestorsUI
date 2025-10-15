import os, sqlite3, asyncio, threading, time, queue
from pathlib import Path
from flask import Flask, g, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import datetime as dt  # safer: module alias
from datetime import timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes
import os
from dotenv import load_dotenv
from flask import render_template, render_template_string, redirect  # ensure import inside create_app scope
import secrets
load_dotenv()

# Access them using os.getenv
TOKEN = os.getenv("TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")


# ========================
# Config
# ========================
DATABASE = "polls.sqlite3"
UPLOAD_DIR = "uploads"
Path(UPLOAD_DIR).mkdir(exist_ok=True)


# Pending delivery items (protected by a lock)
PENDING_DELIVERY = []  # list of dicts: {"poll_id": int, "option_id": int, "user_id": int, "added_at": float}
PENDING_LOCK = threading.Lock()

# Thread-safe queues
SEND_QUEUE: "queue.Queue[tuple[int, str, str, str | None]]" = queue.Queue()  # changed to include button_url
POLL_QUEUE: "queue.Queue[int]" = queue.Queue()


PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:5000")



# -----------------------------
# DB Helpers
# -----------------------------
def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        g.db.row_factory = dict_factory
    return g.db

def query_all(sql, params=()):
    cur = get_db().execute(sql, params)
    rows = cur.fetchall(); cur.close()
    return rows

def query_one(sql, params=()):
    cur = get_db().execute(sql, params)
    row = cur.fetchone(); cur.close()
    return row

def init_db():
    conn = sqlite3.connect(DATABASE, check_same_thread=False)
    conn.row_factory = dict_factory
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS Poll(
      poll_id INTEGER PRIMARY KEY AUTOINCREMENT,
      question TEXT NOT NULL,
      poll_type TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS PollOption(
      option_id INTEGER PRIMARY KEY AUTOINCREMENT,
      poll_id INTEGER NOT NULL,
      option_text TEXT NOT NULL,
      FOREIGN KEY (poll_id) REFERENCES Poll(poll_id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS PollResponse(
      response_id INTEGER PRIMARY KEY AUTOINCREMENT,
      poll_id     INTEGER NOT NULL,
      option_id   INTEGER NOT NULL,
      user_id     INTEGER NOT NULL,
      username    TEXT,
      confirmed   INTEGER NOT NULL DEFAULT 0,
      responded_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
      FOREIGN KEY (poll_id) REFERENCES Poll(poll_id) ON DELETE CASCADE,
      FOREIGN KEY (option_id) REFERENCES PollOption(option_id) ON DELETE CASCADE,
      UNIQUE (poll_id, option_id, user_id)
    );
    -- Unique upload token per user+poll+option to open the upload page
    CREATE TABLE IF NOT EXISTS UploadToken (
      token       TEXT PRIMARY KEY,
      poll_id     INTEGER NOT NULL,
      option_id   INTEGER NOT NULL,
      user_id     INTEGER NOT NULL,
      username    TEXT,
      created_at  TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
      expires_at  TEXT NOT NULL,
      used_at     TEXT,
      FOREIGN KEY (poll_id) REFERENCES Poll(poll_id) ON DELETE CASCADE,
      FOREIGN KEY (option_id) REFERENCES PollOption(option_id) ON DELETE CASCADE
    );

    -- Per-client execution proof (their own screenshot)
    CREATE TABLE IF NOT EXISTS OptionExecution (
      poll_id        INTEGER NOT NULL,
      option_id      INTEGER NOT NULL,
      user_id        INTEGER NOT NULL,
      screenshot_url TEXT NOT NULL,
      created_at     TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
      PRIMARY KEY (poll_id, option_id, user_id),
      FOREIGN KEY (poll_id) REFERENCES Poll(poll_id) ON DELETE CASCADE,
      FOREIGN KEY (option_id) REFERENCES PollOption(option_id) ON DELETE CASCADE
    );
    """)
    conn.executescript("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_pollresp_p_u_o
      ON PollResponse(poll_id, user_id, option_id);
    CREATE TABLE IF NOT EXISTS PollOptionTrade (
      poll_id       INTEGER NOT NULL,
      option_id     INTEGER NOT NULL,
      screenshot_url TEXT NOT NULL,
      created_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
      PRIMARY KEY (poll_id, option_id),
      FOREIGN KEY (poll_id) REFERENCES Poll(poll_id) ON DELETE CASCADE,
      FOREIGN KEY (option_id) REFERENCES PollOption(option_id) ON DELETE CASCADE
    );
    """)
    conn.commit()
    conn.close()

# --- ADD: helper to map screenshot_url -> absolute file path
def _abs_path_from_url(screenshot_url: str) -> str | None:
    """
    Accepts values like:
      http://host/api/uploads/<fname>
      https://host/api/uploads/<fname>
      /api/uploads/<fname>
    Returns absolute local path under UPLOAD_DIR, or None if not parseable.
    """
    if not screenshot_url:
        return None
    marker = "/api/uploads/"
    if marker not in screenshot_url:
        return None
    rel = screenshot_url.split(marker, 1)[1]  # "<fname>" (or nested path if you later add dirs)
    return os.path.join(UPLOAD_DIR, rel)

# -----------------------------
# Flask app
# -----------------------------
def create_app():
    app = Flask(__name__)
    with app.app_context():
        init_db()

    @app.teardown_appcontext
    def close_db(exception):
        db = g.pop("db", None)
        if db is not None:
            db.close()

    @app.get("/ui/polls/uploads/<path:filename>")
    def serve_upload(filename):
        return send_from_directory(UPLOAD_DIR, filename)

    @app.post("/api/polls/<int:poll_id>/options/<int:option_id>/screenshot")
    def upload_option_screenshot(poll_id: int, option_id: int):
        poll = query_one("SELECT poll_id FROM Poll WHERE poll_id=?", (poll_id,))
        opt  = query_one("SELECT option_id FROM PollOption WHERE option_id=? AND poll_id=?", (option_id, poll_id))
        if not poll or not opt:
            return {"error": "Poll or Option not found"}, 404

        if "multipart/form-data" not in (request.content_type or ""):
            return {"error": "Use multipart/form-data with 'file' field"}, 415

        file = request.files.get("file")
        if not file or not file.filename:
            return {"error": "File required"}, 400

        # --- NEW: delete any previous file for this (poll_id, option_id)
        prev = query_one(
            "SELECT screenshot_url FROM PollOptionTrade WHERE poll_id=? AND option_id=?",
            (poll_id, option_id)
        )
        if prev and prev.get("screenshot_url"):
            old_abs = _abs_path_from_url(prev["screenshot_url"])
            try:
                if old_abs and os.path.isfile(old_abs):
                    os.remove(old_abs)
            except Exception:
                # ignore filesystem races
                pass

        # Save new file (keep your naming scheme)
        fname = secure_filename(file.filename)
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"poll{poll_id}_opt{option_id}_{stamp}__{fname}"
        save_path = os.path.join(UPLOAD_DIR, fname)
        file.save(save_path)

        # Persist absolute URL (kept same as your current code)
        base_url = request.url_root.rstrip("/")
        screenshot_url = f"uploads/{fname}" # f"{base_url}/api/uploads/{fname}"

        conn = get_db()
        conn.execute("""
            INSERT INTO PollOptionTrade(poll_id, option_id, screenshot_url, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(poll_id, option_id) DO UPDATE
            SET screenshot_url=excluded.screenshot_url,
                created_at=CURRENT_TIMESTAMP
        """, (poll_id, option_id, screenshot_url))
        conn.commit()

        return {
            "poll_id": poll_id,
            "option_id": option_id,
            "screenshot_url": screenshot_url
        }, 201


    @app.get("/api/listpolls")
    def list_polls():
        try:
            limit = int(request.args.get("limit", 50))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            limit, offset = 50, 0
        limit = max(1, min(200, limit))
        offset = max(0, offset)

        polls = query_all(
            "SELECT poll_id, question, poll_type, created_at FROM Poll ORDER BY poll_id DESC LIMIT ? OFFSET ?",
            (limit, offset)
        )
        for p in polls:
            p["options"] = query_all("SELECT option_id, option_text FROM PollOption WHERE poll_id=? ORDER BY option_id",
                                     (p["poll_id"],))
        return jsonify(polls), 200

    @app.post("/api/polls")
    def create_poll():
        data = request.get_json(force=True, silent=True) or {}
        question = (data.get("question") or "").strip()
        poll_type = (data.get("poll_type") or "single").strip().lower()
        options = data.get("options") or []
        if not question or not options:
            return {"error": "question and options required"}, 400
        if poll_type not in ("single", "multi"):
            poll_type = "single"

        conn = get_db()
        cur = conn.execute(
            "INSERT INTO Poll(question, poll_type, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (question, poll_type)
        )
        poll_id = cur.lastrowid
        for opt in options:
            txt = (opt.get("option_text") or "").strip() if isinstance(opt, dict) else str(opt).strip()
            if not txt:
                conn.rollback()
                return {"error": "option_text cannot be empty"}, 400
            conn.execute("INSERT INTO PollOption(poll_id, option_text) VALUES(?, ?)", (poll_id, txt))
        conn.commit()

        POLL_QUEUE.put(poll_id)

        poll = query_one("SELECT poll_id, question, poll_type, created_at FROM Poll WHERE poll_id=?", (poll_id,))
        poll["options"] = query_all("SELECT option_id, option_text FROM PollOption WHERE poll_id=? ORDER BY option_id",
                                    (poll_id,))
        return poll, 201
        # -----------------------------
    # Web UI (no API changes)
    # -----------------------------
    from flask import render_template  # local import to avoid touching globals

    @app.get("/")
    def ui_root():
        return render_template("polls_list.html")

    @app.get("/ui")
    def ui_home():
        return render_template("polls_list.html")

    @app.get("/ui/polls")
    def ui_list_polls():
        # Server-render basic shell; data loaded via fetch from /api/listpolls
        return render_template("polls_list.html")

    @app.get("/ui/polls/new")
    def ui_new_poll():
        return render_template("poll_new.html")

    @app.get("/ui/polls/<int:poll_id>")
    def ui_poll_detail(poll_id: int):
        p = query_one("SELECT poll_id, question, poll_type, created_at FROM Poll WHERE poll_id=?", (poll_id,))
        if not p:
            p = {"poll_id": poll_id, "question": "Not Found", "poll_type": "", "created_at": "", "options": []}
            return render_template("poll_detail.html", poll=p)

        # --- CHANGE: include screenshot_url for each option
        p["options"] = query_all("""
            SELECT o.option_id, o.option_text, t.screenshot_url
            FROM PollOption o
            LEFT JOIN PollOptionTrade t
              ON t.poll_id = o.poll_id AND t.option_id = o.option_id
            WHERE o.poll_id=?
            ORDER BY o.option_id
        """, (poll_id,))
        return render_template("poll_detail.html", poll=p)

    @app.get("/ui/upload/<token>")
    def ui_upload_form(token: str):
        # Validate token, not used and not expired
        row = query_one("""
            SELECT t.token, t.poll_id, t.option_id, t.user_id, t.username,
                p.question, o.option_text
            FROM UploadToken t
            JOIN Poll p ON p.poll_id = t.poll_id
            JOIN PollOption o ON o.poll_id = t.poll_id AND o.option_id = t.option_id
            WHERE t.token = ? AND t.used_at IS NULL AND t.expires_at > CURRENT_TIMESTAMP
        """, (token,))
        if not row:
            return render_template_string("""
                <html><body>
                <h3>Link invalid or expired</h3>
                <p>Please request a new upload link from the admin.</p>
                </body></html>
            """), 410

        return render_template_string("""
        <html>
        <body>
            <h3>Upload execution screenshot</h3>
            <p><b>Poll:</b> {{question}}<br>
            <b>Option:</b> {{option_text}}<br>
            <b>User:</b> {{username or user_id}}</p>
            <form method="post" enctype="multipart/form-data" action="/ui/upload/{{token}}">
            <input type="file" name="file" accept="image/*" required />
            <button type="submit">Upload</button>
            </form>
        </body>
        </html>
        """, token=token, question=row["question"], option_text=row["option_text"],
        username=row.get("username"), user_id=row["user_id"])


    @app.post("/ui/upload/<token>")
    def ui_upload_submit(token: str):
        # Resolve token and re-check validity
        row = query_one("""
            SELECT t.token, t.poll_id, t.option_id, t.user_id
            FROM UploadToken t
            WHERE t.token = ? AND t.used_at IS NULL AND t.expires_at > CURRENT_TIMESTAMP
        """, (token,))
        if not row:
            return render_template_string("<h3>Link invalid or expired</h3>"), 410

        file = request.files.get("file")
        if not file or not file.filename:
            return render_template_string("<h3>File required</h3>"), 400

        fname = secure_filename(file.filename)
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        saved = f"exec_p{row['poll_id']}_o{row['option_id']}_u{row['user_id']}_{stamp}__{fname}"
        save_path = os.path.join(UPLOAD_DIR, saved)
        file.save(save_path)

        screenshot_url = f"uploads/{saved}"  # served by /ui/polls/uploads/<filename>

        conn = get_db()
        try:
            # Upsert per-user execution proof
            conn.execute("""
                INSERT INTO OptionExecution(poll_id, option_id, user_id, screenshot_url, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(poll_id, option_id, user_id)
                DO UPDATE SET screenshot_url=excluded.screenshot_url,
                            created_at=CURRENT_TIMESTAMP
            """, (row["poll_id"], row["option_id"], row["user_id"], screenshot_url))

            # Mark token used
            conn.execute("UPDATE UploadToken SET used_at=CURRENT_TIMESTAMP WHERE token=?", (token,))
            conn.commit()
        finally:
            pass

        return render_template_string("""
        <html><body>
            <h3>✅ Uploaded successfully</h3>
            <p>Thanks! Your execution screenshot is recorded.</p>
        </body></html>
        """)


    return app


# -----------------------------
# Trade delivery thread
# -----------------------------
def _get_option_trade_screenshot(poll_id: int, option_id: int) -> str | None:
    conn = sqlite3.connect(DATABASE, check_same_thread=False)
    conn.row_factory = dict_factory
    try:
        row = conn.execute(
            "SELECT screenshot_url FROM PollOptionTrade WHERE poll_id=? AND option_id=?",
            (poll_id, option_id)
        ).fetchone()
        return row["screenshot_url"] if row and row.get("screenshot_url") else None
    finally:
        conn.close()
def trade_delivery_thread():
    CHECK_INTERVAL_SEC = 15
    TTL = timedelta(days=5)
    while True:
        now = dt.datetime.utcnow()
        to_remove_idx, to_send = [], []

        with PENDING_LOCK:
            for idx, item in enumerate(PENDING_DELIVERY):
                added_at = dt.datetime.utcfromtimestamp(item["added_at"])
                if now - added_at > TTL:
                    # Drop stale >5 days
                    to_remove_idx.append(idx)
                    continue

                # 1) Fetch ADMIN trade screenshot to send
                conn = sqlite3.connect(DATABASE, check_same_thread=False)
                conn.row_factory = dict_factory
                try:
                    row = conn.execute("""
                        SELECT
                            t.screenshot_url,
                            p.question,
                            o.option_text
                        FROM PollOptionTrade t
                        JOIN Poll p ON p.poll_id = t.poll_id
                        JOIN PollOption o
                          ON o.poll_id = t.poll_id
                         AND o.option_id = t.option_id
                        WHERE t.poll_id = ? AND t.option_id = ?
                    """, (item["poll_id"], item["option_id"])).fetchone()
                finally:
                    conn.close()

                if not (row and row.get("screenshot_url")):
                    # Nothing to send yet for this option
                    continue

                fname = os.path.basename(row["screenshot_url"])
                local_path = os.path.join(UPLOAD_DIR, fname)
                question  = row.get("question") or "Poll"
                opt_text  = row.get("option_text") or "Selected option"

                caption = (
                    f"🗳️ {question}\n"
                    f"➡️ Selected: {opt_text}\n\n"
                    f"📸 Trade screenshot attached"
                )

                # 2) Mint a unique upload token for THIS user + poll + option
                #    Expires in 2 days
                token = secrets.token_urlsafe(24)
                expires_at = (now + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")

                # Get latest username for context (optional)
                conn = sqlite3.connect(DATABASE, check_same_thread=False)
                conn.row_factory = dict_factory
                try:
                    urow = conn.execute("""
                        SELECT username
                        FROM PollResponse
                        WHERE poll_id=? AND option_id=? AND user_id=?
                        ORDER BY responded_at DESC
                        LIMIT 1
                    """, (item["poll_id"], item["option_id"], item["user_id"])).fetchone()

                    conn.execute("""
                        INSERT INTO UploadToken(token, poll_id, option_id, user_id, username, expires_at, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        token, item["poll_id"], item["option_id"], item["user_id"],
                        (urow["username"] if urow and urow.get("username") else None),
                        expires_at
                    ))
                    conn.commit()
                finally:
                    conn.close()

                button_url = f"{PUBLIC_BASE_URL}/ui/upload/{token}"

                # 3) Queue the DM with photo + inline button
                to_send.append((item["user_id"], local_path, caption, button_url))
                to_remove_idx.append(idx)

            # clean out delivered/stale items
            for idx in reversed(to_remove_idx):
                PENDING_DELIVERY.pop(idx)

        # enqueue outside lock
        for user_id, local_path, caption, button_url in to_send:
            SEND_QUEUE.put((user_id, local_path, caption, button_url))

        time.sleep(CHECK_INTERVAL_SEC)



# -----------------------------
# Bot helpers
# -----------------------------
def get_poll(poll_id: int) -> dict:
    conn = sqlite3.connect(DATABASE, check_same_thread=False)
    conn.row_factory = dict_factory
    cur = conn.cursor()
    cur.execute("SELECT poll_id, question, poll_type FROM Poll WHERE poll_id=?", (poll_id,))
    poll = cur.fetchone()
    if not poll:
        conn.close()
        raise ValueError("poll not found")
    cur.execute("SELECT option_id, option_text FROM PollOption WHERE poll_id=? ORDER BY option_id", (poll_id,))
    poll["options"] = cur.fetchall()
    conn.close()
    return poll

def build_keyboard(poll: dict) -> InlineKeyboardMarkup:
    buttons = []
    for opt in poll["options"]:
        buttons.append([InlineKeyboardButton(opt["option_text"], callback_data=f"vote|{poll['poll_id']}|{opt['option_id']}")])
    return InlineKeyboardMarkup(buttons)

async def post_poll_to_group(app: Application, poll_id: int):
    poll = get_poll(poll_id)
    kb = build_keyboard(poll)
    await app.bot.send_message(chat_id=GROUP_CHAT_ID, text=f"🗳️ {poll['question']}", reply_markup=kb)


# -----------------------------
# Bot handlers
# -----------------------------
from html import escape
from telegram.error import Forbidden

# async def vote_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     q = update.callback_query
#     await q.answer()
#     try:
#         _, poll_id_s, option_id_s = (q.data or "").split("|", 2)
#         poll_id, option_id = int(poll_id_s), int(option_id_s)
#     except Exception:
#         return

#     user = q.from_user
#     username = user.username or user.full_name or str(user.id)

#     # Upsert vote (confirmed=0 until DM confirm)
#     conn = sqlite3.connect(DATABASE, check_same_thread=False)
#     conn.row_factory = dict_factory
#     try:
#         conn.execute("""
#             INSERT INTO PollResponse(poll_id, option_id, user_id, username, confirmed)
#             VALUES (?, ?, ?, ?, 0)
#             ON CONFLICT(poll_id, user_id, option_id)
#             DO UPDATE SET responded_at=CURRENT_TIMESTAMP, confirmed=0
#         """, (poll_id, option_id, user.id, username))

#         # enforce single-select semantics
#         row = conn.execute("SELECT poll_type FROM Poll WHERE poll_id=?", (poll_id,)).fetchone()
#         if not row:
#             row = conn.execute("SELECT poll_type FROM Poll WHERE poll_id=?", (poll_id,)).fetchone()  # fallback if .fetch_one() not available
#         if row and row["poll_type"] == "single":
#             conn.execute(
#                 "DELETE FROM PollResponse WHERE poll_id=? AND user_id=? AND option_id<>?",
#                 (poll_id, user.id, option_id)
#             )

#         # fetch poll question + option text for the DM
#         meta = conn.execute("""
#             SELECT p.question, p.poll_type, o.option_text
#             FROM Poll p
#             JOIN PollOption o ON o.poll_id = p.poll_id
#             WHERE p.poll_id = ? AND o.option_id = ?
#         """, (poll_id, option_id)).fetchone()

#         conn.commit()
#     finally:
#         conn.close()

#     # Compose confirmation message
#     question = meta["question"] if meta and meta.get("question") else "Poll"
#     opt_text = meta["option_text"] if meta and meta.get("option_text") else "Your selection"

#     msg = (
#         f"🗳️ <b>{escape(question)}</b>\n"
#         f"➡️ Option: <b>{escape(opt_text)}</b>\n\n"
#         f"Do you confirm?"
#     )

#     kb = InlineKeyboardMarkup([
#         [InlineKeyboardButton("✅ Confirm", callback_data=f"confirm|{poll_id}|{option_id}|1")],
#         [InlineKeyboardButton("❌ Cancel",  callback_data=f"confirm|{poll_id}|{option_id}|0")]
#     ])

#     # Send DM; if user hasn't /start-ed the bot, nudge them in the group
#     try:
#         await context.bot.send_message(
#             chat_id=user.id,
#             text=msg,
#             parse_mode="HTML",
#             reply_markup=kb
#         )
#     except Forbidden:
#         # User has not opened a private chat with the bot
#         if q.message:
#             await q.message.reply_text(
#                 f"@{user.username or user.full_name}, please open a private chat with me and press Start, "
#                 f"then tap the button again so I can DM your confirmation."
#             )
#         return

from html import escape
from telegram.error import Forbidden
import sqlite3

async def vote_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        _, poll_id_s, option_id_s = (q.data or "").split("|", 2)
        poll_id, option_id = int(poll_id_s), int(option_id_s)
    except Exception:
        return

    user = q.from_user
    username = user.username or user.full_name or str(user.id)

    conn = sqlite3.connect(DATABASE, check_same_thread=False)
    conn.row_factory = dict_factory
    try:
        # Fetch meta for success/error messages
        meta = conn.execute("""
            SELECT p.question, p.poll_type, o.option_text
            FROM Poll p
            JOIN PollOption o ON o.poll_id = p.poll_id
            WHERE p.poll_id = ? AND o.option_id = ?
        """, (poll_id, option_id)).fetchone() or {}

        # ---- INSERT-ONLY: no upsert, no ON CONFLICT clause ----
        try:
            conn.execute("""
                INSERT INTO PollResponse (poll_id, option_id, user_id, username, confirmed)
                VALUES (?, ?, ?, ?, 0)
            """, (poll_id, option_id, user.id, username))
            # Enforce single-select semantics (app-level) AFTER a *successful* insert
            row = conn.execute("SELECT poll_type FROM Poll WHERE poll_id=?", (poll_id,)).fetchone()
            if row and row["poll_type"] == "single":
                conn.execute(
                    "DELETE FROM PollResponse WHERE poll_id=? AND user_id=? AND option_id<>?",
                    (poll_id, user.id, option_id)
                )
            conn.commit()

        except sqlite3.IntegrityError:
            # Duplicate click on the SAME (poll_id, option_id, user_id)
            existing = conn.execute("""
                SELECT responded_at, confirmed
                FROM PollResponse
                WHERE poll_id=? AND option_id=? AND user_id=?
            """, (poll_id, option_id, user.id)).fetchone() or {}
            question = meta.get("question") or "Poll"
            opt_text = meta.get("option_text") or "your selection"
            when = existing.get("responded_at") or "previously"
            conf = existing.get("confirmed")
            conf_str = "Yes" if conf == 1 else "No"
            err_text = (
                f"❌ You already selected this option.\n"
                f"🗳️ {escape(question)}\n"
                f"➡️ Option: {escape(opt_text)}\n"
                f"⏱️ First recorded: {escape(str(when))}\n"
                f"✅ Confirmed: {conf_str}\n\n"
                f"Tip: To change your choice, pick a different option."
            )
            try:
                await context.bot.send_message(chat_id=user.id, text=err_text, parse_mode="HTML")
            except Forbidden:
                if q.message:
                    await q.message.reply_text(
                        f"@{username} please DM me (press Start) and retry.\n" + err_text,
                        disable_web_page_preview=True
                    )
            return  # stop flow on error

        except Exception as e:
            question = meta.get("question") or "Poll"
            opt_text = meta.get("option_text") or "your selection"
            err_text = (
                f"❌ Unexpected error while recording your vote.\n"
                f"🗳️ {escape(question)}\n"
                f"➡️ Option: {escape(opt_text)}\n"
                f"Reason: {escape(str(e))}"
            )
            try:
                await context.bot.send_message(chat_id=user.id, text=err_text, parse_mode="HTML")
            except Forbidden:
                if q.message:
                    await q.message.reply_text(
                        f"@{username} please DM me (press Start) and retry.\n" + err_text,
                        disable_web_page_preview=True
                    )
            return

        # Success → normal confirm DM
        question = meta.get("question") or "Poll"
        opt_text = meta.get("option_text") or "Your selection"
        msg = (
            f"🗳️ <b>{escape(question)}</b>\n"
            f"➡️ Option: <b>{escape(opt_text)}</b>\n\n"
            f"Do you confirm?"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data=f"confirm|{poll_id}|{option_id}|1")],
            [InlineKeyboardButton("❌ Cancel",  callback_data=f"confirm|{poll_id}|{option_id}|0")]
        ])
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=msg,
                parse_mode="HTML",
                reply_markup=kb
            )
        except Forbidden:
            if q.message:
                await q.message.reply_text(
                    f"@{username}, please open a private chat with me and press Start, then tap again."
                )

    finally:
        conn.close()

async def confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        _, poll_id_s, option_id_s, yn = (q.data or "").split("|", 3)
        poll_id, option_id, confirmed = int(poll_id_s), int(option_id_s), 1 if yn == "1" else 0
    except Exception:
        return

    conn = sqlite3.connect(DATABASE, check_same_thread=False)
    conn.row_factory = dict_factory
    try:
        conn.execute("UPDATE PollResponse SET confirmed=? WHERE poll_id=? AND option_id=? AND user_id=?",
                     (confirmed, poll_id, option_id, q.from_user.id))
        conn.commit()
    finally:
        conn.close()

    if confirmed == 1:
        with PENDING_LOCK:
            key = (poll_id, option_id, q.from_user.id)
            exists = any((i["poll_id"], i["option_id"], i["user_id"]) == key for i in PENDING_DELIVERY)
            if not exists:
                PENDING_DELIVERY.append({
                    "poll_id": poll_id,
                    "option_id": option_id,
                    "user_id": q.from_user.id,
                    "added_at": time.time()
                })
    await q.edit_message_text("✅ Confirmed" if confirmed else "❌ Cancelled")


# -----------------------------
# Queue drainers
# -----------------------------
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # already imported above

async def drain_send_queue(app: Application):
    while True:
        try:
            user_id, path_or_url, caption, button_url = SEND_QUEUE.get_nowait()
        except queue.Empty:
            break
        try:
            reply_markup = None
            if button_url:
                reply_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📤 Upload my execution", url=button_url)]]
                )

            if os.path.exists(path_or_url):
                with open(path_or_url, "rb") as f:
                    await app.bot.send_photo(
                        chat_id=user_id, photo=f, caption=caption, reply_markup=reply_markup
                    )
            else:
                await app.bot.send_photo(
                    chat_id=user_id, photo=path_or_url, caption=caption, reply_markup=reply_markup
                )
        finally:
            SEND_QUEUE.task_done()


async def drain_poll_queue(app: Application):
    while True:
        try:
            poll_id = POLL_QUEUE.get_nowait()
        except queue.Empty:
            break
        try:
            await post_poll_to_group(app, poll_id)
        finally:
            POLL_QUEUE.task_done()


# -----------------------------
# Runners
# -----------------------------
def run_flask():
    app = create_app()
    app.run(host="0.0.0.0", port=5000, use_reloader=False, threaded=True)

def main():
    if not TOKEN or GROUP_CHAT_ID == 0:
        raise RuntimeError("Set TOKEN and GROUP_CHAT_ID environment variables")

    threading.Thread(target=run_flask, name="FlaskThread", daemon=True).start()
    threading.Thread(target=trade_delivery_thread, name="TradeDeliveryThread", daemon=True).start()

    tg_app = Application.builder().token(TOKEN).build()
    tg_app.add_handler(CallbackQueryHandler(vote_handler, pattern=r"^vote\|"))
    tg_app.add_handler(CallbackQueryHandler(confirm_handler, pattern=r"^confirm\|"))

    async def on_start(app: Application):
        app.job_queue.run_repeating(lambda ctx: drain_poll_queue(app), interval=0.5, first=0.0)
        app.job_queue.run_repeating(lambda ctx: drain_send_queue(app), interval=1.0, first=0.0)

    tg_app.post_init = on_start

    print("Bot+Flask+TradeDelivery running …")
    tg_app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()