#!/usr/bin/env python3
"""
shift_notifier.py — Telegram-уведомления об открытии/закрытии смен R-Keeper 7.

Запуск:  python shift_notifier.py          (одноразовая проверка)
         python shift_notifier.py --loop    (цикл каждые N минут)

Требует:  pip install pymssql requests python-dotenv

Переменные окружения (.env):
    RK7_HOST, RK7_PORT, RK7_USER, RK7_PASSWORD, RK7_DB
    TG_BOT_TOKEN   — токен бота из @BotFather
    TG_CHAT_ID     — id чата/группы для уведомлений
    POLL_MINUTES    — интервал проверки (по умолчанию 10)
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pymssql
import requests
from dotenv import load_dotenv

# --- Настройки ---
_env_file = Path(__file__).parent / "env"
if _env_file.exists():
    load_dotenv(_env_file)
_env_file2 = Path(__file__).parent / ".env"
if _env_file2.exists():
    load_dotenv(_env_file2)

DB = {
    "server": os.environ.get("RK7_HOST", "saturn.carbis.ru"),
    "port": int(os.environ.get("RK7_PORT", "7473")),
    "user": os.environ.get("RK7_USER", "readonly"),
    "password": os.environ.get("RK7_PASSWORD", ""),
    "database": os.environ.get("RK7_DB", "RK7"),
}
TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
POLL_MIN = int(os.environ.get("POLL_MINUTES", "10"))

STATE_FILE = Path(__file__).parent / ".shift_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("shift_notifier")


# --- БД ---
def query(sql, params=None):
    """Выполнить SQL-запрос к R-Keeper и вернуть список dict."""
    try:
        conn = pymssql.connect(
            server=DB["server"], port=DB["port"],
            user=DB["user"], password=DB["password"],
            database=DB["database"],
            login_timeout=15, timeout=30, charset="UTF-8",
        )
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        log.error("SQL ошибка: %s", e)
        return []


def get_current_shifts():
    """Получить все открытые и недавно закрытые смены (за последние 24ч)."""
    return query("""
        SELECT
            gs.MIDSERVER,
            gs.SHIFTNUM,
            gs.CREATETIME,
            gs.CLOSETIME,
            gs.CLOSED,
            e.NAME   AS MANAGER,
            r.NAME   AS REST_NAME
        FROM GLOBALSHIFTS gs
        LEFT JOIN EMPLOYEES  e ON gs.IMANAGER    = e.SIFR
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT = r.SIFR
        WHERE gs.CREATETIME >= DATEADD(HOUR, -24, GETDATE())
          AND r.NAME IS NOT NULL
        ORDER BY gs.CREATETIME DESC
    """)


def get_shift_revenue(midserver, shiftnum):
    """Оборот и чеки за конкретную смену."""
    rows = query("""
        SELECT
            ISNULL(SUM(o.BASICSUM), 0)  AS REVENUE,
            COUNT(DISTINCT o.VISIT)      AS CHECKS
        FROM ORDERS o
        WHERE o.MIDSERVER = %s
          AND o.ICOMMONSHIFT = %s
          AND o.PAID = 1
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS != -1)
    """, (midserver, shiftnum))
    if rows:
        return float(rows[0]["REVENUE"] or 0), int(rows[0]["CHECKS"] or 0)
    return 0.0, 0


# --- Telegram ---
def tg_send(text):
    """Отправить сообщение в Telegram (Markdown)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        log.warning("TG_BOT_TOKEN или TG_CHAT_ID не заданы — сообщение не отправлено")
        log.info("Сообщение:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }, timeout=15)
        if r.status_code != 200:
            log.error("Telegram API %s: %s", r.status_code, r.text[:200])
        else:
            log.info("✓ Telegram: отправлено")
    except Exception as e:
        log.error("Telegram ошибка: %s", e)


# --- State ---
def load_state():
    """Загрузить предыдущее состояние смен."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text("utf-8"))
        except Exception:
            pass
    return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, default=str), "utf-8")


def shift_key(row):
    return f"{row['MIDSERVER']}_{row['SHIFTNUM']}"


# --- Основная логика ---
def check_shifts():
    """Проверить смены и отправить уведомления."""
    shifts = get_current_shifts()
    if not shifts:
        log.info("Нет смен за последние 24ч")
        return

    prev = load_state()
    current = {}

    for s in shifts:
        key = shift_key(s)
        closed = bool(s["CLOSED"])
        current[key] = {
            "closed": closed,
            "rest": s["REST_NAME"] or "?",
            "manager": s["MANAGER"] or "—",
            "create": str(s["CREATETIME"]) if s["CREATETIME"] else "",
            "close": str(s["CLOSETIME"]) if s["CLOSETIME"] else "",
            "midserver": s["MIDSERVER"],
            "shiftnum": s["SHIFTNUM"],
        }

        old = prev.get(key)

        # --- Новая смена (открытие) ---
        if old is None and not closed:
            ct = s["CREATETIME"]
            time_str = ct.strftime("%H:%M") if isinstance(ct, datetime) else str(ct)[:16]
            msg = (
                f"🟢 *Смена открыта*\n"
                f"📍 {s['REST_NAME']}\n"
                f"🕐 {time_str}\n"
                f"👤 {s['MANAGER'] or '—'}"
            )
            tg_send(msg)

        # --- Смена закрылась ---
        if old is not None and not old.get("closed") and closed:
            ct = s["CREATETIME"]
            clt = s["CLOSETIME"]
            open_str = ct.strftime("%H:%M") if isinstance(ct, datetime) else str(ct)[:16]
            close_str = clt.strftime("%H:%M") if isinstance(clt, datetime) else str(clt)[:16]

            revenue, checks = get_shift_revenue(s["MIDSERVER"], s["SHIFTNUM"])

            # Длительность
            duration = ""
            if isinstance(ct, datetime) and isinstance(clt, datetime):
                mins = int((clt - ct).total_seconds() / 60)
                hours, m = divmod(mins, 60)
                duration = f"{hours}ч {m}мин"

            msg = (
                f"🔴 *Смена закрыта*\n"
                f"📍 {s['REST_NAME']}\n"
                f"🕐 {open_str} → {close_str}"
            )
            if duration:
                msg += f" ({duration})"
            msg += (
                f"\n👤 {s['MANAGER'] or '—'}\n"
                f"💰 Оборот: *{revenue:,.0f} ₽*\n"
                f"🧾 Чеков: *{checks}*"
            )
            tg_send(msg)

    save_state(current)
    log.info("Проверено %d смен, состояние сохранено", len(current))


# --- Запуск ---
def main():
    log.info("=== shift_notifier запущен (интервал: %d мин) ===", POLL_MIN)

    if not TG_TOKEN:
        log.warning("⚠ TG_BOT_TOKEN не задан! Сообщения будут выводиться только в лог.")
    if not TG_CHAT_ID:
        log.warning("⚠ TG_CHAT_ID не задан!")

    loop = "--loop" in sys.argv

    while True:
        try:
            check_shifts()
        except Exception as e:
            log.exception("Ошибка в check_shifts: %s", e)

        if not loop:
            break
        log.info("Следующая проверка через %d мин...", POLL_MIN)
        time.sleep(POLL_MIN * 60)


if __name__ == "__main__":
    main()
