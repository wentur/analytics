"""
🍽️ R-Keeper Analytics Dashboard + AI Chat
Расширенная версия: рестораны, категории, персонал, касса
"""

# === Auto-setup: config.toml рядом с dashboard.py → .streamlit/config.toml ===
import os, shutil, pathlib

# Load .env file if exists (supports both ".env" and "env")
_script_dir = os.path.dirname(os.path.abspath(__file__))
_env_candidates = [os.path.join(_script_dir, ".env"), os.path.join(_script_dir, "env")]
_env_file = next((p for p in _env_candidates if os.path.exists(p)), None)
if _env_file:
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except ImportError:
        with open(_env_file, encoding="utf-8-sig") as _ef:
            for _line in _ef:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _v = _line.split("=", 1)
                    _v = _v.strip().strip("'\"")
                    os.environ[_k.strip()] = _v
_script_dir = pathlib.Path(__file__).parent
_config_src = _script_dir / "config.toml"
_config_dst = _script_dir / ".streamlit" / "config.toml"
if _config_src.exists() and (not _config_dst.exists() or _config_src.stat().st_mtime > _config_dst.stat().st_mtime):
    _config_dst.parent.mkdir(exist_ok=True)
    shutil.copy2(_config_src, _config_dst)

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymssql
import requests
import re
import time
import hashlib
import pickle
import sqlite3
import json as _json

# ============================================================
# НАСТРОЙКИ
# ============================================================
DB_CONFIG = {
    "server": os.environ.get("RK7_HOST", "saturn.carbis.ru"),
    "port": os.environ.get("RK7_PORT", "7473"),
    "user": os.environ.get("RK7_USER", "readonly"),
    "password": os.environ.get("RK7_PASSWORD", "ai3nPG7rwtrJRw"),
    "database": os.environ.get("RK7_DB", "RK7"),
    "login_timeout": 15, "timeout": 60,
}
SH_API = {
    "url": os.environ.get("SH_API_URL", "http://saturn.carbis.ru:7477/api"),
    "user": os.environ.get("SH_API_USER", "readonly"),
    "password": os.environ.get("SH_API_PASSWORD", "60iNr1uy"),
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyAsQX1_48p-h_jR5DJrZ-jiylkalila2Lg")
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"

# ============================================================
# АВТОРИЗАЦИЯ
# ============================================================
# Пароли хранятся как SHA-256 хэши. Для добавления пользователя:
#   python3 -c "import hashlib; print(hashlib.sha256('пароль'.encode()).hexdigest())"
# Для миграции на nginx/OAuth — заменить check_auth() на проверку заголовка X-Forwarded-User

AUTH_USERS = {
    "admin": {
        "hash": "390e47d3acfa1c528bbb0682006383c58564ad76e5743d6a4ce690528835645d",
        "role": "admin",
        "name": "Администратор",
        "token": "1a892218533b9581",
    },
    "alisa": {
        "hash": "c2a57c444577c722c3ee30b846c24a250402cfb2d61bd36ff60974e0cfd68c10",
        "role": "user",
        "name": "Алиса",
        "token": "40b977cc927df9c9",
    },
    "demo": {
        "hash": "2a97516c354b68848cdbd8f54a226a0a55b21ed138e207ad6c5cbb9c00aa5aea",
        "role": "admin",
        "name": "Демо",
        "token": "demo_token_2026",
    },
}

# Обратный маппинг token → user для быстрого поиска
_TOKEN_MAP = {u["token"]: {"username": k, "role": u["role"], "name": u["name"]} for k, u in AUTH_USERS.items()}

def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def _verify_user(username: str, password: str) -> dict | None:
    """Проверить логин/пароль. Вернуть dict пользователя или None."""
    user = AUTH_USERS.get(username.strip().lower())
    if user and user["hash"] == _hash_password(password):
        return {"username": username.strip().lower(), "role": user["role"], "name": user["name"]}
    return None

def check_auth() -> dict | None:
    """Главная точка входа авторизации.
    1. Сессия (уже залогинен)
    2. URL-токен (?token=xxx) — автовход по закладке
    При миграции на nginx: return {"username": headers["X-Forwarded-User"], ...}
    """
    # Уже залогинен
    if st.session_state.get("_auth_user"):
        return st.session_state["_auth_user"]

    # Автовход по URL-токену: ?token=1a892218533b9581
    token = st.query_params.get("token", "")
    if token and token in _TOKEN_MAP:
        user = _TOKEN_MAP[token]
        st.session_state["_auth_user"] = user
        return user

    return None

# ============================================================
# НАСТРОЙКИ ПОЛЬЗОВАТЕЛЕЙ (SQLite)
# ============================================================
_SETTINGS_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "goai_settings.db")

def _init_settings_db():
    """Создать таблицу настроек если не существует."""
    conn = sqlite3.connect(_SETTINGS_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
        username TEXT NOT NULL, key TEXT NOT NULL, value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (username, key))""")
    conn.commit()
    conn.close()

def save_user_setting(username, key, value):
    """Сохранить настройку пользователя."""
    try:
        _init_settings_db()
        conn = sqlite3.connect(_SETTINGS_DB)
        conn.execute("""INSERT OR REPLACE INTO user_settings (username, key, value, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)""", (username, key, _json.dumps(value, ensure_ascii=False)))
        conn.commit()
        conn.close()
    except: pass

def load_user_setting(username, key, default=None):
    """Загрузить настройку пользователя."""
    try:
        _init_settings_db()
        conn = sqlite3.connect(_SETTINGS_DB)
        row = conn.execute("SELECT value FROM user_settings WHERE username=? AND key=?", (username, key)).fetchone()
        conn.close()
        if row: return _json.loads(row[0])
    except: pass
    return default

def load_all_user_settings(username):
    """Загрузить все настройки пользователя."""
    try:
        _init_settings_db()
        conn = sqlite3.connect(_SETTINGS_DB)
        rows = conn.execute("SELECT key, value, updated_at FROM user_settings WHERE username=? ORDER BY key", (username,)).fetchall()
        conn.close()
        return {r[0]: _json.loads(r[1]) for r in rows}
    except: pass
    return {}

def delete_user_setting(username, key):
    """Удалить настройку пользователя."""
    try:
        conn = sqlite3.connect(_SETTINGS_DB)
        conn.execute("DELETE FROM user_settings WHERE username=? AND key=?", (username, key))
        conn.commit()
        conn.close()
    except: pass

# ============================================================
# МУЛЬТИЯЗЫЧНОСТЬ / MULTILANGUAGE
# ============================================================
_TRANSLATIONS = {
    "ru": {
        # Login
        "login_title": "Вход", "login_placeholder": "Введите логин", "pw_placeholder": "Введите пароль",
        "login_btn": "Войти", "login_error_empty": "Введите логин и пароль", "login_error_wrong": "Неверный логин или пароль",
        "restaurant_intelligence": "Интеллект ресторана",
        # Period selector
        "today": "Сегодня", "yesterday": "Вчера", "7_days": "7 дней", "30_days": "30 дней",
        "90_days": "90 дней", "custom": "Произвольный", "day_back": "День назад", "day_fwd": "День вперёд",
        "refresh": "Обновить данные",
        # Nav groups
        "nav_sales": "Продажи", "nav_staff": "Точки и персонал", "nav_cash": "Касса и чеки",
        "nav_warehouse": "Склад и закупки", "nav_finance": "Финансы", "nav_ai": "ИИ",
        # Pages
        "p_pulse": "Пульс", "p_revenue": "Выручка", "p_seasonality": "Сезонность", "p_dishes": "Блюда",
        "p_categories": "Категории", "p_prices": "Цены", "p_abc": "ABC",
        "p_restaurants": "Рестораны", "p_staff": "Персонал", "p_shifts": "Смены", "p_speed": "Скорость",
        "p_cash": "Касса", "p_orders": "Заказы", "p_problems": "Проблемы", "p_voids": "Удаление",
        "p_warehouse": "Склад", "p_invoices": "Накладные", "p_foodcost": "Фудкост",
        "p_foodcost_calc": "Фудкост (расчёт)", "p_wh_schema": "Склад: Схема",
        "p_income": "Доход/Расход", "p_ai_chat": "ИИ-чат", "p_proactive": "Проактив",
        "p_account": "Личный кабинет",
        # Metrics
        "revenue": "Выручка", "orders": "Заказов", "checks": "Чеков", "avg_check": "Ср. чек",
        "guests": "Гостей", "dishes": "Блюд", "margin": "Маржа", "net_income": "Чистый доход",
        "foodcost": "Фудкост", "cost": "Себестоимость", "norm": "норма", "above": "выше", "below": "ниже",
        "total": "Итого", "per_day": "Ср. в день", "income_revenue": "Доход (выручка)",
        "expenses_total": "Расходы (всего)", "vs_yesterday": "vs вчера", "vs_prev": "vs пред.",
        # Payment types
        "cash": "Наличные", "card": "Банковская карта", "bank_transfer": "Безнал (счёт)",
        "staff_meal": "Питание сотр.", "bonus": "Бонусы", "payment_types": "Типы оплат",
        "operations": "операций", "pay_type": "Тип оплаты", "amount": "Сумма ₽", "share": "Доля %",
        # Staff meals
        "staff_meals": "Питание сотрудников", "amount_menu": "Сумма (по меню)",
        "cost_exact": "точный расчёт", "cost_estimate": "оценка ~33%",
        "calc_foodcost_hint": "Рассчитайте фудкост на странице «Фудкост (расчёт)» для точной себестоимости питания",
        "transactions": "Транзакций", "pct_revenue": "% от выручки",
        # Taxes
        "taxes": "Налоги", "tax_rate": "Ставка НДС",
        # Foodcost calc
        "calculate_foodcost": "Рассчитать фудкост", "loading_recipes": "Загружаю себестоимость из рецептур...",
        "loading_purchases": "Загружаю закупочные цены из накладных SH...", "load_more": "Загрузить больше",
        "recipes_from_sh": "Себестоимость из рецептур", "purchase_prices": "Закупочные цены из накладных SH",
        "items_no_price": "товаров без закупочной цены", "no_data": "Нет данных",
        # General
        "period": "Период", "from": "С", "to": "По", "details": "Подробнее",
        "download_csv": "Скачать CSV", "no_data_period": "Нет данных за период",
        "by_hours": "По часам", "by_days": "По дням", "per_guest": "На гостя",
        # Sidebar buttons
        "btn_theme": "Тема", "btn_account": "Кабинет", "btn_logout": "Выйти",
        # Пульс
        "today_metric": "Сегодня", "purchases_not_loaded": "Данные о закупках не загружены — маржа и доход завышены.",
        "load_30d": "Загрузить за 30 дней", "no_invoices_30d": "Нет накладных за последние 30 дней",
        "margin_formula": "Маржа = Выручка − Себестоимость. Доход = Маржа − НДС − Постоянные расходы.",
        "taxes_nds": "Налоги (НДС)", "cost_label": "Себестоимость", "income": "Доход",
        "shifts_open": "Смены открыты", "fill_fixed_costs": "Для точного расчёта дохода заполните постоянные расходы в Личном кабинете →",
        "cost_from_recipes": "Себестоимость из рецептур", "open_foodcost_calc": "Для расчёта себестоимости откройте «Фудкост (расчёт)» →",
        "purchases_sh": "Закупки (SH)", "discounts": "Скидки", "staff_cost_label": "Питание сотр. (себест.)",
        "details_label": "Детали", "conn_error": "Ошибка подключения", "query_error": "Ошибка запроса",
        # Выручка extra
        "by_restaurants": "По ресторанам", "by_dishes": "По товарам", "avg_check_guest": "Ср. чек/гость",
        "guests_per_order": "Гостей/заказ", "dishes_per_order": "Блюд/заказ",
        "no_payment_data": "Нет данных по оплатам", "no_details": "Нет детализации",
        # Сезонность
        "monthly_comparison": "Помесячное сравнение выручки", "no_revenue_data": "Нет данных по выручке",
        "no_prev_year_data": "Нет данных за предыдущие годы", "difference": "Разница", "months": "Месяцев",
        "revenue_by_months": "Выручка по месяцам", "change_pct": "Изменение, %",
        # Блюда
        "by_revenue": "По выручке", "top_dishes": "Топ блюд",
        # Категории
        "revenue_by_categories": "Выручка по категориям", "category_share": "Доля категорий",
        "sold": "Продано", "no_category_data": "Нет данных по категориям",
        # Рестораны
        "locations": "Точек", "revenue_by_locations": "Выручка по точкам", "avg_check_by_loc": "Средний чек по точкам",
        "revenue_share": "Доля выручки", "revenue_dynamics": "Динамика выручки (топ-5 точек)",
        "no_restaurant_data": "Нет данных по столовым",
        "locations_note": "Точек — сколько столовых передали данные за период.",
        # Персонал
        "worked": "Работало", "best": "Лучший", "max_revenue": "Макс. выручка",
        "cashier_revenue": "Выручка кассиров", "work_time": "Рабочее время",
        # Проактив
        "comparison": "Сравнение", "revenue_grew": "Выручка выросла", "revenue_fell": "Выручка упала",
        "orders_grew": "Кол-во заказов выросло", "orders_fell": "Кол-во заказов упало",
        "avg_check_grew": "Средний чек вырос", "avg_check_fell": "Средний чек упал",
        "revenue_comparison": "Сравнение выручки", "insufficient_data": "Недостаточно данных",
        "get_ai_recommendations": "Получить рекомендации ИИ", "clear_chat": "Очистить чат",
        # ABC
        "abc_analysis": "ABC-анализ", "abc_desc": "A = 80% выручки (звёзды), B = 15% (серднячки), C = 5% (кандидаты на вылет)",
        "group": "Группа", "dishes_count": "Блюд", "stars": "звёзды", "average": "серднячки", "candidates": "на вылет",
        "abc_revenue_share": "Доля выручки по группам ABC",
        # Фудкост
        "summary_period": "Сводка за период", "with_recipes": "Товары с рецептурами (переработка, акты нарезки)",
        "without_processing": "Товары без переработки (купили → продали, себестоимость = закупочная цена)",
        "from_invoices": "из накладных SH", "total_all_products": "Итого по всем товарам",
        "total_revenue": "Общая выручка", "total_cost_label": "Общая себестоимость",
        "total_margin": "Общая маржа", "total_foodcost": "Общий фудкост",
        "coverage": "Покрыто себестоимостью", "not_matched": "Не сопоставлено",
        "matched": "Сопоставлено", "rk_coverage": "Покрытие RK",
        "select_period_info": "Выберите период и нажмите «Рассчитать» — будут загружены рецептуры и закупочные цены из SH.",
        # Доход/Расход
        "fixed_costs_monthly": "Постоянные расходы (в месяц)", "fixed_costs_desc": "Введите средние ежемесячные расходы — они используются для расчёта чистого дохода.",
        "fixed_costs_settings": "Настройки постоянных расходов",
        "staff_monthly": "Персонал ₽/мес", "rent_monthly": "Аренда ₽/мес", "utilities_monthly": "Комм.услуги ₽/мес",
        "marketing_monthly": "Маркетинг ₽/мес", "other_monthly": "Прочие ₽/мес",
        "expense_structure": "Структура расходов", "revenue_vs_fixed": "Выручка по дням vs постоянные расходы",
        "fixed_expenses_line": "Пост. расходы",
        # Касса
        "deposits": "Внесений", "withdrawals": "Изъятий",
        "cash_ops_note": "Внесения = наличные в кассу. Изъятия = инкассация.",
        "deposits_withdrawals": "Внесения и изъятия", "by_op_type": "По типам операций",
        "no_cash_ops": "Нет кассовых операций",
        # Цены
        "dishes_price_changed": "Блюд с изменённой ценой", "max_diff": "Макс. разница", "avg_diff": "Ср. разница",
        "price_growth": "Наибольший рост цены", "price_range": "Диапазон цен",
        "prices_note": "Показаны только блюда, у которых цена менялась за период",
        # Скорость
        "avg_time": "Среднее время", "fastest": "Самый быстрый", "cashiers_count": "Кассиров",
        # Смены
        "total_shifts": "Смен за период", "open_now": "Открыто", "avg_duration": "Ср. длительность",
        "shifts_total": "Смен всего",
        "late_note": "Опоздание = начало смены позже запланированного времени.",
        "no_work_time": "Нет данных по рабочему времени",
        # Проблемы / проверки
        "total_checks": "Всего проверок", "successful": "Успешных", "errors": "Ошибок", "timeouts": "Таймаутов",
        "total_transactions": "Всего транзакций",
        # Склад
        "forecast_insufficient": "Недостаточно данных для прогноза (нужно минимум 2 недели)",
        # Питание кассиры
        "cashier_revenue_note": "Выручка каждого кассира = сумма оплаченных заказов.",
        "top_cashiers": "Топ кассиров по выручке",
        # Общие метрики
        "from_sh_prices": "из цен SH",
        "staff_cost_puls": "Питание сотр. (себест.)",
        "canceled": "Отменённых",
        "restaurants_with_issues": "Столовых с проблемами",
        "total_problem": "Всего проблемных",
        "total_checks_count": "Всего чеков",
        "not_fiscal": "Не фискализировано",
        "amount_label": "Сумма",
        "total_voids": "Всего отказов",
        "avg_void": "Ср. сумма отказа",
        "canceled_checks": "Отменённых чеков",
        "cancel_ops": "Операций отмен",
        "total_payments": "Всего оплат",
        "total_amount": "Общая сумма",
        "card_share": "Доля карт",
        "products": "Товаров",
        "invoices_count": "Накладных",
        "divisions": "Подразделений",
        "warehouses": "Складов",
        "categories_count": "Категорий",
        "stock_value": "Стоимость",
        "positions": "Позиций",
        "quantity": "Кол-во",
        "no_data_generic": "нет данных",
    },
    "en": {
        # Login
        "login_title": "Sign in", "login_placeholder": "Enter login", "pw_placeholder": "Enter password",
        "login_btn": "Sign in", "login_error_empty": "Enter login and password", "login_error_wrong": "Wrong login or password",
        "restaurant_intelligence": "Restaurant Intelligence",
        # Period selector
        "today": "Today", "yesterday": "Yesterday", "7_days": "7 days", "30_days": "30 days",
        "90_days": "90 days", "custom": "Custom", "day_back": "Day back", "day_fwd": "Day forward",
        "refresh": "Refresh data",
        # Nav groups
        "nav_sales": "Sales", "nav_staff": "Locations & Staff", "nav_cash": "Cash & Checks",
        "nav_warehouse": "Warehouse", "nav_finance": "Finance", "nav_ai": "AI",
        # Pages
        "p_pulse": "Pulse", "p_revenue": "Revenue", "p_seasonality": "Seasonality", "p_dishes": "Dishes",
        "p_categories": "Categories", "p_prices": "Prices", "p_abc": "ABC",
        "p_restaurants": "Restaurants", "p_staff": "Staff", "p_shifts": "Shifts", "p_speed": "Speed",
        "p_cash": "Cash", "p_orders": "Orders", "p_problems": "Issues", "p_voids": "Voids",
        "p_warehouse": "Warehouse", "p_invoices": "Invoices", "p_foodcost": "Food Cost",
        "p_foodcost_calc": "Food Cost (calc)", "p_wh_schema": "WH: Schema",
        "p_income": "Income/Expense", "p_ai_chat": "AI Chat", "p_proactive": "Proactive",
        "p_account": "Account",
        # Metrics
        "revenue": "Revenue", "orders": "Orders", "checks": "Checks", "avg_check": "Avg check",
        "guests": "Guests", "dishes": "Dishes", "margin": "Margin", "net_income": "Net income",
        "foodcost": "Food cost", "cost": "Cost", "norm": "normal", "above": "above", "below": "below",
        "total": "Total", "per_day": "Per day avg", "income_revenue": "Income (revenue)",
        "expenses_total": "Expenses (total)", "vs_yesterday": "vs yesterday", "vs_prev": "vs prev",
        # Payment types
        "cash": "Cash", "card": "Bank card", "bank_transfer": "Bank transfer",
        "staff_meal": "Staff meals", "bonus": "Bonus", "payment_types": "Payment types",
        "operations": "operations", "pay_type": "Payment type", "amount": "Amount", "share": "Share %",
        # Staff meals
        "staff_meals": "Staff meals", "amount_menu": "Amount (menu price)",
        "cost_exact": "exact calculation", "cost_estimate": "estimate ~33%",
        "calc_foodcost_hint": "Calculate food cost on the 'Food Cost (calc)' page for exact cost",
        "transactions": "Transactions", "pct_revenue": "% of revenue",
        # Taxes
        "taxes": "Taxes", "tax_rate": "Tax rate",
        # Foodcost calc
        "calculate_foodcost": "Calculate food cost", "loading_recipes": "Loading recipes...",
        "loading_purchases": "Loading purchase prices...", "load_more": "Load more",
        "recipes_from_sh": "Cost from recipes", "purchase_prices": "Purchase prices from SH invoices",
        "items_no_price": "items without purchase price", "no_data": "No data",
        # General
        "period": "Period", "from": "From", "to": "To", "details": "Details",
        "download_csv": "Download CSV", "no_data_period": "No data for period",
        "by_hours": "By hours", "by_days": "By days", "per_guest": "Per guest",
        # Sidebar buttons
        "btn_theme": "Theme", "btn_account": "Account", "btn_logout": "Sign out",
        # Pulse
        "today_metric": "Today", "purchases_not_loaded": "Purchase data not loaded — margin and income are overstated.",
        "load_30d": "Load 30 days", "no_invoices_30d": "No invoices for last 30 days",
        "margin_formula": "Margin = Revenue − Cost. Income = Margin − Tax − Fixed costs.",
        "taxes_nds": "Taxes (VAT)", "cost_label": "Cost", "income": "Income",
        "shifts_open": "Shifts open", "fill_fixed_costs": "Fill in fixed costs in Account settings for accurate income →",
        "cost_from_recipes": "Cost from recipes", "open_foodcost_calc": "Open 'Food Cost (calc)' to calculate cost →",
        "purchases_sh": "Purchases (SH)", "discounts": "Discounts", "staff_cost_label": "Staff meals (cost)",
        "details_label": "Details", "conn_error": "Connection error", "query_error": "Query error",
        # Revenue extra
        "by_restaurants": "By restaurants", "by_dishes": "By dishes", "avg_check_guest": "Avg check/guest",
        "guests_per_order": "Guests/order", "dishes_per_order": "Dishes/order",
        "no_payment_data": "No payment data", "no_details": "No details",
        # Seasonality
        "monthly_comparison": "Monthly revenue comparison", "no_revenue_data": "No revenue data",
        "no_prev_year_data": "No data for previous years", "difference": "Difference", "months": "Months",
        "revenue_by_months": "Revenue by months", "change_pct": "Change, %",
        # Dishes
        "by_revenue": "By revenue", "top_dishes": "Top dishes",
        # Categories
        "revenue_by_categories": "Revenue by categories", "category_share": "Category share",
        "sold": "Sold", "no_category_data": "No category data",
        # Restaurants
        "locations": "Locations", "revenue_by_locations": "Revenue by location", "avg_check_by_loc": "Avg check by location",
        "revenue_share": "Revenue share", "revenue_dynamics": "Revenue dynamics (top-5 locations)",
        "no_restaurant_data": "No restaurant data",
        "locations_note": "Locations — how many canteens sent data for the period.",
        # Staff
        "worked": "Worked", "best": "Best", "max_revenue": "Max revenue",
        "cashier_revenue": "Cashier revenue", "work_time": "Work time",
        # Proactive
        "comparison": "Comparison", "revenue_grew": "Revenue grew", "revenue_fell": "Revenue fell",
        "orders_grew": "Orders grew", "orders_fell": "Orders fell",
        "avg_check_grew": "Avg check grew", "avg_check_fell": "Avg check fell",
        "revenue_comparison": "Revenue comparison", "insufficient_data": "Insufficient data",
        "get_ai_recommendations": "Get AI recommendations", "clear_chat": "Clear chat",
        # ABC
        "abc_analysis": "ABC Analysis", "abc_desc": "A = 80% revenue (stars), B = 15% (average), C = 5% (candidates for removal)",
        "group": "Group", "dishes_count": "Dishes", "stars": "stars", "average": "average", "candidates": "for removal",
        "abc_revenue_share": "Revenue share by ABC groups",
        # Foodcost
        "summary_period": "Summary for period", "with_recipes": "Products with recipes (processing, cutting acts)",
        "without_processing": "Products without processing (bought → sold, cost = purchase price)",
        "from_invoices": "from SH invoices", "total_all_products": "Total all products",
        "total_revenue": "Total revenue", "total_cost_label": "Total cost",
        "total_margin": "Total margin", "total_foodcost": "Total food cost",
        "coverage": "Cost coverage", "not_matched": "Not matched",
        "matched": "Matched", "rk_coverage": "RK coverage",
        "select_period_info": "Select period and press 'Calculate' — recipes and purchase prices will be loaded from SH.",
        # Income/Expense
        "fixed_costs_monthly": "Fixed costs (monthly)", "fixed_costs_desc": "Enter average monthly expenses — used to calculate net income.",
        "fixed_costs_settings": "Fixed cost settings",
        "staff_monthly": "Staff ₽/mo", "rent_monthly": "Rent ₽/mo", "utilities_monthly": "Utilities ₽/mo",
        "marketing_monthly": "Marketing ₽/mo", "other_monthly": "Other ₽/mo",
        "expense_structure": "Expense structure", "revenue_vs_fixed": "Daily revenue vs fixed costs",
        "fixed_expenses_line": "Fixed costs",
        # Cash
        "deposits": "Deposits", "withdrawals": "Withdrawals",
        "cash_ops_note": "Deposits = cash to register. Withdrawals = collection.",
        "deposits_withdrawals": "Deposits & withdrawals", "by_op_type": "By operation type",
        "no_cash_ops": "No cash operations",
        # Prices
        "dishes_price_changed": "Dishes with price change", "max_diff": "Max difference", "avg_diff": "Avg difference",
        "price_growth": "Largest price increase", "price_range": "Price range",
        "prices_note": "Only dishes with price changes in the period are shown",
        # Speed
        "avg_time": "Avg time", "fastest": "Fastest", "cashiers_count": "Cashiers",
        # Shifts
        "total_shifts": "Shifts in period", "open_now": "Open now", "avg_duration": "Avg duration",
        "shifts_total": "Total shifts",
        "late_note": "Late = shift started after scheduled time.",
        "no_work_time": "No work time data",
        # Issues / checks
        "total_checks": "Total checks", "successful": "Successful", "errors": "Errors", "timeouts": "Timeouts",
        "total_transactions": "Total transactions",
        # Warehouse
        "forecast_insufficient": "Insufficient data for forecast (need at least 2 weeks)",
        # Cashier revenue
        "cashier_revenue_note": "Cashier revenue = sum of paid orders served by the employee.",
        "top_cashiers": "Top cashiers by revenue",
        # Common metrics
        "from_sh_prices": "from SH prices",
        "staff_cost_puls": "Staff meals (cost)",
        "canceled": "Canceled",
        "restaurants_with_issues": "Locations with issues",
        "total_problem": "Total issues",
        "total_checks_count": "Total checks",
        "not_fiscal": "Not fiscalized",
        "amount_label": "Amount",
        "total_voids": "Total voids",
        "avg_void": "Avg void amount",
        "canceled_checks": "Canceled checks",
        "cancel_ops": "Cancel operations",
        "total_payments": "Total payments",
        "total_amount": "Total amount",
        "card_share": "Card share",
        "products": "Products",
        "invoices_count": "Invoices",
        "divisions": "Divisions",
        "warehouses": "Warehouses",
        "categories_count": "Categories",
        "stock_value": "Stock value",
        "positions": "Positions",
        "quantity": "Qty",
        "no_data_generic": "no data",
    },
}

# Маппинг внутренних имён страниц → ключ перевода
_PAGE_KEY_MAP = {
    "Пульс": "p_pulse", "Выручка": "p_revenue", "Сезонность": "p_seasonality", "Блюда": "p_dishes",
    "Категории": "p_categories", "Цены": "p_prices", "ABC": "p_abc",
    "Рестораны": "p_restaurants", "Персонал": "p_staff", "Смены": "p_shifts", "Скорость": "p_speed",
    "Касса": "p_cash", "Заказы": "p_orders", "Проблемы": "p_problems", "Удаление": "p_voids",
    "Склад": "p_warehouse", "Накладные": "p_invoices", "Фудкост": "p_foodcost",
    "Фудкост (расчёт)": "p_foodcost_calc", "Склад: Схема": "p_wh_schema",
    "Доход/Расход": "p_income", "ИИ-чат": "p_ai_chat", "Проактив": "p_proactive",
    "Личный кабинет": "p_account",
}
_NAV_GROUP_MAP = {
    "Продажи": "nav_sales", "Точки и персонал": "nav_staff", "Касса и чеки": "nav_cash",
    "Склад и закупки": "nav_warehouse", "Финансы": "nav_finance", "ИИ": "nav_ai",
}

def _get_lang():
    return st.session_state.get("_lang", "ru")

def t(key):
    """Получить перевод строки по ключу."""
    lang = _get_lang()
    return _TRANSLATIONS.get(lang, _TRANSLATIONS["ru"]).get(key, _TRANSLATIONS["ru"].get(key, key))

def tp(page_name):
    """Перевести название страницы (внутреннее имя → отображаемое)."""
    key = _PAGE_KEY_MAP.get(page_name)
    return t(key) if key else page_name

def tg(group_name):
    """Перевести название группы навигации."""
    key = _NAV_GROUP_MAP.get(group_name)
    return t(key) if key else group_name

# Список пресетов периода (внутренние ключи → отображение)
def _period_options():
    return [t("today"), t("yesterday"), t("7_days"), t("30_days"), t("90_days"), t("custom")]

def _period_key_to_label(key):
    _map = {"Сегодня": "today", "Вчера": "yesterday", "7 дней": "7_days",
            "30 дней": "30_days", "90 дней": "90_days", "Произвольный": "custom"}
    return t(_map.get(key, "today"))

def _period_label_to_key(label):
    _reverse = {t("today"): "Сегодня", t("yesterday"): "Вчера", t("7_days"): "7 дней",
                t("30_days"): "30 дней", t("90_days"): "90 дней", t("custom"): "Произвольный"}
    return _reverse.get(label, "Сегодня")

def show_login_page():
    """Показать форму входа. Вызывается если check_auth() == None."""
    # Минимальный CSS для страницы входа
    st.markdown("""<style>
        :root { --primary-color: #00ff6a; --background-color: #08080e;
                --secondary-background-color: #0e0e16; --text-color: #ffffff; }
        .stApp { background: #08080e !important; color: #fff !important; }
        .stApp * { color: #fff; }
        #MainMenu, footer, header { visibility: hidden; }
        [data-testid="stForm"] {
            background: #0e0e16;
            border: 1px solid rgba(0,255,106,0.1);
            border-radius: 16px; padding: 36px;
            max-width: 400px; margin: 70px auto;
        }
        [data-testid="stForm"] input { color: #fff !important; background: #12121e !important; }
        [data-testid="stForm"] label { color: #888 !important; }
        /* Login button - nuclear specificity */
        .stApp .stButton > button,
        .stApp .stButton > button[kind],
        .stApp .stButton > button[kind="secondaryFormSubmit"],
        .stApp .stButton > button[kind="primary"],
        .stApp .stButton > button[kind="secondary"],
        .stApp [data-testid="stForm"] .stButton > button,
        .stApp [data-testid="stFormSubmitButton"] > button,
        [data-testid="stFormSubmitButton"] > button,
        .stFormSubmitButton > button {
            background-color: #00ff6a !important; color: #000 !important;
            font-weight: 700 !important; border: none !important;
            border-radius: 10px !important; font-size: 1rem !important;
        }
        .stApp .stButton > button:hover,
        .stApp [data-testid="stFormSubmitButton"] > button:hover,
        [data-testid="stFormSubmitButton"] > button:hover {
            background-color: #00cc55 !important; color: #000 !important;
        }
        /* Invert logo for dark background, blend to hide black bg */
        .login-logo { filter: invert(1); mix-blend-mode: screen; }
        @media (max-width: 768px) {
            [data-testid="stForm"] { padding: 24px 18px; margin: 30px 12px; max-width: 100%; border-radius: 12px; }
            .block-container { padding: 0.5rem 0.5rem !important; }
            section[data-testid="stSidebar"] { display: none !important; }
        }
    </style>""", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align:center; margin-bottom: 10px;">
            <img src="data:image/png;base64,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XIYA1TOSQNUTCfvcCWUZlvPifiXZSs9WcjSWmeRnLIusR5Kr/gQlW/15JP0kORpDqpfkpy2Gcy8IVrcLTS+UYVJF2Y7zJE/toNwb9+h2WaIUpZ/aJ8vdLk+SJcnSQAQKN5x7RQilTAYSNf0/DQHJUxpaOrcVAclSKzr6X1oCkqe0xHR+KwJVlCcZzq1KNMX/qlj4KR5PHp00sHI4V/KUA0RdwhGQLEkQ8iQgecqTpq5VRXmS4ZyT3Fax8NM8eq+MHKRhUuS5kqci6fbWtSVLvVXeRT+t5Klowr11/SrKUz/DWcZRbwmsnlYEREAEREAEREAERCA5gT7DWUZzcmg6UwREQAREQAREQAREoPcIyHDuvTLXE4uACIiACIiACIiACAQQkOEcAE0/EQEREAEREAEREAER6D0CMpx7r8z1xCIgAiIgAiIgAiIgAgEEZDgHQNNPREAEREAEREAEREAEeo/AoFGjRlVxNZDeKyk9sQiIgAiIgAiIgAiIQKkEZDiXil83FwEREAEREAEREAERqAoBGc5VKSnlUwREQAREQAREQAREoFQCMpxLxa+bi4AIiIAIiIAIiIAIVIWADOeqlJTyKQIiIAIiIAIiIAIiUCoBGc6l4tfNRUAEREAEREAEREAEqkJAhnNVSkr5FAEREAEREAEREAERKJWADOdS8evmIiACIiACIiACIiACVSEgw7kqJaV8ioAIiIAIiIAIiIAIlEpAhnOp+HVzERABERABERABERCBqhCQ4VyVklI+RUAEREAEREAEREAESiUgw7lU/Lq5CIiACIiACIiACIhAVQjIcK5KSSmfIiACIiACIiACIiACpRKQ4Vwqft1cBERABERABERABESgKgRkOFelpJRPERABERABERABERCBUgnIcC4Vv24uAiIgAiIgAiIgAiJQFQIynKtSUsqnCIiACIiACIiACIhAqQRkOJeKXzcXAREQAREQAREQARGoCgEZzlUpKeVTBERABERABERABESgVAIynEvFr5uLgAiIgAiIgAiIgAhUhYAM56qUlPIpAiIgAiIgAiIgAiJQKgEZzqXi181FQAREQAREQAREQASqQkCGc1VKSvkUAREQAREQAREQAREolYAM51Lx6+YiIAIiIAIiIAIiIAJVISDDuSolpXyKgAiIgAiIgAiIgAiUSkCGc6n4dXMREAEREAEREAEREIGqEJDhXJWSUj5FQAREQAREQAREQARKJSDDuVT8urkIiIAIiIAIiIAIiEBVCMhwrkpJKZ8iIAIiIAIiIAIiIAKlEpDhXCp+3VwEREAEREAEREAERKAqBGQ4V6WklE8REAEREAEREAEREIFSCchwLhW/bi4CIiACIiACIiACIlAVAjKcq1JSyqcIiIAIiIAIiIAIiECpBGQ4l4pfNxcBERABERABERABEagKARnOVSkp5VMEREAEREAEREAERKBUAjKcS8Wvm4uACIiACIiACIiACFSFgAznqpSU8ikCIiACIiACIiACIlAqARnOpeLXzUVABERABERABERABKpCQIZzVUpK+RQBERABERABERABESiVgAznUvHr5iIgAiIgAiIgAiIgAlUhIMO5KiWlfIqACIiACIiACIiACJRKQIZzqfh1cxEQAREQAREQAREQgaoQkOFclZJSPkVABERABERABERABEolIMO5VPy6uQiIgAiIgAiIgAiIQFUIyHCuSkkpnyIgAiIgAiIgAiIgAqUSGDRy1KhvyMGgUrOhm4uACIiACIiACIiACIhAZxOQ4dzZ5aPciYAIiIAIiIAIiIAIdAgBGc4dUhDKhgiIgAiIgAiIgAiIQGcTkOHc2eWj3ImACIiACIiACIiACHQIARnOHVIQyoYIiIAIiIAIiIAIiEBnE+gznMmmJggOXFjffPNN9NVXX0Vffvml248ePTpi47uvv/464v9f28b+m9rnvu/s86BBg6Lxxhuvb8/x+OOPHw2y/Xi1//Gd3yaYYIKIbfDgwQNnrovO+OKLL/q4On7GDr59jGtsm30HClh73p6n/+z39d9TFp45/1MSAREQARFoTYA27ytrB2kX0d2+ffT62rWHvl2s2/sro299O+j2tXZwfNujl8enLWRf09F8ls3i6WnfTgL9DOciblw1wfZGWnwfP8ZA/uyzz6JPPvkk+vTTT/u2zz//vE9ZfGVGHeehSFAcozmubV4JeGMYg3jCCSd0SmFwzUB2hlvt+0kmnjiadNJJo0knm2yswY1BWFMq3vArouzi18y7HD1Tb/jWK9hRo0Y5xnD1nZNWe8/Xn0PeYeOVrDeG4c1x/fe+PCgLeE8yySSuXLyBzZ7ftIu3W+omXgA5HMMcPp55Dpcc05AZT7ikSXnLk38mno/nDEmUMZuXj5BrpPkN+fQGRprf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ERTE7s8CKoqoqH8QVCwoiIUqIogKUqWI0kEQQXoTQpHQCQmEQCD5v9+5d3I3x3PuPTtbzu45z+Sz2b17dqc88847z7zzzmzfzFmz5rsKh3vuvtv96U9/cmeddZa76667MuV09dVXd4cffrjbYMMN3ZJLLpkprnZenvPii+7ee+91v/jFL9xvf/tbN39+NaDu6+tze++9t3v/Bz7g3vSmN7nRo0cvKM4ZZ5zhzjS8L7jgggX3Yi8++clPuk996lPuNSuuuFAasfGV8R519OQTT7hLL73U19v999/vnnvuueikx40b59Zff3331a9+1a277rpuyaWWahrXvHnz3KuvvOLl5dprr3V/+ctf3L///W/35JNPOn5LE0aMGOHGjBnj1lhjDfeWt7zF7bzLLm7NNdd0S7VIu1Xcd915p7vwwgu97N53332p8xHiHTlypE9/3333dVtvvbVbdbXVwk+pz9dcc4374x//6M7+8589NqkjGHhhjz32cLvttpvb4V3vio2irfdmzJjh0GHf+9733CWXXNLWO73wEO2CY9FFF3WLLbaYQzdvaHr5jW98o5ePZZdddkgYHpw2zf3tb39zhx56qHvqqaeidCt6cMqUKW7y5Mlu4sSJQ6ZXlR/XW2899/a3v93ttPPOHrss+Xppzhw3a9Ys98wzz/jj2WefdTNM182aOdPf57c59sycl15yc19+2b08d657xY65A8crpq/C9cv2O88S0KHJo917vPPqq696PcOZuEO84b5PIOV/Y8eOda997WvdZptt5vYxHfS6172u1P7oaZPP22+/3R1yyCHupptuitajK6+8sttuu+18PItOmOCQX4XOINBXdeJ86623uhNPOMETmQceeCATSqussor72te+5t761re6FVdaKVNc7bw80xTQFZdf7olHHkS0nTSHe4bGBpE54IAD3N4f+pB7zWte4/8O7x3361/7gcrVV18dbkWfP2Tx7/fRj7rXv/71DuVV9RCU9Q3XX+/OOeccd8opp7iXrNPgfmxYbrnl3Nve9jav7JYxMpAcpCTjJB06rPPPP99ddNFFnhTEpk0aiy++uHvHO97hdth+e7e5pT/WSEpaRfv000+7O++4ww82Ufh0YrFhmWWWcW9+85vdxz/+cbepdWCQ+5hw7rnnuhOOP97dcMMNjvYVE8Dh85//vCfO6xtRKzI8+t//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" alt="goai.rest" class="login-logo" style="max-height:60px; margin-bottom:8px;" />
            <p style="color: #555568; font-size: 0.65rem; text-transform:uppercase; letter-spacing:.15em;">Интеллект ресторана · Эволюция</p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            username = st.text_input(t("login_title"), placeholder=t("login_placeholder"))
            password = st.text_input(t("pw_placeholder"), type="password", placeholder=t("pw_placeholder"))
            _lf1, _lf2 = st.columns([3, 1])
            with _lf1:
                submitted = st.form_submit_button(t("login_btn"), use_container_width=True)
            with _lf2:
                _lang_opts = ["🇷🇺 RU", "🇬🇧 EN"]
                _cur_lang_idx = 0 if _get_lang() == "ru" else 1
                _lang_sel = st.form_submit_button(_lang_opts[_cur_lang_idx])
                if _lang_sel:
                    st.session_state["_lang"] = "en" if _get_lang() == "ru" else "ru"
                    st.rerun()

            if submitted:
                if not username or not password:
                    st.error(t("login_error_empty"))
                else:
                    user = _verify_user(username, password)
                    if user:
                        st.session_state["_auth_user"] = user
                        st.rerun()
                    else:
                        st.error(t("login_error_wrong"))

        st.markdown("""<div style="text-align:center; margin-top:16px; color:#444; font-size:.6rem; letter-spacing:.05em;">
            v9.41
        </div>""", unsafe_allow_html=True)


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================
import calendar as _cal
import re as _re_util

def _tokenize_name(name):
    """Разбить название товара на токены для нечёткого сопоставления."""
    s = str(name).lower().strip()
    # Убираем скобки но сохраняем содержимое
    s = s.replace("(", " ").replace(")", " ").replace('"', " ").replace("'", " ")
    # Разбиваем по пробелам, точкам, запятым, дефисам, слешам
    tokens = set(_re_util.split(r'[\s.,/\\-]+', s))
    # Убираем пустые и очень короткие (1 символ)
    return {t for t in tokens if len(t) > 1}

def _token_match_score(name_a, name_b):
    """Оценка совпадения двух названий по общим токенам.
    Возвращает долю совпавших токенов от меньшего набора (0.0 — 1.0)."""
    ta = _tokenize_name(name_a)
    tb = _tokenize_name(name_b)
    if not ta or not tb:
        return 0.0
    common = ta & tb
    return len(common) / min(len(ta), len(tb))

def _find_best_purchase_price(dish_name, pp_dict, threshold=0.5):
    """Найти лучшую закупочную цену для блюда из словаря накладных.
    Использует точное, частичное и токенное сопоставление."""
    dn = str(dish_name).strip().lower()
    # 1. Точное совпадение
    if dn in pp_dict:
        return pp_dict[dn]
    # 2. Частичное (подстрока)
    for pn, pv in pp_dict.items():
        if dn in pn or pn in dn:
            return pv
    # 3. Токенное совпадение (≥50% общих слов)
    best_score = 0
    best_price = None
    for pn, pv in pp_dict.items():
        score = _token_match_score(dn, pn)
        if score > best_score and score >= threshold:
            best_score = score
            best_price = pv
    return best_price

_DEFAULT_FOODCOST_PCT = 33.0  # дефолтный фудкост для столовых

def _staff_meal_cost(staff_meal_revenue):
    """Рассчитать себестоимость питания сотрудников.
    Если фудкост загружен — точный расчёт, иначе оценка 33%.
    Возвращает (cost, foodcost_pct, is_exact)."""
    if staff_meal_revenue <= 0:
        return 0.0, 0.0, True
    # Пробуем взять точный фудкост из сессии (рассчитанный на Фудкост (расчёт))
    rc = st.session_state.get("recipe_costs", pd.DataFrame())
    if not rc.empty and "COST_PER_PORTION" in rc.columns:
        # Средний фудкост = средняя себестоимость / средняя цена продажи
        avg_cost = rc["COST_PER_PORTION"].mean()
        # Нужна средняя цена продажи — берём из rk_prices если есть
        _rk_cache = st.session_state.get("_fc_rk_avg_price", None)
        if _rk_cache and _rk_cache > 0:
            fc_pct = avg_cost / _rk_cache * 100
            return staff_meal_revenue * fc_pct / 100, fc_pct, True
    # Fallback: ищем ранее рассчитанный фудкост
    _cached_fc = st.session_state.get("_last_avg_foodcost", None)
    if _cached_fc and 5 < _cached_fc < 80:
        return staff_meal_revenue * _cached_fc / 100, _cached_fc, True
    # Совсем fallback: 33%
    return staff_meal_revenue * _DEFAULT_FOODCOST_PCT / 100, _DEFAULT_FOODCOST_PCT, False

def _fixed_cost_for_period(monthly_amount, d_from, d_to):
    """Рассчитывает постоянные расходы за период с учётом полных/неполных месяцев.
    Полный месяц = полная сумма, неполный = пропорция дней в этом месяце."""
    if monthly_amount <= 0:
        return 0.0
    total = 0.0
    cur = d_from.replace(day=1) if hasattr(d_from, 'replace') else d_from
    while cur <= d_to:
        y, m = cur.year, cur.month
        days_in_month = _cal.monthrange(y, m)[1]
        first = max(d_from, cur.replace(day=1)) if hasattr(d_from, 'replace') else d_from
        last = min(d_to, cur.replace(day=days_in_month))
        covered = (last - first).days + 1
        total += monthly_amount * covered / days_in_month
        # move to next month
        if m == 12:
            cur = cur.replace(year=y+1, month=1, day=1)
        else:
            cur = cur.replace(month=m+1, day=1)
    return total

# ============================================================
# БД
# ============================================================
@st.cache_resource
def get_connection():
    try:
        return pymssql.connect(
            server=DB_CONFIG["server"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            login_timeout=DB_CONFIG["login_timeout"],
            timeout=DB_CONFIG["timeout"], charset="UTF-8")
    except Exception as e:
        st.error(f"{t('conn_error')}: {e}")
        _pw = DB_CONFIG["password"]
        _pw_show = f"{_pw[:3]}***{_pw[-3:]}" if len(_pw) > 6 else "(пусто или короткий)"
        st.info(f"🔍 Диагностика: server={DB_CONFIG['server']}:{DB_CONFIG['port']}, user={DB_CONFIG['user']}, password={_pw_show}, db={DB_CONFIG['database']}, env_file={'found: ' if _get_lang()=='en' else 'найден: ' + str(_env_file) if _env_file else 'NOT FOUND' if _get_lang()=='en' else 'НЕ НАЙДЕН'}")
        return None

def run_query(query, params=None):
    # Demo mode: use in-memory SQLite
    _demo = globals().get("_DEMO_DB")
    if _demo:
        try:
            q = _sql_translate(query)
            if params:
                # Replace %s with ? for SQLite
                q = q.replace('%s', '?')
                params = tuple(str(p) for p in params)
                return pd.read_sql(q, _demo, params=params)
            return pd.read_sql(q, _demo)
        except Exception as e:
            # Silently return empty for unsupported queries in demo
            return pd.DataFrame()
    try:
        conn = pymssql.connect(
            server=DB_CONFIG["server"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            login_timeout=DB_CONFIG["login_timeout"],
            timeout=DB_CONFIG["timeout"], charset="UTF-8")
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"{t('query_error')}: {e}")
        return pd.DataFrame()

def run_query_cached(query, params=None):
    """run_query with session_state caching — no DB hit on page switch."""
    import hashlib
    _h = hashlib.md5(f"{query}|{params}".encode()).hexdigest()[:12]
    _key = f"_qc_{_h}"
    if _key not in st.session_state:
        st.session_state[_key] = run_query(query, params)
    return st.session_state[_key]

def run_query_safe(query):
    q = query.strip().rstrip(";").strip()
    forbidden = ["INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","TRUNCATE","EXEC","EXECUTE","GRANT","REVOKE"]
    first_word = q.split()[0].upper() if q else ""
    if first_word not in ("SELECT","WITH"):
        return None, "Разрешены только SELECT"
    for w in forbidden:
        if w in q.upper().split(): return None, f"Запрещено: {w}"
    # Demo mode
    _demo = globals().get("_DEMO_DB")
    if _demo:
        try:
            return pd.read_sql(_sql_translate(q), _demo), None
        except Exception as e:
            return None, str(e)
    try:
        conn = get_connection()
        if conn is None: return None, "Нет подключения"
        return pd.read_sql(q, conn), None
    except Exception as e:
        return None, str(e)

# ============================================================
# STOREHOUSE REST API
# ============================================================
def sh_api_call(endpoint, payload=None):
    """Базовый вызов StoreHouse API"""
    # Demo mode: return synthetic data for key endpoints
    if globals().get("IS_DEMO"):
        _demo = globals().get("_DEMO_DB")
        if _demo:
            ep = endpoint.lower()
            try:
                if "goodstree" in ep or "goods" in ep:
                    df = pd.read_sql("SELECT RID, NAME, UNIT, PRICE, GROUPRID FROM GOODS", _demo)
                    return {"errorCode": 0, "data": df.to_dict("records")}, None
                if "depart" in ep:
                    df = pd.read_sql("SELECT RID, NAME, REST_ID FROM DEPARTS", _demo)
                    return {"errorCode": 0, "data": df.to_dict("records")}, None
                if "corr" in ep or "supplier" in ep:
                    df = pd.read_sql("SELECT RID, NAME FROM SUPPLIERS", _demo)
                    return {"errorCode": 0, "data": df.to_dict("records")}, None
            except:
                pass
        return {}, "Демо-режим: данные недоступны для этого запроса"
    url = f"{SH_API['url']}/{endpoint}"
    body = {"UserName": SH_API["user"], "Password": SH_API["password"]}
    if payload:
        body.update(payload)
    try:
        resp = requests.post(url, json=body, timeout=30,
                             headers={"Accept-Encoding": "gzip"})
        resp.raise_for_status()
        data = resp.json()
        if data.get("errorCode", 1) != 0:
            err_info = data.get("errorInfo", {})
            module = err_info.get("moduleName", "")
            err_id = err_info.get("errorId", "")
            msg = data.get("errMessage", "Неизвестная ошибка")
            detail = f"{msg} [module={module}, errId={err_id}]" if module else msg
            return data, detail
        return data, None
    except Exception as e:
        return None, str(e)

def sh_parse_table(data, table_index=0):
    """Разбирает shTable из ответа API в DataFrame"""
    tables = data.get("shTable", [])
    if not tables or table_index >= len(tables):
        return pd.DataFrame()
    tbl = tables[table_index]
    original = tbl.get("original", [])
    fields = tbl.get("fields", [])
    values = tbl.get("values", [])
    # Предпочитаем original (есть всегда в exec), иначе fields
    col_names = []
    if original:
        col_names = [str(o) for o in original]
    elif fields:
        for f in fields:
            if isinstance(f, dict):
                col_names.append(f.get("alt", f.get("path", "?")))
            else:
                col_names.append(str(f))
    if not col_names:
        return pd.DataFrame()
    if not values:
        return pd.DataFrame(columns=col_names)
    try:
        n_cols = len(col_names)
        n_rows = max(len(v) for v in values) if values else 0
        rows = []
        for i in range(n_rows):
            row = []
            for j in range(n_cols):
                if j < len(values) and i < len(values[j]):
                    row.append(values[j][i])
                else:
                    row.append(None)
            rows.append(row)
        return pd.DataFrame(rows, columns=col_names)
    except Exception as e:
        return pd.DataFrame({"_parse_error": [str(e)]})

@st.cache_data(ttl=3600)
def sh_get_alt_names(proc_name):
    """Получить маппинг path -> alt/caption из структуры процедуры"""
    struct_data, err = sh_struct(proc_name)
    if err:
        return {}
    mapping = {}
    for tbl in struct_data.get("shTable", []):
        for f in tbl.get("fields", []):
            if isinstance(f, dict):
                path = f.get("path", "")
                alt = f.get("alt", "")
                caption = f.get("caption", "")
                if alt:
                    mapping[path] = alt
                elif caption:
                    mapping[path] = caption
    return mapping

def sh_exec_named(proc_name, params=None):
    """Выполнить процедуру и переименовать колонки через struct alt-имена"""
    df, err = sh_exec(proc_name, params)
    if err or df.empty:
        return df, err
    mapping = sh_get_alt_names(proc_name)
    if mapping:
        rename = {col: mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=rename)
    return df, None

def sh_exec_raw(proc_name, params=None):
    """Выполнить процедуру и вернуть сырой JSON (для отладки)"""
    payload = {"procName": proc_name}
    if params:
        payload["Input"] = params
    data, err = sh_api_call("sh5exec", payload)
    return data, err

def sh_exec(proc_name, params=None):
    """Выполнить процедуру StoreHouse и вернуть DataFrame"""
    payload = {"procName": proc_name}
    if params:
        payload["Input"] = params
    data, err = sh_api_call("sh5exec", payload)
    if err:
        return pd.DataFrame(), err
    return sh_parse_table(data), None

def sh_exec_smart(proc_name):
    """Умный вызов: сначала пробует без параметров, потом с MaxCount"""
    # Попытка 1: без параметров
    df, err = sh_exec(proc_name)
    if err is None and not df.empty:
        return df, None
    # Попытка 2: узнаём структуру и передаём MaxCount
    struct_data, struct_err = sh_struct(proc_name)
    if struct_err:
        return pd.DataFrame(), f"{err or ''} | struct: {struct_err}"
    tables = struct_data.get("shTable", [])
    # Ищем SingleRow=true датасет с MaxCount
    for tbl in tables:
        if tbl.get("SingleRow"):
            fields = tbl.get("fields", [])
            head = tbl.get("head", "")
            for f in fields:
                alt = f.get("alt", "") if isinstance(f, dict) else ""
                path = f.get("path", "") if isinstance(f, dict) else str(f)
                if alt == "MaxCount" or "MaxCount" in str(alt):
                    params = [{"head": head, "original": [path], "values": [[5000]]}]
                    df2, err2 = sh_exec(proc_name, params)
                    if err2 is None:
                        return df2, None
                    return pd.DataFrame(), f"С MaxCount: {err2}"
    # Попытка 3: передать пустой Input для однострочного датасета
    for tbl in tables:
        if tbl.get("SingleRow"):
            head = tbl.get("head", "")
            fields = tbl.get("fields", [])
            originals = [f.get("path","") if isinstance(f,dict) else str(f) for f in fields]
            defaults = [0 if "int" in (f.get("type","") if isinstance(f,dict) else "").lower()
                        or "uint" in (f.get("type","") if isinstance(f,dict) else "").lower()
                        else "" for f in fields]
            if originals:
                params = [{"head": head, "original": originals, "values": [[d] for d in defaults]}]
                df3, err3 = sh_exec(proc_name, params)
                if err3 is None:
                    return df3, None
                return pd.DataFrame(), f"С параметрами: {err3}"
    return pd.DataFrame(), err or "Не удалось определить параметры"

def sh_exec_all_tables(proc_name, params=None):
    """Выполнить процедуру и вернуть все таблицы"""
    payload = {"procName": proc_name}
    if params:
        payload["Input"] = params
    data, err = sh_api_call("sh5exec", payload)
    if err:
        return [], err
    tables = data.get("shTable", [])
    result = []
    for i in range(len(tables)):
        result.append(sh_parse_table(data, i))
    return result, None

def sh_struct(proc_name):
    """Получить структуру датасетов процедуры"""
    data, err = sh_api_call("sh5struct", {"procName": proc_name})
    if err:
        return None, err
    return data, None

def sh_check_rights(proc_list):
    """Проверить права на процедуры"""
    data, err = sh_api_call("sh5able", {"procList": proc_list})
    if err:
        return {}, err
    allows = data.get("allow", [])
    procs = data.get("procList", proc_list)
    return dict(zip(procs, allows)), None

def sh_info():
    """Информация о сервере и БД"""
    data, err = sh_api_call("sh5info")
    return data, err

# ============================================================
# GEMINI
# ============================================================
def get_rkeeper_schema():
    return """
Таблица ORDERS: VISIT, MIDSERVER (->CASHGROUPS.SIFR), IDENTINVISIT, OPENTIME(datetime), ENDSERVICE, GUESTSCOUNT, PRICELISTSUM(money), TOPAYSUM(money), PAIDSUM(money), DISCOUNTSUM(money), TOTALDISHPIECES, TABLEID, TABLENAME, ORDERNAME, MAINWAITER(->EMPLOYEES.SIFR), PAID(1/0), ICOMMONSHIFT, DURATION(сек), DBSTATUS(-1=удалена)
Таблица SESSIONDISHES: VISIT, MIDSERVER, ORDERIDENT(->ORDERS.IDENTINVISIT), UNI, SIFR(->MENUITEMS.SIFR), QUANTITY, PRLISTSUM(money), PRICE(money), PIECES, ICREATOR(->EMPLOYEES.SIFR), DBSTATUS. НЕТ названия блюда — JOIN MENUITEMS!
Таблица MENUITEMS: SIFR, NAME(название), PARENT(->CATEGLIST.SIFR категория), ITEMKIND
Таблица CATEGLIST (категории блюд): SIFR, NAME(название), PARENT(родитель для иерархии)
Таблица PAYMENTS: VISIT, MIDSERVER, ORDERIDENT, PAYLINETYPE(0=нал,1=карта,5=безнал), BASICSUM(money), STATE(6=закрыт), ICREATOR(->EMPLOYEES.SIFR), DBSTATUS
Таблица DISHVOIDS (отказы): VISIT, MIDSERVER, ORDERIDENT, OPENNAME(причина), QUANTITY, PRLISTSUM(money), DATETIME, DBSTATUS
Таблица EMPLOYEES: SIFR, CODE, NAME
Таблица GLOBALSHIFTS (смены): MIDSERVER, SHIFTNUM, CREATETIME, CLOSETIME, SHIFTDATE, IMANAGER(->EMPLOYEES.SIFR), CLOSED(1/0), IRESTAURANT(->RESTAURANTS.SIFR)
Таблица RESTAURANTS: SIFR, NAME
Таблица CASHGROUPS (кассовые серверы): SIFR, NAME — ORDERS.MIDSERVER ссылается на CASHGROUPS.SIFR
Таблица CLOCKRECS (рабочее время): EMPID(->EMPLOYEES.SIFR), ROLEID, STARTTIME, ENDTIME, DURATION(часы), DELAY(мс опоздание), LATENESS(признак), DBSTATUS
Таблица CASHINOUT (внесения/изъятия): MIDSERVER, ISDEPOSIT(1=внесение,0=изъятие), ORIGINALSUM, KIND(0=вручную,1=программой,3=закрытие смены), DATETIME, ICASHIER(->EMPLOYEES.SIFR), DBSTATUS
Таблица DISHDISCOUNTS: VISIT, MIDSERVER, ORDERIDENT, SIFR, CALCAMOUNT(money), DBSTATUS
Таблица DISHMODIFIERS (модификаторы): VISIT, MIDSERVER, ORDERIDENT, DISHUNI, SIFR(->MODIFIERS.SIFR), OPENNAME, PRICE, PRLISTSUM, DBSTATUS
Связи: sd.ORDERIDENT=o.IDENTINVISIT AND sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER
Правила: (DBSTATUS IS NULL OR DBSTATUS!=-1), TOP N (MS SQL), DATEPART/DATEADD/CAST(x AS DATE)
НЕТ доступа к StoreHouse через SQL. Вопросы про склад/остатки/закупки/фудкост/себестоимость — отвечай NONE, данные доступны на страницах дашборда.
"""

def ask_gemini(prompt):
    for attempt in range(3):
        try:
            resp = requests.post(GEMINI_URL,
                headers={"Content-Type":"application/json"},
                json={"contents":[{"parts":[{"text":prompt}]}],
                      "generationConfig":{"temperature":0.1,"maxOutputTokens":8192}}, timeout=30)
            if resp.status_code == 429:
                time.sleep(5*(attempt+1)); continue
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except requests.exceptions.HTTPError as e:
            if "429" in str(e) and attempt < 2: time.sleep(5*(attempt+1)); continue
            return f"Ошибка Gemini: {e}"
        except Exception as e:
            return f"Ошибка Gemini: {e}"
    return "Gemini перегружен. Подождите минуту."

def generate_sql(question):
    return ask_gemini(f"SQL-эксперт r_keeper 7 (MS SQL). Схема: {get_rkeeper_schema()}\nВопрос: {question}\nВерни ТОЛЬКО SQL SELECT. Без ```. TOP 100. Если не про базу — NONE.")

def explain_results(question, sql, df):
    preview = df.head(20).to_string(index=False) if not df.empty else "Нет данных."
    return ask_gemini(f"Аналитик ресторана. Русский, кратко.\nВопрос: {question}\nДанные: {preview}\nОтвет с числами (1 234 ₽). Без SQL.")

# ============================================================
# СТРАНИЦА
# ============================================================
st.set_page_config(page_title="goai.rest", page_icon="🍽️", layout="wide", initial_sidebar_state="expanded")

# --- AUTH GATE ---
if check_auth() is None:
    show_login_page()
    st.stop()

CURRENT_USER = check_auth()

# ============================================================
# DEMO MODE: synthetic data from demo_data.pkl.gz
# ============================================================
IS_DEMO = CURRENT_USER.get("username") == "demo"
_DEMO_DB = None

if IS_DEMO:
    import sqlite3, gzip, json as _json
    _demo_path = pathlib.Path(__file__).parent / "demo_data.json.gz"
    if _demo_path.exists() and "_demo_loaded" not in st.session_state:
        with st.spinner("Loading..."):
            with gzip.open(_demo_path, 'rt', encoding='utf-8') as f:
                _demo_raw = _json.load(f)
            # Create in-memory SQLite DB
            _conn = sqlite3.connect(":memory:", check_same_thread=False)
            for tbl_name, tbl_data in _demo_raw.items():
                if isinstance(tbl_data, dict) and "records" in tbl_data:
                    df = pd.DataFrame(tbl_data["records"], columns=tbl_data["columns"])
                    df.to_sql(tbl_name.upper(), _conn, if_exists="replace", index=False)
            # Create views matching RK7 table names
            _conn.execute("CREATE VIEW IF NOT EXISTS SESSIONDISHES AS SELECT * FROM DISHES")
            _conn.execute("CREATE VIEW IF NOT EXISTS GLOBALSHIFTS AS SELECT * FROM SHIFTS")
            _conn.execute("CREATE VIEW IF NOT EXISTS DISHVOIDS AS SELECT * FROM VOIDS")
            _conn.execute("CREATE VIEW IF NOT EXISTS CATEGLIST AS SELECT * FROM CATEGORIES")
            # Warehouse stat tables are loaded directly with their names (STAT_SH4_SHIFTS_*)
            st.session_state["_demo_loaded"] = True
            st.session_state["_demo_conn"] = _conn
    _DEMO_DB = st.session_state.get("_demo_conn")

    if _DEMO_DB:
        # Patch: translate MS SQL → SQLite
        def _sql_translate(query):
            """Minimal MS SQL → SQLite translation."""
            q = query
            # TOP N → LIMIT N (move to end)
            import re as _re
            m = _re.search(r'SELECT\s+TOP\s+(\d+)\s+', q, _re.IGNORECASE)
            if m:
                n = m.group(1)
                q = _re.sub(r'SELECT\s+TOP\s+\d+\s+', 'SELECT ', q, count=1, flags=_re.IGNORECASE)
                q = q.rstrip().rstrip(';') + f' LIMIT {n}'
            # DATEADD(DAY,1,%s) → date(%s, '+1 day')
            q = _re.sub(r"DATEADD\s*\(\s*DAY\s*,\s*1\s*,\s*(%s|'[^']*')\s*\)", r"date(\1, '+1 day')", q, flags=_re.IGNORECASE)
            # CAST(x AS VARCHAR) → CAST(x AS TEXT)
            q = _re.sub(r'CAST\(([^)]+)\s+AS\s+VARCHAR\)', r'CAST(\1 AS TEXT)', q, flags=_re.IGNORECASE)
            q = _re.sub(r'CAST\(([^)]+)\s+AS\s+DECIMAL\([^)]*\)\)', r'CAST(\1 AS REAL)', q, flags=_re.IGNORECASE)
            q = _re.sub(r'CAST\(([^)]+)\s+AS\s+INT\)', r'CAST(\1 AS INTEGER)', q, flags=_re.IGNORECASE)
            # ISNULL → IFNULL
            q = q.replace('ISNULL(', 'IFNULL(')
            # Table name fixes for SQLite views
            q = q.replace('CATEGLIST', 'CATEGORIES')
            # CONCAT not supported in SQLite — already using CAST+|| in most queries
            return q

# Тема: светлая/тёмная
# Тема: светлая/тёмная — сохраняется в URL query params
_theme_from_url = st.query_params.get("theme", "")
if "_theme" not in st.session_state:
    st.session_state["_theme"] = _theme_from_url if _theme_from_url in ("light", "dark") else "dark"
IS_LIGHT = st.session_state["_theme"] == "light"

# Dynamically override Streamlit's internal theme (affects dataframe, status widget etc)
import streamlit.config as _stconfig
if IS_LIGHT:
    _stconfig.set_option("theme.base", "light")
    _stconfig.set_option("theme.backgroundColor", "#f5f5f8")
    _stconfig.set_option("theme.secondaryBackgroundColor", "#eeeef2")
    _stconfig.set_option("theme.textColor", "#1a1a2e")
    _stconfig.set_option("theme.primaryColor", "#00b847")
else:
    _stconfig.set_option("theme.base", "dark")
    _stconfig.set_option("theme.backgroundColor", "#08080e")
    _stconfig.set_option("theme.secondaryBackgroundColor", "#0e0e16")
    _stconfig.set_option("theme.textColor", "#ffffff")
    _stconfig.set_option("theme.primaryColor", "#00ff6a")

# === VIEWPORT META (critical for mobile) ===
st.markdown("""<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">""", unsafe_allow_html=True)

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

:root {
    --bg:#08080e; --bg2:#0a0a12; --card:#0e0e16;
    --border:rgba(255,255,255,0.04); --border-h:rgba(0,255,106,0.2);
    --t1:#fff; --t2:#7a7a92; --t3:#44445a;
    --green:#00ff6a; --lime:#c8ff00; --yellow:#ffe600; --pink:#ff0090; --purple:#8b5cf6;
    /* Override Streamlit internal theme vars for dark mode */
    --primary-color: #00ff6a; --background-color: #08080e;
    --secondary-background-color: #0e0e16; --text-color: #ffffff;
}

/* === BASE === */
.stApp { background: var(--bg) !important; color: var(--t1); font-family: 'Inter', sans-serif; }
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { height: 0 !important; min-height: 0 !important; padding: 0 !important; margin: 0 !important; overflow: hidden !important; }
/* Keep native sidebar expand/collapse buttons VISIBLE despite header hidden */
[data-testid="stExpandSidebarButton"],
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapseButton"] button {
    visibility: visible !important;
}
/* Also make expand button's parent wrappers visible (visibility doesn't inherit to visible children, but layout needs it) */
header[data-testid="stHeader"] [data-testid="stToolbar"] { visibility: visible !important; pointer-events: none; }
header[data-testid="stHeader"] [data-testid="stToolbar"] [data-testid="stExpandSidebarButton"] { pointer-events: auto; }
.stDeployButton { display: none !important; visibility: hidden !important; }
.stAppToolbar > *:not(:has([data-testid="stExpandSidebarButton"])) { visibility: hidden !important; }
.block-container { padding: 0 1.5rem 1.5rem; max-width: 100%; margin-top: -1rem !important; }
[data-testid="stAppViewContainer"] { padding-top: 0 !important; margin-top: 0 !important; }
[data-testid="stMain"] { padding-top: 0 !important; margin-top: 0 !important; }
[data-testid="stMainBlockContainer"] { padding-top: 0 !important; margin-top: 0 !important; }
.stApp > [data-testid="stAppViewContainer"] > section > div { padding-top: 0 !important; }

/* === SIDEBAR === */
section[data-testid="stSidebar"] { background: var(--bg2); border-right: 1px solid var(--border); }
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding-top: 0 !important; }
section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] { padding-top: 0 !important; }
section[data-testid="stSidebar"] .stRadio > div { gap: 0; }
section[data-testid="stSidebar"] .stRadio label {
    padding: 10px 14px; border-radius: 8px; font-size: .82rem; font-weight: 500;
    color: #6a6a80; transition: all .15s; margin: 0;
}
section[data-testid="stSidebar"] .stRadio label:hover { background: rgba(0,255,106,.03); color: #999; }
section[data-testid="stSidebar"] .stRadio label[data-checked="true"] {
    background: rgba(0,255,106,.06); color: var(--green); font-weight: 600;
    border-left: 2px solid var(--green); padding-left: 12px;
}
/* Sidebar: tighten vertical gaps only for nav area */
section[data-testid="stSidebar"] hr { margin: 4px 0 !important; opacity: 0.3; }
/* Nav buttons in sidebar — compact */
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important; border: none !important;
    color: var(--t2) !important; font-size: .8rem !important;
    padding: 8px 14px !important; text-align: left !important;
    justify-content: flex-start !important; font-weight: 500 !important;
    border-radius: 0 8px 8px 0 !important; transition: all .12s !important;
    box-shadow: none !important; width: 100% !important;
    display: flex !important; align-items: center !important;
    border-left: 2px solid transparent !important;
    min-height: 0 !important; height: auto !important; line-height: 1.4 !important;
    margin: 0 !important;
}
section[data-testid="stSidebar"] .stButton > button > div,
section[data-testid="stSidebar"] .stButton > button > div > p,
section[data-testid="stSidebar"] .stButton > button > div > div,
section[data-testid="stSidebar"] .stButton > button span {
    text-align: left !important; width: 100% !important;
    justify-content: flex-start !important; display: block !important;
    margin: 0 !important; padding: 0 !important;
    font-size: .8rem !important; line-height: 1.4 !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(0,255,106,.04) !important; color: var(--t1) !important;
}
/* Active nav button (primary type) */
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background: rgba(0,255,106,.06) !important;
    color: var(--green) !important; font-weight: 600 !important;
    border-left: 2px solid var(--green) !important;
}
/* Tighten gap between nav items */
section[data-testid="stSidebar"] .stButton { margin-bottom: 0 !important; }/* Sidebar expanders — transparent */
section[data-testid="stSidebar"] div[data-testid="stExpander"] {
    background: transparent !important; border: none !important;
    border-radius: 0 !important; box-shadow: none !important;
}
section[data-testid="stSidebar"] div[data-testid="stExpander"]:hover {
    border-color: transparent !important;
}
section[data-testid="stSidebar"] .streamlit-expanderHeader {
    font-size: .68rem !important; font-weight: 700 !important;
    text-transform: uppercase !important; letter-spacing: .1em !important;
    color: var(--t3) !important; padding: 8px 10px !important;
    background: transparent !important; border: none !important;
}
section[data-testid="stSidebar"] .streamlit-expanderContent {
    padding: 0 !important; border: none !important;
}
section[data-testid="stSidebar"] details { border: none !important; }

/* === METRIC CARDS (JUICE style) === */
[data-testid="stMetric"] {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 14px; padding: 18px 20px;
    transition: all .25s; position: relative; overflow: hidden;
}
[data-testid="stMetric"]::after {
    content:''; position:absolute; top:0; left:0; right:0; height:1.5px;
    background: linear-gradient(90deg, transparent 10%, rgba(0,255,106,.4) 50%, transparent 90%); opacity:.5;
}
[data-testid="stMetric"]:hover {
    border-color: var(--border-h); transform: translateY(-2px);
    box-shadow: 0 10px 30px rgba(0,0,0,.4), 0 0 20px rgba(0,255,106,.03);
}
[data-testid="stMetricValue"] {
    font-size: clamp(1.15rem, 1.6vw, 1.85rem) !important; font-weight: 800; color: #fff;
    letter-spacing: -.03em; font-variant-numeric: tabular-nums; line-height: 1.1;
    white-space: nowrap; overflow: visible;
}
[data-testid="stMetricLabel"] {
    font-size: .62rem; color: var(--t3);
    text-transform: uppercase; letter-spacing: .1em; font-weight: 700; margin-bottom: 6px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
[data-testid="stMetricDelta"] {
    font-size: .7rem; font-weight: 700;
    padding: 2px 8px; border-radius: 12px; display: inline-block;
    background: rgba(0,255,106,.08);
}
[data-testid="stMetricDelta"] svg { display: none; }

/* Accent borders by position */
div[data-testid="stHorizontalBlock"] > div:nth-child(1) [data-testid="stMetric"] { border-top: 1.5px solid var(--green); }
div[data-testid="stHorizontalBlock"] > div:nth-child(2) [data-testid="stMetric"] { border-top: 1.5px solid var(--lime); }
div[data-testid="stHorizontalBlock"] > div:nth-child(3) [data-testid="stMetric"] { border-top: 1.5px solid var(--yellow); }
div[data-testid="stHorizontalBlock"] > div:nth-child(4) [data-testid="stMetric"] { border-top: 1.5px solid var(--pink); }
div[data-testid="stHorizontalBlock"] > div:nth-child(5) [data-testid="stMetric"] { border-top: 1.5px solid var(--purple); }
div[data-testid="stHorizontalBlock"] > div:nth-child(6) [data-testid="stMetric"] { border-top: 1.5px solid #00d4ff; }

/* Tight column gaps */
div[data-testid="stHorizontalBlock"] { gap: 8px !important; }

/* === TYPOGRAPHY === */
h1 { color:#fff !important; font-weight:800 !important; font-size:1.7rem !important; letter-spacing:-.03em !important; margin-bottom:4px !important; }
h2 { color:#fff !important; font-weight:700 !important; font-size:1.35rem !important; letter-spacing:-.02em !important; }
h3 { color:#fff !important; font-weight:700 !important; font-size:1.2rem !important; letter-spacing:-.02em !important; margin-bottom:2px !important; }
h4 { color:var(--t2) !important; font-weight:500 !important; font-size:.92rem !important; }
p, li { color: var(--t2); font-size: .84rem; line-height: 1.5; }
strong { color: #fff; font-weight: 600; }

/* === TABS === */
.stTabs [data-baseweb="tab-list"] { background:var(--card); border-radius:12px; padding:3px; border:1px solid var(--border); gap:3px; }
.stTabs [data-baseweb="tab"] { color:var(--t3); font-weight:500; border-radius:9px; padding:8px 16px; font-size:.8rem; transition:all .15s; }
.stTabs [data-baseweb="tab"]:hover { color:var(--t2); background:rgba(255,255,255,.02); }
.stTabs [aria-selected="true"] { color:var(--green) !important; font-weight:600; background:rgba(0,255,106,.08) !important; }

/* === DATAFRAMES === */
.stDataFrame { border-radius:12px; overflow:hidden; border:1px solid var(--border); }

/* === BUTTONS === */
.stButton > button {
    border-radius:10px; border:1px solid var(--border); background:var(--card);
    color:#fff; font-weight:600; font-size:.82rem; padding:.5rem 1rem; transition:all .15s;
}
.stButton > button:hover { border-color:var(--border-h); box-shadow:0 0 15px rgba(0,255,106,.05); }
.stButton > button:active { transform:scale(.98); }
.stButton > button[kind="primary"] {
    background:linear-gradient(135deg,#00ff6a,#00cc55); border:none; color:#000; font-weight:800;
    box-shadow:0 4px 18px rgba(0,255,106,.25);
}
.stButton > button[kind="primary"]:hover { box-shadow:0 4px 25px rgba(0,255,106,.4); transform:translateY(-1px); }

/* Download */
.stDownloadButton > button { border-radius:10px; border:1px solid rgba(0,255,106,.15); background:rgba(0,255,106,.04); color:var(--green); font-weight:600; }
.stDownloadButton > button:hover { background:rgba(0,255,106,.08); box-shadow:0 0 12px rgba(0,255,106,.08); }

/* === EXPANDER === */
div[data-testid="stExpander"] { background:var(--card); border:1px solid var(--border); border-radius:12px; }
div[data-testid="stExpander"]:hover { border-color:rgba(0,255,106,.1); }
/* Sidebar expanders (menu groups) — NO card background */
section[data-testid="stSidebar"] div[data-testid="stExpander"] {
    background: transparent !important; border: none !important;
    border-radius: 0 !important; box-shadow: none !important;
}
section[data-testid="stSidebar"] div[data-testid="stExpander"]:hover {
    border-color: transparent !important;
}

/* === INPUTS === */
.stSelectbox > div > div, .stMultiSelect > div > div { background:var(--card) !important; border-color:var(--border) !important; border-radius:10px !important; }
/* Dark mode: override light config text colors */
[data-baseweb="select"] span, [data-baseweb="select"] div,
.stSelectbox span, .stMultiSelect span { color: #fff !important; }
.stSelectbox label, .stMultiSelect label, .stTextInput label,
.stDateInput label, .stNumberInput label, .stTextArea label { color: var(--t2) !important; }
.stTextInput > div > div > input, .stTextArea > div > div > textarea {
    background:var(--card) !important; border-color:var(--border) !important; border-radius:10px !important; color:#fff !important;
}
.stTextInput > div > div > input:focus { border-color:var(--green) !important; box-shadow:0 0 0 1px var(--green),0 0 15px rgba(0,255,106,.08) !important; }
.stDateInput input { color: #fff !important; background: var(--card) !important; }

/* === RADIO (horizontal) === */
.stRadio > div { gap:6px; }
.stRadio label { border-radius:8px; padding:6px 12px; transition:all .15s; border:1px solid transparent; color:var(--t2); font-size:.82rem; }
.stRadio label:hover { background:rgba(0,255,106,.03); }
.stRadio label[data-checked="true"] { background:rgba(0,255,106,.06); border-color:rgba(0,255,106,.2); color:var(--green); }

/* === CHAT === */
.stChatMessage { background:var(--card) !important; border:1px solid var(--border); border-radius:12px; }
.stChatInputContainer > div { background:var(--card); border-color:var(--border); border-radius:12px; }
.stChatInputContainer > div:focus-within { border-color:var(--green); box-shadow:0 0 15px rgba(0,255,106,.06); }
/* Bottom fixed container for chat_input */
[data-testid="stBottom"], [data-testid="stBottom"] > div,
[data-testid="stBottomBlockContainer"], [data-testid="stBottomBlockContainer"] > div,
.stChatFloatingInputContainer, [data-testid="stChatInput"] {
    background: var(--bg) !important;
}

/* === DIVIDER === */
hr { border:none !important; height:1px !important; background:var(--border) !important; margin:.8rem 0 !important; }

/* === ALERTS === */
.stAlert { border-radius:12px; }

/* === CAPTION === */
.stCaption, [data-testid="stCaptionContainer"] { color:var(--t3) !important; font-size:.72rem !important; }

/* === SCROLLBAR === */
::-webkit-scrollbar { width:4px; height:4px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background:rgba(0,255,106,.08); border-radius:2px; }
::-webkit-scrollbar-thumb:hover { background:rgba(0,255,106,.15); }

/* === PLOTLY === */
.js-plotly-plot { border-radius:12px; }
.js-plotly-plot .plotly .modebar { opacity: 0 !important; transition: opacity .2s; pointer-events: none; }
.js-plotly-plot:hover .plotly .modebar { opacity: 1 !important; pointer-events: auto; }
.js-plotly-plot .plotly .modebar-group { background: transparent !important; }

/* === LINKS === */
a { color:var(--green); text-decoration:none; }
a:hover { color:#5aff8a; }

/* === CODE === */
code { background:rgba(0,255,106,.06); color:#5aff8a; border-radius:4px; padding:1px 5px; font-size:.78rem; }

/* === TABLES === */
table { border-collapse:collapse; width:100%; }
th { background:rgba(0,255,106,.03); color:#fff; font-weight:600; font-size:.78rem; }
td,th { border:1px solid var(--border); padding:8px 12px; font-size:.8rem; }
tr:hover { background:rgba(0,255,106,.02); }

/* === GLASS CARD (for HTML blocks) === */
.glass-card { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:20px 22px; }
.glass-card:hover { border-color:rgba(0,255,106,.1); }

/* === BADGES === */
.badge-success { background:rgba(0,255,106,.1); color:#00ff6a; padding:3px 10px; border-radius:12px; font-size:.68rem; font-weight:700; }
.badge-warning { background:rgba(255,230,0,.1); color:#ffe600; padding:3px 10px; border-radius:12px; font-size:.68rem; font-weight:700; }
.badge-error { background:rgba(255,71,87,.1); color:#ff4757; padding:3px 10px; border-radius:12px; font-size:.68rem; font-weight:700; }

/* === LOADING === */
/* Hide the "Running func()" status widget with maximum specificity */
.stApp .stStatusWidget,
.stApp [data-testid="stStatusWidget"],
.stApp .stStatusWidget[data-testid="stStatusWidget"],
div.stStatusWidget,
[data-testid="stStatusWidget"].stStatusWidget { display: none !important; visibility: hidden !important; height: 0 !important; overflow: hidden !important; }
[data-testid="stAppRunningIndicator"] { display: none !important; }

/* Streamlit spinner styling — shows in-place where data loads */
.stSpinner > div { display: flex; flex-direction: column; align-items: center; gap: 8px; padding: 20px 0; }
.stSpinner > div > div:first-child {
    border-color: rgba(0,255,106,.15) !important;
    border-top-color: var(--green) !important;
    width: 28px !important; height: 28px !important;
}
.stSpinner > div > div:last-child {
    color: var(--t2) !important; font-size: .78rem; font-weight: 500;
}

/* Notification overlays */
.stApp [data-testid="stNotification"],
[data-testid="stNotificationContentInfo"],
div[data-testid="stMarkdownContainer"] + div[style*="position: fixed"] {
    background: var(--card) !important; color: var(--t1) !important;
    border: 1px solid var(--border) !important; border-radius: 12px !important;
}

/* ================================================================
   MOBILE / ADAPTIVE
   ================================================================ */

@media (max-width: 768px) {

    /* --- Clean transition when switching pages on mobile --- */
    .main .block-container {
        transition: opacity 0.2s ease-out !important;
    }
    /* Sidebar overlay - ensure it covers content fully */
    section[data-testid="stSidebar"] {
        transition: margin-left 0.2s ease !important;
        z-index: 999 !important;
    }
    /* When sidebar closes, main content fades in fresh */
    section[data-testid="stSidebar"][aria-expanded="false"] ~ .main .block-container {
        opacity: 1 !important;
    }

    /* --- Make header visible but only show the sidebar expand button --- */
    header[data-testid="stHeader"] {
        visibility: visible !important;
        background: transparent !important;
        pointer-events: none;
    }
    header[data-testid="stHeader"] * { visibility: hidden; }
    [data-testid="stExpandSidebarButton"],
    [data-testid="stExpandSidebarButton"] * {
        visibility: visible !important; pointer-events: auto !important;
    }

    /* --- Restyle expand button as hamburger --- */
    [data-testid="stExpandSidebarButton"] {
        width: 44px !important; height: 44px !important;
        background: var(--card) !important; border: 1px solid var(--border) !important;
        border-radius: 12px !important; cursor: pointer;
        box-shadow: 0 4px 20px rgba(0,0,0,.4);
        display: flex !important; align-items: center; justify-content: center;
        padding: 0 !important; transition: all .2s;
        color: var(--green) !important;
    }
    [data-testid="stExpandSidebarButton"]:hover {
        border-color: var(--border-h) !important;
        box-shadow: 0 4px 25px rgba(0,0,0,.5), 0 0 15px rgba(0,255,106,.08);
    }
    [data-testid="stExpandSidebarButton"] span {
        color: var(--green) !important; font-size: 22px !important;
    }

    /* Sidebar as mobile overlay */
    section[data-testid="stSidebar"] {
        z-index: 999998 !important;
        min-width: 280px !important; max-width: 85vw !important;
        box-shadow: 4px 0 30px rgba(0,0,0,.6);
    }

    /* Container — less padding, room for hamburger */
    .block-container {
        padding: 60px 10px 16px 10px !important; max-width: 100% !important;
    }

    /* Columns on mobile: default 48% (2-col grid) */
    div[data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important; gap: 6px !important;
    }
    div[data-testid="stHorizontalBlock"] > div {
        flex: 1 1 48% !important; min-width: 0 !important; max-width: 100% !important;
    }
    /* Period selector row (has selectbox inside) — stay horizontal */
    div[data-testid="stHorizontalBlock"]:has([data-testid="stSelectbox"]) {
        flex-wrap: nowrap !important; gap: 4px !important;
    }
    div[data-testid="stHorizontalBlock"]:has([data-testid="stSelectbox"]) > div {
        flex: 0 1 auto !important; min-width: 0 !important; max-width: none !important;
    }
    div[data-testid="stHorizontalBlock"]:has([data-testid="stSelectbox"]) > div:nth-child(2) {
        flex: 1 1 auto !important;
    }
    /* Allow 2-column for small pairs (metrics row) */
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
        flex: 1 1 calc(50% - 4px) !important; min-width: calc(50% - 4px) !important;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        padding: 12px 14px !important; border-radius: 10px;
    }
    [data-testid="stMetricValue"] { font-size: 1.3rem !important; }
    [data-testid="stMetricLabel"] { font-size: .52rem !important; }

    /* Typography */
    h1 { font-size: 1.3rem !important; }
    h2 { font-size: 1.1rem !important; }
    h3 { font-size: 1rem !important; }
    p, li { font-size: .78rem !important; }

    /* Tabs — scrollable */
    .stTabs [data-baseweb="tab-list"] {
        overflow-x: auto; flex-wrap: nowrap; -webkit-overflow-scrolling: touch;
        scrollbar-width: none; padding: 2px;
    }
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar { display: none; }
    .stTabs [data-baseweb="tab"] {
        white-space: nowrap; flex-shrink: 0;
        font-size: .72rem !important; padding: 6px 10px;
    }

    /* Plotly charts responsive */
    .js-plotly-plot, .plotly { width: 100% !important; }
    .js-plotly-plot .plotly .main-svg { width: 100% !important; }

    /* Tables scroll */
    .stDataFrame { overflow-x: auto; -webkit-overflow-scrolling: touch; }
    table { font-size: .72rem !important; }
    td, th { padding: 5px 8px !important; font-size: .72rem !important; }

    /* Expanders */
    div[data-testid="stExpander"] { border-radius: 10px; }
    div[data-testid="stExpander"] summary { font-size: .8rem !important; }

    /* Buttons */
    .stButton > button { font-size: .75rem !important; padding: .4rem .7rem; }
    .stDownloadButton > button { font-size: .75rem !important; }

    /* Selectbox / inputs */
    .stSelectbox, .stMultiSelect, .stDateInput { font-size: .78rem !important; }

    /* Glass cards */
    .glass-card { padding: 12px 14px !important; border-radius: 10px; }

    /* Chat */
    .stChatMessage { font-size: .8rem !important; }
}

/* --- Tablet (768-1024) --- */
@media (min-width: 769px) and (max-width: 1024px) {
    .block-container { padding: 0 1rem 1.5rem !important; margin-top: -1rem !important; }
    div[data-testid="stHorizontalBlock"] { gap: 6px !important; }
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
        min-width: calc(33% - 4px) !important;
    }
    [data-testid="stMetricValue"] { font-size: 1.5rem !important; }
}
</style>""", unsafe_allow_html=True)

# === LIGHT THEME OVERRIDES ===
if IS_LIGHT:
    st.markdown("""<style>
    /* === NUCLEAR OVERRIDE: Force Streamlit internal theme vars to light === */
    :root, [data-testid="stAppViewContainer"], .stApp {
        --bg:#f5f5f8; --bg2:#eeeef2; --card:#ffffff; --border:rgba(0,0,0,0.08);
        --border-h:rgba(0,180,80,0.3); --t1:#1a1a2e; --t2:#5a5a70; --t3:#9a9ab0;
        --green:#00b847; --lime:#7ab800; --yellow:#cc9900; --pink:#cc0070; --purple:#6b3fc6;
        /* Override Streamlit's internal vars from config.toml */
        --primary-color: #00b847 !important;
        --background-color: #f5f5f8 !important;
        --secondary-background-color: #eeeef2 !important;
        --text-color: #1a1a2e !important;
        color: #1a1a2e !important;
    }
    /* Force ALL text inside app to dark */
    .stApp, .stApp * { color: inherit; }
    .stApp [data-baseweb] { color: #1a1a2e; }
    .stApp label, .stApp span, .stApp p, .stApp div { color: inherit; }
    /* Specific Streamlit widget text overrides */
    [data-baseweb="select"] span,
    [data-baseweb="select"] div,
    [data-baseweb="input"] input,
    [data-baseweb="textarea"] textarea,
    .stSelectbox span,
    .stMultiSelect span,
    .stTextInput input,
    .stNumberInput input,
    .stTextArea textarea,
    .stDateInput input,
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stCaptionContainer"] {
        color: #1a1a2e !important;
    }
    /* Download button text */
    .stDownloadButton button span { color: var(--green) !important; }

    /* === BASE === */
    .stApp { background: var(--bg) !important; }
    header[data-testid="stHeader"] { background: transparent !important; }

    /* === SIDEBAR === */
    section[data-testid="stSidebar"] { background: var(--bg2); border-right: 1px solid var(--border); }
    section[data-testid="stSidebar"] .stRadio label { color: var(--t2); }
    section[data-testid="stSidebar"] > div { padding-top: 0 !important; }
    section[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding-top: 0 !important; }
    section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] { padding-top: 0 !important; }
    section[data-testid="stSidebar"] .stRadio label:hover { background: rgba(0,180,80,.05); color: var(--t1); }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"] {
        background: rgba(0,180,80,.08); color: var(--green); border-left-color: var(--green); }
    section[data-testid="stSidebar"] .stSelectbox > div > div,
    section[data-testid="stSidebar"] .stMultiSelect > div > div {
        background: #fff !important; border-color: var(--border) !important; color: var(--t1) !important; }
    /* Sidebar nav buttons */
    section[data-testid="stSidebar"] .stButton > button {
        background: transparent !important; border: none !important; color: var(--t2) !important;
        box-shadow: none !important; text-align: left !important;
        justify-content: flex-start !important; display: flex !important;
        border-left: 2px solid transparent !important;
        border-radius: 0 6px 6px 0 !important;
    }
    section[data-testid="stSidebar"] .stButton > button > div,
    section[data-testid="stSidebar"] .stButton > button > div > p,
    section[data-testid="stSidebar"] .stButton > button span {
        text-align: left !important; width: 100% !important; display: block !important;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(0,180,80,.06) !important; color: var(--t1) !important;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
        background: rgba(0,180,80,.08) !important;
        color: var(--green) !important; font-weight: 600 !important;
        border-left: 2px solid var(--green) !important;
    }
    section[data-testid="stSidebar"] .stButton { margin-bottom: 0 !important; }
    /* Sidebar expanders (groups) */
    section[data-testid="stSidebar"] details {
        border: none !important; background: transparent !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stExpander"] {
        background: transparent !important; border: none !important;
        border-radius: 0 !important; box-shadow: none !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stExpander"]:hover {
        border-color: transparent !important;
    }
    section[data-testid="stSidebar"] .streamlit-expanderHeader {
        color: var(--t3) !important; background: transparent !important;
    }

    /* === METRICS === */
    [data-testid="stMetric"] { background: var(--card); border-color: var(--border); box-shadow: 0 1px 3px rgba(0,0,0,.06); }
    [data-testid="stMetric"]:hover { border-color: var(--border-h); box-shadow: 0 4px 12px rgba(0,0,0,.08); transform: translateY(-1px); }
    [data-testid="stMetric"]::after { background: linear-gradient(90deg, transparent 10%, rgba(0,180,80,.3) 50%, transparent 90%); }
    [data-testid="stMetricValue"] { color: var(--t1) !important; }
    [data-testid="stMetricLabel"] { color: var(--t3); }
    [data-testid="stMetricDelta"] { background: rgba(0,180,80,.08); }

    /* === TYPOGRAPHY === */
    h1, h2, h3 { color: var(--t1) !important; }
    h4 { color: var(--t2) !important; }
    p, li { color: var(--t2); }
    strong { color: var(--t1); }
    .stCaption, [data-testid="stCaptionContainer"] { color: var(--t3) !important; }

    /* === TABS === */
    .stTabs [data-baseweb="tab-list"] { background: var(--card); border-color: var(--border); }
    .stTabs [data-baseweb="tab"] { color: var(--t3); }
    .stTabs [aria-selected="true"] { color: var(--green) !important; background: rgba(0,180,80,.06) !important; }

    /* === INPUTS === */
    .stSelectbox > div > div, .stMultiSelect > div > div {
        background: var(--card) !important; border-color: var(--border) !important; color: var(--t1) !important; }
    .stSelectbox [data-baseweb="select"] span { color: var(--t1) !important; }
    .stTextInput > div > div > input, .stTextArea > div > div > textarea {
        background: var(--card) !important; border-color: var(--border) !important; color: var(--t1) !important; }
    .stTextInput > div > div > input:focus { border-color: var(--green) !important; box-shadow: 0 0 0 1px var(--green) !important; }
    .stDateInput > div > div > input { background: var(--card) !important; color: var(--t1) !important; }
    .stRadio label { color: var(--t2) !important; }
    .stRadio label[data-checked="true"] { background: rgba(0,180,80,.06); border-color: rgba(0,180,80,.2); color: var(--green) !important; }

    /* === BUTTONS === */
    .stButton > button { background: var(--card); border-color: var(--border); color: var(--t1); }
    .stButton > button:hover { border-color: var(--border-h); box-shadow: 0 2px 8px rgba(0,0,0,.06); }
    .stButton > button[kind="primary"] { background: linear-gradient(135deg, #00b847, #009938); color: #fff; border: none; }
    .stDownloadButton > button { background: var(--card); border-color: rgba(0,180,80,.15); color: var(--green); }
    .stDownloadButton > button:hover { background: rgba(0,180,80,.04); }

    /* === TABLES === */
    .stDataFrame { border-color: var(--border); }
    table th { background: rgba(0,180,80,.04); color: var(--t1); }
    table td, th { border-color: var(--border); color: var(--t2); }
    tr:hover { background: rgba(0,180,80,.02); }

    /* === EXPANDER === */
    div[data-testid="stExpander"] { background: var(--card); border-color: var(--border); }
    div[data-testid="stExpander"]:hover { border-color: rgba(0,180,80,.15); }

    /* === CHAT === */
    .stChatMessage { background: var(--card) !important; border: 1px solid var(--border); }
    .stChatInputContainer > div { background: var(--card) !important; border-color: var(--border) !important; }
    .stChatInputContainer > div:focus-within { border-color: var(--green) !important; }
    .stChatInputContainer textarea { color: var(--t1) !important; background: transparent !important; }
    /* Fix dark chat bar at bottom — all possible selectors */
    [data-testid="stBottom"],
    [data-testid="stBottom"] > div,
    [data-testid="stBottomBlockContainer"],
    [data-testid="stBottomBlockContainer"] > div,
    .stChatFloatingInputContainer,
    [data-testid="stChatInput"],
    .stBottom > div {
        background: var(--bg) !important;
    }
    .stChatInputContainer { background: transparent !important; }
    .stChatInputContainer > div > div { background: var(--card) !important; }
    [data-testid="stChatInput"] > div { background: var(--card) !important; border-color: var(--border) !important; border-radius: 12px; }
    [data-testid="stChatInput"] textarea { color: var(--t1) !important; }

    /* === DIVIDERS / LINKS / CODE === */
    hr { background: var(--border) !important; }
    a { color: var(--green); }
    a:hover { color: #009938; }
    code { background: rgba(0,180,80,.06); color: var(--green); }
    ::-webkit-scrollbar-thumb { background: rgba(0,180,80,.15); }
    .glass-card { background: var(--card); border-color: var(--border); }

    /* === ALERTS === */
    .stAlert { border-radius: 12px; }

    /* === SPINNER === */
    .stSpinner > div > div:first-child { border-color: rgba(0,180,80,.15) !important; border-top-color: var(--green) !important; }
    .stSpinner > div > div:last-child { color: var(--t2) !important; }

    /* Running overlay */
    .stApp [data-testid="stNotification"],
    [data-testid="stNotificationContentInfo"] {
        background: var(--card) !important; color: var(--t1) !important;
        border-color: var(--border) !important;
    }
    [data-testid="stNotificationContentInfo"] code { background: rgba(0,180,80,.08) !important; color: var(--green) !important; }

    /* Dataframe dark fix */
    .stDataFrame, .stDataFrame > div, [data-testid="stDataFrame"] > div > div {
        background: var(--card) !important;
    }
    .stDataFrame iframe { background: var(--card) !important; }

    /* === PLOTLY === */
    .js-plotly-plot .plotly .gridlayer line { stroke: rgba(0,0,0,.06) !important; }
    .js-plotly-plot .plotly .zerolinelayer line { stroke: rgba(0,0,0,.1) !important; }
    /* Modebar (chart toolbar) */
    /* Plotly modebar - hidden by default, visible on chart hover */
    .js-plotly-plot .plotly .modebar { opacity: 0 !important; transition: opacity .2s; pointer-events: none; background: rgba(255,255,255,.95) !important; border-radius: 8px; }
    .js-plotly-plot:hover .plotly .modebar { opacity: 1 !important; pointer-events: auto; }
    .js-plotly-plot .plotly .modebar-group { background: transparent !important; }
    .js-plotly-plot .plotly .modebar-btn path { fill: #5a5a70 !important; }
    .js-plotly-plot .plotly .modebar-btn:hover path { fill: var(--green) !important; }

    /* === DATAFRAME (glide-data-grid, config.toml base=dark workaround) === */
    .stDataFrame, .stDataFrame > div, [data-testid="stDataFrame"],
    [data-testid="stDataFrame"] > div, [data-testid="stDataFrame"] > div > div {
        background: var(--card) !important; color: var(--t1) !important;
    }
    .stDataFrame iframe { background: var(--card) !important; color-scheme: light !important; }
    /* Override glide-data-grid dark cells */
    [data-testid="stDataFrame"] [data-testid="glideDataEditor"] {
        --gdg-bg-cell: #ffffff !important;
        --gdg-bg-header: #f8f8fa !important;
        --gdg-bg-header-has: #f0f0f4 !important;
        --gdg-text-dark: #1a1a2e !important;
        --gdg-text-medium: #5a5a70 !important;
        --gdg-text-light: #9a9ab0 !important;
        --gdg-border-color: rgba(0,0,0,.08) !important;
        --gdg-bg-cell-medium: #fafafa !important;
        --gdg-link-color: #00b847 !important;
    }

    /* === STATUS WIDGET / RUNNING OVERLAY === */
    .stApp .stStatusWidget,
    .stApp [data-testid="stStatusWidget"],
    .stApp .stStatusWidget[data-testid="stStatusWidget"],
    div.stStatusWidget,
    [data-testid="stStatusWidget"].stStatusWidget,
    [data-testid="stStatusWidget"] > div {
        display: none !important; visibility: hidden !important; height: 0 !important; overflow: hidden !important;
    }

    /* === TABLE (st.table HTML) === */
    table { background: var(--card); }
    table td { color: var(--t2); }

    /* === WARNINGS / ALERTS === */
    .stAlert > div { color: var(--t1) !important; }

    /* === Mobile expand button === */
    [data-testid="stExpandSidebarButton"] {
        background: #ffffff !important; border-color: rgba(0,0,0,.1) !important;
        box-shadow: 0 4px 20px rgba(0,0,0,.12) !important;
    }
    [data-testid="stExpandSidebarButton"] span { color: var(--green) !important; }

    /* === Accent borders for light theme === */
    div[data-testid="stHorizontalBlock"] > div:nth-child(1) [data-testid="stMetric"] { border-top-color: var(--green); }
    div[data-testid="stHorizontalBlock"] > div:nth-child(2) [data-testid="stMetric"] { border-top-color: var(--lime); }
    div[data-testid="stHorizontalBlock"] > div:nth-child(3) [data-testid="stMetric"] { border-top-color: var(--yellow); }
    div[data-testid="stHorizontalBlock"] > div:nth-child(4) [data-testid="stMetric"] { border-top-color: var(--pink); }
    div[data-testid="stHorizontalBlock"] > div:nth-child(5) [data-testid="stMetric"] { border-top-color: var(--purple); }
    div[data-testid="stHorizontalBlock"] > div:nth-child(6) [data-testid="stMetric"] { border-top-color: #0891b2; }
    </style>""", unsafe_allow_html=True)

# === JS FIX: Restyle stStatusWidget — Russian text, theme-aware colors ===
import streamlit.components.v1 as _components
_is_light_js = "true" if IS_LIGHT else "false"
_components.html(f"""<script>
(function(){{
    const doc = window.parent.document;
    const isLight = {_is_light_js};
    const bg = isLight ? '#ffffff' : '#0e0e16';
    const border = isLight ? 'rgba(0,0,0,0.08)' : 'rgba(255,255,255,0.06)';
    const textColor = isLight ? '#5a5a70' : '#7a7a92';
    const codeColor = isLight ? '#00b847' : '#00ff6a';
    const codeBg = isLight ? 'rgba(0,180,80,0.08)' : 'rgba(0,255,106,0.08)';

    function fixStatus() {{
        doc.querySelectorAll('[data-testid="stStatusWidget"], .stStatusWidget').forEach(el => {{
            el.style.cssText = `background:${{bg}}!important;border:1px solid ${{border}}!important;border-radius:12px!important;box-shadow:0 4px 20px rgba(0,0,0,0.1)!important;overflow:hidden!important;`;
            // Fix inner elements
            el.querySelectorAll('div,span,label').forEach(c => {{
                c.style.setProperty('color', textColor, 'important');
                c.style.setProperty('background', 'transparent', 'important');
            }});
            el.querySelectorAll('code').forEach(c => {{
                c.style.cssText = `color:${{codeColor}}!important;background:${{codeBg}}!important;border-radius:4px;padding:1px 5px;`;
            }});
            // Replace "Running" with Russian
            el.querySelectorAll('span,label').forEach(s => {{
                if (s.textContent.trim() === 'Running') s.textContent = 'Загрузка';
            }});
        }});
    }}
    fixStatus();
    new MutationObserver(fixStatus).observe(doc.body, {{childList:true, subtree:true}});
}})();
</script>""", height=0)

# === JS FIX: Mobile — auto-close sidebar after nav click, clean transition ===
_components.html("""<script>
(function() {
    const isMobile = () => window.innerWidth <= 768;

    function closeSidebar() {
        // Click the collapse button
        const collapseBtn = document.querySelector('[data-testid="stSidebarCollapsedControl"] button, [data-testid="collapsedControl"] button');
        if (collapseBtn) { collapseBtn.click(); return; }
        // Fallback: set attribute
        const sidebar = document.querySelector('section[data-testid="stSidebar"]');
        if (sidebar) sidebar.setAttribute('aria-expanded', 'false');
    }

    function clearMainContent() {
        const main = document.querySelector('.main .block-container');
        if (main) {
            main.style.visibility = 'hidden';
            // Scroll to top immediately
            window.scrollTo({top: 0, behavior: 'instant'});
            const mainSection = document.querySelector('[data-testid="stMain"]');
            if (mainSection) mainSection.scrollTop = 0;
            // Add loading overlay
            let ov = document.getElementById('_nav_overlay');
            if (!ov) {
                ov = document.createElement('div');
                ov.id = '_nav_overlay';
                ov.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:var(--bg,#0a0a1a);';
                ov.innerHTML = '<div style="color:var(--t3,#999);font-size:.85rem;">Загрузка...</div>';
                document.body.appendChild(ov);
            } else {
                ov.style.display = 'flex';
            }
        }
    }
    function restoreMainContent() {
        const main = document.querySelector('.main .block-container');
        if (main) main.style.visibility = 'visible';
        const ov = document.getElementById('_nav_overlay');
        if (ov) ov.style.display = 'none';
        // Scroll to top after restore
        window.scrollTo({top: 0, behavior: 'instant'});
    }
    // Detect when Streamlit finishes rerender
    let _navTimer = null;
    new MutationObserver(function() {
        const ov = document.getElementById('_nav_overlay');
        if (!ov || ov.style.display === 'none') return;
        clearTimeout(_navTimer);
        _navTimer = setTimeout(restoreMainContent, 300);
    }).observe(document.body, {childList:true, subtree:true});

    // Watch for button clicks inside sidebar
    function attachNavListeners() {
        const sidebar = document.querySelector('section[data-testid="stSidebar"]');
        if (!sidebar) return;

        sidebar.addEventListener('click', function(e) {
            const btn = e.target.closest('button');
            if (!btn) return;
            const key = btn.getAttribute('data-testid') || '';
            if (key.includes('theme') || key.includes('logout') || key.includes('refresh') || key.includes('lk_top')) return;
            clearMainContent();
            if (isMobile()) setTimeout(closeSidebar, 100);
        }, true);
    }

    // Run after DOM ready
    if (document.readyState === 'complete') attachNavListeners();
    else window.addEventListener('load', attachNavListeners);
    // Also re-attach on mutations (Streamlit rerenders)
    new MutationObserver(attachNavListeners).observe(document.body, {childList:true, subtree:true});
})();
</script>""", height=0)

with st.sidebar:
    # --- Логотип + слоган ---
    _logo_filter = "invert(1)" if not IS_LIGHT else "none"
    _logo_blend = "screen" if not IS_LIGHT else "normal"
    st.markdown(f'''<div style="padding:0;margin-top:-4rem;">
        <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAs4AAAD8CAYAAABnwSKjAAAKtWlDQ1BJQ0MgUHJvZmlsZQAASImVlwdQU+kWgP9700NCCyCdUEMRpAgEkBJ6AOndRkhCCCWGQFAQFRFxBVYUERFQV3QFRMG1ALIWBBTboqiAfUEWBWVdLNhAeRcYgrtv3nvzzsy555tzz3/O+f+5/8y5AJCpLKEwCZYFIFmQJgr2cqVGRkVTcS8BDBSBHLAFFBY7VcgIDPQDiMzZv8uHXgBN27um07n+/f1/FTkON5UNABSIcCwnlZ2M8GlEJ9lCURoAqBOIX3dNmnCa7yGsIEIaRHh4mnmzPDnNsTOMlp2JCQ12Q1gPADyJxRLxACCZI35qOpuH5CFN1zIXcPgChLMRdkpOXs1BuBVhQyRGiPB0fnrsd3l4f8sZK8nJYvEkPLuXGcG781OFSayM//M4/rckJ4nnatAQJcWLvIMRq4yc2R+Jq30lLIj1D5hjPmcmfobjxd5hc8xOdYue49SkEOYcc1juvpI8Sf5+cxzH95TE8NOYoXPMTfUImWPR6mBJ3TiRG2OOWaL5HsSJYRJ/PJcpyZ8ZHxoxx+n8cH9Jb4khvvMxbhK/SBws2QtX4OU6X9dTcg7Jqd/tnc+UrE2LD/WWnANrvn+ugDGfMzVS0huH6+4xHxMmiRemuUpqCZMCJfHcJC+JPzU9RLI2Dfk459cGSs4wgeUTOMfAD3gBKghDbCgIBgzgCZjAH3ikcdemTW/GbbUwQ8TnxadRGciN41KZArbZQqqluaUNANP3d/bzeHd/5l5CSvh535YeABw/IaAz72NuAeDEUwBkvvPRspCraQxA+whbLEqf9aGnHxhABDJAAagATaALDIEpsAQ2wAG4AA/gAwKQfqPASsAG8SAZiMAakAU2gTxQAHaA3aAcHACHQA04Dk6CJnAOXAJXwA1wG/SAR6AfDIFXYAx8ABMQBOEgMkSBVCAtSB8ygSwhOuQEeUB+UDAUBcVAPEgAiaEsaDNUABVD5dBBqBb6BToLXYKuQd3QA2gAGoHeQl9gFEyCFWAN2ABeBNNhBuwLh8IrYB6cAmfCufB2uAyugo/BjfAl+AbcA/fDr+BxFEBJoZRQ2ihTFB3lhgpARaPiUCLUBlQ+qhRVhapHtaA6UXdR/ahR1Gc0Fk1BU9GmaAe0NzoMzUanoDegC9Hl6Bp0I7oDfRc9gB5Df8OQMeoYE4w9homJxPAwazB5mFLMEcwZzGVMD2YI8wGLxSphaVhbrDc2CpuAXYctxO7DNmBbsd3YQew4DodTwZngHHEBOBYuDZeH24s7hruIu4Mbwn3CS+G18JZ4T3w0XoDPwZfij+Iv4O/gX+AnCLIEfYI9IYDAIWQQigiHCS2EW4QhwgRRjkgjOhJDiQnETcQyYj3xMvEx8Z2UlJSOlJ1UkBRfKluqTOqE1FWpAanPJHmSMcmNtJwkJm0nVZNaSQ9I78hksgHZhRxNTiNvJ9eS28lPyZ+kKdJm0kxpjvRG6QrpRuk70q9lCDL6MgyZlTKZMqUyp2RuyYzKEmQNZN1kWbIbZCtkz8r2yY7LUeQs5ALkkuUK5Y7KXZMblsfJG8h7yHPkc+UPybfLD1JQFF2KG4VN2Uw5TLlMGVLAKtAUmAoJCgUKxxW6FMYU5RUXK4YrrlWsUDyv2K+EUjJQYiolKRUpnVTqVfqyQGMBYwF3wbYF9QvuLPiorKbsosxVzlduUO5R/qJCVfFQSVTZqdKk8kQVrWqsGqS6RnW/6mXVUTUFNQc1tlq+2km1h+qwurF6sPo69UPqN9XHNTQ1vDSEGns12jVGNZU0XTQTNEs0L2iOaFG0nLT4WiVaF7VeUhWpDGoStYzaQR3TVtf21hZrH9Tu0p7QoemE6eToNOg80SXq0nXjdEt023TH9LT0lupl6dXpPdQn6NP14/X36HfqfzSgGUQYbDVoMhimKdOYtExaHe2xIdnQ2TDFsMrwnhHWiG6UaLTP6LYxbGxtHG9cYXzLBDaxMeGb7DPpXohZaLdQsLBqYZ8pyZRhmm5aZzpgpmTmZ5Zj1mT2epHeouhFOxd1Lvpmbm2eZH7Y/JGFvIWPRY5Fi8VbS2NLtmWF5T0rspWn1UarZqs3i00WcxfvX3zfmmK91HqrdZv1VxtbG5FNvc2IrZ5tjG2lbR9dgR5IL6RftcPYudpttDtn99nexj7N/qT9Xw6mDokORx2Gl9CWcJccXjLoqOPIcjzo2O9EdYpx+smp31nbmeVc5fzMRdeF43LE5QXDiJHAOMZ47WruKnI94/rRzd5tvVurO8rdyz3fvctD3iPMo9zjqaeOJ8+zznPMy9prnVerN8bb13undx9Tg8lm1jLHfGx91vt0+JJ8Q3zLfZ/5GfuJ/FqWwkt9lu5a+thf31/g3xQAApgBuwKeBNICUwJ/DcIGBQZVBD0PtgjOCu4MoYSsCjka8iHUNbQo9FGYYZg4rC1cJnx5eG34xwj3iOKI/shFkesjb0SpRvGjmqNx0eHRR6LHl3ks271saLn18rzlvStoK9auuLZSdWXSyvOrZFaxVp2KwcRExByNmWQFsKpY47HM2MrYMbYbew/7FceFU8IZ4Tpyi7kv4hzjiuOGeY68XbyReOf40vhRvhu/nP8mwTvhQMLHxIDE6sSppIikhmR8ckzyWYG8IFHQsVpz9drV3UITYZ6wP8U+ZXfKmMhXdCQVSl2R2pymgAxKN8WG4i3igXSn9Ir0T2vC15xaK7dWsPZmhnHGtowXmZ6ZP69Dr2Ova8vSztqUNbCesf7gBmhD7Ia2jbobczcOZXtl12wibkrc9FuOeU5xzvvNEZtbcjVys3MHt3htqcuTzhPl9W112HrgB/QP/B+6tllt27vtWz4n/3qBeUFpwWQhu/D6jxY/lv04tT1ue1eRTdH+Hdgdgh29O5131hTLFWcWD+5auquxhFqSX/J+96rd10oXlx7YQ9wj3tNf5lfWvFdv7469k+Xx5T0VrhUNleqV2yo/7uPsu7PfZX/9AY0DBQe+/MT/6f5Br4ONVQZVpYewh9IPPT8cfrjzZ/rPtUdUjxQc+VotqO6vCa7pqLWtrT2qfrSoDq4T140cW37s9nH34831pvUHG5QaCk6AE+ITL3+J+aX3pO/JtlP0U/Wn9U9XnqGcyW+EGjMax5rim/qbo5q7z/qcbWtxaDnzq9mv1ee0z1WcVzxfdIF4IffC1MXMi+OtwtbRS7xLg22r2h61R7bf6wjq6Lrse/nqFc8r7Z2MzotXHa+eu2Z/7ex1+vWmGzY3Gm9a3zzzm/VvZ7psuhpv2d5qvm13u6V7SfeFO853Lt11v3vlHvPejR7/nu7esN77fcv7+u9z7g8/SHrw5mH6w4lH2Y8xj/OfyD4pfar+tOp3o98b+m36zw+4D9x8FvLs0SB78NUfqX9MDuU+Jz8vfaH1onbYcvjciOfI7ZfLXg69Er6aGM37U+7PyteGr0//5fLXzbHIsaE3ojdTbwvfqbyrfr/4fdt44PjTD8kfJj7mf1L5VPOZ/rnzS8SXFxNrJnGTZV+NvrZ88/32eCp5akrIErFmRgEUonBcHABvqwEgRwFAuQ0AcdnsfD0j0Ow/wQyB/8SzM/iMIJNLvQsAQQhOj3HHETXIRmaSVgACEX+oC4CtrCQ6NwvPzO3Toon8Nyy7AQieo48zdhHAP2R2pv+u739aIMn6N/svESwLGtLu8c4AAACKZVhJZk1NACoAAAAIAAQBGgAFAAAAAQAAAD4BGwAFAAAAAQAAAEYBKAADAAAAAQACAACHaQAEAAAAAQAAAE4AAAAAAAAAkAAAAAEAAACQAAAAAQADkoYABwAAABIAAAB4oAIABAAAAAEAAALOoAMABAAAAAEAAAD8AAAAAEFTQ0lJAAAAU2NyZWVuc2hvdNGZ0GgAAAAJcEhZcwAAFiUAABYlAUlSJPAAAAHWaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPGV4aWY6UGl4ZWxZRGltZW5zaW9uPjI1MjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj43MTg8L2V4aWY6UGl4ZWxYRGltZW5zaW9uPgogICAgICAgICA8ZXhpZjpVc2VyQ29tbWVudD5TY3JlZW5zaG90PC9leGlmOlVzZXJDb21tZW50PgogICAgICA8L3JkZjpEZXNjcmlwdGlvbj4KICAgPC9yZGY6UkRGPgo8L3g6eG1wbWV0YT4K3WnnYQAAABxpRE9UAAAAAgAAAAAAAAB+AAAAKAAAAH4AAAB+AAAxtR1Ih6gAADGBSURBVHgB7J0HuCRF1YZ7gSXnnFlyBskZlywsOSiCIJL9RQVBJAgiCAgCKogEBRXJOSwZyTnnHJYsQcIumQX+89ZO3e07OzO3u7p7enrmq+fp2z1ze7qr3zp16tSpU9WDRo4c9U2kJAIiIAIiIAIiIAIiIAIi0JLAIBnOLfnonyIgAiIgAiIgAiIgAiLgCMhwliCIgAiIgAiIgAiIgAiIQAICMpwTQNIpIiACIiACIiACIiACIiDDWTIgAiIgAiIgAiIgAiIgAgkIyHBOAEmniIAIiIAIiIAIiIAIiIAMZ8mACIiACIiACIiACIiACCQgIMM5ASSdIgIiIAIiIAIiIAIiIAIynCUDIiACIiACIiACIiACIpCAgAznBJB0igiIgAiIgAiIgAiIgAjIcJYMiIAIiIAIiIAIiIAIiEACAjKcE0DSKSIgAiIgAiIgAiIgAiIgw1kyIAIiIAIiIAIiIAIiIAIJCMhwTgBJp4iACIiACIiACIiACIiADGfJgAiIgAiIgAiIgAiIgAgkICDDOQEknSICIiACIiACIiACIiACMpwlAyIgAiIgAiIgAiIgAiKQgIAM5wSQdIoIiIAIiIAIiIAIiIAIyHCWDIiACIiACIiACIiACIhAAgIynBNA0ikiIAIiIAIiIAIiIAIiIMNZMiACIiACIiACIiACIiACCQjIcE4ASaeIgAiIgAiIgAiIgAiIgAxnyYAIiIAIiIAIiIAIiIAIJCDQHsN5UIKcVPGUb6qY6Zzy3K1lmhOe4Mv0mkxJjoJFpeUPe02O4jAkU3Ea+R33mkxJjvKTnfiVukCOijecu134ukAI4jKd6LjbyzQRhAJP6hWZkhwVKER26V6RozhFyVScRv7HvSJTkqP8ZSd+xYrLUWvDWcITL2odi4AIiIAIiIAIiIAI9DCBMYazDOQeFgE9ugiIgAiIgAiIgAiIQBICMpyTUNI5IiACIiACIiACIiACPU9AhnPPi4AAiIAIiIAIiIAIiIAIJCEgwzkJJZ0jAiIgAiIgAiIgAiLQ8wQGjRw1quLzG3u+DAVABERABERABERABESgDQRkOLcBsm4hAiIgAiIgAiIgAiJQfQIynKtfhnoCERABERABERABERCBNhCQ4dwGyLqFCIiACIiACIiACIhA9QnIcK5+GfbkE7Rr6XFNAOge8WqXzHhikh1Porf27ZIzyVf3yFW7ZCZOTPITp5HuWIZzOl46u0MItEvRSLl0SIHnkI12yYzPqmTHk+itfbvkTPLVPXLVLpmJE5P8xGmkO5bhnI6Xzu4AAu1WMlIwHVDoOWSh3XJDliU7ORRchS7RbhmTfFVIOFpktd1yQ1YkOy0KZIB/dZzhXIYA1TOSQNUTCfvcCWUZlvPifiXZSs9WcjSWmeRnLIusR5Kr/gQlW/15JP0kORpDqpfkpy2Gcy8IVrcLTS+UYVJF2Y7zJE/toNwb9+h2WaIUpZ/aJ8vdLk+SJcnSQAQKN5x7RQilTAYSNf0/DQHJUxpaOrcVAclSKzr6X1oCkqe0xHR+KwJVlCcZzq1KNMX/qlj4KR5PHp00sHI4V/KUA0RdwhGQLEkQ8iQgecqTpq5VRXmS4ZyT3Fax8NM8eq+MHKRhUuS5kqci6fbWtSVLvVXeRT+t5Klowr11/SrKUz/DWcZRbwmsnlYEREAEREAEREAERCA5gT7DWUZzcmg6UwREQAREQAREQAREoPcIyHDuvTLXE4uACIiACIiACIiACAQQkOEcAE0/EQEREAEREAEREAER6D0CMpx7r8z1xCIgAiIgAiIgAiIgAgEEZDgHQNNPREAEREAEREAEREAEeo/AoFGjRlVxNZDeKyk9sQiIgAiIgAiIgAiIQKkEZDiXil83FwEREAEREAEREAERqAoBGc5VKSnlUwREQAREQAREQAREoFQCMpxLxa+bi4AIiIAIiIAIiIAIVIWADOeqlJTyKQIiIAIiIAIiIAIiUCoBGc6l4tfNRUAEREAEREAEREAEqkJAhnNVSkr5FAEREAEREAEREAERKJWADOdS8evmIiACIiACIiACIiACVSEgw7kqJaV8ioAIiIAIiIAIiIAIlEpAhnOp+HVzERABERABERABERCBqhCQ4VyVklI+RUAEREAEREAEREAESiUgw7lU/Lq5CIiACIiACIiACIhAVQjIcK5KSSmfIiACIiACIiACIiACpRKQ4Vwqft1cBERABERABERABESgKgRkOFelpJRPERABERABERABERCBUgnIcC4Vv24uAiIgAiIgAiIgAiJQFQIynKtSUsqnCIiACIiACIiACIhAqQRkOJeKXzcXAREQAREQAREQARGoCgEZzlUpKeVTBERABERABERABESgVAIynEvFr5uLgAiIgAiIgAiIgAhUhYAM56qUlPIpAiIgAiIgAiIgAiJQKgEZzqXi181FQAREQAREQAREQASqQkCGc1VKSvkUAREQAREQAREQAREolYAM51Lx6+YiIAIiIAIiIAIiIAJVISDDuSolpXyKgAiIgAiIgAiIgAiUSkCGc6n4dXMREAEREAEREAEREIGqEJDhXJWSUj5FQAREQAREQAREQARKJSDDuVT8urkIiIAIiIAIiIAIiEBVCMhwrkpJKZ8iIAIiIAIiIAIiIAKlEpDhXCp+3VwEREAEREAEREAERKAqBGQ4V6WklE8REAEREAEREAEREIFSCchwLhW/bi4CIiACIiACIiACIlAVAjKcq1JSyqcIiIAIiIAIiIAIiECpBGQ4l4pfNxcBERABERABERABEagKARnOVSkp5VMEREAEREAEREAERKBUAjKcS8Wvm4uACIiACIiACIiACFSFgAznqpSU8ikCIiACIiACIiACIlAqARnOpeLXzUVABERABERABERABKpCQIZzVUpK+RQBERABERABERABESiVgAznUvHr5iIgAiIgAiIgAiIgAlUhIMO5KiWlfIqACIiACIiACIiACJRKQIZzqfh1cxEQAREQAREQAREQgaoQkOFclZJSPkVABERABERABERABEolIMO5VPy6uQiIgAiIgAiIgAiIQFUIyHCuSkkpnyIgAiIgAiIgAiIgAqUSGDRy1KhvyMGgUrOhm4uACIiACIiACIiACIhAZxOQ4dzZ5aPciYAIiIAIiIAIiIAIdAgBGc4dUhDKhgiIgAiIgAiIgAiIQGcTkOHc2eWj3ImACIiACIiACIiACHQIARnOHVIQyoYIiIAIiIAIiIAIiEBnE+gznMmmJggOXFjffPNN9NVXX0Vffvml248ePTpi47uvv/464v9f28b+m9rnvu/s86BBg6Lxxhuvb8/x+OOPHw2y/Xi1//Gd3yaYYIKIbfDgwQNnrovO+OKLL/q4On7GDr59jGtsm30HClh73p6n/+z39d9TFp45/1MSAREQARFoTYA27ytrB2kX0d2+ffT62rWHvl2s2/sro299O+j2tXZwfNujl8enLWRf09F8ls3i6WnfTgL9DOciblw1wfZGWnwfP8ZA/uyzz6JPPvkk+vTTT/u2zz//vE9ZfGVGHeehSFAcozmubV4JeGMYg3jCCSd0SmFwzUB2hlvt+0kmnjiadNJJo0knm2yswY1BWFMq3vArouzi18y7HD1Tb/jWK9hRo0Y5xnD1nZNWe8/Xn0PeYeOVrDeG4c1x/fe+PCgLeE8yySSuXLyBzZ7ftIu3W+omXgA5HMMcPp55Dpcc05AZT7ikSXnLk38mno/nDEmUMZuXj5BrpPkN+fQGRprf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ERTE7s8CKoqoqH8QVCwoiIUqIogKUqWI0kEQQXoTQpHQCQmEQCD5v9+5d3I3x3PuPTtbzu45z+Sz2b17dqc88847z7zzzmzfzFmz5rsKh3vuvtv96U9/cmeddZa76667MuV09dVXd4cffrjbYMMN3ZJLLpkprnZenvPii+7ee+91v/jFL9xvf/tbN39+NaDu6+tze++9t3v/Bz7g3vSmN7nRo0cvKM4ZZ5zhzjS8L7jgggX3Yi8++clPuk996lPuNSuuuFAasfGV8R519OQTT7hLL73U19v999/vnnvuueikx40b59Zff3331a9+1a277rpuyaWWahrXvHnz3KuvvOLl5dprr3V/+ctf3L///W/35JNPOn5LE0aMGOHGjBnj1lhjDfeWt7zF7bzLLm7NNdd0S7VIu1Xcd915p7vwwgu97N53332p8xHiHTlypE9/3333dVtvvbVbdbXVwk+pz9dcc4374x//6M7+8589NqkjGHhhjz32cLvttpvb4V3vio2irfdmzJjh0GHf+9733CWXXNLWO73wEO2CY9FFF3WLLbaYQzdvaHr5jW98o5ePZZdddkgYHpw2zf3tb39zhx56qHvqqaeidCt6cMqUKW7y5Mlu4sSJQ6ZXlR/XW2899/a3v93ttPPOHrss+Xppzhw3a9Ys98wzz/jj2WefdTNM182aOdPf57c59sycl15yc19+2b08d657xY65A8crpq/C9cv2O88S0KHJo917vPPqq696PcOZuEO84b5PIOV/Y8eOda997WvdZptt5vYxHfS6172u1P7oaZPP22+/3R1yyCHupptuitajK6+8sttuu+18PItOmOCQX4XOINBXdeJ86623uhNPOMETmQceeCATSqussor72te+5t761re6FVdaKVNc7bw80xTQFZdf7olHHkS0nTSHe4bGBpE54IAD3N4f+pB7zWte4/8O7x3361/7gcrVV18dbkWfP2Tx7/fRj7rXv/71DuVV9RCU9Q3XX+/OOeccd8opp7iXrNPgfmxYbrnl3Nve9jav7JYxMpAcpCTjJB06rPPPP99ddNFFnhTEpk0aiy++uHvHO97hdth+e7e5pT/WSEpaRfv000+7O++4ww82Ufh0YrFhmWWWcW9+85vdxz/+cbepdWCQ+5hw7rnnuhOOP97dcMMNjvYVE8Dh85//vCfO6xtRKzI8+t//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" alt="goai.rest" style="max-height:44px; filter:{_logo_filter}; mix-blend-mode:{_logo_blend};" />
    </div>''', unsafe_allow_html=True)
    _cap_color = "#666" if not IS_LIGHT else "#999"
    st.markdown(f'<div style="font-size:.7rem;color:{_cap_color};margin:-8px 0 0;">{t("restaurant_intelligence")}</div>', unsafe_allow_html=True)
    # --- Карточка пользователя ---
    _user_initials = CURRENT_USER["name"][:2].upper()
    _avatar_bg = "#534AB7" if not IS_LIGHT else "#7F77DD"
    _card_bg = "rgba(30,30,50,.5)" if not IS_LIGHT else "#fff"
    _card_border = "rgba(255,255,255,.08)" if not IS_LIGHT else "rgba(0,0,0,.1)"
    st.markdown(f'''<div style="background:{_card_bg};border:0.5px solid {_card_border};border-radius:10px;margin:4px 0 4px;overflow:hidden;">
        <div style="display:flex;align-items:center;gap:10px;padding:10px 12px;">
            <div style="width:30px;height:30px;border-radius:50%;background:{_avatar_bg};display:flex;align-items:center;justify-content:center;color:#fff;font-size:11px;font-weight:500;flex-shrink:0;">{_user_initials}</div>
            <div style="flex:1;min-width:0;">
                <div style="font-size:.8rem;font-weight:600;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{CURRENT_USER["name"]}</div>
                <div style="font-size:.62rem;color:var(--t3);">{CURRENT_USER["username"]}</div>
            </div>
        </div>
    </div>''', unsafe_allow_html=True)
    # --- Action buttons row (compact, Unicode symbols) ---
    _bc1, _bc2, _bc3, _bc4, _bc5 = st.columns(5)
    with _bc1:
        if st.button("↻", key="refresh_btn", help=t("refresh")):
            st.cache_data.clear()
            for k in list(st.session_state.keys()):
                if k.startswith("_pdata_") or k.startswith("sh_purchases_") or k.startswith("_qc_") or k.startswith("_stock_"):
                    del st.session_state[k]
            st.rerun()
    with _bc2:
        _ti = "◐" if IS_LIGHT else "◑"
        if st.button(_ti, key="theme_btn", help=t("btn_theme")):
            new_theme = "light" if st.session_state["_theme"] == "dark" else "dark"
            st.session_state["_theme"] = new_theme
            st.query_params["theme"] = new_theme
            st.rerun()
    with _bc3:
        _lang_icon = "EN" if _get_lang() == "ru" else "RU"
        if st.button(_lang_icon, key="lang_btn", help="English / Русский"):
            st.session_state["_lang"] = "en" if _get_lang() == "ru" else "ru"
            st.rerun()
    with _bc4:
        if st.button("⚙", key="nav_lk_top", help=t("btn_account")):
            st.session_state["_page"] = "Личный кабинет"
            st.rerun()
    with _bc5:
        if st.button("↗", key="logout_btn", help=t("btn_logout")):
            st.session_state.pop("_auth_user", None)
            st.rerun()

# --- Period selector function (called inside each page) ---
def period_selector(key_suffix="main"):
    """Inline period selector — returns (date_from, date_to)."""
    today = datetime.now().date()
    if "_period" not in st.session_state:
        st.session_state["_period"] = "Сегодня"
    if "_date_from" not in st.session_state:
        st.session_state["_date_from"] = today
        st.session_state["_date_to"] = today

    _PERIOD_KEYS = ["Сегодня","Вчера","7 дней","30 дней","90 дней","Произвольный"]
    _period_labels = [_period_key_to_label(k) for k in _PERIOD_KEYS]
    _is_custom = st.session_state["_period"] == "Произвольный"

    with st.container():
        if _is_custom:
            pc = st.columns([1, 3, 2, 2, 1, 1])
        else:
            pc = st.columns([1, 3, 1, 1, 14])
        # ◀ Назад
        with pc[0]:
            if st.button("◀", key=f"prev_{key_suffix}", help=t("day_back")):
                st.session_state["_date_from"] = st.session_state["_date_from"] - timedelta(1)
                st.session_state["_date_to"] = st.session_state["_date_to"] - timedelta(1)
                st.session_state["_period"] = "Произвольный"
                st.rerun()
        # Селектор периода
        with pc[1]:
            _cur_label = _period_key_to_label(st.session_state["_period"])
            _cur_idx = _period_labels.index(_cur_label) if _cur_label in _period_labels else 0
            _sel_label = st.selectbox(t("period"), _period_labels, index=_cur_idx,
                key=f"_period_sel_{key_suffix}", label_visibility="collapsed")
            period = _period_label_to_key(_sel_label)
            if period != st.session_state["_period"]:
                st.session_state["_period"] = period
                st.rerun()
            st.session_state["_period"] = period
        if _is_custom:
            with pc[2]:
                d1 = st.date_input(t("from"), st.session_state["_date_from"], key=f"_d1_{key_suffix}", label_visibility="collapsed")
            with pc[3]:
                d2 = st.date_input(t("to"), st.session_state["_date_to"], key=f"_d2_{key_suffix}", label_visibility="collapsed")
            _fwd_idx, _ref_idx = 4, 5
        else:
            if period == "Сегодня": d1 = d2 = today
            elif period == "Вчера": d1 = d2 = today - timedelta(1)
            elif period == "7 дней": d1, d2 = today - timedelta(7), today
            elif period == "30 дней": d1, d2 = today - timedelta(30), today
            elif period == "90 дней": d1, d2 = today - timedelta(90), today
            else: d1, d2 = today - timedelta(7), today
            _fwd_idx, _ref_idx = 2, 3
        # ▶ Вперёд
        with pc[_fwd_idx]:
            _can_fwd = d2 < today
            if st.button("▶", key=f"next_{key_suffix}", help=t("day_fwd"), disabled=not _can_fwd):
                st.session_state["_date_from"] = st.session_state["_date_from"] + timedelta(1)
                st.session_state["_date_to"] = min(st.session_state["_date_to"] + timedelta(1), today)
                st.session_state["_period"] = "Произвольный"
                st.rerun()
        # ↻ Обновить
        with pc[_ref_idx]:
            if st.button("↻", key=f"refresh_{key_suffix}", help=t("refresh")):
                st.cache_data.clear()
                for k in list(st.session_state.keys()):
                    if k.startswith("_pdata_") or k.startswith("sh_purchases_") or k.startswith("_qc_") or k.startswith("_stock_"):
                        del st.session_state[k]
                st.rerun()
        st.session_state["_date_from"] = d1
        st.session_state["_date_to"] = d2
    return d1, d2

# Default period (used before any page calls period_selector)
today = datetime.now().date()
date_from = st.session_state.get("_date_from", today)
date_to = st.session_state.get("_date_to", today)

if not IS_DEMO:
    conn = get_connection()
    if conn is None: st.stop()
else:
    conn = None
    st.markdown("""<div style="background:linear-gradient(90deg,rgba(0,255,106,.08),rgba(200,255,0,.08));
        border:1px solid rgba(0,255,106,.15);border-radius:10px;padding:8px 16px;margin-bottom:12px;
        display:flex;align-items:center;gap:10px;">
        <span style="background:var(--green);color:#000;font-weight:800;font-size:.65rem;padding:2px 8px;border-radius:6px;letter-spacing:.05em;">DEMO</span>
        <span style="font-size:.78rem;color:var(--t2);">Синтетические данные · 2 заведения · 2024–2026</span>
    </div>""", unsafe_allow_html=True)

# ============================================================
# СПРАВОЧНИКИ (кэш на 1 час)
# ============================================================
@st.cache_data(ttl=3600)
def load_restaurants():
    return run_query("SELECT SIFR, NAME FROM RESTAURANTS WHERE NAME IS NOT NULL AND STATUS > 0")

@st.cache_data(ttl=3600)
def load_employees():
    return run_query("SELECT SIFR, NAME FROM EMPLOYEES WHERE NAME IS NOT NULL AND (DBSTATUS IS NULL OR DBSTATUS!=-1)")

@st.cache_data(ttl=3600)
def load_categories():
    return run_query("SELECT SIFR, NAME, PARENT FROM CATEGLIST WHERE NAME IS NOT NULL AND (DBSTATUS IS NULL OR DBSTATUS!=-1)")

@st.cache_data(ttl=3600)
def load_cashgroups():
    return run_query("SELECT SIFR, NAME FROM CASHGROUPS WHERE NAME IS NOT NULL AND (DBSTATUS IS NULL OR DBSTATUS!=-1)")

# ============================================================
# ЗАПРОСЫ ДАННЫХ
# ============================================================
@st.cache_data(ttl=3600)
def load_orders(d1, d2):
    return run_query("""
        SELECT VISIT, MIDSERVER, IDENTINVISIT, OPENTIME, ENDSERVICE,
            GUESTSCOUNT, PRICELISTSUM, TOPAYSUM, PAIDSUM, DISCOUNTSUM,
            TOTALDISHPIECES, TABLENAME, MAINWAITER, PAID, ICOMMONSHIFT, DURATION
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) ORDER BY OPENTIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_dishes(d1, d2):
    return run_query("""
        SELECT TOP 30 sd.SIFR as DISH_ID, mi.NAME as DISH_NAME,
            SUM(sd.QUANTITY) as TOTAL_QTY, SUM(sd.PRLISTSUM) as TOTAL_SUM,
            COUNT(DISTINCT CONCAT(sd.VISIT,'-',sd.ORDERIDENT)) as ORDER_COUNT
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0
        GROUP BY sd.SIFR, mi.NAME ORDER BY TOTAL_SUM DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_payments(d1, d2):
    return run_query("""
        SELECT p.PAYLINETYPE, SUM(p.BASICSUM) as TOTAL_SUM, COUNT(*) as PAY_COUNT
        FROM PAYMENTS p JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1) AND p.STATE=6
        GROUP BY p.PAYLINETYPE""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_check_count(d1, d2):
    """Кол-во уникальных чеков (PRINTCHECKUNI) — ближе к ОФД чем кол-во заказов."""
    return run_query("""
        SELECT
          COUNT(DISTINCT CAST(p.VISIT AS VARCHAR) + CAST(p.MIDSERVER AS VARCHAR) + CAST(p.PRINTCHECKUNI AS VARCHAR)) as CHECKS,
          COUNT(DISTINCT CAST(o.VISIT AS VARCHAR) + CAST(o.MIDSERVER AS VARCHAR) + CAST(o.IDENTINVISIT AS VARCHAR)) as ORDERS
        FROM PAYMENTS p
        JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_tax_breakdown(d1, d2):
    """Выручка по ставкам НДС из SESSIONDISHES.ITAXDISHTYPE → TAXDISHTYPES."""
    return run_query("""
        SELECT
            tdt.NAME as TAX_NAME,
            sd.ITAXDISHTYPE as TAX_TYPE,
            COUNT(DISTINCT CAST(sd.VISIT AS VARCHAR) + CAST(sd.MIDSERVER AS VARCHAR) + CAST(sd.ORDERIDENT AS VARCHAR)) as ORDER_COUNT,
            SUM(sd.QUANTITY) as QTY,
            SUM(sd.PRLISTSUM) as REVENUE
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN TAXDISHTYPES tdt ON sd.ITAXDISHTYPE=tdt.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
          AND sd.QUANTITY > 0
        GROUP BY tdt.NAME, sd.ITAXDISHTYPE
        ORDER BY SUM(sd.PRLISTSUM) DESC""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_fiscal_checks(d1, d2):
    """Статистика фискальных чеков из PRINTCHECKS по кассам/столовым."""
    return run_query("""
        SELECT r.NAME as REST_NAME, cg.NAME as CASH_NAME,
            COUNT(*) as TOTAL_CHECKS,
            SUM(CASE WHEN pc.DELETED=0 OR pc.DELETED IS NULL THEN 1 ELSE 0 END) as ACTIVE_CHECKS,
            SUM(CASE WHEN pc.DELETED=1 THEN 1 ELSE 0 END) as DELETED_CHECKS,
            CAST(SUM(CASE WHEN pc.DELETED=0 OR pc.DELETED IS NULL THEN pc.BASICSUM ELSE 0 END) / 100.0 AS INT) as ACTIVE_SUM,
            SUM(CASE WHEN pc.CORRECTIONRECEIPTTYPE IS NOT NULL AND pc.CORRECTIONRECEIPTTYPE > 0 THEN 1 ELSE 0 END) as CORRECTIONS,
            SUM(CASE WHEN pc.BILLERROR IS NOT NULL AND pc.BILLERROR > 0 THEN 1 ELSE 0 END) as BILL_ERRORS,
            SUM(CASE WHEN pc.FISCALIZATIONTYPE IS NULL OR pc.FISCALIZATIONTYPE = 0 THEN 1 ELSE 0 END) as NOT_FISCAL
        FROM PRINTCHECKS pc
        JOIN ORDERS o ON pc.VISIT=o.VISIT AND pc.MIDSERVER=o.MIDSERVER AND pc.ORDERIDENT=o.IDENTINVISIT
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        LEFT JOIN CASHGROUPS cg ON pc.MIDSERVER=cg.SIFR
        WHERE pc.CLOSEDATETIME >= %s AND pc.CLOSEDATETIME < DATEADD(DAY,1,%s)
          AND (pc.DBSTATUS IS NULL OR pc.DBSTATUS!=-1)
          AND r.NAME IS NOT NULL
        GROUP BY r.NAME, cg.NAME
        ORDER BY TOTAL_CHECKS DESC""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_fiscal_summary(d1, d2):
    """Общая сводка фискальных чеков."""
    return run_query("""
        SELECT
            COUNT(*) as TOTAL,
            SUM(CASE WHEN DELETED=0 OR DELETED IS NULL THEN 1 ELSE 0 END) as ACTIVE,
            SUM(CASE WHEN DELETED=1 THEN 1 ELSE 0 END) as DELETED,
            CAST(SUM(CASE WHEN DELETED=0 OR DELETED IS NULL THEN BASICSUM ELSE 0 END) / 100.0 AS INT) as ACTIVE_SUM,
            SUM(CASE WHEN CORRECTIONRECEIPTTYPE IS NOT NULL AND CORRECTIONRECEIPTTYPE > 0 THEN 1 ELSE 0 END) as CORRECTIONS,
            SUM(CASE WHEN BILLERROR IS NOT NULL AND BILLERROR > 0 THEN 1 ELSE 0 END) as BILL_ERRORS,
            SUM(CASE WHEN FISCALIZATIONTYPE IS NULL OR FISCALIZATIONTYPE = 0 THEN 1 ELSE 0 END) as NOT_FISCAL,
            SUM(CASE WHEN FISCALIZATIONTYPE = 2 THEN 1 ELSE 0 END) as FISCAL_OK
        FROM PRINTCHECKS
        WHERE CLOSEDATETIME >= %s AND CLOSEDATETIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1)""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_voids(d1, d2):
    return run_query("""
        SELECT dv.OPENNAME as VOID_REASON, dv.DATETIME, dv.QUANTITY, dv.PRLISTSUM, dv.DISHUNI,
            dv.ICREATOR as CREATOR_ID, e1.NAME as CREATOR_NAME,
            dv.IAUTHOR as AUTHOR_ID, e2.NAME as AUTHOR_NAME,
            mi.NAME as DISH_NAME, o.MIDSERVER,
            r.NAME as REST_NAME
        FROM DISHVOIDS dv
        JOIN ORDERS o ON dv.VISIT=o.VISIT AND dv.MIDSERVER=o.MIDSERVER AND dv.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN SESSIONDISHES sd ON dv.VISIT=sd.VISIT AND dv.MIDSERVER=sd.MIDSERVER AND dv.ORDERIDENT=sd.ORDERIDENT AND dv.DISHUNI=sd.UNI
        LEFT JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        LEFT JOIN EMPLOYEES e1 ON dv.ICREATOR=e1.SIFR
        LEFT JOIN EMPLOYEES e2 ON dv.IAUTHOR=e2.SIFR
        LEFT JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (dv.DBSTATUS IS NULL OR dv.DBSTATUS!=-1)""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_deleted_checks(d1, d2):
    """Отменённые (удалённые) чеки"""
    return run_query("""
        SELECT pc.DELETEDATETIME, pc.CLOSEDATETIME, pc.BILLDATETIME,
            pc.BASICSUM, pc.TOPAYSUM, pc.PRLISTSUM, pc.DISCOUNTSUM,
            pc.GUESTCNT, pc.DELETED, pc.ISBILL,
            pc.OPENVOIDNAME,
            e1.NAME as CREATOR_NAME,
            e2.NAME as DELETE_MANAGER_NAME,
            e3.NAME as DELETE_PERSON_NAME,
            pc.UNDOTRANSACTIONS,
            r.NAME as REST_NAME
        FROM PRINTCHECKS pc
        LEFT JOIN EMPLOYEES e1 ON pc.ICREATOR=e1.SIFR
        LEFT JOIN EMPLOYEES e2 ON pc.IDELETEMANAGER=e2.SIFR
        LEFT JOIN EMPLOYEES e3 ON pc.IDELETEPERSON=e3.SIFR
        LEFT JOIN ORDERS o ON pc.VISIT=o.VISIT AND pc.MIDSERVER=o.MIDSERVER AND pc.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE pc.DELETED=1
          AND (pc.DELETEDATETIME >= %s AND pc.DELETEDATETIME < DATEADD(DAY,1,%s)
               OR pc.CLOSEDATETIME >= %s AND pc.CLOSEDATETIME < DATEADD(DAY,1,%s))
          AND (pc.DBSTATUS IS NULL OR pc.DBSTATUS!=-1)
        ORDER BY pc.DELETEDATETIME DESC""", (str(d1),str(d2),str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_cancel_operations(d1, d2):
    """Операции отмен из журнала"""
    return run_query("""
        SELECT ol.DATETIME, op.NAME as OPERATION,
            e1.NAME as OPERATOR_NAME, e2.NAME as MANAGER_NAME,
            ol.ORDERSUMBEFORE, ol.ORDERSUMAFTER,
            ol.ORDERSUMBEFORE - ol.ORDERSUMAFTER as DIFF,
            r.NAME as REST_NAME
        FROM OPERATIONLOG ol
        LEFT JOIN OPERATIONS op ON ol.OPERATION=op.SIFR
        LEFT JOIN EMPLOYEES e1 ON ol.OPERATOR=e1.SIFR
        LEFT JOIN EMPLOYEES e2 ON ol.MANAGER=e2.SIFR
        LEFT JOIN GLOBALSHIFTS gs ON ol.MIDSERVER=gs.MIDSERVER AND ol.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE ol.DATETIME >= %s AND ol.DATETIME < DATEADD(DAY,1,%s)
          AND ol.OPERATION IN (228, 460, 482, 206, 904, 261)
        ORDER BY ol.DATETIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_payments_by_type(d1, d2):
    """Платежи по типам оплат"""
    return run_query("""
        SELECT p.PAYLINETYPE, p.STATE,
            SUM(p.BASICSUM) as TOTAL_SUM, COUNT(*) as PAY_COUNT,
            e.NAME as CASHIER_NAME
        FROM PAYMENTS p
        JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN EMPLOYEES e ON p.ICREATOR=e.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1)
        GROUP BY p.PAYLINETYPE, p.STATE, e.NAME""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_cashier_speed(d1, d2):
    """Скорость обслуживания по кассирам"""
    return run_query("""
        SELECT e.NAME as CASHIER,
            COUNT(*) as ORDERS,
            AVG(o.DURATION) as AVG_SEC,
            MIN(o.DURATION) as MIN_SEC,
            MAX(o.DURATION) as MAX_SEC,
            AVG(o.TOTALDISHPIECES) as AVG_DISHES,
            SUM(o.TOPAYSUM) as REVENUE,
            AVG(o.TOPAYSUM) as AVG_CHECK
        FROM ORDERS o
        LEFT JOIN EMPLOYEES e ON o.MAINWAITER=e.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1)
          AND o.PAID=1 AND o.DURATION > 0 AND o.DURATION < 3600
          AND o.MAINWAITER > 0
        GROUP BY e.NAME ORDER BY AVG(o.DURATION)""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_speed_distribution(d1, d2):
    """Распределение заказов по времени обслуживания"""
    return run_query("""
        SELECT
            CASE
                WHEN DURATION < 15 THEN '01. < 15 сек'
                WHEN DURATION < 30 THEN '02. 15-30 сек'
                WHEN DURATION < 60 THEN '03. 30-60 сек'
                WHEN DURATION < 120 THEN '04. 1-2 мин'
                WHEN DURATION < 300 THEN '05. 2-5 мин'
                WHEN DURATION < 600 THEN '06. 5-10 мин'
                ELSE '07. > 10 мин'
            END as TIME_RANGE,
            COUNT(*) as ORDER_COUNT,
            AVG(TOPAYSUM) as AVG_CHECK
        FROM ORDERS
        WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
          AND DURATION > 0 AND DURATION < 7200
        GROUP BY CASE
                WHEN DURATION < 15 THEN '01. < 15 сек'
                WHEN DURATION < 30 THEN '02. 15-30 сек'
                WHEN DURATION < 60 THEN '03. 30-60 сек'
                WHEN DURATION < 120 THEN '04. 1-2 мин'
                WHEN DURATION < 300 THEN '05. 2-5 мин'
                WHEN DURATION < 600 THEN '06. 5-10 мин'
                ELSE '07. > 10 мин'
            END
        ORDER BY 1""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_speed_by_hour(d1, d2):
    """Средняя скорость по часам дня"""
    return run_query("""
        SELECT DATEPART(HOUR, OPENTIME) as HOUR,
            AVG(DURATION) as AVG_SEC,
            COUNT(*) as ORDERS
        FROM ORDERS
        WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
          AND DURATION > 0 AND DURATION < 3600
        GROUP BY DATEPART(HOUR, OPENTIME) ORDER BY HOUR""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_speed_by_restaurant(d1, d2):
    """Средняя скорость по столовым"""
    return run_query("""
        SELECT r.NAME as REST_NAME,
            COUNT(*) as ORDERS,
            AVG(o.DURATION) as AVG_SEC,
            AVG(o.TOTALDISHPIECES) as AVG_DISHES
        FROM ORDERS o
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
          AND o.DURATION > 0 AND o.DURATION < 3600 AND r.NAME IS NOT NULL
        GROUP BY r.NAME ORDER BY AVG(o.DURATION)""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_card_problems(d1, d2):
    """Проблемы с банковскими картами"""
    return run_query("""
        SELECT pe.TRANSACTIONSTATUS, pe.AUTHTYPE,
            COUNT(*) as CNT,
            r.NAME as REST_NAME
        FROM PAYMENTSEXTRA pe
        JOIN ORDERS o ON pe.VISIT=o.VISIT AND pe.MIDSERVER=o.MIDSERVER AND pe.ORDERIDENT=o.IDENTINVISIT
        LEFT JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (pe.DBSTATUS IS NULL OR pe.DBSTATUS!=-1)
        GROUP BY pe.TRANSACTIONSTATUS, pe.AUTHTYPE, r.NAME""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_hourly(d1, d2):
    return run_query("""
        SELECT DATEPART(HOUR,OPENTIME) as HOUR,
            COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDER_COUNT,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
        GROUP BY DATEPART(HOUR,OPENTIME) ORDER BY HOUR""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_daily(d1, d2):
    return run_query("""
        SELECT CAST(OPENTIME AS DATE) as DAY,
            COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDER_COUNT,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS, AVG(TOPAYSUM) as AVG_CHECK
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
        GROUP BY CAST(OPENTIME AS DATE) ORDER BY DAY""", (str(d1),str(d2)))

# --- НОВЫЕ ЗАПРОСЫ ---

@st.cache_data(ttl=3600)
def load_revenue_by_restaurant(d1, d2):
    return run_query("""
        SELECT gs.IRESTAURANT as REST_ID, r.NAME as REST_NAME,
            COUNT(DISTINCT CONCAT(o.VISIT,'-',o.IDENTINVISIT)) as ORDER_COUNT,
            SUM(o.TOPAYSUM) as REVENUE, SUM(o.GUESTSCOUNT) as GUESTS,
            AVG(o.TOPAYSUM) as AVG_CHECK, SUM(o.TOTALDISHPIECES) as DISHES
        FROM ORDERS o
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1 AND r.NAME IS NOT NULL
        GROUP BY gs.IRESTAURANT, r.NAME ORDER BY REVENUE DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_revenue_by_category(d1, d2):
    return run_query("""
        SELECT TOP 20 c.SIFR as CAT_ID, c.NAME as CATEGORY,
            SUM(sd.QUANTITY) as TOTAL_QTY, SUM(sd.PRLISTSUM) as TOTAL_SUM,
            COUNT(DISTINCT CONCAT(sd.VISIT,'-',sd.ORDERIDENT)) as ORDER_COUNT
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        JOIN CATEGLIST c ON mi.PARENT=c.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0
          AND c.NAME IS NOT NULL
        GROUP BY c.SIFR, c.NAME ORDER BY TOTAL_SUM DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_dishes_by_category(d1, d2, cat_id):
    """Топ блюд внутри конкретной категории."""
    return run_query("""
        SELECT TOP 30 mi.NAME as DISH_NAME,
            SUM(sd.QUANTITY) as TOTAL_QTY, SUM(sd.PRLISTSUM) as TOTAL_SUM,
            AVG(sd.PRICE) as AVG_PRICE,
            COUNT(DISTINCT CONCAT(sd.VISIT,'-',sd.ORDERIDENT)) as ORDER_COUNT
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0
          AND mi.PARENT = %s
        GROUP BY mi.NAME ORDER BY TOTAL_SUM DESC""", (str(d1), str(d2), int(cat_id)))

@st.cache_data(ttl=3600)
def load_top_employees(d1, d2):
    return run_query("""
        SELECT TOP 20 o.MAINWAITER as EMP_ID, e.NAME as EMP_NAME,
            COUNT(DISTINCT CONCAT(o.VISIT,'-',o.IDENTINVISIT)) as ORDER_COUNT,
            SUM(o.TOPAYSUM) as REVENUE, AVG(o.TOPAYSUM) as AVG_CHECK,
            SUM(o.GUESTSCOUNT) as GUESTS
        FROM ORDERS o
        LEFT JOIN EMPLOYEES e ON o.MAINWAITER=e.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1 AND o.MAINWAITER>0
        GROUP BY o.MAINWAITER, e.NAME ORDER BY REVENUE DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_monthly_revenue_yoy():
    """Помесячная выручка за все доступные годы — для сезонности год к году."""
    return run_query("""
        SELECT
            YEAR(o.OPENTIME) as Y,
            MONTH(o.OPENTIME) as M,
            FORMAT(o.OPENTIME, 'yyyy-MM') as MONTH_STR,
            COUNT(DISTINCT CONCAT(o.VISIT,'-',o.IDENTINVISIT)) as ORDERS,
            SUM(o.TOPAYSUM) as REVENUE,
            SUM(o.GUESTSCOUNT) as GUESTS,
            SUM(o.TOTALDISHPIECES) as DISHES,
            AVG(o.TOPAYSUM) as AVG_CHECK
        FROM ORDERS o
        WHERE (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
          AND o.OPENTIME >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY YEAR(o.OPENTIME), MONTH(o.OPENTIME), FORMAT(o.OPENTIME, 'yyyy-MM')
        ORDER BY Y, M""")

@st.cache_data(ttl=3600)
def load_clockrecs(d1, d2):
    return run_query("""
        SELECT cr.EMPID, e.NAME as EMP_NAME,
            COUNT(*) as SHIFT_COUNT,
            AVG(cr.DURATION) as AVG_HOURS,
            SUM(CASE WHEN cr.LATENESS=1 THEN 1 ELSE 0 END) as LATE_COUNT,
            MIN(cr.STARTTIME) as FIRST_SHIFT, MAX(cr.ENDTIME) as LAST_SHIFT
        FROM CLOCKRECS cr
        LEFT JOIN EMPLOYEES e ON cr.EMPID=e.SIFR
        WHERE cr.STARTTIME >= %s AND cr.STARTTIME < DATEADD(DAY,1,%s)
          AND (cr.DBSTATUS IS NULL OR cr.DBSTATUS!=-1)
        GROUP BY cr.EMPID, e.NAME ORDER BY SHIFT_COUNT DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_cashinout(d1, d2):
    return run_query("""
        SELECT DATETIME, ISDEPOSIT, ORIGINALSUM, KIND, MIDSERVER,
            ICASHIER, OPENREASONNAME
        FROM CASHINOUT
        WHERE DATETIME >= %s AND DATETIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1)
        ORDER BY DATETIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_daily_by_restaurant(d1, d2):
    return run_query("""
        SELECT CAST(o.OPENTIME AS DATE) as DAY, r.NAME as REST_NAME,
            SUM(o.TOPAYSUM) as REVENUE
        FROM ORDERS o
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1 AND r.NAME IS NOT NULL
        GROUP BY CAST(o.OPENTIME AS DATE), r.NAME
        ORDER BY DAY""", (str(d1),str(d2)))


# --- CANCEL & PAYMENT QUERIES ---

# --- PRICE & ABC QUERIES ---

@st.cache_data(ttl=3600)
def load_shifts(d1, d2):
    return run_query("""
        SELECT gs.SHIFTDATE, gs.CREATETIME, gs.CLOSETIME, gs.CLOSED,
            e.NAME as MANAGER, r.NAME as REST_NAME, gs.MIDSERVER,
            DATEDIFF(MINUTE, gs.CREATETIME, gs.CLOSETIME) as DURATION_MIN
        FROM GLOBALSHIFTS gs
        LEFT JOIN EMPLOYEES e ON gs.IMANAGER=e.SIFR
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE gs.CREATETIME >= %s AND gs.CREATETIME < DATEADD(DAY,1,%s)
          AND r.NAME IS NOT NULL
        ORDER BY gs.CREATETIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_checkmark_errors(d1, d2):
    return run_query("""
        SELECT cm.DATETIME, cm.RES, cm.MESSAGEFROMDRIVER, cm.ERRORCODE,
            mi.NAME as PRODUCT, cm.MIDSERVER,
            r.NAME as REST_NAME
        FROM CHECKMARKRESULTS cm
        LEFT JOIN MENUITEMS mi ON cm.MENUITEMID=mi.SIFR
        LEFT JOIN GLOBALSHIFTS gs ON cm.MIDSERVER=gs.MIDSERVER
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE cm.DATETIME >= %s AND cm.DATETIME < DATEADD(DAY,1,%s)
          AND cm.RES != 0
        ORDER BY cm.DATETIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_checkmark_stats(d1, d2):
    return run_query("""
        SELECT cm.RES, COUNT(*) as CNT
        FROM CHECKMARKRESULTS cm
        WHERE cm.DATETIME >= %s AND cm.DATETIME < DATEADD(DAY,1,%s)
        GROUP BY cm.RES""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_card_errors(d1, d2):
    return run_query("""
        SELECT pe.TRANSACTIONSTATUS, COUNT(*) as CNT
        FROM PAYMENTSEXTRA pe
        JOIN ORDERS o ON pe.VISIT=o.VISIT AND pe.MIDSERVER=o.MIDSERVER AND pe.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (pe.DBSTATUS IS NULL OR pe.DBSTATUS!=-1)
        GROUP BY pe.TRANSACTIONSTATUS""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_card_errors_by_restaurant(d1, d2):
    """Проблемы с картами по столовым — статусы 0 (нет транзакции), 1 (отменена), 6 (возврат)."""
    return run_query("""
        SELECT r.NAME as REST_NAME, pe.TRANSACTIONSTATUS,
            COUNT(*) as CNT, SUM(p.BASICSUM) as TOTAL_SUM
        FROM PAYMENTSEXTRA pe
        JOIN ORDERS o ON pe.VISIT=o.VISIT AND pe.MIDSERVER=o.MIDSERVER AND pe.ORDERIDENT=o.IDENTINVISIT
        JOIN PAYMENTS p ON pe.VISIT=p.VISIT AND pe.MIDSERVER=p.MIDSERVER AND pe.ORDERIDENT=p.ORDERIDENT
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (pe.DBSTATUS IS NULL OR pe.DBSTATUS!=-1)
          AND r.NAME IS NOT NULL
        GROUP BY r.NAME, pe.TRANSACTIONSTATUS
        ORDER BY CNT DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_price_history(d1, d2):
    return run_query("""
        SELECT CAST(o.OPENTIME AS DATE) as DAY, mi.NAME as DISH_NAME,
            AVG(sd.PRICE) as AVG_PRICE, MIN(sd.PRICE) as MIN_PRICE,
            MAX(sd.PRICE) as MAX_PRICE, SUM(sd.QUANTITY) as QTY
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.PRICE>0 AND sd.QUANTITY>0
        GROUP BY CAST(o.OPENTIME AS DATE), mi.NAME
        ORDER BY DAY""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_current_prices(d1, d2):
    """Блюда с изменёнными ценами"""
    return run_query("""
        SELECT TOP 50 mi.NAME as DISH_NAME,
            AVG(sd.PRICE) as AVG_PRICE,
            MIN(sd.PRICE) as MIN_PRICE, MAX(sd.PRICE) as MAX_PRICE,
            COUNT(DISTINCT sd.PRICE) as PRICE_VARIANTS,
            SUM(sd.QUANTITY) as TOTAL_QTY,
            SUM(sd.PRLISTSUM) as TOTAL_SUM,
            MAX(sd.PRICE) - MIN(sd.PRICE) as PRICE_DIFF
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.PRICE>0 AND sd.QUANTITY>0
        GROUP BY mi.NAME
        HAVING COUNT(DISTINCT sd.PRICE) > 1
        ORDER BY MAX(sd.PRICE) - MIN(sd.PRICE) DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=3600)
def load_abc_analysis(d1, d2):
    """ABC-анализ по выручке"""
    return run_query("""
        SELECT mi.NAME as DISH_NAME, c.NAME as CATEGORY,
            SUM(sd.QUANTITY) as TOTAL_QTY,
            SUM(sd.PRLISTSUM) as TOTAL_SUM,
            AVG(sd.PRICE) as AVG_PRICE
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        LEFT JOIN CATEGLIST c ON mi.PARENT=c.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0 AND sd.PRLISTSUM>0
        GROUP BY mi.NAME, c.NAME
        ORDER BY TOTAL_SUM DESC""", (str(d1),str(d2)))


# --- PROACTIVE ANALYSIS ---

@st.cache_data(ttl=3600)
def load_period_comparison(d1, d2):
    """Сравнение текущего и предыдущего периода по столовым"""
    days = (d2 - d1).days + 1
    prev_d2 = d1 - timedelta(days=1)
    prev_d1 = prev_d2 - timedelta(days=days-1)
    
    current = run_query("""
        SELECT gs.IRESTAURANT as REST_ID, r.NAME as REST_NAME,
            COUNT(DISTINCT CONCAT(o.VISIT,'-',o.IDENTINVISIT)) as ORDERS,
            SUM(o.TOPAYSUM) as REVENUE, SUM(o.GUESTSCOUNT) as GUESTS,
            AVG(o.TOPAYSUM) as AVG_CHECK, SUM(o.TOTALDISHPIECES) as DISHES
        FROM ORDERS o
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1 AND r.NAME IS NOT NULL
        GROUP BY gs.IRESTAURANT, r.NAME""", (str(d1),str(d2)))
    
    previous = run_query("""
        SELECT gs.IRESTAURANT as REST_ID, r.NAME as REST_NAME,
            COUNT(DISTINCT CONCAT(o.VISIT,'-',o.IDENTINVISIT)) as ORDERS,
            SUM(o.TOPAYSUM) as REVENUE, SUM(o.GUESTSCOUNT) as GUESTS,
            AVG(o.TOPAYSUM) as AVG_CHECK, SUM(o.TOTALDISHPIECES) as DISHES
        FROM ORDERS o
        JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
        LEFT JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1 AND r.NAME IS NOT NULL
        GROUP BY gs.IRESTAURANT, r.NAME""", (str(prev_d1),str(prev_d2)))
    
    return current, previous, days, prev_d1, prev_d2

@st.cache_data(ttl=3600)
def load_period_totals(d1, d2):
    """Общие метрики за два периода"""
    days = (d2 - d1).days + 1
    prev_d2 = d1 - timedelta(days=1)
    prev_d1 = prev_d2 - timedelta(days=days-1)
    
    cur = run_query("""
        SELECT COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDERS,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS,
            AVG(TOPAYSUM) as AVG_CHECK, SUM(DISCOUNTSUM) as DISCOUNTS,
            SUM(TOTALDISHPIECES) as DISHES
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1""", (str(d1),str(d2)))
    
    prev = run_query("""
        SELECT COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDERS,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS,
            AVG(TOPAYSUM) as AVG_CHECK, SUM(DISCOUNTSUM) as DISCOUNTS,
            SUM(TOTALDISHPIECES) as DISHES
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1""", (str(prev_d1),str(prev_d2)))
    
    return cur, prev, days, prev_d1, prev_d2

@st.cache_data(ttl=3600)
def load_voids_comparison(d1, d2):
    days = (d2 - d1).days + 1
    prev_d2 = d1 - timedelta(days=1)
    prev_d1 = prev_d2 - timedelta(days=days-1)
    
    cur = run_query("""
        SELECT COUNT(*) as VOID_COUNT, SUM(dv.PRLISTSUM) as VOID_SUM
        FROM DISHVOIDS dv JOIN ORDERS o ON dv.VISIT=o.VISIT AND dv.MIDSERVER=o.MIDSERVER AND dv.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (dv.DBSTATUS IS NULL OR dv.DBSTATUS!=-1)""", (str(d1),str(d2)))
    
    prev = run_query("""
        SELECT COUNT(*) as VOID_COUNT, SUM(dv.PRLISTSUM) as VOID_SUM
        FROM DISHVOIDS dv JOIN ORDERS o ON dv.VISIT=o.VISIT AND dv.MIDSERVER=o.MIDSERVER AND dv.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (dv.DBSTATUS IS NULL OR dv.DBSTATUS!=-1)""", (str(prev_d1),str(prev_d2)))
    
    return cur, prev

@st.cache_data(ttl=3600)
def load_dishes_comparison(d1, d2):
    days = (d2 - d1).days + 1
    prev_d2 = d1 - timedelta(days=1)
    prev_d1 = prev_d2 - timedelta(days=days-1)
    
    cur = run_query("""
        SELECT TOP 20 mi.NAME as DISH, SUM(sd.QUANTITY) as QTY, SUM(sd.PRLISTSUM) as SUM
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0
        GROUP BY mi.NAME ORDER BY SUM DESC""", (str(d1),str(d2)))
    
    prev = run_query("""
        SELECT TOP 20 mi.NAME as DISH, SUM(sd.QUANTITY) as QTY, SUM(sd.PRLISTSUM) as SUM
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY>0
        GROUP BY mi.NAME ORDER BY SUM DESC""", (str(prev_d1),str(prev_d2)))
    
    return cur, prev

def generate_proactive_insights(anomalies_text):
    """Gemini анализирует аномалии и даёт рекомендации"""
    return ask_gemini(f"""Ты — опытный управляющий сетью столовых МГУ. Проанализируй аномалии и дай КОНКРЕТНЫЕ рекомендации.

Данные аномалий:
{anomalies_text}

Для каждой аномалии:
1. Назови возможную ПРИЧИНУ (конкретно, не абстрактно)
2. Предложи ДЕЙСТВИЕ (что именно сделать, кому, когда)
3. Оцени СРОЧНОСТЬ (🔴 срочно / 🟡 внимание / 🟢 норма)

Отвечай кратко, по делу, с эмоджи. На русском. Не больше 5-7 пунктов.""")


# ============================================================
# STOREHOUSE ЗАПРОСЫ (REST API)
# ============================================================

# --- Справочники ---

@st.cache_data(ttl=3600)
def sh_load_goods():
    """Товары через GoodsTree — 18 203 товара!"""
    payload = {"procName": "GoodsTree"}
    data, err = sh_api_call("sh5exec", payload)
    if err:
        return pd.DataFrame(), err
    # Таблица 0 = head 209 (пустая), таблица 1 = head 210 (товары)
    df = sh_parse_table(data, table_index=1)
    if df.empty:
        df = sh_parse_table(data, table_index=0)
    # Переименуем ключевые колонки
    mapping = sh_get_alt_names("GoodsTree")
    if mapping:
        df = df.rename(columns={c: mapping.get(c, c) for c in df.columns})
    return df, None

@st.cache_data(ttl=3600)
def sh_load_goods_categories():
    """Категории товаров — 18 категорий"""
    return sh_exec_named("GoodsCategories")

@st.cache_data(ttl=3600)
def sh_load_departs():
    """Склады (Departs) — 37 складов, берём таблицу 106 (index=1)"""
    payload = {"procName": "Departs"}
    data, err = sh_api_call("sh5exec", payload)
    if err:
        return pd.DataFrame(), err
    # Таблица 0 = head 108 (пустая SingleRow), таблица 1 = head 106 (37 складов)
    df = sh_parse_table(data, table_index=1)
    if df.empty:
        df = sh_parse_table(data, table_index=0)
    # Переименуем колонки
    mapping = sh_get_alt_names("Departs")
    if mapping:
        df = df.rename(columns={c: mapping.get(c, c) for c in df.columns})
    return df, None

@st.cache_data(ttl=3600)
def sh_load_divisions():
    """Подразделения (Divisions) — 35 столовых МГУ"""
    return sh_exec_named("Divisions")

# --- Русские названия колонок StoreHouse ---
SH_COL_NAMES = {
    # Общие
    "Rid": "ID", "Name": "Название", "Guid": "GUID",
    "1": "ID", "3": "Название", "4": "GUID",
    # Departs (склады)
    "TypeMask": "Тип", "entity Rid": "ID юрлица", "entity Name": "Юрлицо",
    "entity Tin": "ИНН", "venture Rid": "ID подразделения", "venture Name": "Подразделение",
    "UserOwned": "Свой", "GrpMask": "Маска групп", "MaxCount": "Макс. записей",
    "8": "Тип", "34": "Код", "31": "Свой", "9": "Порядок",
    "102\\1": "ID юрлица", "102\\3": "Юрлицо", "102\\2": "ИНН",
    "103\\1": "ID подразделения", "103\\3": "Подразделение", "111\\8": "Тип связи", "32": "Маска групп",
    # Divisions (подразделения)
    "209\\1": "ID группы", "209\\3": "Группа", "104": "Код рег.", "103": "Дата начала",
    "7\\$ContractNum": "№ договора", "7\\$PriceLstNum": "№ прайса",
    "7\\$Qush": "Qush ID", "7\\$PDocNum": "№ приход. док.",
    "7\\$IDocNum": "№ инвент. док.", "7\\$GDocNum": "№ расход. док.", "7\\Chef": "Шеф-повар",
    # GoodsTree колонки (alt-имена из struct)
    "Rid": "ID", "Flags": "Флаги", "Expiration": "Срок годн. (дни)",
    "GoodsType": "Тип товара",
    "PercInv": "% инвентаризации", "Perc1": "Наценка 1 %", "Perc2": "Наценка 2 %",
    "Price0": "Цена закупки", "Price1": "Цена продажи",
    "Taxe NDS": "НДС", "Taxe NP": "НП", "Taxe NDS*100 ": "НДС×100", "Taxe NP*100": "НП×100",
    "Marsrut": "Маршрут",
    "Group: Rid": "ID группы", "Group: Name": "Группа",
    "Ei: Rid": "ID ед.изм.", "Ei: Name": "Ед. измерения",
    "Ei for otch: Rid": "ID ед.(отчёт)", "Ei for otch: Name": "Ед. (отчёт)",
    "Ei for z: Rid": "ID ед.(заказ)", "Ei for z: Name": "Ед. (заказ)",
    "Ei for a: Rid": "ID ед.(а)", "Ei for a: Name": "Ед. (а)",
    "Ei for k: Rid": "ID ед.(калькул.)", "Ei for k: Name": "Ед. (калькул.)",
    "Komplekt: Rid": "ID комплекта", "Komplekt : Name": "Комплект",
    " Kontr: Rid": "ID поставщика", " Kontr: Name": "Поставщик",
    "Type alk: Rid": "ID типа алк.", "Type alk: Flags": "Флаги алк.",
    "Type alk: Code": "Код алк.", "Type alk: Name": "Тип алкоголя",
    "Energy: BEnergy": "Калорийность", "Energy: Energy0": "Белки",
    "Energy: Energy1": "Жиры", "Energy: Energy2": "Углеводы",
    "MakerRef": "ID производителя",
    # GoodsTree — специфичные alt из struct (Rid-ссылки)
    "RidCmp": "ID комплекта", "RidGGref": "ID группы",
    "RidbMUref": "ID ед.изм.(базов.)", "RidrMUref": "ID ед.изм.(отчёт)",
    "RidoMUref": "ID ед.изм.(заказ)", "RiddMUref": "ID ед.изм.(доп.)",
    "RidcMUref": "ID ед.изм.(калькул.)",
    "210\\3": "Название", "210\\23": "Выход порции",
    "210\\240": "Тип маркировки", "210\\241": "Код маркировки",
    "210\\54": "Цена закупки 2", "210\\57": "Цена продажи 2",
    "210\\106\\1": "ID подразд.", "210\\106\\3": "Подразделение",
    "210\\114\\107\\1": "ID страны произв.", "210\\114\\107\\3": "Страна произв.",
    "210\\114\\3": "Производитель",
    # GoodsCategories
    "200\\3": "Название",
}

def sh_rename_cols(df):
    """Переименовать колонки StoreHouse в русские"""
    if df.empty:
        return df
    rename = {c: SH_COL_NAMES.get(c, c) for c in df.columns}
    # Убираем дубликаты имён
    seen = {}
    final = {}
    for old, new in rename.items():
        if new in seen:
            seen[new] += 1
            final[old] = f"{new} ({seen[new]})"
        else:
            seen[new] = 1
            final[old] = new
    return df.rename(columns=final)

def sh_clean_df(df):
    """Убрать пустые колонки и GUID, переименовать в русские"""
    if df.empty:
        return df
    clean = df.copy()
    drop_cols = []
    for c in clean.columns:
        if clean[c].isna().all():
            drop_cols.append(c)
        elif clean[c].astype(str).str.match(r'^\{[A-Fa-f0-9-]+\}$').all():
            drop_cols.append(c)
    clean = clean.drop(columns=drop_cols, errors="ignore")
    return sh_rename_cols(clean)

# --- SQL статистическая база StoreHouse ---
SH_STAT_DB = "RK7_STAT_SH4_SHIFTS_FOODCOST"

def sh_stat_query(query, params=None):
    """Запрос к SQL-базе статистики StoreHouse"""
    # Demo mode: use SQLite
    _demo = globals().get("_DEMO_DB")
    if _demo:
        try:
            q = _sql_translate(query)
            if params:
                q = q.replace('%s', '?')
                params = tuple(str(p) for p in params)
                return pd.read_sql(q, _demo, params=params)
            return pd.read_sql(q, _demo)
        except:
            return pd.DataFrame()
    try:
        conn = pymssql.connect(
            server=DB_CONFIG["server"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            database=SH_STAT_DB, login_timeout=15, timeout=60, charset="UTF-8")
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def sh_stat_goodgroups():
    """Группы товаров из SQL (89 групп, иерархия)"""
    return sh_stat_query("""
        SELECT RID, PARENTRID, NAME, CODESTR, EXTCODE
        FROM STAT_SH4_SHIFTS_GOODGROUPS
        ORDER BY PARENTRID, NAME""")

@st.cache_data(ttl=3600)
def sh_stat_corr():
    """Контрагенты/поставщики из SQL (512 записей)"""
    return sh_stat_query("""
        SELECT RID, NAME, TYPECORR, CODE, GROUPRID
        FROM STAT_SH4_SHIFTS_CORR
        ORDER BY TYPECORR, NAME""")

@st.cache_data(ttl=3600)
def sh_stat_invoices():
    """Накладные из SQL"""
    return sh_stat_query("""
        SELECT RID, INVOICEDATE, INVOICESTRING, INVOICENUMBER,
            TYPEINVOICE, RIDSHIPPER, RIDDESTINATION,
            PAYSUMNOTAX, TAXSUM
        FROM STAT_SH4_SHIFTS_INVOICES
        ORDER BY INVOICEDATE DESC""")

@st.cache_data(ttl=3600)
def sh_stat_table_counts():
    """Количество записей во всех таблицах статистики"""
    tables = ["STAT_SH4_SHIFTS_ATTR", "STAT_SH4_SHIFTS_BALANCE_LIST", "STAT_SH4_SHIFTS_CORR",
              "STAT_SH4_SHIFTS_EXPCTGS", "STAT_SH4_SHIFTS_FOODCOST", "STAT_SH4_SHIFTS_GOODGROUPS",
              "STAT_SH4_SHIFTS_GOODS", "STAT_SH4_SHIFTS_INVOICES", "STAT_SH4_SHIFTS_INVOICES_DETAIL",
              "STAT_SH4_SHIFTS_SELLING", "STAT_SH4_SHIFTS_TRIAL_BALANCE"]
    rows = []
    for t in tables:
        df = sh_stat_query(f"SELECT COUNT(*) as CNT FROM [{t}]")
        cnt = int(df.iloc[0]["CNT"]) if not df.empty else 0
        rows.append({"Таблица": t.replace("STAT_SH4_SHIFTS_",""), "Записей": cnt,
                      "Статус": "✅" if cnt > 0 else "⬚"})
    return pd.DataFrame(rows)

# --- GDoc: документы через API по RID из SQL ---

GDOC_TYPES = {
    0: ("GDoc0", "Приходная накладная"),
    1: ("GDoc1", "Расходная накладная"),
    4: ("GDoc4", "Внутреннее перемещение"),
    5: ("GDoc5", "Инвентаризация"),
    8: ("GDoc8", "Акт списания"),
    9: ("GDoc9", "Акт реализации"),
    10: ("GDoc10", "Возвратная накладная"),
    11: ("GDoc11", "Акт разбора"),
    12: ("GDoc12", "Акт нарезки"),
}

def sh_load_gdoc(rid, doc_type_id):
    """Загрузить документ по RID через GDoc API"""
    proc, _ = GDOC_TYPES.get(doc_type_id, (None, None))
    if not proc:
        return None, None, f"Неизвестный тип документа: {doc_type_id}"
    params = [{"head": "111", "original": ["1"], "values": [[rid]]}]
    payload = {"procName": proc, "Input": params}
    data, err = sh_api_call("sh5exec", payload)
    if err:
        return None, None, err
    tables = data.get("shTable", [])
    # Таблица 0 = заголовок (head 111), Таблица 1 = позиции (head 112)
    header = sh_parse_table(data, 0) if len(tables) > 0 else pd.DataFrame()
    items = sh_parse_table(data, 1) if len(tables) > 1 else pd.DataFrame()
    return header, items, None

@st.cache_data(ttl=3600)
def sh_load_all_docs_from_sql():
    """Загрузить все документы: RID из SQL → детали из GDoc API"""
    invoices = sh_stat_invoices()
    if invoices.empty:
        return pd.DataFrame(), pd.DataFrame()
    all_items = []
    doc_headers = []
    seen_rids = set()
    for _, row in invoices.iterrows():
        rid = int(row.get("RID", 0))
        type_id = int(row.get("TYPEINVOICE", 0))
        if rid == 0 or rid in seen_rids:
            continue
        seen_rids.add(rid)
        # Определяем тип
        proc_name, type_name = GDOC_TYPES.get(type_id, (None, f"Тип {type_id}"))
        if not proc_name:
            continue
        header, items, err = sh_load_gdoc(rid, type_id)
        if err:
            doc_headers.append({"RID": rid, "Тип": type_name, "Ошибка": err})
            continue
        # Заголовок
        h = {"RID": rid, "Тип": type_name, "Процедура": proc_name}
        if not header.empty:
            row_data = header.iloc[0].to_dict()
            # Дата
            for k in ["31", "Date"]:
                if k in row_data and row_data[k]:
                    h["Дата"] = row_data[k]
            # Склад/подразделение
            for k in ["105\\3", "106\\3"]:
                if k in row_data and row_data[k]:
                    h["Склад"] = row_data[k]
            # Контрагент
            for k in ["105\\3"]:
                if k in row_data and row_data[k]:
                    h["Контрагент"] = row_data[k]
        inv_date = row.get("INVOICEDATE", "")
        if inv_date and "Дата" not in h:
            h["Дата"] = str(inv_date)[:10]
        doc_headers.append(h)
        # Позиции
        if not items.empty:
            items_clean = items.copy()
            items_clean["_DOC_RID"] = rid
            items_clean["_DOC_TYPE"] = type_name
            all_items.append(items_clean)
    headers_df = pd.DataFrame(doc_headers) if doc_headers else pd.DataFrame()
    items_df = pd.concat(all_items, ignore_index=True) if all_items else pd.DataFrame()
    # Переименуем колонки позиций
    if not items_df.empty:
        item_rename = {
            "210\\1": "ID товара", "210\\3": "Товар", "210\\206\\3": "Ед.изм.",
            "210\\206\\1": "ID ед.", "210\\114\\3": "Производитель",
            "1": "ID позиции", "_DOC_RID": "RID документа", "_DOC_TYPE": "Тип документа",
            "210\\6\\$ImportId": "Импорт ID", "210\\6\\SDInd": "Срок хран.",
            "210\\114\\1": "ID произв.", "210\\114\\105\\1": "ID поставщ.",
        }
        items_df = items_df.rename(columns={k:v for k,v in item_rename.items() if k in items_df.columns})
        # Убираем пустые/технические колонки
        drop = [c for c in items_df.columns if items_df[c].isna().all()
                or (items_df[c].astype(str).str.match(r'^\{[A-Fa-f0-9-]+\}$').all())]
        items_df = items_df.drop(columns=drop, errors="ignore")
    return headers_df, items_df

# --- Расчётный фудкост: сопоставление SH + RK ---

@st.cache_data(ttl=3600)
def load_rk_dish_prices(d1, d2):
    """Средние цены продажи блюд из R-Keeper"""
    return run_query("""
        SELECT mi.NAME as DISH_NAME,
            AVG(sd.PRICE) as SALE_PRICE,
            SUM(sd.QUANTITY) as TOTAL_QTY,
            SUM(sd.PRLISTSUM) as TOTAL_SUM,
            COUNT(DISTINCT CONCAT(sd.VISIT,'-',sd.ORDERIDENT)) as ORDER_COUNT
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.PRICE>0 AND sd.QUANTITY>0
        GROUP BY mi.NAME
        ORDER BY SUM(sd.PRLISTSUM) DESC""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_rk_monthly_sales(d1, d2):
    """Продажи по блюдам и месяцам из R-Keeper — для помесячного фудкоста"""
    return run_query("""
        SELECT
            FORMAT(CAST(o.OPENTIME AS DATE), 'yyyy-MM') as MONTH,
            mi.NAME as DISH_NAME,
            AVG(sd.PRICE) as SALE_PRICE,
            SUM(sd.QUANTITY) as TOTAL_QTY,
            SUM(sd.PRLISTSUM) as TOTAL_SUM
        FROM SESSIONDISHES sd
        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.PRICE>0 AND sd.QUANTITY>0
        GROUP BY FORMAT(CAST(o.OPENTIME AS DATE), 'yyyy-MM'), mi.NAME
        ORDER BY FORMAT(CAST(o.OPENTIME AS DATE), 'yyyy-MM'), SUM(sd.PRLISTSUM) DESC""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_sh_goods_prices():
    """Цены товаров из StoreHouse GoodsTree
    Price0 (53) = закупочная — ПУСТА (цены в накладных, не в карточках)
    Price1 (56) = продажная — 12 427 заполнены
    """
    goods, err = sh_load_goods()
    if err or goods.empty:
        return pd.DataFrame()
    clean = sh_rename_cols(goods)
    name_col = next((c for c in clean.columns if "название" in c.lower()), None)
    # Price1 = цена продажи SH
    sale_col = next((c for c in clean.columns if "цена продажи" in c.lower()), None)
    # Price0 = закупочная (обычно пустая)
    cost_col = next((c for c in clean.columns if "цена закупки" in c.lower()), None)
    group_col = next((c for c in clean.columns if c.lower() == "группа"), None)
    if not name_col:
        return pd.DataFrame()
    cols = {"SH_NAME": name_col}
    if sale_col: cols["SH_SALE_PRICE"] = sale_col
    if cost_col: cols["COST_PRICE"] = cost_col
    if group_col: cols["SH_GROUP"] = group_col
    result = clean[[v for v in cols.values()]].copy()
    result = result.rename(columns={v: k for k, v in cols.items()})
    for nc in ["SH_SALE_PRICE", "COST_PRICE"]:
        if nc in result.columns:
            result[nc] = pd.to_numeric(result[nc], errors="coerce")
    result = result.dropna(subset=["SH_NAME"])
    result["SH_NAME"] = result["SH_NAME"].astype(str).str.strip()
    result = result[result["SH_NAME"] != ""]
    # Убираем нулевые цены
    if "SH_SALE_PRICE" in result.columns:
        result = result[result["SH_SALE_PRICE"] > 0]
    return result

def match_foodcost(rk_dishes, sh_goods, purchase_prices=None):
    """Сопоставить блюда RK с товарами SH по названию, сравнить цены.
    Если есть purchase_prices (из приходных накладных) — рассчитать реальный фудкост."""
    if rk_dishes.empty or sh_goods.empty:
        return pd.DataFrame()
    rk = rk_dishes.copy()
    sh = sh_goods.copy()
    rk["_norm"] = rk["DISH_NAME"].str.strip().str.lower()
    sh["_norm"] = sh["SH_NAME"].str.strip().str.lower()
    sh = sh.drop_duplicates(subset=["_norm"], keep="first")
    # Точное совпадение
    merged = rk.merge(sh, on="_norm", how="inner")
    merged["_match"] = "точное"
    # Частичное совпадение
    matched_norms = set(merged["_norm"])
    rk_unmatched = rk[~rk["_norm"].isin(matched_norms)]
    partial_rows = []
    sh_dict = dict(zip(sh["_norm"], sh.to_dict("records")))
    sh_norms = list(sh_dict.keys())
    for _, rk_row in rk_unmatched.iterrows():
        rk_name = rk_row["_norm"]
        if len(rk_name) < 4:
            continue
        # Частичное совпадение (подстрока)
        found = False
        for sn in sh_norms:
            if rk_name in sn or (len(rk_name) > 5 and rk_name[:12] in sn):
                row = {**rk_row.to_dict(), **sh_dict[sn], "_match": "частичное"}
                partial_rows.append(row)
                found = True
                break
        # Токенное совпадение (≥50% общих слов)
        if not found:
            best_score = 0
            best_sn = None
            for sn in sh_norms:
                score = _token_match_score(rk_name, sn)
                if score > best_score:
                    best_score = score
                    best_sn = sn
            if best_score >= 0.5 and best_sn:
                row = {**rk_row.to_dict(), **sh_dict[best_sn], "_match": "токенное"}
                partial_rows.append(row)
    if partial_rows:
        merged = pd.concat([merged, pd.DataFrame(partial_rows)], ignore_index=True)
    # Добавляем НЕсопоставленные товары RK (не нашлись в SH каталоге)
    # Они всё равно нужны — для них цена подтянется из накладных
    all_matched_norms = set(merged["_norm"]) if "_norm" in merged.columns else set()
    if "_norm" not in merged.columns and "DISH_NAME" in merged.columns:
        all_matched_norms = set(merged["DISH_NAME"].str.strip().str.lower())
    rk_still_unmatched = rk[~rk["_norm"].isin(all_matched_norms)]
    if not rk_still_unmatched.empty:
        unmatched_df = rk_still_unmatched.copy()
        unmatched_df["_match"] = "не найден в SH"
        merged = pd.concat([merged, unmatched_df], ignore_index=True)
    merged = merged.drop(columns=["_norm"], errors="ignore")

    # --- Подмешиваем закупочные цены из приходных накладных ---
    if purchase_prices is not None and not purchase_prices.empty:
        pp = purchase_prices.copy()
        # Определяем формат: recipe costs (DISH_NAME + COST_PER_PORTION) или purchase prices (PRODUCT_NAME + AVG_PURCHASE_PRICE)
        if "DISH_NAME" in pp.columns and "COST_PER_PORTION" in pp.columns:
            pp["_pp_norm"] = pp["DISH_NAME"].str.strip().str.lower()
            pp_dict = dict(zip(pp["_pp_norm"], pp["COST_PER_PORTION"]))
        elif "PRODUCT_NAME" in pp.columns and "AVG_PURCHASE_PRICE" in pp.columns:
            pp["_pp_norm"] = pp["PRODUCT_NAME"].str.strip().str.lower()
            pp_dict = dict(zip(pp["_pp_norm"], pp["AVG_PURCHASE_PRICE"]))
        else:
            pp_dict = {}
        pp_norms = list(pp_dict.keys())
        merged["_m_norm"] = merged["DISH_NAME"].str.strip().str.lower()
        costs = []
        for m_name in merged["_m_norm"]:
            if m_name in pp_dict:
                costs.append(pp_dict[m_name])
            else:
                found = None
                for pn in pp_norms:
                    if m_name in pn or pn in m_name or (len(m_name) > 5 and m_name[:12] in pn):
                        found = pp_dict[pn]
                        break
                costs.append(found)
        merged["COST_PRICE"] = costs
        merged = merged.drop(columns=["_m_norm"], errors="ignore")

    # Разница цен SH vs RK
    if "SH_SALE_PRICE" in merged.columns and "SALE_PRICE" in merged.columns:
        merged["PRICE_DIFF"] = (merged["SALE_PRICE"] - merged["SH_SALE_PRICE"]).round(2)
        merged["PRICE_DIFF_PCT"] = ((merged["PRICE_DIFF"] / merged["SH_SALE_PRICE"]) * 100).round(1)
    # Фудкост
    if "COST_PRICE" in merged.columns and "SALE_PRICE" in merged.columns:
        cv = merged["COST_PRICE"].notna() & (merged["COST_PRICE"] > 0) & (merged["SALE_PRICE"] > 0)
        merged.loc[cv, "FOODCOST_PCT"] = (merged.loc[cv, "COST_PRICE"] / merged.loc[cv, "SALE_PRICE"] * 100).round(1)
        merged.loc[cv, "MARGIN"] = (merged.loc[cv, "SALE_PRICE"] - merged.loc[cv, "COST_PRICE"]).round(2)
        merged.loc[cv, "MARKUP_PCT"] = ((merged.loc[cv, "SALE_PRICE"] - merged.loc[cv, "COST_PRICE"]) / merged.loc[cv, "COST_PRICE"] * 100).round(1)
    return merged

# --- Процедуры, требующие доп. прав ---

@st.cache_data(ttl=3600)
def sh_load_remains():
    """Остатки на складах (Remains) — требует права"""
    df, err = sh_exec("Remains")
    return df, err

@st.cache_data(ttl=3600)
def sh_load_remains_depart(depart_rid):
    """Остатки по конкретному складу"""
    params = [{"head": "111", "original": ["Rid"], "values": [[depart_rid]]}]
    df, err = sh_exec("Remains", params)
    return df, err

@st.cache_data(ttl=3600)
def sh_load_selling(date_from_str, date_to_str):
    """Продажи (Selling) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Selling", params)
    return df, err

@st.cache_data(ttl=3600)
def sh_load_foodcost_api(date_from_str, date_to_str):
    """Фудкост (FoodCost) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("FoodCost", params)
    return df, err

@st.cache_data(ttl=3600)
def load_selling_foodcost(d1, d2):
    """Фудкост из STAT_SH4_SHIFTS_SELLING — экспорт StoreHouse → SQL.
    Содержит закупочные и продажные цены по каждому товару за день."""
    return run_query("""
        SELECT CAST(s.SELLINGDATE AS DATE) as SELL_DATE,
            s.GOODRID, s.GROUPRID, s.QUANTITY,
            CAST(s.PURCHASESUMNOTAX AS DECIMAL(18,2)) as PURCHASE,
            CAST(s.WHOLESALESUMNOTAX AS DECIMAL(18,2)) as SELLING,
            s.RKSIFR
        FROM STAT_SH4_SHIFTS_SELLING s
        WHERE s.SELLINGDATE >= %s AND s.SELLINGDATE < DATEADD(DAY,1,%s)""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_selling_foodcost_summary(d1, d2):
    """Сводка фудкоста по дням из SELLING."""
    return run_query("""
        SELECT CAST(s.SELLINGDATE AS DATE) as SELL_DATE,
            COUNT(*) as ITEMS,
            CAST(SUM(s.PURCHASESUMNOTAX) AS INT) as PURCHASE,
            CAST(SUM(s.WHOLESALESUMNOTAX) AS INT) as SELLING
        FROM STAT_SH4_SHIFTS_SELLING s
        WHERE s.SELLINGDATE >= %s AND s.SELLINGDATE < DATEADD(DAY,1,%s)
        GROUP BY CAST(s.SELLINGDATE AS DATE)
        ORDER BY SELL_DATE""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def load_selling_foodcost_by_group(d1, d2):
    """Фудкост по группам товаров из SELLING."""
    return run_query("""
        SELECT g.GOODGROUPNAME as GROUP_NAME,
            COUNT(DISTINCT s.GOODRID) as GOODS_COUNT,
            CAST(SUM(s.QUANTITY) AS INT) as TOTAL_QTY,
            CAST(SUM(s.PURCHASESUMNOTAX) AS INT) as PURCHASE,
            CAST(SUM(s.WHOLESALESUMNOTAX) AS INT) as SELLING
        FROM STAT_SH4_SHIFTS_SELLING s
        LEFT JOIN STAT_SH4_SHIFTS_GOODGROUPS g ON s.GROUPRID = g.GOODGROUPRID
        WHERE s.SELLINGDATE >= %s AND s.SELLINGDATE < DATEADD(DAY,1,%s)
        GROUP BY g.GOODGROUPNAME
        ORDER BY SELLING DESC""", (str(d1), str(d2)))

@st.cache_data(ttl=3600)
def sh_load_invoices(date_from_str, date_to_str):
    """Накладные (Invoices) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Invoices", params)
    return df, err

@st.cache_data(ttl=3600)
def sh_load_trial_balance(date_from_str, date_to_str):
    """Оборотная ведомость (TrialBalance) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("TrialBalance", params)
    return df, err

@st.cache_data(ttl=3600)
def sh_load_documents_api(date_from_str, date_to_str):
    """Документы (Documents) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Documents", params)
    return df, err

# ============================================================
# ЗАКУПОЧНЫЕ ЦЕНЫ ИЗ ПРИХОДНЫХ НАКЛАДНЫХ (РЕАЛЬНЫЙ ФУДКОСТ)
# ============================================================

@st.cache_data(ttl=3600)
def sh_load_gdoc0_ext_list(date_from_str, date_to_str):
    """Список приходных накладных за период (GDoc0ExtList).
    Таблица 0 = head 108 (эхо входных параметров, 1 строка) — ПРОПУСКАЕМ
    Таблица 1 = head 112 (список накладных) — БЕРЁМ
    Возврат: DataFrame с колонками 111\\1 (RID), 111\\3 (номер), 111\\31 (дата) и др."""
    params = [{"head": "108", "original": ["1", "2"], "values": [[date_from_str], [date_to_str]]}]
    data, err = sh_api_call("sh5exec", {"procName": "GDoc0ExtList", "Input": params})
    if err:
        return pd.DataFrame(), err
    tables = data.get("shTable", [])
    # Берём таблицу с наибольшим числом строк (пропускаем эхо входных параметров)
    best_df = pd.DataFrame()
    for i in range(len(tables)):
        df = sh_parse_table(data, i)
        if not df.empty and len(df) > len(best_df):
            best_df = df
    if best_df.empty:
        return pd.DataFrame(), "Пустой ответ GDoc0ExtList"
    return best_df, None

def sh_load_gdoc0_items(rid, _debug_first=[True]):
    """Позиции одной приходной накладной.
    Возврат: list[dict] с PRODUCT_NAME, QTY, AMOUNT, UNIT_PRICE, UNIT, SH_GOODS_ID"""
    params = [{"head": "111", "original": ["1"], "values": [[rid]]}]
    data, err = sh_api_call("sh5exec", {"procName": "GDoc0", "Input": params})
    if err:
        return [], err
    tables = data.get("shTable", [])
    if not tables:
        return [], None

    # Берём таблицу с наибольшим числом строк (пропускаем эхо заголовка)
    best_df = pd.DataFrame()
    best_idx = -1
    for i in range(len(tables)):
        df = sh_parse_table(data, i)
        if len(df) > len(best_df):
            best_df = df
            best_idx = i

    items_df = best_df
    if items_df.empty:
        return [], None

    # Гибкий поиск колонок
    cols = list(items_df.columns)

    # Название товара: 210\3 или колонка со строковыми значениями похожими на названия
    name_col = None
    for candidate in ["210\\3", "210/3"]:
        if candidate in cols:
            name_col = candidate
            break
    if name_col is None:
        for c in cols:
            if "210" in str(c) and str(c).endswith("3") and len(str(c)) <= 6:
                name_col = c
                break
    if name_col is None:
        # Ищем первую строковую колонку с непустыми значениями длиной > 3
        for c in cols:
            try:
                strs = items_df[c].dropna().astype(str)
                if len(strs) > 0 and strs.str.len().mean() > 3 and not strs.str.match(r'^[\d\.\-]+$').all():
                    name_col = c
                    break
            except Exception:
                continue

    # Количество: колонка 31
    qty_col = None
    for candidate in ["31"]:
        if candidate in cols:
            qty_col = candidate
            break

    # Сумма: колонка 40
    sum_col = None
    for candidate in ["40"]:
        if candidate in cols:
            sum_col = candidate
            break

    # Единица: 210\206\3
    unit_col = None
    for candidate in ["210\\206\\3", "210/206/3"]:
        if candidate in cols:
            unit_col = candidate
            break

    # ID товара: 210\1
    id_col = None
    for candidate in ["210\\1", "210/1"]:
        if candidate in cols:
            id_col = candidate
            break

    # Отладка — сохраняем инфо для первой накладной
    if _debug_first[0]:
        _debug_first[0] = False
        debug = {
            "rid": rid, "n_tables": len(tables), "best_table_idx": best_idx,
            "n_rows": len(items_df), "columns": cols[:20],
            "name_col": name_col, "qty_col": qty_col, "sum_col": sum_col,
            "sample": items_df.head(2).to_dict("records") if len(items_df) > 0 else []
        }
        st.session_state["_gdoc0_debug"] = debug

    if not name_col or not qty_col or not sum_col:
        return [], None

    result = []
    for _, row in items_df.iterrows():
        name = row.get(name_col)
        qty = row.get(qty_col)
        amount = row.get(sum_col)
        if not name or pd.isna(qty) or pd.isna(amount):
            continue
        try:
            qty_f, amount_f = float(qty), float(amount)
            if qty_f > 0 and amount_f >= 0:
                item = {"PRODUCT_NAME": str(name).strip(), "QTY": qty_f,
                        "AMOUNT": amount_f, "UNIT_PRICE": round(amount_f / qty_f, 2) if qty_f > 0 else 0}
                if unit_col and unit_col in items_df.columns and pd.notna(row.get(unit_col)):
                    item["UNIT"] = str(row[unit_col]).strip()
                if id_col and id_col in items_df.columns and pd.notna(row.get(id_col)):
                    item["SH_GOODS_ID"] = row[id_col]
                result.append(item)
        except (ValueError, TypeError):
            continue
    return result, None

def sh_load_purchase_prices(date_from_str, date_to_str, progress_container=None, max_rids=200):
    """Полный пайплайн закупочных цен из приходных накладных.
    1) GDoc0ExtList(даты) → уникальные RID
    2) GDoc0(RID) × N → товар + количество + сумма (макс max_rids накладных)
    3) Средневзвешенная = SUM(сумма) / SUM(кол-во) по каждому товару
    Возврат: DataFrame[PRODUCT_NAME, AVG_PURCHASE_PRICE, TOTAL_QTY, TOTAL_AMOUNT, DOC_COUNT, ENTRY_COUNT, UNIT]"""
    # Шаг 1
    inv_list, err = sh_load_gdoc0_ext_list(date_from_str, date_to_str)
    if err:
        return pd.DataFrame(), f"GDoc0ExtList: {err}"
    if inv_list.empty:
        return pd.DataFrame(), "Нет приходных накладных за период"

    # Найти RID-колонку — пробуем разные варианты имён
    rid_col = None
    # Вариант 1: точное имя
    for candidate in ["111\\1", "1", "111/1", "Rid", "RID", "rid"]:
        if candidate in inv_list.columns:
            rid_col = candidate
            break
    # Вариант 2: содержит "111" и "1" (путь может отличаться)
    if rid_col is None:
        for c in inv_list.columns:
            if "111" in str(c) and str(c).endswith("1") and len(str(c)) <= 6:
                rid_col = c
                break
    # Вариант 3: первая колонка с большими числами
    if rid_col is None:
        for c in inv_list.columns:
            try:
                vals = pd.to_numeric(inv_list[c], errors="coerce")
                valid = vals.dropna()
                if len(valid) > 0 and valid.max() > 1000:
                    rid_col = c
                    break
            except Exception:
                continue
    if rid_col is None:
        # Диагностика: покажем колонки и первую строку
        sample = {c: str(inv_list[c].iloc[0])[:30] for c in inv_list.columns[:15]} if len(inv_list) > 0 else {}
        return pd.DataFrame(), f"Не найден RID. Колонки: {list(inv_list.columns)[:15]}, пример значений: {sample}"

    # Парсим RID — поддерживаем int, float, строки
    raw_vals = inv_list[rid_col].dropna()
    rids = set()
    for v in raw_vals:
        try:
            r = int(float(str(v).strip()))
            if r > 0:
                rids.add(r)
        except (ValueError, TypeError):
            continue
    rids = sorted(rids)

    if not rids:
        # Диагностика
        sample_vals = list(raw_vals.head(10).astype(str))
        return pd.DataFrame(), f"Нет RID в колонке '{rid_col}'. Примеры значений: {sample_vals}"

    # Ограничиваем количество (для скорости)
    total_rids = len(rids)
    if max_rids and len(rids) > max_rids:
        rids = rids[:max_rids]

    # Шаг 2
    all_items = []
    errors = 0
    pb = progress_container.progress(0) if progress_container else None
    st_text = progress_container.empty() if progress_container else None

    for i, rid in enumerate(rids):
        if pb:
            pb.progress((i + 1) / len(rids))
        if st_text:
            st_text.caption(f"Накладная {i+1}/{len(rids)} (RID={rid}) · {len(all_items)} позиций · {errors} ошибок")
        items, item_err = sh_load_gdoc0_items(rid)
        if item_err:
            errors += 1
            continue
        for item in items:
            item["DOC_RID"] = rid
        all_items.extend(items)
        if (i + 1) % 5 == 0 and i < len(rids) - 1:
            time.sleep(0.05)

    if pb:
        pb.empty()
    if st_text:
        st_text.empty()

    if not all_items:
        return pd.DataFrame(), f"Найдено {len(rids)} накладных, но товарные позиции пусты ({errors} ошибок API). Возможно, накладные ещё не заполнены в SH."

    items_df = pd.DataFrame(all_items)

    # Шаг 3 — средневзвешенная цена
    grouped = items_df.groupby("PRODUCT_NAME").agg(
        TOTAL_QTY=("QTY", "sum"),
        TOTAL_AMOUNT=("AMOUNT", "sum"),
        DOC_COUNT=("DOC_RID", "nunique"),
        ENTRY_COUNT=("QTY", "count"),
    ).reset_index()
    grouped["AVG_PURCHASE_PRICE"] = (grouped["TOTAL_AMOUNT"] / grouped["TOTAL_QTY"]).round(2)

    if "UNIT" in items_df.columns:
        units = items_df.groupby("PRODUCT_NAME")["UNIT"].agg(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "")
        grouped = grouped.merge(units.rename("UNIT"), on="PRODUCT_NAME", how="left")

    return grouped.sort_values("TOTAL_AMOUNT", ascending=False).reset_index(drop=True), None


# ============================================================
# СКЛАД: движение товаров, перемещения, сроки годности
# ============================================================

@st.cache_data(ttl=3600)
def sh_load_gdoc1_5_list(date_from_str, date_to_str, doc_type=4):
    """Список документов типов 1-5 за период (GDoc1_5LstDocs).
    doc_type: 1=расходная, 4=перемещение, 5=инвентаризация.
    Пробуем несколько форматов вызова (API не документирован точно)."""
    # Попытка 1: только даты (без типа) — самый безопасный вариант
    params_v1 = [{"head": "108", "original": ["1", "2"],
                  "values": [[date_from_str], [date_to_str]]}]
    data, err = sh_api_call("sh5exec", {"procName": "GDoc1_5LstDocs", "Input": params_v1})
    if err:
        # Попытка 2: тип через original "5"
        params_v2 = [{"head": "108", "original": ["1", "2", "5"],
                      "values": [[date_from_str], [date_to_str], [str(doc_type)]]}]
        data, err = sh_api_call("sh5exec", {"procName": "GDoc1_5LstDocs", "Input": params_v2})
    if err:
        # Попытка 3: тип как int
        params_v3 = [{"head": "108", "original": ["1", "2", "3"],
                      "values": [[date_from_str], [date_to_str], [doc_type]]}]
        data, err = sh_api_call("sh5exec", {"procName": "GDoc1_5LstDocs", "Input": params_v3})
    if err:
        return pd.DataFrame(), err
    tables = data.get("shTable", [])
    best_df = pd.DataFrame()
    for i in range(len(tables)):
        df = sh_parse_table(data, i)
        if not df.empty and len(df) > len(best_df):
            best_df = df
    if best_df.empty:
        return pd.DataFrame(), "Пустой ответ GDoc1_5LstDocs"
    # Фильтруем по типу документа если есть колонка типа
    if doc_type is not None:
        type_col = None
        for c in best_df.columns:
            if c in ("111\\5", "5", "DocType", "Type"):
                type_col = c
                break
        if type_col:
            best_df[type_col] = pd.to_numeric(best_df[type_col], errors="coerce")
            filtered = best_df[best_df[type_col] == doc_type]
            if not filtered.empty:
                return filtered, None
    return best_df, None

def sh_load_gdoc4_items(rid):
    """Позиции внутреннего перемещения (GDoc4) по RID.
    Возврат: list[dict] с PRODUCT_NAME, QTY, AMOUNT, и склад-отправитель/получатель."""
    params = [{"head": "111", "original": ["1"], "values": [[rid]]}]
    data, err = sh_api_call("sh5exec", {"procName": "GDoc4", "Input": params})
    if err:
        return pd.DataFrame(), pd.DataFrame(), err
    tables = data.get("shTable", [])
    if not tables:
        return pd.DataFrame(), pd.DataFrame(), None
    # Таблица 0 = заголовок (head 111), остальные = позиции
    header = sh_parse_table(data, 0) if len(tables) > 0 else pd.DataFrame()
    # Берём таблицу с наибольшим числом строк (пропуская заголовок если он 1 строка)
    best_df = pd.DataFrame()
    for i in range(len(tables)):
        df = sh_parse_table(data, i)
        if not df.empty and len(df) > len(best_df):
            best_df = df
    return header, best_df, None

@st.cache_data(ttl=3600)
def sh_load_transfers(date_from_str, date_to_str, _progress_container=None, max_rids=100):
    """Полный пайплайн перемещений: список + детали каждого.
    1) GDoc1_5LstDocs type=4 → список RID перемещений
    2) GDoc4(RID) × N → товары в каждом перемещении"""
    list_df, err = sh_load_gdoc1_5_list(date_from_str, date_to_str, doc_type=4)
    if err:
        return pd.DataFrame(), pd.DataFrame(), err
    if list_df.empty:
        return pd.DataFrame(), pd.DataFrame(), "Нет перемещений за период"

    # Ищем RID-колонку
    rid_col = None
    for candidate in ["111\\1", "1"]:
        if candidate in list_df.columns:
            rid_col = candidate
            break
    if not rid_col:
        for c in list_df.columns:
            try:
                vals = pd.to_numeric(list_df[c], errors="coerce").dropna()
                if len(vals) > 0 and vals.min() > 1000:
                    rid_col = c
                    break
            except Exception:
                continue
    if not rid_col:
        return list_df, pd.DataFrame(), "Не удалось определить RID-колонку"

    rids = list_df[rid_col].dropna().astype(int).unique().tolist()
    if len(rids) > max_rids:
        rids = rids[:max_rids]

    all_items = []
    pb = _progress_container.progress(0) if _progress_container else None
    for i, rid in enumerate(rids):
        if pb:
            pb.progress((i + 1) / len(rids), text=f"Перемещение {i+1}/{len(rids)}")
        header, items, item_err = sh_load_gdoc4_items(rid)
        if item_err or items.empty:
            continue
        items_copy = items.copy()
        items_copy["_DOC_RID"] = rid
        # Извлекаем склады из заголовка если есть
        if not header.empty:
            row0 = header.iloc[0]
            for k in ["106\\3", "105\\3"]:
                if k in row0.index and pd.notna(row0[k]):
                    items_copy["_DEPART_FROM"] = str(row0[k])
                    break
            for k in ["107\\3"]:
                if k in row0.index and pd.notna(row0[k]):
                    items_copy["_DEPART_TO"] = str(row0[k])
        all_items.append(items_copy)

    if pb:
        pb.empty()

    items_df = pd.concat(all_items, ignore_index=True) if all_items else pd.DataFrame()
    return list_df, items_df, None

@st.cache_data(ttl=3600)
@st.cache_data(ttl=3600)
def sh_load_stock(date_str="2026-03-14", by_depart=True):
    """Остатки товаров через GRemns.
    date_str: дата на которую считать остатки
    by_depart: True = разбивка по складам (dept=1), False = общие (dept=0)
    Возврат: DataFrame[PRODUCT_NAME, QTY, AMOUNT, UNIT, DEPART (если by_depart)]"""
    dept_flag = 1 if by_depart else 0
    params = [{"head": "108", "original": ["1", "3", "11"],
               "values": [[date_str], [dept_flag], [0]]}]
    data, err = sh_api_call("sh5exec", {"procName": "GRemns", "Input": params})
    if err:
        return pd.DataFrame(), err

    tables = data.get("shTable", [])

    # Справочник складов (head=106)
    dep_map = {}
    if by_depart:
        t106 = next((t for t in tables if t.get("head") == "106"), None)
        if t106 and t106.get("values"):
            orig106 = t106.get("original", [])
            ridIdx = orig106.index("1") if "1" in orig106 else 0
            nameIdx = orig106.index("3") if "3" in orig106 else 1
            for i in range(len(t106["values"][ridIdx])):
                dep_map[t106["values"][ridIdx][i]] = t106["values"][nameIdx][i]

    # Данные остатков (head=151)
    t151 = next((t for t in tables if t.get("head") == "151"), None)
    if not t151 or not t151.get("values") or not t151["values"][0]:
        return pd.DataFrame(), "GRemns: пустой ответ (head=151)"

    df = sh_parse_table(data, tables.index(t151))
    if df.empty:
        return pd.DataFrame(), "GRemns: не удалось распарсить таблицу"

    # Переименовываем колонки
    rename = {
        "31": "QTY", "40": "AMOUNT", "41": "AMOUNT2", "42": "AMOUNT3",
        "210\\1": "GOODS_ID", "210\\3": "PRODUCT_NAME",
        "210\\206\\3": "UNIT", "210\\206\\1": "UNIT_ID",
        "52": "DEPART_RID", "53": "EXTRA",
        "210\\6\\SDInd": "SHELF_LIFE", "210\\77": "F77", "210\\78": "F78",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Числовые колонки
    for c in ["QTY", "AMOUNT", "AMOUNT2", "AMOUNT3"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Привязываем названия складов
    if by_depart and "DEPART_RID" in df.columns and dep_map:
        df["DEPART"] = df["DEPART_RID"].map(dep_map).fillna("Неизвестный")

    # Фильтруем ненулевые
    if "QTY" in df.columns:
        df = df[df["QTY"] != 0].copy()

    return df.reset_index(drop=True), None

@st.cache_data(ttl=3600)
def sh_load_stock_dynamics(date_from_str, date_to_str):
    """Остатки (GRemns) за каждый день в диапазоне → DataFrame[DATE, DEPART, AMOUNT, QTY, PRODUCTS].
    Загружает по складам (by_depart=True) и агрегирует по дням + складам."""
    from datetime import datetime as _dt, timedelta as _td
    d1 = _dt.strptime(date_from_str, "%Y-%m-%d").date()
    d2 = _dt.strptime(date_to_str, "%Y-%m-%d").date()
    days = []
    d = d1
    while d <= d2:
        days.append(d)
        d += _td(1)
    # Limit to 31 days
    if len(days) > 31:
        step = max(1, len(days) // 31)
        days = days[::step]
        if days[-1] != d2:
            days.append(d2)
    rows = []
    for day in days:
        stock, err = sh_load_stock(str(day), by_depart=True)
        if err or stock.empty:
            continue
        if "DEPART" in stock.columns and "AMOUNT" in stock.columns:
            by_dep = stock.groupby("DEPART").agg(
                AMOUNT=("AMOUNT", "sum"),
                QTY=("QTY", "sum") if "QTY" in stock.columns else ("AMOUNT", "count"),
                PRODUCTS=("PRODUCT_NAME", "nunique") if "PRODUCT_NAME" in stock.columns else ("AMOUNT", "count"),
            ).reset_index()
            for _, r in by_dep.iterrows():
                rows.append({"DATE": str(day), "DEPART": r["DEPART"], "AMOUNT": r["AMOUNT"],
                             "QTY": r.get("QTY", 0), "PRODUCTS": r.get("PRODUCTS", 0)})
        elif "AMOUNT" in stock.columns:
            rows.append({"DATE": str(day), "DEPART": "Все", "AMOUNT": stock["AMOUNT"].sum(),
                         "QTY": stock["QTY"].sum() if "QTY" in stock.columns else 0,
                         "PRODUCTS": stock["PRODUCT_NAME"].nunique() if "PRODUCT_NAME" in stock.columns else 0})
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def sh_load_incoming_full(date_from_str, date_to_str, _progress_container=None, max_rids=200):
    """Единая загрузка приходных накладных: товары + склады + поставщики за один проход.
    Для каждого GDoc0(RID) читаем заголовок (склад, поставщик, дата) + позиции (товар, кол-во, сумма).
    Возврат: (items_df, docs_df, error)
      items_df: PRODUCT_NAME, QTY, AMOUNT, UNIT_PRICE, UNIT, DOC_RID, DEPART, SUPPLIER, DOC_DATE
      docs_df: DOC_RID, DEPART, SUPPLIER, DOC_DATE, ITEMS_COUNT, TOTAL_AMOUNT"""
    inv_list, err = sh_load_gdoc0_ext_list(date_from_str, date_to_str)
    if err:
        return pd.DataFrame(), pd.DataFrame(), f"GDoc0ExtList: {err}"
    if inv_list.empty:
        return pd.DataFrame(), pd.DataFrame(), "Нет приходных накладных за период"

    # Ищем RID-колонку
    rid_col = None
    for candidate in ["111\\1", "1", "111/1", "Rid", "RID", "rid"]:
        if candidate in inv_list.columns:
            rid_col = candidate
            break
    if not rid_col:
        for c in inv_list.columns:
            if "111" in str(c) and str(c).endswith("1") and len(str(c)) <= 6:
                rid_col = c
                break
    if not rid_col:
        for c in inv_list.columns:
            try:
                vals = pd.to_numeric(inv_list[c], errors="coerce").dropna()
                if len(vals) > 0 and vals.max() > 1000:
                    rid_col = c
                    break
            except Exception:
                continue
    if not rid_col:
        return pd.DataFrame(), pd.DataFrame(), f"Не найден RID. Колонки: {list(inv_list.columns)[:10]}"

    rids = []
    for v in inv_list[rid_col].dropna():
        try:
            r = int(float(str(v).strip()))
            if r > 0:
                rids.append(r)
        except (ValueError, TypeError):
            continue
    rids = sorted(set(rids))
    if len(rids) > max_rids:
        rids = rids[:max_rids]

    pb = _progress_container.progress(0) if _progress_container else None
    st_text = _progress_container.empty() if _progress_container else None

    all_items = []
    docs = []
    errors = 0

    for i, rid in enumerate(rids):
        if pb:
            pb.progress((i + 1) / len(rids))
        if st_text:
            st_text.caption(f"Накладная {i+1}/{len(rids)} (RID={rid}) · {len(all_items)} позиций · {errors} ошибок")

        params = [{"head": "111", "original": ["1"], "values": [[rid]]}]
        data, call_err = sh_api_call("sh5exec", {"procName": "GDoc0", "Input": params})
        if call_err:
            errors += 1
            continue
        tables = data.get("shTable", [])
        if not tables:
            continue

        # Разделяем заголовок (1 строка) и позиции (много строк)
        header_df = pd.DataFrame()
        items_df_raw = pd.DataFrame()
        for ti in range(len(tables)):
            tdf = sh_parse_table(data, ti)
            if tdf.empty:
                continue
            if len(tdf) == 1 and header_df.empty:
                header_df = tdf
            elif len(tdf) > len(items_df_raw):
                items_df_raw = tdf

        # Извлекаем склад, поставщика, дату из заголовка
        doc_info = {"DOC_RID": rid, "DEPART": "", "SUPPLIER": "", "DOC_DATE": ""}
        if not header_df.empty:
            h = header_df.iloc[0]
            for k in ["106\\3", "107\\3"]:
                if k in h.index and pd.notna(h[k]) and str(h[k]).strip():
                    doc_info["DEPART"] = str(h[k]).strip()
                    break
            for k in ["105\\3"]:
                if k in h.index and pd.notna(h[k]) and str(h[k]).strip():
                    doc_info["SUPPLIER"] = str(h[k]).strip()
            for k in ["31"]:
                if k in h.index and pd.notna(h[k]):
                    doc_info["DOC_DATE"] = str(h[k])[:10]

        # Парсим позиции товаров
        doc_items_count = 0
        doc_total_amount = 0.0
        if not items_df_raw.empty:
            cols = list(items_df_raw.columns)
            # Название товара
            name_col = None
            for cand in ["210\\3", "210/3"]:
                if cand in cols:
                    name_col = cand
                    break
            if not name_col:
                for c in cols:
                    if "210" in str(c) and str(c).endswith("3") and len(str(c)) <= 6:
                        name_col = c
                        break
            if not name_col:
                for c in cols:
                    try:
                        strs = items_df_raw[c].dropna().astype(str)
                        if len(strs) > 0 and strs.str.len().mean() > 3 and not strs.str.match(r'^[\d\.\-]+$').all():
                            name_col = c
                            break
                    except Exception:
                        continue
            qty_col = "31" if "31" in cols else None
            sum_col = "40" if "40" in cols else None
            unit_col = None
            for cand in ["210\\206\\3", "210/206/3"]:
                if cand in cols:
                    unit_col = cand
                    break
            # Срок хранения: 210\6\SDInd или похожие
            sdind_col = None
            for cand in ["210\\6\\SDInd", "210\\6\\$SDInd", "210/6/SDInd"]:
                if cand in cols:
                    sdind_col = cand
                    break
            # Дата окончания срока годности: 6\ExpDate
            expdate_col = None
            for cand in ["6\\ExpDate", "6/ExpDate", "6\\$ExpDate"]:
                if cand in cols:
                    expdate_col = cand
                    break
            if not expdate_col:
                for c in cols:
                    if "expdate" in c.lower() or "exp_date" in c.lower():
                        expdate_col = c
                        break

            # Диагностика — сохраняем колонки первой накладной
            if i == 0:
                st.session_state["_gdoc0_all_cols"] = cols
                # Сохраняем примеры значений для полей сроков
                shelf_debug = {}
                if sdind_col:
                    vals = items_df_raw[sdind_col].dropna().astype(str).unique()[:5]
                    shelf_debug["SDInd"] = list(vals)
                if expdate_col:
                    vals = items_df_raw[expdate_col].dropna().astype(str).unique()[:5]
                    shelf_debug["ExpDate"] = list(vals)
                st.session_state["_gdoc0_shelf_debug"] = shelf_debug

            if name_col and qty_col and sum_col:
                for _, row in items_df_raw.iterrows():
                    name = row.get(name_col)
                    qty = row.get(qty_col)
                    amount = row.get(sum_col)
                    if not name or pd.isna(qty) or pd.isna(amount):
                        continue
                    try:
                        qty_f, amount_f = float(qty), float(amount)
                        if qty_f > 0 and amount_f >= 0:
                            item = {
                                "PRODUCT_NAME": str(name).strip(),
                                "QTY": qty_f, "AMOUNT": amount_f,
                                "UNIT_PRICE": round(amount_f / qty_f, 2) if qty_f > 0 else 0,
                                "DOC_RID": rid,
                                "DEPART": doc_info["DEPART"],
                                "SUPPLIER": doc_info["SUPPLIER"],
                                "DOC_DATE": doc_info["DOC_DATE"],
                            }
                            if unit_col and pd.notna(row.get(unit_col)):
                                item["UNIT"] = str(row[unit_col]).strip()
                            if sdind_col and pd.notna(row.get(sdind_col)):
                                val = row[sdind_col]
                                if str(val).strip() and str(val).strip() != "0":
                                    item["SHELF_LIFE"] = str(val).strip()
                            if expdate_col and pd.notna(row.get(expdate_col)):
                                val = row[expdate_col]
                                if str(val).strip() and str(val).strip() != "0" and str(val).strip().lower() != "none":
                                    item["EXP_DATE"] = str(val).strip()
                            all_items.append(item)
                            doc_items_count += 1
                            doc_total_amount += amount_f
                    except (ValueError, TypeError):
                        continue
            else:
                # Fallback: считаем сумму из колонки 40 без разбора
                if sum_col:
                    amounts = pd.to_numeric(items_df_raw[sum_col], errors="coerce")
                    doc_total_amount = round(amounts.sum(), 2)
                doc_items_count = len(items_df_raw)

        doc_info["ITEMS_COUNT"] = doc_items_count
        doc_info["TOTAL_AMOUNT"] = round(doc_total_amount, 2)
        docs.append(doc_info)

        if (i + 1) % 10 == 0 and i < len(rids) - 1:
            time.sleep(0.05)

    if pb:
        pb.empty()
    if st_text:
        st_text.empty()

    items_result = pd.DataFrame(all_items) if all_items else pd.DataFrame()
    docs_result = pd.DataFrame(docs) if docs else pd.DataFrame()
    if items_result.empty and docs_result.empty:
        return pd.DataFrame(), pd.DataFrame(), f"Нет данных из {len(rids)} накладных ({errors} ошибок)"
    return items_result, docs_result, None


# ============================================================
# ФУДКОСТ ЧЕРЕЗ РЕЦЕПТУРЫ (Акты нарезки GDoc12)
# ============================================================
# Акт нарезки (GDoc12) = StoreHouse раскладывает блюда на ингредиенты
# по рецептуре (комплекту) и считает себестоимость каждой порции.
# Поле 40 = себестоимость всех порций, поле 31 = кол-во порций.
# Себестоимость 1 порции = 40 / 31.

def sh_find_gdoc12_rids(cmp_rids, max_docs=50, progress_container=None):
    """Найти RID'ы Актов нарезки (GDoc12) через FindLinksToCmp.
    Для каждого комплекта RID возвращает связанные документы типа 12.
    Берём самые свежие (высокий RID = новее)."""
    all_doc_rids = set()
    errors = 0
    pb = progress_container.progress(0) if progress_container else None
    st_text = progress_container.empty() if progress_container else None

    for i, cmp_rid in enumerate(cmp_rids):
        if pb:
            pb.progress((i + 1) / len(cmp_rids))
        if st_text:
            st_text.caption(f"Комплект {i+1}/{len(cmp_rids)} (RID={cmp_rid}) · {len(all_doc_rids)} документов найдено")

        data, err = sh_api_call("sh5exec", {"procName": "FindLinksToCmp",
            "Input": [{"head": "108", "original": ["215\\1"], "values": [[cmp_rid]]}]})
        if err:
            errors += 1
            continue
        tables = data.get("shTable", [])
        for t in tables:
            if t.get("head") == "532":
                vals = t.get("values", [])
                orig = t.get("original", [])
                if not vals or not vals[0]:
                    continue
                type_idx = orig.index("5") if "5" in orig else -1
                rid_idx = orig.index("1") if "1" in orig else -1
                if type_idx < 0 or rid_idx < 0:
                    continue
                for j in range(len(vals[0])):
                    doc_type = vals[type_idx][j] if type_idx < len(vals) else None
                    doc_rid = vals[rid_idx][j] if rid_idx < len(vals) else None
                    if doc_type == 12 and doc_rid:
                        all_doc_rids.add(int(doc_rid))

        if len(all_doc_rids) >= max_docs * 3:
            break  # Достаточно — возьмём самые свежие
        if (i + 1) % 5 == 0:
            time.sleep(0.05)

    if pb:
        pb.empty()
    if st_text:
        st_text.empty()

    # Берём самые свежие (высокий RID)
    sorted_rids = sorted(all_doc_rids, reverse=True)[:max_docs]
    return sorted_rids, errors

def sh_load_gdoc12_costs(doc_rids, progress_container=None):
    """Загрузить себестоимость блюд из Актов нарезки (GDoc12).
    Возврат: DataFrame[DISH_NAME, COST_PER_PORTION, TOTAL_COST, PORTIONS, DOC_RID, CMP_RID, UNIT]"""
    all_items = []
    errors = 0
    pb = progress_container.progress(0) if progress_container else None
    st_text = progress_container.empty() if progress_container else None

    for i, rid in enumerate(doc_rids):
        if pb:
            pb.progress((i + 1) / len(doc_rids))
        if st_text:
            st_text.caption(f"Акт нарезки {i+1}/{len(doc_rids)} (RID={rid}) · {len(all_items)} блюд")

        data, err = sh_api_call("sh5exec", {"procName": "GDoc12",
            "Input": [{"head": "111", "original": ["1"], "values": [[rid]]}]})
        if err:
            errors += 1
            continue

        # Берём таблицу с позициями (head 112)
        tables = data.get("shTable", [])
        items_df = pd.DataFrame()
        for t in tables:
            df = sh_parse_table(data, tables.index(t))
            if len(df) > len(items_df):
                items_df = df

        if items_df.empty:
            continue

        cols = list(items_df.columns)
        name_col = "210\\3" if "210\\3" in cols else None
        qty_col = "31" if "31" in cols else None
        sum_col = "40" if "40" in cols else None
        unit_col = "210\\206\\3" if "210\\206\\3" in cols else None
        cmp_rid_col = "210\\215\\1" if "210\\215\\1" in cols else None
        cmp_name_col = "210\\215\\3" if "210\\215\\3" in cols else None

        if not name_col or not qty_col or not sum_col:
            # Гибкий поиск
            if not name_col:
                for c in cols:
                    try:
                        if items_df[c].dropna().astype(str).str.len().mean() > 5:
                            name_col = c
                            break
                    except:
                        continue
            if not name_col or not qty_col or not sum_col:
                errors += 1
                continue

        for _, row in items_df.iterrows():
            name = row.get(name_col)
            qty = row.get(qty_col)
            total_cost = row.get(sum_col)
            if not name or pd.isna(qty) or pd.isna(total_cost):
                continue
            try:
                qty_f = float(qty)
                cost_f = float(total_cost)
                if qty_f > 0 and cost_f > 0:
                    item = {
                        "DISH_NAME": str(name).strip(),
                        "PORTIONS": qty_f,
                        "TOTAL_COST": cost_f,
                        "COST_PER_PORTION": round(cost_f / qty_f, 2),
                        "DOC_RID": rid,
                    }
                    if unit_col and pd.notna(row.get(unit_col)):
                        item["UNIT"] = str(row[unit_col]).strip()
                    if cmp_rid_col and pd.notna(row.get(cmp_rid_col)):
                        item["CMP_RID"] = int(row[cmp_rid_col])
                    if cmp_name_col and pd.notna(row.get(cmp_name_col)):
                        item["CMP_NAME"] = str(row[cmp_name_col]).strip()
                    all_items.append(item)
            except (ValueError, TypeError):
                continue

        if (i + 1) % 5 == 0:
            time.sleep(0.05)

    if pb:
        pb.empty()
    if st_text:
        st_text.empty()

    if not all_items:
        return pd.DataFrame(), f"Нет позиций в {len(doc_rids)} актах ({errors} ошибок)"

    df = pd.DataFrame(all_items)

    # Средневзвешенная себестоимость по блюду (из разных документов)
    grouped = df.groupby("DISH_NAME").agg(
        COST_PER_PORTION=("COST_PER_PORTION", lambda x: round((df.loc[x.index, "TOTAL_COST"].sum() / df.loc[x.index, "PORTIONS"].sum()), 2)),
        TOTAL_PORTIONS=("PORTIONS", "sum"),
        TOTAL_COST=("TOTAL_COST", "sum"),
        DOC_COUNT=("DOC_RID", "nunique"),
        LAST_DOC_RID=("DOC_RID", "max"),  # самый свежий документ
    ).reset_index()

    # Добавляем CMP_RID (комплект)
    if "CMP_RID" in df.columns:
        cmp_rids = df.groupby("DISH_NAME")["CMP_RID"].first()
        grouped = grouped.merge(cmp_rids.rename("CMP_RID"), on="DISH_NAME", how="left")

    if "UNIT" in df.columns:
        units = df.groupby("DISH_NAME")["UNIT"].agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "")
        grouped = grouped.merge(units.rename("UNIT"), on="DISH_NAME", how="left")

    return grouped.sort_values("TOTAL_COST", ascending=False).reset_index(drop=True), None

def sh_load_gdoc12_detail(doc_rid):
    """Загрузить полный Акт нарезки (GDoc12) для детализации.
    Возвращает (header_dict, items_df) — заголовок документа и все позиции."""
    data, err = sh_api_call("sh5exec", {"procName": "GDoc12",
        "Input": [{"head": "111", "original": ["1"], "values": [[doc_rid]]}]})
    if err:
        return {}, pd.DataFrame(), err
    tables = data.get("shTable", [])
    if not tables:
        return {}, pd.DataFrame(), "Пустой ответ"

    # Заголовок (head 111, 1 строка)
    header = {}
    for t_idx, t in enumerate(tables):
        df = sh_parse_table(data, t_idx)
        if len(df) == 1 and len(df.columns) > 5:
            row = df.iloc[0].to_dict()
            header = {
                "RID": row.get("1", doc_rid),
                "Номер": row.get("3", ""),
                "Дата": row.get("31", ""),
                "Дата проведения": row.get("47", ""),
                "Склад (откуда)": row.get("105\\3", ""),
                "Склад (куда)": row.get("105#1\\3", ""),
                "Подразделение (откуда)": row.get("109\\3", ""),
                "Подразделение (куда)": row.get("109#1\\3", ""),
            }
            # Убираем None
            header = {k: v for k, v in header.items() if v is not None and v != ""}
            break

    # Позиции (самая большая таблица)
    items_df = pd.DataFrame()
    for t_idx, t in enumerate(tables):
        df = sh_parse_table(data, t_idx)
        if len(df) > len(items_df):
            items_df = df

    if items_df.empty:
        return header, pd.DataFrame(), None

    # Переименуем колонки для читаемости
    rename_map = {
        "210\\3": "Dish" if _get_lang()=="en" else "Блюдо",
        "210\\215\\3": "Комплект",
        "210\\215\\1": "RID комплекта",
        "210\\206\\3": "Ед. изм.",
        "210\\114\\3": "Производитель",
        "31": "Кол-во порций",
        "40": "Себестоимость ₽",
        "32": "Масса",
        "41": "Доп. сумма",
        "42": "Тип",
    }
    items_clean = items_df.rename(columns={k: v for k, v in rename_map.items() if k in items_df.columns})

    # Добавляем расчётную себестоимость за порцию
    if "Кол-во порций" in items_clean.columns and "Себестоимость ₽" in items_clean.columns:
        items_clean["Кол-во порций"] = pd.to_numeric(items_clean["Кол-во порций"], errors="coerce")
        items_clean["Себестоимость ₽"] = pd.to_numeric(items_clean["Себестоимость ₽"], errors="coerce")
        mask = items_clean["Кол-во порций"] > 0
        items_clean.loc[mask, "Себест./порция ₽"] = (
            items_clean.loc[mask, "Себестоимость ₽"] / items_clean.loc[mask, "Кол-во порций"]).round(2)

    # Оставляем только информативные колонки
    keep = [c for c in items_clean.columns if not (
        items_clean[c].isna().all() or
        items_clean[c].astype(str).str.match(r'^\{[A-Fa-f0-9-]+\}$').all() or
        c in ("1",)
    )]
    return header, items_clean[keep], None

def sh_load_recipe_foodcost(progress_container=None, max_complects=100, max_docs=50):
    """Полный пайплайн: рецептурный фудкост через Акты нарезки (GDoc12).

    1) GoodsTree → уникальные RidCmp (комплекты)
    2) FindLinksToCmp(RidCmp) → RID'ы GDoc12
    3) GDoc12(RID) → блюдо + себестоимость порции

    Возврат: DataFrame[DISH_NAME, COST_PER_PORTION, TOTAL_PORTIONS, TOTAL_COST, DOC_COUNT, UNIT]"""

    if progress_container:
        progress_container.caption("Загрузка списка комплектов из GoodsTree...")

    # Шаг 1: получить все RidCmp из GoodsTree
    goods, err = sh_load_goods()
    if err:
        return pd.DataFrame(), f"GoodsTree: {err}"
    if goods.empty:
        return pd.DataFrame(), "GoodsTree пуст"

    # Находим колонку RidCmp (215\1)
    rid_cmp_col = None
    for c in goods.columns:
        if c in ("215\\1", "215/1", "RidCmp"):
            rid_cmp_col = c
            break
    if not rid_cmp_col:
        return pd.DataFrame(), f"Нет колонки RidCmp (215\\1) в GoodsTree. Колонки: {list(goods.columns)[:15]}"

    all_cmps = pd.to_numeric(goods[rid_cmp_col], errors="coerce").dropna().unique()
    unique_cmps = sorted(set(int(c) for c in all_cmps if c > 0))

    if not unique_cmps:
        return pd.DataFrame(), "Нет комплектов в GoodsTree"

    # Берём подмножество
    cmp_sample = unique_cmps[:max_complects]

    if progress_container:
        progress_container.caption(f"Поиск Актов нарезки для {len(cmp_sample)} комплектов...")

    # Шаг 2: найти GDoc12 RID'ы
    sub_progress = progress_container if progress_container else None
    doc_rids, find_errors = sh_find_gdoc12_rids(cmp_sample, max_docs=max_docs, progress_container=sub_progress)

    if not doc_rids:
        return pd.DataFrame(), f"Не найдено Актов нарезки для {len(cmp_sample)} комплектов ({find_errors} ошибок)"

    if progress_container:
        progress_container.caption(f"Загрузка {len(doc_rids)} Актов нарезки...")

    # Шаг 3: загрузить себестоимость
    costs_df, costs_err = sh_load_gdoc12_costs(doc_rids, progress_container=sub_progress)
    if costs_err:
        return pd.DataFrame(), costs_err

    return costs_df, None


# ============================================================
# НАВИГАЦИЯ (ленивая загрузка — грузится только выбранная страница)
# ============================================================
PAGES_ALL = [
    "Пульс", "Выручка", "Сезонность", "Блюда", "Рестораны", "Категории",
    "Персонал", "Касса", "Цены", "ABC", "Скорость",
    "Смены", "Проблемы", "Удаление", "Заказы",
    "Склад", "Накладные", "Фудкост", "Фудкост (расчёт)", "Склад: Схема",
    "Доход/Расход",
    "Личный кабинет",
    "ИИ-чат", "Проактив"
]

# Группы меню
MENU_GROUPS = [
    ("", ["Пульс"]),
    ("Продажи", ["Выручка", "Сезонность", "Блюда", "Категории", "Цены", "ABC"]),
    ("Точки и персонал", ["Рестораны", "Персонал", "Смены", "Скорость"]),
    ("Касса и чеки", ["Касса", "Заказы", "Проблемы", "Удаление"]),
    ("Склад и закупки", ["Склад", "Накладные", "Фудкост", "Фудкост (расчёт)", "Склад: Схема"]),
    ("Финансы", ["Доход/Расход"]),
    ("ИИ", ["ИИ-чат", "Проактив"]),
]

# Страницы доступные только admin
ADMIN_ONLY_PAGES = {"ИИ-чат", "Проактив"}

# Фильтруем по роли текущего пользователя
if CURRENT_USER["role"] == "admin":
    PAGES = PAGES_ALL
else:
    PAGES = [p for p in PAGES_ALL if p not in ADMIN_ONLY_PAGES]

with st.sidebar:
    st.divider()
    if "_page" not in st.session_state or st.session_state["_page"] not in PAGES:
        st.session_state["_page"] = "Выручка"

    for group_name, group_pages in MENU_GROUPS:
        # Filter by available pages
        visible = [p for p in group_pages if p in PAGES]
        if not visible:
            continue

        if group_name == "":
            # Top-level items (Пульс)
            for p in visible:
                active = st.session_state["_page"] == p
                if st.button(tp(p), key=f"nav_{p}", use_container_width=True,
                             type="primary" if active else "secondary"):
                    st.session_state["_page"] = p
                    st.rerun()
        else:
            # Group header — always visible, no accordion
            st.markdown(f'<div style="font-size:.62rem;font-weight:700;text-transform:uppercase;'
                        f'letter-spacing:.12em;color:var(--t3);padding:12px 10px 4px;">{tg(group_name)}</div>',
                        unsafe_allow_html=True)
            for p in visible:
                active = st.session_state["_page"] == p
                if st.button(tp(p), key=f"nav_{p}", use_container_width=True,
                             type="primary" if active else "secondary"):
                    st.session_state["_page"] = p
                    st.rerun()

    page = st.session_state["_page"]
    st.divider()
    st.caption(f"{len(load_restaurants())} точек · SH · v9.41")

if IS_LIGHT:
    CHART_THEME = dict(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#5a5a70", size=11),
        title_font=dict(color="#1a1a2e", size=13, family="Inter"),
        hoverlabel=dict(bgcolor="#ffffff", bordercolor="#e0e0e8", font_size=12, font_family="Inter"),
        colorway=["#00b847","#7ab800","#e65100","#6b3fc6","#0891b2","#cc9900","#cc0070","#3b82f6"],
        dragmode=False,
    )
else:
    CHART_THEME = dict(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#7a7a92", size=11),
        title_font=dict(color="#ffffff", size=13, family="Inter"),
        hoverlabel=dict(bgcolor="#0e0e16", bordercolor="#1a1a28", font_size=12, font_family="Inter"),
        colorway=["#00ff6a","#c8ff00","#ff6b9d","#8b5cf6","#00d4ff","#ffaa00","#ff4d6a","#22d3ee"],
        dragmode=False,
    )

# Подписи для осей и подсказок plotly — двуязычные
def _labels():
    en = _get_lang() == "en"
    return {
        "TOTAL_SUM": "Revenue, ₽" if en else "Выручка, ₽",
        "TOTAL_QTY": "Qty" if en else "Кол-во",
        "ORDER_COUNT": "Orders" if en else "Заказов",
        "DISH_NAME": "Dish" if en else "Блюдо",
        "DISH_ID": "ID", "REVENUE": "Revenue, ₽" if en else "Выручка, ₽",
        "AVG_CHECK": "Avg check, ₽" if en else "Ср. чек, ₽",
        "GUESTS": "Guests" if en else "Гостей",
        "ORDERS": "Orders" if en else "Заказов",
        "REST_NAME": "Location" if en else "Точка", "REST_ID": "ID",
        "EMP_NAME": "Employee" if en else "Сотрудник", "EMP_ID": "ID",
        "CATEGORY": "Category" if en else "Категория", "CAT_ID": "ID",
        "COUNT": "Qty" if en else "Кол-во", "SUM": "Amount, ₽" if en else "Сумма, ₽",
        "CNT": "Qty" if en else "Кол-во",
        "AVG_SEC": "Avg time, sec" if en else "Ср. время, сек",
        "MIN_SEC": "Min, sec" if en else "Мин, сек",
        "MAX_SEC": "Max, sec" if en else "Макс, сек",
        "CASHIER": "Cashier" if en else "Кассир",
        "CASHIER_NAME": "Cashier" if en else "Кассир",
        "VOID_REASON": "Reason" if en else "Причина",
        "PRLISTSUM": "Amount, ₽" if en else "Сумма, ₽",
        "QUANTITY": "Qty" if en else "Кол-во", "HOUR": "Hour" if en else "Час",
        "DAY": "Date" if en else "Дата",
        "PRICE": "Price, ₽" if en else "Цена, ₽",
        "AVG_PRICE": "Avg price, ₽" if en else "Ср. цена, ₽",
        "TIME_RANGE": "Range" if en else "Диапазон",
        "LABEL": "Time" if en else "Время",
        "PAY_TYPE": "Payment type" if en else "Тип оплаты",
        "PAYLINETYPE": "Payment type" if en else "Тип оплаты",
        "OPERATION": "Operation" if en else "Операция",
        "OPERATOR_NAME": "Cashier" if en else "Кассир",
        "MANAGER_NAME": "Manager" if en else "Менеджер",
        "DIFF": "Diff, ₽" if en else "Разница, ₽",
        "ORDERSUMBEFORE": "Before" if en else "Сумма до",
        "ORDERSUMAFTER": "After" if en else "Сумма после",
        "LATE_COUNT": "Late" if en else "Опозданий",
        "SHIFT_COUNT": "Shifts" if en else "Смен",
        "AVG_HOURS": "Avg hours" if en else "Ср. часов",
        "DURATION_MIN": "Duration" if en else "Длительность",
        "SHIFTS": "Shifts" if en else "Смен",
        "AVG_DISHES": "Avg dishes" if en else "Ср. блюд",
        "PRICE_DIFF": "Diff, ₽" if en else "Разница, ₽",
        "PRICE_VARIANTS": "Price variants" if en else "Вариантов цен",
        "MIN_PRICE": "Min price" if en else "Мин. цена",
        "MAX_PRICE": "Max price" if en else "Макс. цена",
        "STATUS": "Status" if en else "Статус",
        "PRODUCT": "Product" if en else "Товар",
        "MESSAGEFROMDRIVER": "Message" if en else "Сообщение",
        "CREATOR_NAME": "Cashier" if en else "Кассир",
        "AUTHOR_NAME": "Manager" if en else "Менеджер",
        "DELETE_PERSON_NAME": "Voided by" if en else "Кто отменил",
        "DELETE_MANAGER_NAME": "Approved by" if en else "Менеджер отмены",
        "TOPAYSUM": "Amount, ₽" if en else "Сумма, ₽",
        "BASICSUM": "Amount, ₽" if en else "Сумма, ₽",
    }
RU = _labels()  # backward compat — but charts should use labels=_labels()

# Умное кэширование — данные грузятся один раз, обновляются только по кнопке
def page_header(title, icon="", show_period=True):
    """Заголовок страницы с кнопкой обновления и селектором периода."""
    global date_from, date_to
    st.markdown(f"## {tp(title)}")
    if show_period:
        date_from, date_to = period_selector(key_suffix=title.replace(" ","_"))
        st.caption(f"{date_from} — {date_to}")
    return False

def fix_bar_hover(fig):
    """Fix Plotly charts: hover tooltips, pie legends, gridlines, undefined title."""
    # Fix "undefined" title bug in plotly_dark template
    if not fig.layout.title or not fig.layout.title.text:
        fig.update_layout(title="")
    for trace in fig.data:
        if hasattr(trace, 'type') and trace.type == 'bar' and trace.text is not None:
            if not trace.hovertemplate:
                trace.hovertemplate = "<b>%{y}</b><br>%{x:,.0f}<extra></extra>" if trace.orientation == 'h' else "<b>%{x}</b><br>%{y:,.0f}<extra></extra>"
        if hasattr(trace, 'type') and trace.type == 'pie':
            fig.update_layout(legend=dict(orientation="h", yanchor="top", y=-0.05, font_size=10))
    # Fix gridlines and axis labels for light theme
    if IS_LIGHT:
        fig.update_xaxes(gridcolor="rgba(0,0,0,0.04)", zerolinecolor="rgba(0,0,0,0.06)", gridwidth=0.5,
                         tickfont=dict(color="#5a5a70"), title_font=dict(color="#5a5a70"))
        fig.update_yaxes(gridcolor="rgba(0,0,0,0.04)", zerolinecolor="rgba(0,0,0,0.06)", gridwidth=0.5,
                         tickfont=dict(color="#5a5a70"), title_font=dict(color="#5a5a70"))
        # Also fix legend text
        fig.update_layout(legend_font_color="#5a5a70")
    return fig

# ============================================================
# --- ПУЛЬС (Executive Dashboard) ---
# ============================================================
if page == "Пульс":
    page_header("Пульс")

    today = datetime.now().date()
    yesterday = today - timedelta(1)

    # --- Load data: today, yesterday, this week, last week ---
    # Cache in session_state so page switches don't reload
    _pulse_key = f"_pdata_pulse_{date_from}_{date_to}"
    if _pulse_key not in st.session_state:
        with st.spinner("Loading..."):
            _pd = {}
            _pd["ord_today"] = load_orders(today, today)
            _pd["ord_yest"] = load_orders(yesterday, yesterday)
            _pd["ord_period"] = load_orders(date_from, date_to)

            # Previous period same length
            period_days = (date_to - date_from).days or 1
            prev_from = date_from - timedelta(period_days)
            prev_to = date_from - timedelta(1)
            _pd["prev_from"] = prev_from
            _pd["prev_to"] = prev_to
            _pd["period_days"] = period_days
            _pd["ord_prev"] = load_orders(prev_from, prev_to)

            _pd["rest_data"] = run_query("""
                SELECT r.NAME as REST_NAME,
                    SUM(o.TOPAYSUM) as REVENUE, COUNT(*) as ORDERS,
                    AVG(o.TOPAYSUM) as AVG_CHECK, SUM(o.GUESTSCOUNT) as GUESTS
                FROM ORDERS o
                JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
                JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                  AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
                GROUP BY r.NAME ORDER BY REVENUE DESC""", (str(date_from), str(date_to)))

            _pd["voids_cnt"] = run_query("""
                SELECT COUNT(*) as CNT, SUM(dv.PRLISTSUM) as TOTAL
                FROM DISHVOIDS dv
                JOIN ORDERS o ON dv.VISIT=o.VISIT AND dv.MIDSERVER=o.MIDSERVER AND dv.ORDERIDENT=o.IDENTINVISIT
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                  AND (dv.DBSTATUS IS NULL OR dv.DBSTATUS!=-1)""", (str(date_from), str(date_to)))

            _pd["coverage"] = run_query("""
                SELECT
                    (SELECT COUNT(*) FROM RESTAURANTS WHERE NAME IS NOT NULL AND STATUS > 0) as TOTAL_REST,
                    COUNT(DISTINCT r.SIFR) as ACTIVE_REST,
                    COUNT(DISTINCT o.MIDSERVER) as ACTIVE_SERVERS,
                    (SELECT COUNT(DISTINCT MIDSERVER) FROM ORDERS WHERE OPENTIME >= DATEADD(DAY,-7,GETDATE()) AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1) as TOTAL_SERVERS,
                    MAX(o.OPENTIME) as LAST_ORDER
                FROM ORDERS o
                JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
                JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                  AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1""", (str(date_from), str(date_to)))

            st.session_state[_pulse_key] = _pd

    _pd = st.session_state[_pulse_key]
    ord_today = _pd["ord_today"]
    ord_yest = _pd["ord_yest"]
    ord_period = _pd["ord_period"]
    ord_prev = _pd["ord_prev"]
    rest_data = _pd["rest_data"]
    voids_cnt = _pd["voids_cnt"]
    coverage = _pd["coverage"]
    prev_from = _pd["prev_from"]
    prev_to = _pd["prev_to"]
    period_days = _pd["period_days"]

    # === HELPERS ===
    def safe_sum(df, col):
        try: return float(df[col].sum()) if not df.empty and col in df.columns else 0
        except: return 0
    def safe_cnt(df):
        return len(df) if not df.empty else 0
    def pct_change(cur, prev):
        if prev and prev > 0: return (cur / prev - 1) * 100
        return None
    def delta_fmt(pct):
        if pct is None: return ""
        return f"{pct:+.1f}%"

    # === KEY NUMBERS ===
    rev_today = safe_sum(ord_today, "TOPAYSUM")
    rev_yest = safe_sum(ord_yest, "TOPAYSUM")
    rev_period = safe_sum(ord_period, "TOPAYSUM")
    rev_prev = safe_sum(ord_prev, "TOPAYSUM")
    orders_period = safe_cnt(ord_period)
    orders_prev = safe_cnt(ord_prev)
    guests_period = safe_sum(ord_period, "GUESTSCOUNT")
    avg_check = rev_period / orders_period if orders_period else 0
    avg_check_prev = rev_prev / orders_prev if orders_prev else 0
    voids_count = int(voids_cnt.iloc[0]["CNT"]) if not voids_cnt.empty else 0
    voids_sum = float(voids_cnt.iloc[0]["TOTAL"] or 0) if not voids_cnt.empty else 0

    # ============================================================
    # ROW 1: KEY METRICS
    # ============================================================
    accent = "#00ff6a" if not IS_LIGHT else "#00b847"
    muted = "var(--t3)"
    card_bg = "var(--card)"
    t1 = "var(--t1)"

    st.markdown("### " + ("Key Metrics" if _get_lang()=="en" else "Ключевые показатели"))
    is_today_period = (date_from == datetime.now().date() and date_to == datetime.now().date())

    if is_today_period:
        c1, c2, c3, c4 = st.columns(4)
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        d = pct_change(rev_period, rev_prev)
        st.metric(t("revenue"), f"{rev_period:,.0f} ₽", delta_fmt(d))
    with c2:
        d = pct_change(orders_period, orders_prev)
        st.metric(t("orders"), f"{orders_period:,}", delta_fmt(d))
    with c3:
        d = pct_change(avg_check, avg_check_prev)
        st.metric(t("avg_check"), f"{avg_check:,.0f} ₽", delta_fmt(d))
    with c4:
        st.metric(t("guests"), f"{int(guests_period):,}")
    if not is_today_period:
        with c5:
            st.metric(t("today_metric"), f"{rev_today:,.0f} ₽",
                       delta_fmt(pct_change(rev_today, rev_yest)) if rev_yest else "")

    st.caption(f"% — изменение к предыдущему аналогичному периоду ({prev_from} — {prev_to}). Зелёный ▲ = рост, красный ▼ = снижение." +
               (" «Сегодня» сравнивается со вчера." if not is_today_period else " Сравнение со вчера (день ещё идёт)."))

    # === DATA COVERAGE STRIP ===
    if not coverage.empty:
        total_rest = int(coverage.iloc[0]["TOTAL_REST"] or 0)
        active_rest = int(coverage.iloc[0]["ACTIVE_REST"] or 0)
        active_servers = int(coverage.iloc[0]["ACTIVE_SERVERS"] or 0)
        total_servers = int(coverage.iloc[0]["TOTAL_SERVERS"] or 0)
        last_order = coverage.iloc[0]["LAST_ORDER"]
        cov_pct = (active_rest / total_rest * 100) if total_rest > 0 else 0

        if last_order:
            from datetime import datetime as _dt
            try:
                if isinstance(last_order, str):
                    last_order = _dt.strptime(last_order[:19], "%Y-%m-%d %H:%M:%S")
                lag_minutes = int((_dt.now() - last_order).total_seconds() / 60)
                lag_text = f"{lag_minutes} мин назад" if lag_minutes < 60 else f"{lag_minutes // 60} ч {lag_minutes % 60} мин назад"
            except:
                lag_text = str(last_order)[:16]
        else:
            lag_text = "нет данных"

        # Color based on coverage
        if cov_pct >= 80:
            cov_color = "#00ff6a" if not IS_LIGHT else "#00b847"
            cov_status = "Хорошее"
        elif cov_pct >= 50:
            cov_color = "#ffaa00"
            cov_status = "Частичное"
        else:
            cov_color = "#ff4d6a" if not IS_LIGHT else "#e53e3e"
            cov_status = "Низкое"

        st.markdown(f"""<div style="display:flex; gap:24px; align-items:center; padding:10px 16px;
            background:var(--card); border:1px solid var(--border); border-radius:10px; margin-top:12px;">
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:8px;height:8px;border-radius:50%;background:{cov_color};"></div>
                <span style="font-size:.75rem;color:var(--t2);">Покрытие: <strong style="color:{cov_color};">{active_rest}/{total_rest} точек ({cov_pct:.0f}%)</strong></span>
            </div>
            <span style="font-size:.75rem;color:var(--t3);">·</span>
            <span style="font-size:.75rem;color:var(--t2);">Касс: <strong style="color:var(--t1);">{active_servers}/{total_servers}</strong></span>
            <span style="font-size:.75rem;color:var(--t3);">·</span>
            <span style="font-size:.75rem;color:var(--t2);">Данные: <strong style="color:var(--t1);">{lag_text}</strong></span>
            <span style="font-size:.75rem;color:var(--t3);">·</span>
            <span style="font-size:.68rem;padding:2px 8px;border-radius:8px;background:{cov_color}20;color:{cov_color};font-weight:600;">{cov_status}</span>
        </div>""", unsafe_allow_html=True)

    # ============================================================
    # ROW 1.5: FINANCIAL & OPERATIONAL METRICS
    # ============================================================
    st.divider()
    st.markdown("### " + ("Finance & Operations" if _get_lang()=="en" else "Финансы и операции"))

    # Cache financial data in session_state
    _pfin_key = f"_pdata_pulse_fin_{date_from}_{date_to}"
    if _pfin_key not in st.session_state:
        _pf = {}
        _pf["tax_data"] = pd.DataFrame()
        try:
            _pf["tax_data"] = run_query("""
                SELECT tdt.NAME as TAX_LABEL, CAST(SUM(sd.PRLISTSUM) AS INT) as REVENUE
                FROM SESSIONDISHES sd
                JOIN TAXDISHTYPES tdt ON sd.ITAXDISHTYPE=tdt.SIFR
                JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                  AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1) AND sd.QUANTITY > 0
                  AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
                GROUP BY tdt.NAME ORDER BY REVENUE DESC""", (str(date_from), str(date_to)))
        except: pass

        _pf["shifts_data"] = None
        try:
            _pf["shifts_data"] = run_query("""
                SELECT COUNT(DISTINCT IRESTAURANT) as OPEN_REST,
                       (SELECT COUNT(*) FROM RESTAURANTS WHERE NAME IS NOT NULL AND STATUS > 0) as TOTAL_REST
                FROM GLOBALSHIFTS
                WHERE STARTTIME >= %s AND STARTTIME < DATEADD(DAY,1,%s)""", (str(date_from), str(date_to)))
        except: pass

        _pf["today_purchases"] = 0
        try:
            p_df = sh_stat_query("""SELECT CAST(SUM(PAYSUMNOTAX) AS INT) as S
                FROM STAT_SH4_SHIFTS_INVOICES WHERE INVOICEDATE >= %s AND INVOICEDATE <= %s""",
                (str(date_from), str(date_to)))
            if not p_df.empty and p_df.iloc[0]["S"] is not None:
                _pf["today_purchases"] = float(p_df.iloc[0]["S"])
        except: pass

        _pf["p_staff_meals"] = 0
        try:
            sm = run_query("""SELECT SUM(p.BASICSUM) as T FROM PAYMENTS p
                JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1) AND p.PAYLINETYPE=3""", (str(date_from), str(date_to)))
            if not sm.empty and sm.iloc[0]["T"] is not None:
                _pf["p_staff_meals"] = float(sm.iloc[0]["T"])
        except: pass

        st.session_state[_pfin_key] = _pf

    _pf = st.session_state[_pfin_key]
    tax_data = _pf["tax_data"]
    shifts_data = _pf["shifts_data"]
    today_purchases = _pf["today_purchases"]
    p_staff_meals = _pf["p_staff_meals"]

    # Если SQL пуст — пробуем закэшированные данные из SH API (всегда за 30 дней)
    _purch_cache_key = "sh_purchases_30d"
    if today_purchases == 0 and _purch_cache_key in st.session_state:
        _cached_purch = st.session_state[_purch_cache_key]
        if not _cached_purch.empty and "TOTAL_AMOUNT" in _cached_purch.columns:
            today_purchases = float(_cached_purch["TOTAL_AMOUNT"].sum())

    # --- Скидки ---
    p_discounts = safe_sum(ord_period, "DISCOUNTSUM")

    # --- Расчёт P&L ---
    fixed_costs = load_user_setting(CURRENT_USER["username"], "fixed_costs", {"staff":0,"rent":0,"utilities":0,"marketing":0,"other":0})
    fixed_monthly = sum(fixed_costs.values())
    fixed_for_period = _fixed_cost_for_period(fixed_monthly, date_from, date_to)

    # Себестоимость: используем данные из Фудкост (расчёт) если загружены, иначе только переменные расходы
    recipe_sebes = 0
    if "recipe_costs" in st.session_state and not st.session_state["recipe_costs"].empty:
        try:
            rc = st.session_state["recipe_costs"]
            rk_p = load_rk_dish_prices(date_from, date_to)
            sh_p = load_sh_goods_prices()
            if not rk_p.empty and not sh_p.empty:
                fc_match = match_foodcost(rk_p, sh_p, purchase_prices=rc)
                if not fc_match.empty and "FOODCOST_PCT" in fc_match.columns:
                    # С рецептурами
                    fc_with = fc_match[fc_match["FOODCOST_PCT"].notna() & (fc_match["FOODCOST_PCT"] > 0) & (fc_match["FOODCOST_PCT"] < 300)]
                    if not fc_with.empty:
                        recipe_sebes += float((fc_with["COST_PRICE"] * fc_with["TOTAL_QTY"]).sum())
                    # Без рецептур (по цене SH)
                    fc_no = fc_match[(fc_match["FOODCOST_PCT"].isna()) | (fc_match["FOODCOST_PCT"] <= 0)]
                    if not fc_no.empty and "COST_PRICE" in fc_no.columns:
                        recipe_sebes += float((fc_no["COST_PRICE"].fillna(0) * fc_no["TOTAL_QTY"]).sum())
        except: pass

    sebestoimost = max(recipe_sebes, today_purchases) + p_discounts
    # Питание сотрудников: вычитаем себестоимость, а не сумму по меню
    _sm_cost, _sm_fc_pct, _sm_fc_exact = _staff_meal_cost(p_staff_meals)
    sebestoimost += _sm_cost
    # Маржа = выручка - себестоимость
    margin = rev_period - sebestoimost
    margin_pct = (margin / rev_period * 100) if rev_period > 0 else 0

    # Налоги — вычисляем СУММУ НДС из выручки по ставкам
    # НДС включён в цену: налог = выручка * ставка / (100 + ставка)
    def _extract_rate(label):
        """Извлечь ставку НДС из названия: 'НДС 22%' → 22, 'НДС 0%' → 0, 'Без НДС' → 0."""
        import re
        m = re.search(r'(\d+)', str(label))
        return int(m.group(1)) if m else 0

    tax_sum = 0
    if not tax_data.empty:
        tax_data = tax_data.copy()
        tax_data["RATE"] = tax_data["TAX_LABEL"].apply(_extract_rate)
        tax_data["TAX_AMOUNT"] = tax_data.apply(
            lambda r: r["REVENUE"] * r["RATE"] / (100 + r["RATE"]) if r["RATE"] > 0 else 0, axis=1)
        tax_sum = float(tax_data["TAX_AMOUNT"].sum())

    # Фудкост % (себестоимость / выручка)
    foodcost_pct = (sebestoimost / rev_period * 100) if rev_period > 0 else 0
    # Доход = маржа - налоги - постоянные расходы
    income = margin - tax_sum - fixed_for_period
    income_daily = income / period_days if period_days > 0 else 0

    # Открытые смены
    open_rest = 0; total_rest_shifts = 0
    if shifts_data is not None and not shifts_data.empty:
        open_rest = int(shifts_data.iloc[0]["OPEN_REST"] or 0)
        total_rest_shifts = int(shifts_data.iloc[0]["TOTAL_REST"] or 0)

    # --- Главная линейка P&L ---
    st.markdown(f"##### P&L")
    _has_cost_data = today_purchases > 0 or recipe_sebes > 0
    if not _has_cost_data:
        st.warning(t("purchases_not_loaded"))
        if st.button(f"📥 {t('load_30d')}", key="load_purchases_btn"):
            _pp_progress = st.container()
            _pp_d1 = (datetime.now().date() - timedelta(30)).isoformat()
            _pp_d2 = datetime.now().date().isoformat()
            with _pp_progress:
                with st.spinner(t("loading_purchases")):
                    _pp_data, _pp_err = sh_load_purchase_prices(
                        _pp_d1, _pp_d2,
                        progress_container=_pp_progress, max_rids=50)
                    if _pp_err:
                        st.warning(f"{_pp_err}")
                    elif not _pp_data.empty:
                        st.session_state[_purch_cache_key] = _pp_data
                        st.rerun()
                    else:
                        st.info(t("no_invoices_30d"))
    st.caption(t("margin_formula"))
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        st.metric(t("margin"), f"{margin:,.0f} ₽", delta=f"{margin_pct:.1f}% {'of revenue' if _get_lang()=='en' else 'от выручки'}")
    with p2:
        st.metric(t("taxes_nds"), f"{tax_sum:,.0f} ₽",
                   delta=f"{tax_sum/rev_period*100:.1f}% {'of revenue' if _get_lang()=='en' else 'от выручки'}" if rev_period > 0 else "")
    with p3:
        st.metric(t("cost_label"), f"{sebestoimost:,.0f} ₽",
                   delta=f"{t('foodcost')} {foodcost_pct:.1f}%")
    with p4:
        st.metric(t("income"), f"{income:,.0f} ₽",
                   delta=f"{income_daily:,.0f} ₽/{'day' if _get_lang()=='en' else 'день'}")

    # Детализация налогов
    if not tax_data.empty:
        tax_items = []
        for _, row in tax_data.iterrows():
            rate = row.get("RATE", 0)
            tax_amt = row.get("TAX_AMOUNT", 0)
            rev = row["REVENUE"]
            if tax_amt > 0:
                tax_items.append(f"{row['TAX_LABEL']}: {tax_amt:,.0f} ₽")
            else:
                tax_items.append(f"{row['TAX_LABEL']}: 0 ₽")
        st.caption(f"{t('taxes_nds')}: " + " · ".join(tax_items))

    # --- Детали расходов ---
    st.markdown(f"##### {t("details_label")}")
    d1, d2, d3, d4 = st.columns(4)
    _purch_from_api = _purch_cache_key in st.session_state
    with d1:
        if today_purchases > 0:
            _purch_label = "Закупки (API)" if _purch_from_api else "Закупки (SH)"
            st.metric(_purch_label, f"{today_purchases:,.0f} ₽",
                       delta=f"{today_purchases/rev_period*100:.1f}%" if rev_period > 0 else "")
        else:
            st.metric(t("purchases_sh"), "нет данных")
    with d2:
        st.metric(t("discounts"), f"{p_discounts:,.0f} ₽",
                   delta=f"{p_discounts/rev_period*100:.1f}%" if rev_period > 0 else "")
    with d3:
        _sm_delta = f"по меню {p_staff_meals:,.0f} ₽ · {'точный' if _sm_fc_exact else '~33%'}"
        st.metric(t("staff_cost_puls"), f"{_sm_cost:,.0f} ₽", delta=_sm_delta)
    with d4:
        st.metric(t("shifts_open"), f"{open_rest}/{total_rest_shifts}" if total_rest_shifts > 0 else "—")

    if fixed_monthly == 0:
        st.caption(t("fill_fixed_costs"))
    if recipe_sebes > 0:
        st.caption(f"{t('cost_from_recipes')}: {recipe_sebes:,.0f} ₽")
    elif recipe_sebes == 0 and today_purchases == 0:
        st.caption(t("open_foodcost_calc"))
    elif _purch_from_api:
        _n_items = len(st.session_state[_purch_cache_key])
        st.caption(f"{t('purchases_sh')} (30d): {_n_items} · {t('fixed_expenses_line')}: {fixed_for_period:,.0f} ₽")
    else:
        st.caption(f"{t('fixed_expenses_line')}: {fixed_for_period:,.0f} ₽ ({fixed_monthly:,.0f} ₽/mo)")

    # ============================================================
    # ROW 2: Revenue TODAY vs YESTERDAY + Top Restaurants
    # ============================================================
    st.divider()
    left, right = st.columns([3, 2])

    with left:
        st.markdown("### " + ("Revenue: today vs yesterday" if _get_lang()=="en" else "Выручка: сегодня vs вчера"))
        bar_data = pd.DataFrame({
            "День": ["Вчера", "Сегодня"],
            "Выручка": [rev_yest, rev_today]
        })
        fig = go.Figure()
        fig.add_trace(go.Bar(x=["Вчера"], y=[rev_yest],
            marker_color="rgba(255,255,255,0.12)" if not IS_LIGHT else "rgba(0,0,0,0.08)",
            text=[f"{rev_yest:,.0f} ₽"], textposition="auto",
            textfont=dict(color="#666" if not IS_LIGHT else "#999"), name="Вчера"))
        fig.add_trace(go.Bar(x=["Сегодня"], y=[rev_today],
            marker_color=accent,
            text=[f"{rev_today:,.0f} ₽"], textposition="auto",
            textfont=dict(color="#fff" if not IS_LIGHT else "#1a1a2e"), name="Сегодня"))
        fig.update_layout(height=280, showlegend=False, **CHART_THEME)
        fix_bar_hover(fig)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    with right:
        st.markdown("### " + ("Top Locations" if _get_lang()=="en" else "Топ точек"))
        if not rest_data.empty:
            top5 = rest_data.head(5)
            max_rev = float(top5["REVENUE"].max()) if not top5.empty else 1
            for _, row in top5.iterrows():
                name = row["REST_NAME"]
                rev = float(row["REVENUE"])
                pct = rev / max_rev * 100
                orders = int(row["ORDERS"])
                st.markdown(f"""<div style="margin-bottom:10px;">
                    <div style="display:flex;justify-content:space-between;align-items:baseline;">
                        <span style="font-size:.82rem;font-weight:600;color:{t1};">{name}</span>
                        <span style="font-size:.9rem;font-weight:800;color:{t1};">{rev:,.0f} ₽</span>
                    </div>
                    <div style="height:4px;background:var(--border);border-radius:2px;margin-top:4px;">
                        <div style="height:100%;width:{pct:.0f}%;background:{accent};border-radius:2px;"></div>
                    </div>
                    <div style="font-size:.65rem;color:{muted};margin-top:2px;">{orders} заказов</div>
                </div>""", unsafe_allow_html=True)

    # ============================================================
    # ROW 3: Problems + Period comparison
    # ============================================================
    st.divider()
    p1, p2, p3 = st.columns(3)

    with p1:
        st.markdown("### " + ("Issues" if _get_lang()=="en" else "Проблемы"))
        if voids_count > 0:
            voids_pct = voids_sum / rev_period * 100 if rev_period > 0 else 0
            st.markdown(f"""<div style="background:{card_bg};border:1px solid var(--border);border-radius:14px;padding:18px 20px;">
                <div style="font-size:.62rem;color:{muted};text-transform:uppercase;letter-spacing:.1em;font-weight:700;">Удаление блюд за период</div>
                <div style="font-size:1.5rem;font-weight:800;color:{'#ff4d6a' if not IS_LIGHT else '#e53e3e'};">{voids_count}</div>
                <div style="font-size:.78rem;color:var(--t2);margin-top:4px;">Сумма: {voids_sum:,.0f} ₽ ({voids_pct:.1f}% выручки)</div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div style="background:{card_bg};border:1px solid var(--border);border-radius:14px;padding:18px 20px;">
                <div style="font-size:.62rem;color:{muted};text-transform:uppercase;letter-spacing:.1em;font-weight:700;">Удаление блюд за период</div>
                <div style="font-size:1.5rem;font-weight:800;color:{accent};">0</div>
                <div style="font-size:.78rem;color:var(--t2);margin-top:4px;">Нет удалений блюд</div>
            </div>""", unsafe_allow_html=True)

    with p2:
        st.markdown("### " + (t("period")))
        d_rev = pct_change(rev_period, rev_prev)
        d_ord = pct_change(orders_period, orders_prev)
        color_rev = accent if (d_rev and d_rev >= 0) else ("#ff4d6a" if not IS_LIGHT else "#e53e3e")
        st.markdown(f"""<div style="background:{card_bg};border:1px solid var(--border);border-radius:14px;padding:18px 20px;">
            <div style="font-size:.62rem;color:{muted};text-transform:uppercase;letter-spacing:.1em;font-weight:700;">vs предыдущий период</div>
            <div style="margin-top:12px;">
                <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="color:var(--t2);font-size:.82rem;">Выручка</span>
                    <span style="color:{color_rev};font-weight:700;font-size:.82rem;">{delta_fmt(d_rev)}</span>
                </div>
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:var(--t2);font-size:.82rem;">Заказы</span>
                    <span style="color:var(--t1);font-weight:700;font-size:.82rem;">{delta_fmt(d_ord)}</span>
                </div>
            </div>
            <div style="font-size:.65rem;color:{muted};margin-top:10px;">{prev_from} → {prev_to}</div>
        </div>""", unsafe_allow_html=True)

    with p3:
        st.markdown("### " + ("Dishes/order" if _get_lang()=="en" else "Блюд/заказ"))
        dishes_period = safe_sum(ord_period, "TOTALDISHPIECES")
        bpz = dishes_period / orders_period if orders_period else 0
        gpz = guests_period / orders_period if orders_period else 0
        st.markdown(f"""<div style="background:{card_bg};border:1px solid var(--border);border-radius:14px;padding:18px 20px;">
            <div style="font-size:.62rem;color:{muted};text-transform:uppercase;letter-spacing:.1em;font-weight:700;">Эффективность</div>
            <div style="margin-top:12px;">
                <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="color:var(--t2);font-size:.82rem;">Блюд/заказ</span>
                    <span style="color:{t1};font-weight:800;font-size:1.1rem;">{bpz:.1f}</span>
                </div>
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:var(--t2);font-size:.82rem;">Гостей/заказ</span>
                    <span style="color:{t1};font-weight:800;font-size:1.1rem;">{gpz:.1f}</span>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

    # ============================================================
    # ROW 4: Revenue by restaurant (horizontal bars)
    # ============================================================
    if not rest_data.empty and len(rest_data) > 5:
        st.divider()
        st.markdown("### " + ("All Locations" if _get_lang()=="en" else "Все точки"))
        fig = px.bar(rest_data, x="REVENUE", y="REST_NAME", orientation="h",
            color="REVENUE", color_continuous_scale="Tealgrn",
            text=rest_data["REVENUE"].apply(lambda x: f"{x:,.0f} ₽"),
            labels=_labels())
        fig.update_layout(height=max(300, len(rest_data) * 32),
            yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
        fix_bar_hover(fig)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

# --- ИИ ЧАТ ---
if page == "ИИ-чат":
    page_header("ИИ-чат")
    st.caption("*Top-5 dishes in Canteen 1 • Revenue by location • Who is late most?*" if _get_lang()=="en" else "*«Топ-5 блюд в Столовой 1» • «Выручка по столовым» • «Кто опаздывает?»*")
    if "chat_history" not in st.session_state: st.session_state.chat_history = []
    qcols = st.columns(4)
    quick = ["Выручка по столовым за 7 дней","Топ-10 блюд за месяц","Топ-5 кассиров по выручке","Остатки на складах"]
    sel_q = None
    for i,q in enumerate(quick):
        with qcols[i]:
            if st.button(q, key=f"q{i}", use_container_width=True): sel_q = q
    st.divider()
    for msg in st.session_state.chat_history:
        with st.chat_message("user" if msg["role"]=="user" else "assistant"):
            st.markdown(msg["content"])
            if msg.get("dataframe") is not None:
                st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)
            if msg.get("sql"):
                with st.expander("📝 SQL"): st.code(msg["sql"], language="sql")
    user_input = st.chat_input("Ask a question..." if _get_lang()=="en" else "Задайте вопрос...")
    question = sel_q or user_input
    if question:
        st.session_state.chat_history.append({"role":"user","content":question})
        with st.spinner("Генерирую SQL..."):
            raw = generate_sql(question).strip()
        if raw.startswith("Ошибка Gemini") or raw.startswith("Gemini перегружен"):
            st.session_state.chat_history.append({"role":"assistant","content":f"{raw}","sql":None,"dataframe":None})
            st.rerun()
        sql = raw
        for p in ["```sql","```SQL","```"]: sql = sql.replace(p,"")
        sql = sql.strip()
        is_none = sql.upper().strip() == "NONE"
        if not is_none:
            match = re.search(r'((?:SELECT|WITH)\b.+)', sql, re.IGNORECASE|re.DOTALL)
            if match: sql = match.group(1).strip().rstrip(";").strip()
            else: is_none = True
        if is_none:
            st.session_state.chat_history.append({"role":"assistant","content":"Не связано с данными ресторана. Данные по складу/остаткам/фудкосту — на страницах Склад и Накладные.","sql":None,"dataframe":None})
        else:
            with st.spinner("Loading..."):
                df, err = run_query_safe(sql)
            if err:
                fix = ask_gemini(f"SQL ошибка: {err}\nЗапрос: {sql}\nСхема: {get_rkeeper_schema()}\nИсправь. ТОЛЬКО SQL.")
                fix = fix.strip()
                for p in ["```sql","```SQL","```"]: fix = fix.replace(p,"")
                m2 = re.search(r'((?:SELECT|WITH)\b.+)', fix, re.IGNORECASE|re.DOTALL)
                if m2: fix = m2.group(1).strip().rstrip(";").strip()
                df2,err2 = run_query_safe(fix)
                if not err2: sql,df,err = fix,df2,None
                else:
                    st.session_state.chat_history.append({"role":"assistant","content":f"Ошибка: {err2}","sql":sql,"dataframe":None})
                    st.rerun()
            if err is None:
                with st.spinner("💬 Ответ..."):
                    answer = explain_results(question, sql, df)
                st.session_state.chat_history.append({"role":"assistant","content":answer,"sql":sql,
                    "dataframe": df if df is not None and not df.empty else None})
        st.rerun()
    if st.session_state.chat_history:
        if st.button(t("clear_chat"), use_container_width=True):
            st.session_state.chat_history = []; st.rerun()

# --- ПРОАКТИВНЫЙ АНАЛИЗ ---
if page == "Проактив":
    page_header("Проактив")
    days_in_period = (date_to - date_from).days + 1
    prev_end = date_from - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days_in_period-1)
    st.caption(f"{t('comparison')}: **{date_from} — {date_to}** vs **{prev_start} — {prev_end}**")

    cur_totals, prev_totals, _, _, _ = load_period_totals(date_from, date_to)
    anomalies = []

    if not cur_totals.empty and not prev_totals.empty:
        def safe(df, col):
            v = df.iloc[0][col]
            return float(v) if v and pd.notna(v) else 0
        def pct(cur, prev):
            return ((cur - prev) / prev * 100) if prev else 0
        def delta_str(val):
            return f"+{val:.1f}%" if val > 0 else f"{val:.1f}%"

        c_rev = safe(cur_totals,"REVENUE"); p_rev = safe(prev_totals,"REVENUE")
        c_ord = safe(cur_totals,"ORDERS"); p_ord = safe(prev_totals,"ORDERS")
        c_avg = safe(cur_totals,"AVG_CHECK"); p_avg = safe(prev_totals,"AVG_CHECK")
        c_gst = safe(cur_totals,"GUESTS"); p_gst = safe(prev_totals,"GUESTS")
        c_dsc = safe(cur_totals,"DISCOUNTS"); p_dsc = safe(prev_totals,"DISCOUNTS")

        pct_rev = pct(c_rev, p_rev); pct_ord = pct(c_ord, p_ord)
        pct_avg = pct(c_avg, p_avg); pct_gst = pct(c_gst, p_gst)

        # Метрики с дельтой
        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric(t("revenue"), f"{c_rev:,.0f} ₽", delta=delta_str(pct_rev), delta_color="normal")
        with c2: st.metric(t("orders"), f"{c_ord:,.0f}", delta=delta_str(pct_ord), delta_color="normal")
        with c3: st.metric(t("avg_check"), f"{c_avg:,.0f} ₽", delta=delta_str(pct_avg), delta_color="normal")
        with c4: st.metric(t("guests"), f"{c_gst:,.0f}", delta=delta_str(pct_gst), delta_color="normal")
        st.caption(f"% — изменение к предыдущему периоду ({prev_start} — {prev_end}). Зелёный ▲ = рост, красный ▼ = снижение.")

        st.divider()

        # Собираем аномалии
        if abs(pct_rev) > 5:
            direction = "выросла" if pct_rev > 0 else "упала"
            anomalies.append(f"{'🟢' if pct_rev>0 else '🔴'} Выручка {direction} на {abs(pct_rev):.1f}% ({p_rev:,.0f} → {c_rev:,.0f} ₽)")
        if abs(pct_ord) > 5:
            direction = "выросло" if pct_ord > 0 else "упало"
            anomalies.append(f"{'🟢' if pct_ord>0 else '🔴'} Кол-во заказов {direction} на {abs(pct_ord):.1f}%")
        if abs(pct_avg) > 5:
            direction = "вырос" if pct_avg > 0 else "упал"
            anomalies.append(f"{'🟡' if pct_avg>0 else '🔴'} Средний чек {direction} на {abs(pct_avg):.1f}% ({p_avg:,.0f} → {c_avg:,.0f} ₽)")

        # Отказы
        cur_voids, prev_voids = load_voids_comparison(date_from, date_to)
        if not cur_voids.empty and not prev_voids.empty:
            cv = safe(cur_voids,"VOID_COUNT"); pv = safe(prev_voids,"VOID_COUNT")
            cvs = safe(cur_voids,"VOID_SUM"); pvs = safe(prev_voids,"VOID_SUM")
            if pv > 0 and abs(pct(cv,pv)) > 10:
                anomalies.append(f"{'🔴' if cv>pv else '🟢'} Удаление блюд: {pv:.0f} → {cv:.0f} ({delta_str(pct(cv,pv))}), сумма {cvs:,.0f} ₽")

        # По столовым
        cur_rest, prev_rest, _, _, _ = load_period_comparison(date_from, date_to)
        if not cur_rest.empty and not prev_rest.empty:
            merged = cur_rest.merge(prev_rest, on="REST_NAME", suffixes=("_cur","_prev"), how="outer").fillna(0)
            for _, row in merged.iterrows():
                cr = float(row.get("REVENUE_cur",0)); pr = float(row.get("REVENUE_prev",0))
                if pr > 0:
                    ch = pct(cr, pr)
                    if ch < -15:
                        anomalies.append(f"🔴 {row['REST_NAME']}: выручка упала на {abs(ch):.0f}% ({pr:,.0f} → {cr:,.0f} ₽)")
                    elif ch > 20:
                        anomalies.append(f"🟢 {row['REST_NAME']}: выручка выросла на {ch:.0f}% ({pr:,.0f} → {cr:,.0f} ₽)")
                elif cr > 0:
                    anomalies.append(f"🟡 {row['REST_NAME']}: новая точка или возобновила работу ({cr:,.0f} ₽)")

        # Блюда — падения
        cur_dishes, prev_dishes = load_dishes_comparison(date_from, date_to)
        if not cur_dishes.empty and not prev_dishes.empty:
            dm = cur_dishes.merge(prev_dishes, on="DISH", suffixes=("_cur","_prev"), how="outer").fillna(0)
            for _, row in dm.iterrows():
                cq = float(row.get("QTY_cur",0)); pq = float(row.get("QTY_prev",0))
                if pq > 50 and cq == 0:
                    anomalies.append(f"🟡 Блюдо «{row['DISH']}» перестало продаваться (было {pq:.0f} шт)")
                elif pq > 100 and pq > 0:
                    ch = pct(cq, pq)
                    if ch < -30:
                        anomalies.append(f"🟡 «{row['DISH']}»: продажи упали на {abs(ch):.0f}% ({pq:.0f} → {cq:.0f})")

        # Выводим аномалии
        st.markdown("### " + ("Detected Anomalies" if _get_lang()=="en" else "Обнаруженные аномалии"))
        if anomalies:
            for a in anomalies:
                st.markdown(f"**{a}**")

            # Графики сравнения
            if not cur_rest.empty and not prev_rest.empty:
                st.divider()
                merged_chart = cur_rest.merge(prev_rest, on="REST_NAME", suffixes=(" (сейчас)"," (до)"), how="outer").fillna(0)
                merged_chart = merged_chart.sort_values("REVENUE (now)" if _get_lang()=="en" else "REVENUE (сейчас)", ascending=True).tail(15)
                fig = go.Figure()
                fig.add_trace(go.Bar(y=merged_chart["REST_NAME"], x=merged_chart["REVENUE (до)"],
                    name=("Prev period" if _get_lang()=="en" else "Пред. период"), orientation="h", marker_color="rgba(99,102,241,0.4)"))
                fig.add_trace(go.Bar(y=merged_chart["REST_NAME"], x=merged_chart["REVENUE (сейчас)"],
                    name=("Current period" if _get_lang()=="en" else "Текущий период"), orientation="h", marker_color="#00ff6a"))
                fig.update_layout(title=t("revenue_comparison"), barmode="overlay",
                    height=500, **CHART_THEME, legend=dict(orientation="h",y=1.1))
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # ИИ-рекомендации
            st.divider()
            st.markdown("### " + ("Recommendations" if _get_lang()=="en" else "Рекомендации"))
            anomalies_text = "\n".join(anomalies)
            if st.button(f"🧠 {t('get_ai_recommendations')}", use_container_width=True, type="primary"):
                with st.spinner("🤔 Анализирую и формирую рекомендации..."):
                    recs = generate_proactive_insights(anomalies_text)
                st.markdown(recs)
        else:
            st.success("✅ Всё в норме! Значительных отклонений от предыдущего периода не обнаружено.")
    else:
        st.warning(t("insufficient_data"))

# --- ВЫРУЧКА ---
if page == "Выручка":
    page_header("Выручка")
    orders = load_orders(date_from, date_to)
    if orders.empty:
        st.warning(t("no_data_period"))
    else:
        paid = orders[orders["PAID"]==1] if "PAID" in orders.columns else orders
        rev=float(paid["TOPAYSUM"].sum()); n=len(paid)
        avg_c=rev/n if n else 0; guests=int(paid["GUESTSCOUNT"].sum())
        dishes_n=int(paid["TOTALDISHPIECES"].sum()); disc=float(paid["DISCOUNTSUM"].sum())
        # Кол-во чеков (ближе к ОФД)
        checks_df = load_check_count(date_from, date_to)
        n_checks = int(checks_df.iloc[0]["CHECKS"]) if not checks_df.empty else n
        # --- Данные за вчера для сравнения ---
        _period_days = (date_to - date_from).days + 1
        _yest_to = date_from - timedelta(1)
        _yest_from = _yest_to - timedelta(_period_days - 1)
        _yest_orders = load_orders(_yest_from, _yest_to)
        if not _yest_orders.empty:
            _yp = _yest_orders[_yest_orders["PAID"]==1] if "PAID" in _yest_orders.columns else _yest_orders
            _y_rev = float(_yp["TOPAYSUM"].sum()); _y_n = len(_yp)
            _y_avg = _y_rev / _y_n if _y_n else 0
            _y_guests = int(_yp["GUESTSCOUNT"].sum())
            _y_dishes = int(_yp["TOTALDISHPIECES"].sum())
            _y_checks_df = load_check_count(_yest_from, _yest_to)
            _y_checks = int(_y_checks_df.iloc[0]["CHECKS"]) if not _y_checks_df.empty else _y_n
        else:
            _y_rev=0; _y_n=0; _y_avg=0; _y_guests=0; _y_dishes=0; _y_checks=0
        def _vyr_delta(cur, prev):
            if prev and prev > 0:
                pct = (cur - prev) / prev * 100
                _lbl = t("vs_yesterday") if _period_days == 1 else t("vs_prev")
                return f"{pct:+.1f}% {_lbl}"
            return None
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        with c1: st.metric(t("revenue"),f"{rev:,.0f} ₽", delta=_vyr_delta(rev, _y_rev))
        with c2: st.metric(t("orders"),f"{n:,}", delta=_vyr_delta(n, _y_n))
        with c3: st.metric(t("checks"),f"{n_checks:,}", delta=_vyr_delta(n_checks, _y_checks))
        with c4: st.metric(t("avg_check"),f"{avg_c:,.0f} ₽", delta=_vyr_delta(avg_c, _y_avg))
        with c5: st.metric(t("guests"),f"{guests:,}", delta=_vyr_delta(guests, _y_guests))
        with c6: st.metric(t("dishes"),f"{dishes_n:,}", delta=_vyr_delta(dishes_n, _y_dishes))

        # --- Налоги (НДС) плашка ---
        _tax_vyr = load_tax_breakdown(date_from, date_to)
        if not _tax_vyr.empty and "REVENUE" in _tax_vyr.columns:
            import re as _re
            _tax_total = 0
            _tax_parts = []
            for _, _tr in _tax_vyr.iterrows():
                _label = str(_tr.get("TAX_NAME", _tr.get("TAX_LABEL", "")))
                _rev_t = float(_tr["REVENUE"])
                _m = _re.search(r'(\d+)', _label)
                _rate = int(_m.group(1)) if _m else 0
                _amt = _rev_t * _rate / (100 + _rate) if _rate > 0 else 0
                _tax_total += _amt
                _display_label = _label.replace("НДС", "VAT") if _get_lang()=="en" else _label
                _tax_parts.append(f"{_display_label}: {_amt:,.0f} ₽")
            if _tax_total > 0:
                _tax_pct = _tax_total / rev * 100 if rev > 0 else 0
                _tax_color = "#00ff6a" if not IS_LIGHT else "#00b847"
                st.markdown(f"""<div style="display:inline-flex;align-items:center;gap:12px;padding:6px 16px;
                    background:var(--card);border:1px solid var(--border);border-radius:10px;margin-top:4px;">
                    <span style="font-size:.78rem;font-weight:600;color:{_tax_color};">{t("taxes_nds")}: {_tax_total:,.0f} ₽</span>
                    <span style="font-size:.72rem;color:var(--t3);">({_tax_pct:.1f}% {"of revenue" if _get_lang()=="en" else "от выручки"})</span>
                    <span style="font-size:.68rem;color:var(--t3);">{'  ·  '.join(_tax_parts)}</span>
                </div>""", unsafe_allow_html=True)

        st.divider()
        cl,cr = st.columns([2,1])
        with cl:
            if (date_to-date_from).days<=1:
                h=load_hourly(date_from,date_to)
                if not h.empty:
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=h["HOUR"],y=h["REVENUE"],name=t("revenue"),marker_color="#00ff6a"))
                    fig.add_trace(go.Scatter(x=h["HOUR"],y=h["ORDER_COUNT"],name=t("orders"),yaxis="y2",mode="lines+markers",line=dict(color="#f59e0b",width=3)))
                    fig.update_layout(title=t("by_hours"),yaxis=dict(title="₽"),yaxis2=dict(title="Qty" if _get_lang()=="en" else "Шт",side="right",overlaying="y"),height=400,legend=dict(orientation="h",y=1.1),**CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
            else:
                d=load_daily(date_from,date_to)
                if not d.empty:
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=d["DAY"],y=d["REVENUE"],name=t("revenue"),marker_color="#00ff6a"))
                    fig.add_trace(go.Scatter(x=d["DAY"],y=d["AVG_CHECK"],name=t("avg_check"),yaxis="y2",mode="lines+markers",line=dict(color="#10b981",width=3)))
                    fig.update_layout(title=t("by_days"),yaxis=dict(title="₽"),yaxis2=dict(title="Avg check" if _get_lang()=="en" else "Ср.чек",side="right",overlaying="y"),height=400,legend=dict(orientation="h",y=1.1),**CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
        with cr:
            if guests>0:
                st.markdown(f"### {t("per_guest")}")
                st.metric(t("avg_check_guest"),f"{rev/guests:,.0f} ₽")
                st.metric(t("guests_per_order"),f"{guests/n:.1f}" if n else "—")
                st.metric(t("dishes_per_order"),f"{dishes_n/n:.1f}" if n else "—")

        # --- Налоги (НДС) ---
        st.divider()
        tax_data = load_tax_breakdown(date_from, date_to)
        if not tax_data.empty and "REVENUE" in tax_data.columns:
            st.markdown(f"### {t('taxes')}")
            total_tax_rev = float(tax_data["REVENUE"].sum())
            tax_data["SHARE"] = (tax_data["REVENUE"] / total_tax_rev * 100).round(1)
            tax_data["TAX_LABEL"] = tax_data["TAX_NAME"].fillna("N/A" if _get_lang()=="en" else "Не указан")
            if _get_lang() == "en":
                tax_data["TAX_LABEL"] = tax_data["TAX_LABEL"].str.replace("НДС", "VAT").str.replace("Без НДС", "No VAT")

            tc1, tc2 = st.columns([2, 1])
            with tc1:
                fig_tax = px.bar(tax_data, x="TAX_LABEL", y="REVENUE",
                    text=tax_data["REVENUE"].apply(lambda x: f"{x:,.0f} ₽"),
                    color="SHARE", color_continuous_scale="Tealgrn",
                    labels={"TAX_LABEL": t("tax_rate"), "REVENUE": t("revenue") + " ₽", "SHARE": t("share")})
                fig_tax.update_traces(textposition="outside")
                fig_tax.update_layout(height=350, coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig_tax)
                st.plotly_chart(fig_tax, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with tc2:
                import re as _re_tax
                for _, row in tax_data.iterrows():
                    label = row["TAX_LABEL"]
                    share = row["SHARE"]
                    rev = float(row["REVENUE"])
                    qty = int(row["QTY"]) if pd.notna(row["QTY"]) else 0
                    bar_color = "#00ff6a" if not IS_LIGHT else "#00b847"
                    # Вычисляем сумму НДС
                    _m_rate = _re_tax.search(r'(\d+)', str(label))
                    _rate = int(_m_rate.group(1)) if _m_rate else 0
                    _tax_amt = rev * _rate / (100 + _rate) if _rate > 0 else 0
                    _tax_line = f'<div style="font-size:.78rem;font-weight:600;color:{"#f59e0b" if not IS_LIGHT else "#b45309"};margin-top:4px;">{t("taxes_nds")}: {_tax_amt:,.0f} ₽</div>' if _rate > 0 else ""
                    _items_label = "items" if _get_lang()=="en" else "позиций"
                    _display_lbl = label.replace("НДС", "VAT").replace("Без НДС", "No VAT") if _get_lang()=="en" else label
                    st.markdown(f'<div style="margin-bottom:12px;">'
                        f'<div style="font-size:.75rem;color:var(--t3);text-transform:uppercase;letter-spacing:.08em;font-weight:600;">{_display_lbl}</div>'
                        f'<div style="font-size:1.3rem;font-weight:800;color:var(--t1);">{rev:,.0f} ₽ <span style="font-size:.8rem;font-weight:500;color:var(--t2);">({share}%)</span></div>'
                        f'{_tax_line}'
                        f'<div style="height:4px;background:var(--border);border-radius:2px;margin-top:6px;">'
                        f'<div style="height:100%;width:{share}%;background:{bar_color};border-radius:2px;"></div>'
                        f'</div>'
                        f'<div style="font-size:.7rem;color:var(--t3);margin-top:3px;">{qty:,} {_items_label}</div>'
                        f'</div>', unsafe_allow_html=True)

            # --- Детализация налогов: по ресторанам и товарам ---
            with st.expander(t("details")):
                _tax_tab1, _tax_tab2 = st.tabs([t("by_restaurants"), t("by_dishes")])
                with _tax_tab1:
                    _tax_by_rest = run_query_cached("""
                        SELECT cg.NAME as REST_NAME, tdt.NAME as TAX_LABEL,
                            SUM(sd.PRLISTSUM) as REVENUE, COUNT(*) as QTY
                        FROM SESSIONDISHES sd
                        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
                        JOIN CASHGROUPS cg ON o.MIDSERVER=cg.SIFR
                        JOIN TAXDISHTYPES tdt ON sd.ITAXDISHTYPE=tdt.SIFR
                        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1)
                          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
                          AND sd.QUANTITY > 0
                        GROUP BY cg.NAME, tdt.NAME
                        ORDER BY tdt.NAME, SUM(sd.PRLISTSUM) DESC
                    """, (str(date_from), str(date_to)))
                    if not _tax_by_rest.empty:
                        # Группируем: для каждой ставки — таблица ресторанов
                        import re as _re_tax2
                        for _tl in _tax_by_rest["TAX_LABEL"].unique():
                            _subset = _tax_by_rest[_tax_by_rest["TAX_LABEL"] == _tl].copy()
                            _total_rev = float(_subset["REVENUE"].sum())
                            _m2 = _re_tax2.search(r'(\d+)', str(_tl))
                            _r2 = int(_m2.group(1)) if _m2 else 0
                            _tax_total = _total_rev * _r2 / (100 + _r2) if _r2 > 0 else 0
                            st.markdown(f"**{_tl}** — {t('revenue')} {_total_rev:,.0f} ₽, {t('taxes_nds')} {_tax_total:,.0f} ₽")
                            _subset["REST_NAME"] = _subset["REST_NAME"].apply(lambda x: x.replace(" сервер ", " с.") if x else "?")
                            # Группируем серверы одной столовой
                            _grouped = _subset.groupby(
                                _subset["REST_NAME"].str.extract(r'(Столовая \d+|.+)', expand=False)
                            ).agg({"REVENUE": "sum", "QTY": "sum"}).reset_index()
                            _rev_col = t("revenue") + " ₽"
                            _pos_col = t("positions")
                            _grouped.columns = ["Restaurant" if _get_lang()=="en" else "Ресторан", _rev_col, _pos_col]
                            _grouped = _grouped.sort_values(_rev_col, ascending=False)
                            _grouped[_rev_col] = _grouped[_rev_col].apply(lambda x: f"{x:,.0f}")
                            _grouped[_pos_col] = _grouped[_pos_col].astype(int)
                            st.dataframe(_grouped, hide_index=True, use_container_width=True)
                    else:
                        st.info(t("no_data"))

                with _tax_tab2:
                    _tax_by_dish = run_query_cached("""
                        SELECT TOP 50 mi.NAME as DISH_NAME, tdt.NAME as TAX_LABEL,
                            SUM(sd.PRLISTSUM) as REVENUE, SUM(sd.QUANTITY) as QTY
                        FROM SESSIONDISHES sd
                        JOIN ORDERS o ON sd.VISIT=o.VISIT AND sd.MIDSERVER=o.MIDSERVER AND sd.ORDERIDENT=o.IDENTINVISIT
                        JOIN MENUITEMS mi ON sd.SIFR=mi.SIFR
                        JOIN TAXDISHTYPES tdt ON sd.ITAXDISHTYPE=tdt.SIFR
                        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                          AND (sd.DBSTATUS IS NULL OR sd.DBSTATUS!=-1)
                          AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
                          AND sd.QUANTITY > 0
                        GROUP BY mi.NAME, tdt.NAME
                        ORDER BY SUM(sd.PRLISTSUM) DESC
                    """, (str(date_from), str(date_to)))
                    if not _tax_by_dish.empty:
                        _tax_filter = st.selectbox(t("tax_rate"), ["All" if _get_lang()=="en" else "Все"] + list(_tax_by_dish["TAX_LABEL"].unique()), key="tax_dish_filter")
                        _filtered = _tax_by_dish if _tax_filter in ("Все", "All") else _tax_by_dish[_tax_by_dish["TAX_LABEL"] == _tax_filter]
                        _show = _filtered[["DISH_NAME", "TAX_LABEL", "REVENUE", "QTY"]].copy()
                        _rev_col_t = t("revenue") + " ₽"
                        _show.columns = ["Dish" if _get_lang()=="en" else "Блюдо", t("tax_rate"), _rev_col_t, t("quantity")]
                        _show[_rev_col_t] = _show[_rev_col_t].apply(lambda x: f"{x:,.0f}")
                        st.dataframe(_show, hide_index=True, use_container_width=True)
                    else:
                        st.info(t("no_data"))

        # --- Типы оплат (нал, безнал, СБП) ---
        st.divider()
        st.markdown(f"### {t('payment_types')}")
        pay_data = load_payments(date_from, date_to)
        if not pay_data.empty:
            _PAY_NAMES = {0: t("cash"), 1: t("card"), 2: t("bank_transfer"), 3: t("staff_meal"),
                          4: "Room" if _get_lang()=="en" else "На комнату", 5: t("bank_transfer"), 6: "Internal" if _get_lang()=="en" else "Внутренний", 7: t("bonus")}
            pay_data["PAY_NAME"] = pay_data["PAYLINETYPE"].map(lambda x: _PAY_NAMES.get(int(x), f"Тип {x}"))
            pay_data["TOTAL_SUM"] = pd.to_numeric(pay_data["TOTAL_SUM"], errors="coerce").fillna(0)
            pay_data["PAY_COUNT"] = pd.to_numeric(pay_data["PAY_COUNT"], errors="coerce").fillna(0).astype(int)
            pay_total = float(pay_data["TOTAL_SUM"].sum())
            pay_data["SHARE"] = (pay_data["TOTAL_SUM"] / pay_total * 100).round(1) if pay_total > 0 else 0

            # Метрики — только основные типы
            _main_types = pay_data[~pay_data["PAYLINETYPE"].isin([3])].copy()  # без питания сотр.
            _n_cols = min(len(_main_types), 4)
            if _n_cols > 0:
                _pay_cols = st.columns(_n_cols)
                for i, (_, _pr) in enumerate(_main_types.head(4).iterrows()):
                    with _pay_cols[i]:
                        _pct = f"{_pr['SHARE']:.1f}%"
                        st.metric(_pr["PAY_NAME"], f"{float(_pr['TOTAL_SUM']):,.0f} ₽",
                                  delta=f"{_pct} · {int(_pr['PAY_COUNT']):,} операций")

            # Диаграмма + таблица
            _pc1, _pc2 = st.columns([2, 1])
            with _pc1:
                _colors = {t("cash"): "#00b847", t("card"): "#3b82f6", t("bank_transfer"): "#f59e0b",
                           t("staff_meal"): "#8b5cf6", t("bonus"): "#ec4899"}
                _clr_list = [_colors.get(n, "#6b7280") for n in pay_data["PAY_NAME"]]
                fig_pay = go.Figure(go.Pie(
                    labels=pay_data["PAY_NAME"], values=pay_data["TOTAL_SUM"],
                    hole=0.5, textinfo="label+percent",
                    marker=dict(colors=_clr_list)))
                fig_pay.update_layout(height=300, showlegend=False, margin=dict(l=20,r=20,t=20,b=20), **CHART_THEME)
                st.plotly_chart(fig_pay, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with _pc2:
                _show_pay = pay_data[["PAY_NAME", "TOTAL_SUM", "PAY_COUNT", "SHARE"]].copy()
                _show_pay.columns = [t("pay_type"), t("amount"), t("operations"), t("share")]
                _show_pay[t("amount")] = _show_pay[t("amount")].apply(lambda x: f"{x:,.0f}" if isinstance(x, (int,float)) else x)
                _show_pay = _show_pay.sort_values(t("share"), ascending=False).reset_index(drop=True)
                st.dataframe(_show_pay, hide_index=True, use_container_width=True)
        else:
            st.info(t("no_payment_data"))

        # --- Питание сотрудников ---
        staff_meals = run_query_cached("""
            SELECT COUNT(*) as CNT, SUM(p.BASICSUM) as TOTAL
            FROM PAYMENTS p
            JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
            WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
              AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1)
              AND p.PAYLINETYPE = 3""", (str(date_from), str(date_to)))
        if not staff_meals.empty and staff_meals.iloc[0]["TOTAL"] and float(staff_meals.iloc[0]["TOTAL"]) > 0:
            sm_total = float(staff_meals.iloc[0]["TOTAL"])
            sm_cnt = int(staff_meals.iloc[0]["CNT"])
            sm_pct = (sm_total / rev * 100) if rev > 0 else 0
            sm_cost, sm_fc_pct, sm_fc_exact = _staff_meal_cost(sm_total)
            st.divider()
            st.markdown(f"### {t('staff_meals')}")
            sc1, sc2, sc3, sc4, sc5 = st.columns(5)
            with sc1: st.metric(t("amount_menu"), f"{sm_total:,.0f} ₽")
            with sc2:
                _sm_label = f"{t('cost')} ({sm_fc_pct:.0f}%)"
                st.metric(_sm_label, f"{sm_cost:,.0f} ₽",
                          delta=t("cost_exact") if sm_fc_exact else t("cost_estimate"))
            with sc3: st.metric(t("transactions"), f"{sm_cnt:,}")
            with sc4: st.metric(t("pct_revenue"), f"{sm_pct:.1f}%")
            with sc5: st.metric("Ø", f"{sm_total/sm_cnt:,.0f} ₽" if sm_cnt > 0 else "—")
            if not sm_fc_exact:
                st.caption(f"💡 {t('calc_foodcost_hint')}")

            # Детализация по сотрудникам и столовым
            with st.expander(t("details"), expanded=False):
                staff_detail = run_query_cached("""
                    SELECT c.NAME as EMPLOYEE, r.NAME as RESTAURANT,
                        COUNT(*) as MEALS, CAST(SUM(p.BASICSUM) AS INT) as TOTAL_SUM,
                        CAST(AVG(p.BASICSUM) AS INT) as AVG_CHECK,
                        CAST(MIN(o.OPENTIME) AS DATE) as FIRST_MEAL,
                        CAST(MAX(o.OPENTIME) AS DATE) as LAST_MEAL
                    FROM PAYMENTS p
                    JOIN CURRENCIES c ON p.SIFR = c.SIFR
                    JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
                    JOIN GLOBALSHIFTS gs ON o.MIDSERVER=gs.MIDSERVER AND o.ICOMMONSHIFT=gs.SHIFTNUM
                    JOIN RESTAURANTS r ON gs.IRESTAURANT=r.SIFR
                    WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                      AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1)
                      AND p.PAYLINETYPE = 3
                      AND r.NAME IS NOT NULL
                    GROUP BY c.NAME, r.NAME
                    ORDER BY TOTAL_SUM DESC""", (str(date_from), str(date_to)))

                if not staff_detail.empty:
                    # Сводка по сотрудникам
                    st.markdown("#### " + ("By Employees" if _get_lang()=="en" else "По сотрудникам"))
                    by_emp = staff_detail.groupby("EMPLOYEE").agg(
                        MEALS=("MEALS", "sum"),
                        TOTAL_SUM=("TOTAL_SUM", "sum"),
                        RESTAURANTS=("RESTAURANT", lambda x: ", ".join(sorted(x.unique()))),
                        REST_COUNT=("RESTAURANT", "nunique"),
                    ).reset_index().sort_values("TOTAL_SUM", ascending=False)
                    by_emp["AVG_CHECK"] = (by_emp["TOTAL_SUM"] / by_emp["MEALS"]).round(0).astype(int)

                    top_emp = by_emp.head(20)
                    fig = px.bar(top_emp, x="TOTAL_SUM", y="EMPLOYEE", orientation="h",
                        title=t("staff_meals"),
                        color="MEALS", color_continuous_scale="YlOrRd",
                        text=top_emp["TOTAL_SUM"].apply(lambda x: f"{x:,}₽"),
                        labels={"TOTAL_SUM": t("amount") + " ₽", "EMPLOYEE": "Employee" if _get_lang()=="en" else "Сотрудник", "MEALS": "Count" if _get_lang()=="en" else "Раз"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_emp)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    disp_emp = by_emp.rename(columns={
                        "EMPLOYEE": "Employee" if _get_lang()=="en" else "Сотрудник", "MEALS": "Count" if _get_lang()=="en" else "Раз",
                        "TOTAL_SUM": t("amount"), "AVG_CHECK": "Ø",
                        "REST_COUNT": "Столовых", "RESTAURANTS": "Где ел(а)"})
                    st.dataframe(disp_emp, use_container_width=True, hide_index=True, height=400)

                    # Сводка по столовым
                    st.divider()
                    st.markdown("#### " + ("By Canteens" if _get_lang()=="en" else "По столовым"))
                    by_rest = staff_detail.groupby("RESTAURANT").agg(
                        MEALS=("MEALS", "sum"),
                        TOTAL_SUM=("TOTAL_SUM", "sum"),
                        EMPLOYEES=("EMPLOYEE", "nunique"),
                    ).reset_index().sort_values("TOTAL_SUM", ascending=False)

                    fig2 = px.bar(by_rest, x="TOTAL_SUM", y="RESTAURANT", orientation="h",
                        title=t("staff_meals"),
                        color="EMPLOYEES", color_continuous_scale="Viridis",
                        text=by_rest["TOTAL_SUM"].apply(lambda x: f"{x:,}₽"),
                        labels=_labels())
                    fig2.update_traces(textposition="auto")
                    fig2.update_layout(height=max(350, len(by_rest)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig2)

                    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    disp_rest = by_rest.rename(columns={
                        "RESTAURANT": "Столовая", "MEALS": "Раз",
                        "TOTAL_SUM": "Сумма ₽", "EMPLOYEES": "Сотрудников"})
                    st.dataframe(disp_rest, use_container_width=True, hide_index=True)

                    # Полная детализация
                    st.divider()
                    st.markdown("#### " + ("Data" if _get_lang()=="en" else "Данные"))
                    disp_full = staff_detail.rename(columns={
                        "EMPLOYEE": "Сотрудник", "RESTAURANT": "Столовая",
                        "MEALS": "Раз", "TOTAL_SUM": "Сумма ₽", "AVG_CHECK": "Ø Чек ₽",
                        "FIRST_MEAL": "Первый", "LAST_MEAL": "Последний"})
                    st.dataframe(disp_full, use_container_width=True, hide_index=True, height=400)
                    st.download_button("Скачать CSV", staff_detail.to_csv(index=False).encode("utf-8"),
                        "staff_meals.csv", "text/csv", use_container_width=True)
                else:
                    st.info(t("no_details"))

        # ============================================================
        # ПРОГНОЗ НА ЗАВТРА
        # ============================================================
        st.divider()
        st.markdown("### " + ("Forecast" if _get_lang()=="en" else "Прогноз"))

        tomorrow = datetime.now().date() + timedelta(1)
        dow_tomorrow = tomorrow.weekday()  # 0=Mon
        dow_names_ru = ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"]
        dow_names_en = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        _dow_name = dow_names_en[dow_tomorrow] if _get_lang()=="en" else dow_names_ru[dow_tomorrow]

        # Load same-day-of-week for last 8 weeks
        forecast_data = run_query_cached("""
            SELECT CAST(o.OPENTIME AS DATE) as DAY,
                COUNT(*) as ORDERS, SUM(o.TOPAYSUM) as REVENUE,
                AVG(o.TOPAYSUM) as AVG_CHECK, SUM(o.GUESTSCOUNT) as GUESTS
            FROM ORDERS o
            WHERE o.OPENTIME >= DATEADD(DAY, -56, GETDATE())
              AND (o.DBSTATUS IS NULL OR o.DBSTATUS!=-1) AND o.PAID=1
              AND DATEPART(WEEKDAY, o.OPENTIME) = %s
            GROUP BY CAST(o.OPENTIME AS DATE)
            ORDER BY DAY DESC""", (dow_tomorrow + 2,))  # SQL DATEPART: 1=Sun, 2=Mon...

        # Fallback for SQLite (demo mode)
        if forecast_data.empty:
            forecast_data = run_query_cached("""
                SELECT SUBSTR(OPENTIME,1,10) as DAY,
                    COUNT(*) as ORDERS, SUM(TOPAYSUM) as REVENUE,
                    AVG(TOPAYSUM) as AVG_CHECK, SUM(GUESTSCOUNT) as GUESTS
                FROM ORDERS
                WHERE OPENTIME >= date('now', '-56 days')
                  AND (DBSTATUS IS NULL OR DBSTATUS != -1) AND PAID=1
                  AND CAST(strftime('%%w', SUBSTR(OPENTIME,1,10)) AS INTEGER) = %s
                GROUP BY SUBSTR(OPENTIME,1,10)
                ORDER BY DAY DESC""", (dow_tomorrow,))  # SQLite: 0=Sun, 1=Mon...

        if not forecast_data.empty and len(forecast_data) >= 2:
            # Weighted average: recent weeks weigh more
            weights = np.array([1.0 / (i + 1) for i in range(len(forecast_data))])
            weights = weights / weights.sum()

            fc_rev = (forecast_data["REVENUE"].values * weights).sum()
            fc_orders = int((forecast_data["ORDERS"].values * weights).sum())
            fc_check = fc_rev / fc_orders if fc_orders else 0
            fc_guests = int((forecast_data["GUESTS"].values * weights).sum())

            # Trend: is it growing or declining?
            if len(forecast_data) >= 4:
                recent = forecast_data["REVENUE"].head(2).mean()
                older = forecast_data["REVENUE"].tail(2).mean()
                trend_pct = (recent / older - 1) * 100 if older > 0 else 0
            else:
                trend_pct = 0

            # Min/Max for confidence range
            fc_min = float(forecast_data["REVENUE"].min())
            fc_max = float(forecast_data["REVENUE"].max())

            accent = "#00ff6a" if not IS_LIGHT else "#00b847"
            card_bg = "var(--card)"
            t1 = "var(--t1)"
            t2 = "var(--t2)"
            t3 = "var(--t3)"
            trend_color = accent if trend_pct >= 0 else ("#ff4d6a" if not IS_LIGHT else "#e53e3e")
            trend_arrow = "↗" if trend_pct >= 0 else "↘"

            st.markdown(f"""<div style="background:{card_bg};border:1px solid var(--border);border-radius:14px;
                padding:20px 24px;margin-bottom:16px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                    <div>
                        <div style="font-size:.62rem;color:{t3};text-transform:uppercase;letter-spacing:.12em;font-weight:700;">
                            {"Forecast for tomorrow" if _get_lang()=="en" else "Прогноз на завтра"} · {_dow_name}, {tomorrow.strftime('%d.%m')}
                        </div>
                        <div style="font-size:1.8rem;font-weight:800;color:{t1};margin-top:4px;">{fc_rev:,.0f} ₽</div>
                        <div style="font-size:.72rem;color:{t2};margin-top:2px;">
                            {"Based on" if _get_lang()=="en" else "На основе"} {len(forecast_data)} {"same weekdays over last 8 weeks" if _get_lang()=="en" else f"{_dow_name.lower()[:-1]}ов за последние 8 недель"}
                        </div>
                    </div>
                    <div style="text-align:right;">
                        <div style="font-size:1.2rem;color:{trend_color};font-weight:700;">{trend_arrow} {trend_pct:+.1f}%</div>
                        <div style="font-size:.65rem;color:{t3};">{"trend" if _get_lang()=="en" else "тренд"}</div>
                    </div>
                </div>
                <div style="display:flex;gap:24px;">
                    <div>
                        <div style="font-size:.6rem;color:{t3};text-transform:uppercase;letter-spacing:.1em;">{t("orders")}</div>
                        <div style="font-size:1.1rem;font-weight:700;color:{t1};">{fc_orders:,}</div>
                    </div>
                    <div>
                        <div style="font-size:.6rem;color:{t3};text-transform:uppercase;letter-spacing:.1em;">{t("avg_check")}</div>
                        <div style="font-size:1.1rem;font-weight:700;color:{t1};">{fc_check:,.0f} ₽</div>
                    </div>
                    <div>
                        <div style="font-size:.6rem;color:{t3};text-transform:uppercase;letter-spacing:.1em;">{t("guests")}</div>
                        <div style="font-size:1.1rem;font-weight:700;color:{t1};">{fc_guests:,}</div>
                    </div>
                    <div>
                        <div style="font-size:.6rem;color:{t3};text-transform:uppercase;letter-spacing:.1em;">{"Range" if _get_lang()=="en" else "Диапазон"}</div>
                        <div style="font-size:.85rem;font-weight:600;color:{t2};">{fc_min:,.0f} — {fc_max:,.0f} ₽</div>
                    </div>
                </div>
            </div>""", unsafe_allow_html=True)

            # Mini chart: same-day history
            fc_chart = forecast_data.sort_values("DAY")
            fig_fc = go.Figure()
            fig_fc.add_trace(go.Bar(x=fc_chart["DAY"], y=fc_chart["REVENUE"],
                marker_color=accent, opacity=0.7, name=("Actual" if _get_lang()=="en" else "Факт")))
            fig_fc.add_hline(y=fc_rev, line_dash="dash", line_color=accent, opacity=0.5,
                annotation_text=f"{'Forecast' if _get_lang()=='en' else 'Прогноз'}: {fc_rev:,.0f} ₽", annotation_position="top left")
            fig_fc.update_layout(height=200, title=f"{'History of' if _get_lang()=='en' else 'История'} {_dow_name}{'s' if _get_lang()=='en' else ''}",
                showlegend=False, margin=dict(l=40,r=20,t=40,b=30), **CHART_THEME)
            fix_bar_hover(fig_fc)
            st.plotly_chart(fig_fc, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # Save forecast context for Gemini
            _forecast_ctx = f"""
ПРОГНОЗ НА ЗАВТРА ({dow_names_ru[dow_tomorrow]}, {tomorrow}):
- Ожидаемая выручка: {fc_rev:,.0f} ₽ (диапазон {fc_min:,.0f}–{fc_max:,.0f} ₽)
- Заказов: ~{fc_orders:,}, Ср.чек: {fc_check:,.0f} ₽, Гостей: ~{fc_guests:,}
- Тренд: {trend_pct:+.1f}% (сравнение последних 2 vs предыдущих 2 недель)
"""
        else:
            st.info(t("forecast_insufficient"))
            _forecast_ctx = ""

        # ============================================================
        # ИИ-СТРАТЕГИЯ (Gemini recommendations)
        # ============================================================
        st.divider()
        st.markdown("### " + ("AI Strategy" if _get_lang()=="en" else "Стратегия ИИ"))

        ai_key = f"ai_strategy_{date_from}_{date_to}"
        if ai_key not in st.session_state:
            st.session_state[ai_key] = None

        def _build_revenue_context():
            """Собрать контекст метрик для Gemini."""
            ctx = []
            ctx.append(f"Период: {date_from} — {date_to}")
            ctx.append(f"Выручка: {rev:,.0f} ₽, Заказов: {n:,}, Ср.чек: {avg_c:,.0f} ₽")
            ctx.append(f"Гостей: {guests:,}, Блюд: {dishes_n:,}, Скидки: {disc:,.0f} ₽")
            # По дням
            try:
                d = load_daily(date_from, date_to)
                if not d.empty and "REVENUE" in d.columns:
                    best = d.loc[d["REVENUE"].idxmax()]
                    worst = d.loc[d["REVENUE"].idxmin()]
                    ctx.append(f"Лучший день: {best['DAY']} ({best['REVENUE']:,.0f} ₽)")
                    ctx.append(f"Худший день: {worst['DAY']} ({worst['REVENUE']:,.0f} ₽)")
            except: pass
            # По часам
            try:
                h = load_hourly(date_from, date_to)
                if not h.empty and "REVENUE" in h.columns:
                    peak = h.loc[h["REVENUE"].idxmax()]
                    ctx.append(f"Пиковый час: {int(peak['HOUR'])}:00 ({peak['REVENUE']:,.0f} ₽, {int(peak['ORDER_COUNT'])} заказов)")
                    low = h.loc[h["REVENUE"].idxmin()]
                    ctx.append(f"Тихий час: {int(low['HOUR'])}:00 ({low['REVENUE']:,.0f} ₽)")
            except: pass
            # Налоги
            try:
                if not tax_data.empty:
                    for _, t in tax_data.iterrows():
                        ctx.append(f"НДС {t['TAX_LABEL']}: {float(t['REVENUE']):,.0f} ₽ ({t['SHARE']}%)")
            except: pass
            return "\n".join(ctx)

        if st.button(t("get_ai_recommendations"), key="ai_strat_btn", use_container_width=True):
            with st.spinner("Gemini анализирует данные..."):
                context = _build_revenue_context()
                prompt = f"""Ты — AI-консультант для сети столовых и кафе. Проанализируй данные и прогноз, дай 4 конкретные рекомендации.

ТЕКУЩИЕ ДАННЫЕ:
{context}

{_forecast_ctx}

ПРАВИЛА:
- Каждая рекомендация: категория (ОДНО СЛОВО заглавными), заголовок (до 8 слов), текст (2-3 предложения с КОНКРЕТНЫМИ числами из данных)
- Категории: ВЫРУЧКА, ПЕРСОНАЛ, МЕНЮ, ЗАГРУЗКА, ПРОГНОЗ, ЗАКУПКИ, РИСК
- ОБЯЗАТЕЛЬНО одна рекомендация про завтрашний день на основе прогноза (категория ПРОГНОЗ) — конкретно сколько персонала ставить, сколько закупать
- Формат ответа — ТОЛЬКО JSON массив, без markdown:
[{{"cat":"ПРОГНОЗ","title":"...","text":"...","color":"#0ea5e9"}},{{"cat":"ВЫРУЧКА","title":"...","text":"...","color":"#00ff6a"}}]
- Цвета: зелёный #00ff6a для позитива, голубой #0ea5e9 для прогноза/оптимизации, оранжевый #f59e0b для предупреждений, красный #ef4444 для рисков
- Рубли в формате 1 234 ₽. Русский язык. Без воды — только суть."""

                result = ask_gemini(prompt)
                st.session_state[ai_key] = result

        # Display recommendations
        recs = st.session_state.get(ai_key)
        if recs:
            try:
                import json as _json
                # Clean up response — remove markdown fences if any
                clean = recs.strip()
                if clean.startswith("```"): clean = clean.split("\n", 1)[1]
                if clean.endswith("```"): clean = clean.rsplit("```", 1)[0]
                clean = clean.strip()
                cards = _json.loads(clean)

                card_bg = "#0e0e16" if not IS_LIGHT else "#ffffff"
                card_border = "rgba(255,255,255,0.06)" if not IS_LIGHT else "rgba(0,0,0,0.08)"
                text_color = "#e0e0e8" if not IS_LIGHT else "#1a1a2e"
                sub_color = "#7a7a92" if not IS_LIGHT else "#5a5a70"

                cols = st.columns(len(cards))
                for i, card in enumerate(cards):
                    with cols[i]:
                        cat = card.get("cat", "")
                        title = card.get("title", "")
                        text = card.get("text", "")
                        color = card.get("color", "#00ff6a")

                        st.markdown(f"""<div style="background:{card_bg};border:1px solid {card_border};
                            border-radius:14px;padding:20px;height:100%;border-top:3px solid {color};">
                            <div style="font-size:.6rem;font-weight:800;letter-spacing:.12em;
                                color:{color};margin-bottom:8px;">{cat}</div>
                            <div style="font-size:.92rem;font-weight:700;color:{text_color};
                                margin-bottom:10px;line-height:1.3;">{title}</div>
                            <div style="font-size:.78rem;color:{sub_color};line-height:1.5;">{text}</div>
                        </div>""", unsafe_allow_html=True)
            except Exception as e:
                # If JSON parsing fails, show raw text
                st.markdown(recs)

# --- СЕЗОННОСТЬ ---
if page == "Сезонность":
    page_header("Сезонность")
    st.caption(t("monthly_comparison"))

    yoy = load_monthly_revenue_yoy()
    if yoy.empty:
        st.warning(t("no_revenue_data"))
    else:
        years = sorted(yoy["Y"].unique())
        MONTH_NAMES_RU = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
                          7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
        MONTH_SHORT = {1:"Янв",2:"Фев",3:"Мар",4:"Апр",5:"Май",6:"Июн",
                       7:"Июл",8:"Авг",9:"Сен",10:"Окт",11:"Ноя",12:"Дек"}

        if len(years) >= 2:
            cur_year = max(years)
            prev_year = cur_year - 1
        else:
            cur_year = years[0]
            prev_year = None

        # Выбор годов для сравнения
        if len(years) >= 2:
            cl, cr = st.columns(2)
            with cl:
                cur_year = st.selectbox("Current year:" if _get_lang()=="en" else "Текущий год:", sorted(years, reverse=True), index=0, key="season_cur")
            with cr:
                prev_options = [y for y in years if y < cur_year]
                if prev_options:
                    prev_year = st.selectbox("Compare with:" if _get_lang()=="en" else "Сравнить с:", sorted(prev_options, reverse=True), index=0, key="season_prev")
                else:
                    prev_year = None
                    st.info(t("no_prev_year_data"))

        cur_data = yoy[yoy["Y"] == cur_year].set_index("M")
        prev_data = yoy[yoy["Y"] == prev_year].set_index("M") if prev_year else pd.DataFrame()

        # Сводные метрики за год
        cur_total = cur_data["REVENUE"].sum() if not cur_data.empty else 0
        prev_total = prev_data["REVENUE"].sum() if not prev_data.empty else 0
        yoy_diff_pct = ((cur_total / prev_total - 1) * 100) if prev_total > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric(f"{cur_year}", f"{cur_total:,.0f} ₽")
        with c2:
            if prev_year:
                st.metric(f"{prev_year}", f"{prev_total:,.0f} ₽")
        with c3:
            if prev_year and prev_total > 0:
                diff = cur_total - prev_total
                st.metric(t("difference"), f"{diff:+,.0f} ₽", f"{yoy_diff_pct:+.1f}%")
        with c4:
            n_months_cur = len(cur_data)
            st.metric(t("months"), f"{n_months_cur}")
        if prev_year and prev_total > 0:
            st.caption(f"{t('revenue')} {cur_year} vs {prev_year}")

        st.divider()

        # =============== ПЛАШКИ ПО МЕСЯЦАМ ===============
        st.markdown(f"### Помесячно: {cur_year} vs {prev_year or '—'}")

        # Для текущего года — показываем только прошедшие месяцы
        now = datetime.now()
        max_month_cur = now.month if cur_year == now.year else 12

        # По 4 месяца в ряд
        for row_start in range(1, 13, 4):
            # Фильтруем: для текущего года — только прошедшие месяцы
            months_in_row = [m for m in range(row_start, min(row_start + 4, 13)) if m <= max_month_cur or cur_year < now.year]
            if not months_in_row:
                continue
            cols = st.columns(len(months_in_row))
            for i, m in enumerate(months_in_row):
                with cols[i]:
                    cur_rev = cur_data.loc[m, "REVENUE"] if m in cur_data.index else 0
                    cur_orders = cur_data.loc[m, "ORDERS"] if m in cur_data.index else 0
                    prev_rev = prev_data.loc[m, "REVENUE"] if not prev_data.empty and m in prev_data.index else 0

                    if prev_rev > 0:
                        diff_pct = (cur_rev / prev_rev - 1) * 100
                        diff_abs = cur_rev - prev_rev
                        if diff_pct > 5:
                            color = "#4caf50"  # green
                            arrow = "↑"
                        elif diff_pct < -5:
                            color = "#f44336"  # red
                            arrow = "↓"
                        else:
                            color = "#ff9800"  # orange
                            arrow = "→"
                        diff_text = f"{arrow} {diff_pct:+.1f}% ({diff_abs:+,.0f}₽)"
                    elif cur_rev > 0:
                        color = "#2196f3"
                        diff_text = "нет данных за прошлый год"
                    else:
                        color = "#555"
                        diff_text = "—"

                    card_bg = "#ffffff" if IS_LIGHT else "linear-gradient(145deg, rgba(18,18,30,0.9), rgba(14,14,22,0.95))"
                    card_text = "#1a1a2e" if IS_LIGHT else "#e0e0e8"
                    card_sub = "#5a5a70" if IS_LIGHT else "#9090a8"
                    card_muted = "#9a9ab0" if IS_LIGHT else "#666"

                    # Плашка
                    st.markdown(f"""
                    <div style="background: {card_bg};
                        border: 1px solid {color}40; border-left: 4px solid {color};
                        border-radius: 12px; padding: 14px 16px; margin-bottom: 8px;">
                        <div style="font-size: 0.8rem; color: {card_sub}; margin-bottom: 4px;">
                            {MONTH_NAMES_RU.get(m, m)}</div>
                        <div style="font-size: 1.3rem; font-weight: 700; color: {card_text};">
                            {cur_rev:,.0f} ₽</div>
                        <div style="font-size: 0.75rem; color: {color}; margin-top: 4px;">
                            {diff_text}</div>
                        <div style="font-size: 0.7rem; color: {card_muted}; margin-top: 2px;">
                            {prev_year or '—'}: {prev_rev:,.0f} ₽ · {cur_orders:,.0f} заказов</div>
                    </div>""", unsafe_allow_html=True)

        st.divider()

        # =============== ГРАФИКИ ===============
        st.markdown("### " + ("Dynamics" if _get_lang()=="en" else "Динамика"))

        # Подготовка данных для графика
        chart_rows = []
        for m in range(1, max_month_cur + 1 if cur_year == now.year else 13):
            cur_rev = float(cur_data.loc[m, "REVENUE"]) if m in cur_data.index else 0
            prev_rev = float(prev_data.loc[m, "REVENUE"]) if not prev_data.empty and m in prev_data.index else 0
            chart_rows.append({"Месяц": MONTH_SHORT[m], "M": m,
                               str(cur_year): cur_rev, str(prev_year): prev_rev if prev_year else 0})
        chart_df = pd.DataFrame(chart_rows)

        # Bar chart — рядом
        prev_bar_color = "rgba(0,0,0,0.08)" if IS_LIGHT else "rgba(255,255,255,0.12)"
        prev_bar_line = "rgba(0,0,0,0.12)" if IS_LIGHT else "rgba(255,255,255,0.15)"
        prev_text_color = "#999" if IS_LIGHT else "#666"
        cur_bar_color = "#00b847" if IS_LIGHT else "#00ff6a"
        cur_text_color = "#1a1a2e" if IS_LIGHT else "#fff"
        pos_color = "#00b847" if IS_LIGHT else "#00ff6a"
        neg_color = "#e53e3e" if IS_LIGHT else "#ff4d6a"

        fig = go.Figure()
        if prev_year:
            fig.add_trace(go.Bar(name=str(prev_year), x=chart_df["Месяц"],
                y=chart_df[str(prev_year)], marker_color=prev_bar_color,
                marker_line=dict(color=prev_bar_line, width=1),
                text=chart_df[str(prev_year)].apply(lambda x: f"{x/1e6:.1f}М" if x > 0 else ""),
                textposition="auto", textfont=dict(color=prev_text_color)))
        fig.add_trace(go.Bar(name=str(cur_year), x=chart_df["Месяц"],
            y=chart_df[str(cur_year)], marker_color=cur_bar_color,
            text=chart_df[str(cur_year)].apply(lambda x: f"{x/1e6:.1f}М" if x > 0 else ""),
            textposition="auto", textfont=dict(color=cur_text_color)))
        fig.update_layout(barmode="group", title=f"{t('revenue_by_months')}: {cur_year} vs {prev_year or '—'}",
            height=450, **CHART_THEME)
        fix_bar_hover(fig)

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # % изменения по месяцам
        if prev_year and not prev_data.empty:
            st.divider()
            pct_rows = []
            for m in range(1, (max_month_cur + 1) if cur_year == now.year else 13):
                cur_rev = float(cur_data.loc[m, "REVENUE"]) if m in cur_data.index else 0
                prev_rev = float(prev_data.loc[m, "REVENUE"]) if m in prev_data.index else 0
                pct = ((cur_rev / prev_rev - 1) * 100) if prev_rev > 0 else None
                pct_rows.append({"Месяц": MONTH_SHORT[m], "M": m, "Изменение %": pct, "Текущий": cur_rev, "Прошлый": prev_rev})
            pct_df = pd.DataFrame(pct_rows).dropna(subset=["Изменение %"])

            if not pct_df.empty:
                colors = [pos_color if v > 0 else neg_color for v in pct_df["Изменение %"]]
                fig2 = go.Figure(go.Bar(x=pct_df["Месяц"], y=pct_df["Изменение %"],
                    marker_color=colors,
                    marker_line=dict(width=0),
                    text=pct_df["Изменение %"].apply(lambda x: f"{x:+.1f}%"),
                    textposition="outside", textfont=dict(color=cur_text_color, size=13, family="Inter")))
                fig2.add_hline(y=0, line_dash="dot", line_color="rgba(0,0,0,0.15)" if IS_LIGHT else "rgba(255,255,255,0.1)")
                fig2.update_layout(title=f"{t('change_pct')}: {cur_year} vs {prev_year}",
                    height=400, **CHART_THEME,
                    yaxis_title=t("change_pct"),
                    bargap=0.4)
                fix_bar_hover(fig2)

                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Таблица
        st.divider()
        st.markdown("### " + ("Table" if _get_lang()=="en" else "Таблица"))
        table_rows = []
        for m in range(1, (max_month_cur + 1) if cur_year == now.year else 13):
            r = {"Месяц": MONTH_NAMES_RU[m]}
            r[f"Выручка {cur_year}"] = f"{float(cur_data.loc[m, 'REVENUE']):,.0f} ₽" if m in cur_data.index else "—"
            r[f"Заказов {cur_year}"] = f"{int(cur_data.loc[m, 'ORDERS']):,}" if m in cur_data.index else "—"
            if prev_year:
                r[f"Выручка {prev_year}"] = f"{float(prev_data.loc[m, 'REVENUE']):,.0f} ₽" if not prev_data.empty and m in prev_data.index else "—"
                r[f"Заказов {prev_year}"] = f"{int(prev_data.loc[m, 'ORDERS']):,}" if not prev_data.empty and m in prev_data.index else "—"
                cur_rev = float(cur_data.loc[m, "REVENUE"]) if m in cur_data.index else 0
                prev_rev = float(prev_data.loc[m, "REVENUE"]) if not prev_data.empty and m in prev_data.index else 0
                if prev_rev > 0 and cur_rev > 0:
                    r["Δ %"] = f"{(cur_rev/prev_rev - 1)*100:+.1f}%"
                    r["Δ ₽"] = f"{cur_rev - prev_rev:+,.0f}"
                else:
                    r["Δ %"] = "—"
                    r["Δ ₽"] = "—"
            table_rows.append(r)
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

# --- БЛЮДА ---
if page == "Блюда":
    page_header("Блюда")
    ds=load_dishes(date_from,date_to)
    if not ds.empty:
        cl,cr=st.columns(2)
        with cl:
            fig=px.bar(ds.head(15),x="TOTAL_SUM",y="DISH_NAME",orientation="h",title=t("by_revenue"),color="TOTAL_SUM",color_continuous_scale="Viridis", labels=_labels())
            fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
        with cr:
            tq=ds.sort_values("TOTAL_QTY",ascending=False).head(15)
            fig=px.bar(tq,x="TOTAL_QTY",y="DISH_NAME",orientation="h",title="🔥 " + ("By quantity" if _get_lang()=="en" else "По количеству"),color="TOTAL_QTY",color_continuous_scale="Inferno", labels=_labels())
            fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
        st.dataframe(ds.rename(columns={"DISH_ID":"ID","DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка","ORDER_COUNT":"Заказов"}),use_container_width=True,hide_index=True)
    else: st.info(t("no_data"))

# --- СТОЛОВЫЕ ---
if page == "Рестораны":
    page_header("Рестораны")
    rest_data = load_revenue_by_restaurant(date_from, date_to)
    if not rest_data.empty:
        total_rev = float(rest_data["REVENUE"].sum())
        total_ord = int(rest_data["ORDER_COUNT"].sum())
        total_guests = int(rest_data["GUESTS"].sum())
        active_rest = len(rest_data)

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric(t("locations"), f"{active_rest}")
        with c2: st.metric(t("revenue"), f"{total_rev:,.0f} ₽")
        with c3: st.metric(t("orders"), f"{total_ord:,}")
        with c4: st.metric(t("guests"), f"{total_guests:,}")
        st.caption(t("locations_note"))
        st.divider()

        # Выручка по столовым
        fig = px.bar(rest_data, x="REVENUE", y="REST_NAME", orientation="h",
            title=t("revenue_by_locations"), color="REVENUE", color_continuous_scale="Viridis",
            hover_data={"ORDER_COUNT":True, "AVG_CHECK":":.0f", "GUESTS":True}, labels=_labels())
        fig.update_layout(height=max(400, len(rest_data)*35), yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
        fix_bar_hover(fig)

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Средний чек по столовым
        cl,cr = st.columns(2)
        with cl:
            fig = px.bar(rest_data.sort_values("AVG_CHECK",ascending=False), x="AVG_CHECK", y="REST_NAME",
                orientation="h", title=t("avg_check_by_loc"), color="AVG_CHECK", color_continuous_scale="Tealgrn", labels=_labels())
            fig.update_layout(height=max(400,len(rest_data)*30), yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
        with cr:
            fig = px.pie(rest_data.head(10), values="REVENUE", names="REST_NAME", title=t("revenue_share"), hole=0.4, labels=_labels())
            fig.update_layout(height=400, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Динамика по столовым
        if (date_to-date_from).days > 1:
            daily_rest = load_daily_by_restaurant(date_from, date_to)
            if not daily_rest.empty:
                top5 = rest_data.head(5)["REST_NAME"].tolist()
                dr_top = daily_rest[daily_rest["REST_NAME"].isin(top5)]
                fig = px.line(dr_top, x="DAY", y="REVENUE", color="REST_NAME",
                    title=t("revenue_dynamics"), markers=True, labels=_labels())
                fig.update_layout(height=400, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        st.dataframe(rest_data.rename(columns={"REST_ID":"ID","REST_NAME":"Точка","ORDER_COUNT":"Заказов",
            "REVENUE":"Выручка","GUESTS":"Гостей","AVG_CHECK":"Ср.чек","DISHES":"Блюд"}),
            use_container_width=True, hide_index=True)
    else: st.info(t("no_restaurant_data"))

# --- КАТЕГОРИИ ---
if page == "Категории":
    page_header("Категории")
    cat_data = load_revenue_by_category(date_from, date_to)
    if not cat_data.empty:
        cl,cr = st.columns(2)
        with cl:
            fig = px.bar(cat_data.head(15), x="TOTAL_SUM", y="CATEGORY", orientation="h",
                title=t("revenue_by_categories"), color="TOTAL_SUM", color_continuous_scale="Sunset", labels=_labels())
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
        with cr:
            fig = px.pie(cat_data.head(10), values="TOTAL_SUM", names="CATEGORY",
                title=t("category_share"), hole=0.35, labels=_labels())
            fig.update_layout(height=500, legend=dict(orientation="h", yanchor="top", y=-0.05, font_size=10), **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
        st.dataframe(cat_data.rename(columns={"CAT_ID":"ID","CATEGORY":"Категория","TOTAL_QTY":"Кол-во",
            "TOTAL_SUM":"Выручка","ORDER_COUNT":"Заказов"}), use_container_width=True, hide_index=True)

        # === Drill-down: топ блюд в категории ===
        st.divider()
        st.markdown("### " + ("Top Dishes in Category" if _get_lang()=="en" else "Топ блюд в категории"))
        cat_options = cat_data[["CAT_ID", "CATEGORY"]].copy()
        cat_options["label"] = cat_options["CATEGORY"] + " (" + cat_data["TOTAL_SUM"].apply(lambda x: f"{x:,.0f} ₽") + ")"
        selected_cat_label = st.selectbox("Выберите категорию:", cat_options["label"].tolist(), key="cat_drill")
        selected_idx = cat_options["label"].tolist().index(selected_cat_label)
        selected_cat_id = int(cat_options.iloc[selected_idx]["CAT_ID"])
        selected_cat_name = cat_options.iloc[selected_idx]["CATEGORY"]

        dishes_in_cat = load_dishes_by_category(date_from, date_to, selected_cat_id)
        if not dishes_in_cat.empty:
            c1,c2,c3 = st.columns(3)
            with c1: st.metric("🍽 Блюд", f"{len(dishes_in_cat)}")
            with c2: st.metric(t("revenue"), f"{dishes_in_cat['TOTAL_SUM'].sum():,.0f} ₽")
            with c3: st.metric(t("sold"), f"{dishes_in_cat['TOTAL_QTY'].sum():,.0f}")

            fig_d = px.bar(dishes_in_cat.head(20), x="TOTAL_SUM", y="DISH_NAME", orientation="h",
                title=f"Топ блюд — {selected_cat_name}",
                color="TOTAL_SUM", color_continuous_scale="Viridis",
                text=dishes_in_cat.head(20)["TOTAL_SUM"].apply(lambda x: f"{x:,.0f}₽"),
                labels=_labels())
            fig_d.update_traces(textposition="auto")
            fig_d.update_layout(height=max(400, len(dishes_in_cat.head(20))*25),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig_d)

            st.plotly_chart(fig_d, use_container_width=True, config={"displayModeBar":False,"scrollZoom":False})

            disp_d = dishes_in_cat.rename(columns={
                "DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка ₽",
                "AVG_PRICE":"Ø Цена ₽","ORDER_COUNT":"Заказов"})
            st.dataframe(disp_d, use_container_width=True, hide_index=True)
        else:
            st.info(f"{t('no_data')}: {selected_cat_name}")
    else: st.info(t("no_category_data"))

# --- ПЕРСОНАЛ ---
if page == "Персонал":
    page_header("Персонал")
    sub1,sub2 = st.tabs([t("cashier_revenue"), t("work_time")])

    with sub1:
        emp = load_top_employees(date_from, date_to)
        if not emp.empty:
            c1,c2,c3 = st.columns(3)
            with c1: st.metric(t("worked"), f"{len(emp)}")
            with c2: st.metric(t("best"), emp.iloc[0]["EMP_NAME"] if emp.iloc[0]["EMP_NAME"] else "—")
            with c3: st.metric(t("max_revenue"), f'{float(emp.iloc[0]["REVENUE"]):,.0f} ₽')
            st.caption(t("cashier_revenue_note"))

            fig = px.bar(emp.head(15), x="REVENUE", y="EMP_NAME", orientation="h",
                title=t("top_cashiers"), color="REVENUE", color_continuous_scale="Viridis",
                hover_data={"ORDER_COUNT":True,"AVG_CHECK":":.0f"}, labels=_labels())
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            st.dataframe(emp.rename(columns={"EMP_ID":"ID","EMP_NAME":"Сотрудник","ORDER_COUNT":"Заказов",
                "REVENUE":"Выручка","AVG_CHECK":"Ср.чек","GUESTS":"Гостей"}), use_container_width=True, hide_index=True)
        else: st.info(t("no_data"))

    with sub2:
        clock = load_clockrecs(date_from, date_to)
        if not clock.empty:
            total_shifts = int(clock["SHIFT_COUNT"].sum())
            total_late = int(clock["LATE_COUNT"].sum())
            late_pct = total_late / total_shifts * 100 if total_shifts else 0

            c1,c2,c3 = st.columns(3)
            with c1: st.metric(t("shifts_total"), f"{total_shifts}")
            with c2: st.metric("⏰ Опозданий", f"{total_late}")
            with c3: st.metric("📉 % опозданий", f"{late_pct:.1f}%")
            st.caption(t("late_note"))

            # Опоздания
            late_emp = clock[clock["LATE_COUNT"]>0].sort_values("LATE_COUNT", ascending=False).head(15)
            if not late_emp.empty:
                fig = px.bar(late_emp, x="LATE_COUNT", y="EMP_NAME", orientation="h",
                    title="⏰ Топ по опозданиям", color="LATE_COUNT", color_continuous_scale="Reds", labels=_labels())
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            st.dataframe(clock.rename(columns={"EMPID":"ID","EMP_NAME":"Сотрудник","SHIFT_COUNT":"Смен",
                "AVG_HOURS":"Ср.часов","LATE_COUNT":"Опозданий"}), use_container_width=True, hide_index=True)
        else: st.info(t("no_work_time"))

# --- КАССА ---
if page == "Касса":
    page_header("Касса")
    cash = load_cashinout(date_from, date_to)
    if not cash.empty:
        deposits = cash[cash["ISDEPOSIT"]==1]
        collections = cash[cash["ISDEPOSIT"]==0]
        dep_sum = float(deposits["ORIGINALSUM"].sum()) if not deposits.empty else 0
        col_sum = float(collections["ORIGINALSUM"].abs().sum()) if not collections.empty else 0

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric(t("deposits"), f"{len(deposits)}")
        with c2: st.metric("💵 Сумма внесений", f"{dep_sum:,.0f} ₽")
        with c3: st.metric("📤 Изъятий", f"{len(collections)}")
        with c4: st.metric("💸 Сумма изъятий", f"{col_sum:,.0f} ₽")
        st.caption(t("cash_ops_note"))

        # По дням
        cash_copy = cash.copy()
        cash_copy["DAY"] = pd.to_datetime(cash_copy["DATETIME"]).dt.date
        cash_copy["ABS_SUM"] = cash_copy["ORIGINALSUM"].abs()
        cash_copy["TYPE"] = cash_copy["ISDEPOSIT"].map({1:"Внесение",0:"Изъятие"})
        daily_cash = cash_copy.groupby(["DAY","TYPE"]).agg(SUM=("ABS_SUM","sum"),COUNT=("ORIGINALSUM","count")).reset_index()

        if not daily_cash.empty:
            fig = px.bar(daily_cash, x="DAY", y="SUM", color="TYPE", barmode="group",
                title=t("deposits_withdrawals"),
                color_discrete_map={"Внесение":"#10b981","Изъятие":"#ef4444"}, labels=_labels())
            fig.update_layout(height=400, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # По причинам
        kind_map = {0:"Вручную",1:"Программой",3:"Закрытие смены",2:"Чаевые",4:"Закр.общей смены",5:"Откр.общей смены",6:"Пополнение карты"}
        cash_copy["KIND_NAME"] = cash_copy["KIND"].map(kind_map).fillna("Другое")
        by_kind = cash_copy.groupby("KIND_NAME").agg(SUM=("ABS_SUM","sum"),COUNT=("ORIGINALSUM","count")).reset_index().sort_values("SUM",ascending=False)
        if not by_kind.empty:
            fig = px.pie(by_kind, values="SUM", names="KIND_NAME", title=t("by_op_type"), hole=0.4, labels=_labels())
            fig.update_layout(height=400, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
    else: st.info(t("no_cash_ops"))

# --- ЦЕНЫ ---
if page == "Цены":
    page_header("Цены")
    st.markdown("### " + ("Price Analysis" if _get_lang()=="en" else "Анализ цен"))
    st.caption(t("prices_note"))
    prices = load_current_prices(date_from, date_to)
    if not prices.empty:
        c1,c2,c3 = st.columns(3)
        with c1: st.metric(t("dishes_price_changed"), f"{len(prices)}")
        with c2: st.metric(t("max_diff"), f'{float(prices["PRICE_DIFF"].max()):,.0f} ₽')
        with c3: st.metric(t("avg_diff"), f'{float(prices["PRICE_DIFF"].mean()):,.0f} ₽')
        st.divider()

        # Топ по разнице цен
        cl,cr = st.columns(2)
        with cl:
            top_diff = prices.head(15)
            fig = px.bar(top_diff, x="PRICE_DIFF", y="DISH_NAME", orientation="h",
                title=t("price_growth"), color="PRICE_DIFF", color_continuous_scale="Reds",
                hover_data={"MIN_PRICE":True,"MAX_PRICE":True,"PRICE_VARIANTS":True}, labels=_labels())
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
        with cr:
            fig = px.bar(top_diff, x="PRICE_VARIANTS", y="DISH_NAME", orientation="h",
                title="🔢 Количество вариантов цен", color="PRICE_VARIANTS", color_continuous_scale="Viridis",
                hover_data={"MIN_PRICE":True,"MAX_PRICE":True}, labels=_labels())
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Min vs Max (диапазон)
        fig = go.Figure()
        top20 = prices.head(20)
        fig.add_trace(go.Bar(x=top20["DISH_NAME"], y=top20["MIN_PRICE"], name=("Min price" if _get_lang()=="en" else "Мин. цена"), marker_color="#00ff6a"))
        fig.add_trace(go.Bar(x=top20["DISH_NAME"], y=top20["PRICE_DIFF"], name=(t("difference")), marker_color="#ef4444"))
        fig.update_layout(title=t("price_range"), barmode="stack",
            height=400, xaxis_tickangle=-45, **CHART_THEME)
        fix_bar_hover(fig)

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # История цен
        if (date_to - date_from).days > 1:
            st.divider()
            st.markdown("#### " + ("Price History" if _get_lang()=="en" else "История цен"))
            ph = load_price_history(date_from, date_to)
            if not ph.empty:
                top_dishes = prices.head(10)["DISH_NAME"].tolist()
                dish_select = st.multiselect(t("dishes") + ":", top_dishes, default=top_dishes[:3])
                if dish_select:
                    ph_filt = ph[ph["DISH_NAME"].isin(dish_select)]
                    if not ph_filt.empty:
                        fig = px.line(ph_filt, x="DAY", y="AVG_PRICE", color="DISH_NAME",
                            title="Price dynamics" if _get_lang()=="en" else "Динамика цен", markers=True, labels=_labels())
                        fig.update_layout(height=400, **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        st.divider()
        disp = prices.copy()
        disp.columns = ["Dish","Avg","Min","Max","Variants","Qty","Revenue","Diff"] if _get_lang()=="en" else ["Dish" if _get_lang()=="en" else "Блюдо","Ср.цена","Мин","Макс","Вариантов цен","Кол-во","Выручка","Разница"]
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else: st.info(t("no_data"))

# --- ABC ---
if page == "ABC":
    page_header("ABC")
    st.caption(t("abc_desc"))
    abc = load_abc_analysis(date_from, date_to)
    if not abc.empty:
        total_rev = float(abc["TOTAL_SUM"].sum())
        abc = abc.copy()
        abc["CUMSUM"] = abc["TOTAL_SUM"].cumsum()
        abc["CUM_PCT"] = abc["CUMSUM"] / total_rev * 100
        abc["ABC"] = abc["CUM_PCT"].apply(lambda x: "A" if x <= 80 else ("B" if x <= 95 else "C"))

        a_count = len(abc[abc["ABC"]=="A"])
        b_count = len(abc[abc["ABC"]=="B"])
        c_count = len(abc[abc["ABC"]=="C"])
        a_rev = float(abc[abc["ABC"]=="A"]["TOTAL_SUM"].sum())
        b_rev = float(abc[abc["ABC"]=="B"]["TOTAL_SUM"].sum())
        c_rev = float(abc[abc["ABC"]=="C"]["TOTAL_SUM"].sum())

        _d = t("dishes_count")
        c1,c2,c3 = st.columns(3)
        with c1:
            st.metric(f"🅰️ {t('group')} A", f"{a_count} {_d}")
            st.caption(f"{a_rev:,.0f} ₽ (80% {t('revenue').lower()})")
        with c2:
            st.metric(f"🅱️ {t('group')} B", f"{b_count} {_d}")
            st.caption(f"{b_rev:,.0f} ₽ (15% {t('revenue').lower()})")
        with c3:
            st.metric(f"🅲 {t('group')} C", f"{c_count} {_d}")
            st.caption(f"{c_rev:,.0f} ₽ (5% {t('revenue').lower()})")
        st.divider()

        _stars = t("stars"); _avg = t("average"); _cand = t("candidates")
        cl, cr = st.columns(2)
        with cl:
            pie_data = pd.DataFrame({
                t("group"): [f"A — {a_count}", f"B — {b_count}", f"C — {c_count}"],
                t("revenue"): [a_rev, b_rev, c_rev]
            })
            fig = px.pie(pie_data, values=t("revenue"), names=t("group"),
                title=t("abc_revenue_share"), hole=0.4,
                color_discrete_sequence=["#10b981","#f59e0b","#ef4444"])
            fig.update_layout(height=400, **CHART_THEME)
            fix_bar_hover(fig)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        with cr:
            summary = pd.DataFrame({
                t("group"): [f"A — {_stars}", f"B — {_avg}", f"C — {_cand}"],
                t("dishes_count"): [a_count, b_count, c_count],
                t("revenue"): [f"{a_rev:,.0f} ₽", f"{b_rev:,.0f} ₽", f"{c_rev:,.0f} ₽"],
                t("share"): [f"{a_rev/total_rev*100:.0f}%", f"{b_rev/total_rev*100:.0f}%", f"{c_rev/total_rev*100:.0f}%"]
            })
            st.dataframe(summary, use_container_width=True, hide_index=True)

        st.divider()
        _radio_opts = [f"A — {_stars}", f"B — {_avg}", f"C — {_cand}"]
        abc_grp_view = st.radio(f"{t('dishes_count')}:", _radio_opts, horizontal=True, key="abc_grp_radio")
        grp_letter = abc_grp_view[0]
        grp_data = abc[abc["ABC"]==grp_letter].copy()
        grp_rev = float(grp_data["TOTAL_SUM"].sum())
        if not grp_data.empty:
            show = grp_data.head(30)
            fig = px.bar(show, x="TOTAL_SUM", y="DISH_NAME", orientation="h",
                title=f"{t('group')} {grp_letter}: {len(grp_data)} {_d} · {grp_rev:,.0f} ₽ · {grp_rev/total_rev*100:.0f}%",
                color="TOTAL_SUM", color_continuous_scale={
                    "A":"Tealgrn","B":"YlOrBr","C":"OrRd"}[grp_letter],
                text=show["TOTAL_SUM"].apply(lambda x: f"{x:,.0f} ₽"), labels=_labels())
            fig.update_traces(textposition="auto")
            fig.update_layout(height=max(400, min(30, len(show))*28),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
        else:
            st.info(t("no_data"))

        # Распределение по категориям
        if "CATEGORY" in abc.columns:
            abc_cat = abc.groupby(["CATEGORY","ABC"]).agg(
                COUNT=("DISH_NAME","count"), SUM=("TOTAL_SUM","sum")).reset_index()
            abc_cat = abc_cat[abc_cat["CATEGORY"].notna()]
            if not abc_cat.empty:
                fig = px.bar(abc_cat, x="CATEGORY", y="COUNT", color="ABC",
                    title="ABC by categories" if _get_lang()=="en" else "ABC по категориям",
                    color_discrete_map={"A":"#10b981","B":"#f59e0b","C":"#ef4444"},
                    barmode="stack", labels=_labels())
                fig.update_layout(height=400, xaxis_tickangle=-45, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Фильтр по группам
        st.divider()
        abc_filter = st.selectbox(t("group") + ":", ["All" if _get_lang()=="en" else "Все","A","B","C"])
        abc_disp = abc if abc_filter=="Все" else abc[abc["ABC"]==abc_filter]
        disp = abc_disp[["DISH_NAME","CATEGORY","TOTAL_QTY","TOTAL_SUM","AVG_PRICE","ABC","CUM_PCT"]].copy()
        disp.columns = ["Dish","Category","Qty","Revenue","Avg price","Group","Cumul.%"] if _get_lang()=="en" else ["Dish" if _get_lang()=="en" else "Блюдо","Категория","Кол-во","Выручка","Ср.цена","Группа","Накоп.%"]
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else: st.info(t("no_data"))

# --- СКОРОСТЬ ---
if page == "Скорость":
    page_header("Скорость")
    st.caption("Order time: creation to payment close" if _get_lang()=="en" else "Время от создания заказа до закрытия после оплаты")

    speed = load_cashier_speed(date_from, date_to)
    dist = load_speed_distribution(date_from, date_to)

    if not speed.empty:
        avg_all = float(speed["AVG_SEC"].mean())
        fastest = speed.iloc[0]["CASHIER"] if speed.iloc[0]["CASHIER"] else "—"
        fastest_sec = float(speed.iloc[0]["AVG_SEC"])
        slowest = speed.iloc[-1]["CASHIER"] if speed.iloc[-1]["CASHIER"] else "—"
        slowest_sec = float(speed.iloc[-1]["AVG_SEC"])

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric(t("avg_time"), f"{avg_all:.0f} сек")
        with c2: st.metric(t("fastest"), f"{fastest}", delta=f"{fastest_sec:.0f} сек")
        with c3: st.metric("🐢 " + ("Slowest" if _get_lang()=="en" else "Самый медленный"), f"{slowest}", delta=f"{slowest_sec:.0f} sec" if _get_lang()=="en" else f"{slowest_sec:.0f} сек", delta_color="inverse")
        with c4: st.metric(t("cashiers_count"), f"{len(speed)}")
        st.caption("Avg service time in seconds. Green = fast, red = slow." if _get_lang()=="en" else "Среднее время обслуживания в секундах. Зелёное = быстрый, красное = медленный.")
        st.divider()

        # Распределение времени
        if not dist.empty:
            dist_clean = dist.copy()
            dist_clean["LABEL"] = dist_clean["TIME_RANGE"].str[4:]  # убираем 01. 02. ...
            fig = px.bar(dist_clean, x="LABEL", y="ORDER_COUNT",
                title="Order time distribution" if _get_lang()=="en" else "Распределение заказов по времени обслуживания",
                color="ORDER_COUNT", color_continuous_scale="RdYlGn_r",
                text="ORDER_COUNT", labels=_labels())
            fig.update_traces(textposition="auto")
            fig.update_layout(height=400, coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Кассиры
        cl, cr = st.columns(2)
        with cl:
            fig = px.bar(speed, x="AVG_SEC", y="CASHIER", orientation="h",
                title="Avg time by cashier (sec)" if _get_lang()=="en" else "Среднее время по кассирам (сек)",
                color="AVG_SEC", color_continuous_scale="RdYlGn_r",
                hover_data={"ORDERS":True, "AVG_DISHES":":.1f", "AVG_CHECK":":.0f"}, labels=_labels())
            fig.update_layout(height=max(400, len(speed)*30), yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        with cr:
            # Скорость vs Выручка (scatter)
            fig = px.scatter(speed, x="AVG_SEC", y="REVENUE", size="ORDERS",
                hover_name="CASHIER", title="Speed vs Revenue" if _get_lang()=="en" else "Скорость vs Выручка",
                labels=_labels(),
                color="AVG_DISHES", color_continuous_scale="Viridis")
            fig.update_layout(height=max(400, len(speed)*30), coloraxis_colorbar_title="Avg dishes" if _get_lang()=="en" else "Ср.блюд", **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # По часам
        speed_hour = load_speed_by_hour(date_from, date_to)
        if not speed_hour.empty:
            st.divider()
            fig = go.Figure()
            fig.add_trace(go.Bar(x=speed_hour["HOUR"], y=speed_hour["AVG_SEC"],
                name=("Avg time (sec)" if _get_lang()=="en" else "Ср. время (сек)"), marker_color="#f59e0b"))
            fig.add_trace(go.Scatter(x=speed_hour["HOUR"], y=speed_hour["ORDERS"],
                name=t("orders"), yaxis="y2", mode="lines+markers",
                line=dict(color="#00ff6a", width=3)))
            fig.update_layout(title="⏰ Скорость по часам дня",
                yaxis=dict(title="Avg time, sec" if _get_lang()=="en" else "Ср. время, сек"), yaxis2=dict(title="Orders" if _get_lang()=="en" else "Заказов", side="right", overlaying="y"),
                height=400, legend=dict(orientation="h", y=1.1), **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # По столовым
        speed_rest = load_speed_by_restaurant(date_from, date_to)
        if not speed_rest.empty:
            st.divider()
            fig = px.bar(speed_rest, x="AVG_SEC", y="REST_NAME", orientation="h",
                title="Speed by location" if _get_lang()=="en" else "Скорость по точкам", color="AVG_SEC", color_continuous_scale="RdYlGn_r",
                hover_data={"ORDERS":True, "AVG_DISHES":":.1f"}, labels=_labels())
            fig.update_layout(height=max(400, len(speed_rest)*30),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Таблица
        st.divider()
        disp = speed.copy()
        disp["AVG_MIN"] = disp["AVG_SEC"].apply(lambda x: f"{x/60:.1f} мин" if x >= 60 else f"{x:.0f} сек")
        disp_show = disp[["CASHIER","ORDERS","AVG_MIN","AVG_SEC","MIN_SEC","MAX_SEC","AVG_DISHES","REVENUE","AVG_CHECK"]].copy()
        disp_show.columns = ["Cashier","Orders","Avg time","Sec","Min sec","Max sec","Avg dishes","Revenue","Avg check"] if _get_lang()=="en" else ["Кассир","Заказов","Ср.время","Сек","Мин.сек","Макс.сек","Ср.блюд","Выручка","Ср.чек"]
        st.dataframe(disp_show, use_container_width=True, hide_index=True)
    else:
        st.info(t("no_data"))

# --- СМЕНЫ ---
if page == "Смены":
    page_header("Смены")
    shifts = load_shifts(date_from, date_to)
    if not shifts.empty:
        total_shifts = len(shifts)
        open_now = len(shifts[shifts["CLOSED"]==0])
        avg_hours = float(shifts[shifts["DURATION_MIN"]>0]["DURATION_MIN"].mean())/60 if len(shifts[shifts["DURATION_MIN"]>0]) else 0

        c1,c2,c3 = st.columns(3)
        with c1: st.metric(t("total_shifts"), f"{total_shifts}")
        with c2: st.metric(t("open_now"), f"{open_now}")
        with c3: st.metric(t("avg_duration"), f"{avg_hours:.1f} ч")
        st.divider()

        st.markdown("""
        **Описание полей:**
        | Поле | Что значит |
        |---|---|
        | **Дата смены** | Логическая дата рабочего дня |
        | **Открытие** | Когда менеджер открыл смену (включил кассу) |
        | **Закрытие** | Когда смену закрыли (конец рабочего дня) |
        | **Менеджер** | Ответственный за смену (открывает/закрывает) |
        | **Точка** | Столовая/буфет |
        | **Статус** | 🟢 Открыта / 🔴 Закрыта |
        """)
        st.divider()

        # По точкам — сводка
        by_rest = shifts.groupby("REST_NAME").agg(
            SHIFTS=("MIDSERVER","count"),
            AVG_MIN=("DURATION_MIN","mean"),
            LAST_OPEN=("CREATETIME","max")
        ).reset_index().sort_values("SHIFTS",ascending=False)
        
        by_rest["AVG_HOURS"] = by_rest["AVG_MIN"] / 60
        
        fig = px.bar(by_rest, x="SHIFTS", y="REST_NAME", orientation="h",
            title="Shifts by location" if _get_lang()=="en" else "Количество смен по точкам", color="AVG_HOURS",
            color_continuous_scale="RdYlGn", hover_data={"AVG_HOURS":":.1f"}, labels=_labels())
        fig.update_layout(height=max(400,len(by_rest)*28), yaxis=dict(autorange="reversed"),
            coloraxis_colorbar_title="Avg hours" if _get_lang()=="en" else "Ср.часов", **CHART_THEME)
        fix_bar_hover(fig)

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Таблица смен
        st.divider()
        disp = shifts.copy()
        disp["СТАТУС"] = disp["CLOSED"].map({0:"🟢 Открыта", 1:"🔴 Закрыта"})
        disp["ЧАСОВ"] = (disp["DURATION_MIN"] / 60).round(1)
        disp_show = disp[["SHIFTDATE","CREATETIME","CLOSETIME","MANAGER","REST_NAME","СТАТУС","ЧАСОВ"]].copy()
        disp_show.columns = ["Shift date","Open","Close","Manager","Location","Status","Hours"] if _get_lang()=="en" else ["Дата смены","Открытие","Закрытие","Менеджер","Точка","Статус","Часов"]
        st.dataframe(disp_show, use_container_width=True, hide_index=True, height=500)
    else:
        st.info(t("no_data"))

# --- ПРОБЛЕМЫ ---
if page == "Проблемы":
    page_header("Проблемы")
    
    sub_mark, sub_cards, sub_fiscal = st.tabs(["Honest Mark" if _get_lang()=="en" else "Честный знак", "Card issues" if _get_lang()=="en" else "Проблемы с картами", "Fiscalization" if _get_lang()=="en" else "Фискализация"])

    # ========== ЧЕСТНЫЙ ЗНАК ==========
    with sub_mark:
        st.markdown("""
        **Что это:** При продаже маркированных товаров (вода, соки, молочка) касса проверяет код маркировки через систему «Честный знак».
        
        **Типы результатов:**
        | Код | Значение | Что делать |
        |---|---|---|
        | 🟢 **0** | Проверка пройдена, маркировка валидная | Всё ок |
        | 🔴 **1** | Ошибка проверки (сервис недоступен, код выведен из оборота) | Проверить товар, связаться с поставщиком |
        | 🟡 **2** | Предупреждение (частичная проверка) | Обратить внимание |
        | **3** | Таймаут (сервер не ответил вовремя) | Проблема с интернетом или нагрузкой сервиса |
        """)
        st.divider()

        stats = load_checkmark_stats(date_from, date_to)
        if not stats.empty:
            res_map = {0:"🟢 ОК", 1:"🔴 Ошибка", 2:"🟡 Предупреждение", 3:"Таймаут"}
            stats["STATUS"] = stats["RES"].map(res_map).fillna("Неизвестно")
            total = int(stats["CNT"].sum())
            ok_cnt = int(stats[stats["RES"]==0]["CNT"].sum()) if 0 in stats["RES"].values else 0
            err_cnt = int(stats[stats["RES"]==1]["CNT"].sum()) if 1 in stats["RES"].values else 0
            timeout_cnt = int(stats[stats["RES"]==3]["CNT"].sum()) if 3 in stats["RES"].values else 0
            err_pct = (total - ok_cnt) / total * 100 if total else 0

            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric(t("total_checks"), f"{total:,}")
            with c2: st.metric(t("successful"), f"{ok_cnt:,}")
            with c3: st.metric(t("errors"), f"{err_cnt:,}")
            with c4: st.metric(t("timeouts"), f"{timeout_cnt:,}")

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(stats, values="CNT", names="STATUS", title="Check Results" if _get_lang()=="en" else "Результаты проверок",
                    color_discrete_map={"🟢 ОК":"#10b981","🔴 Ошибка":"#ef4444","🟡 Предупреждение":"#f59e0b","Таймаут":"#00ff6a"}, hole=0.4, labels=_labels())
                fig.update_layout(height=400, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with cr:
                fig = px.bar(stats[stats["RES"]!=0], x="CNT", y="STATUS", orientation="h",
                    title="Problem checks" if _get_lang()=="en" else "Проблемные проверки", color="STATUS",
                    color_discrete_map={"🔴 Ошибка":"#ef4444","🟡 Предупреждение":"#f59e0b","Таймаут":"#00ff6a"}, labels=_labels())
                fig.update_layout(height=400, showlegend=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Детали ошибок
        errors = load_checkmark_errors(date_from, date_to)
        if not errors.empty:
            st.divider()
            st.markdown("#### " + ("Error Details" if _get_lang()=="en" else "Детали ошибок"))

            # По типам ошибок
            by_msg = errors.groupby("MESSAGEFROMDRIVER").agg(CNT=("RES","count")).reset_index().sort_values("CNT",ascending=False)
            if not by_msg.empty:
                fig = px.bar(by_msg.head(10), x="CNT", y="MESSAGEFROMDRIVER", orientation="h",
                    title="Error types" if _get_lang()=="en" else "Типы ошибок", color="CNT", color_continuous_scale="Reds", labels=_labels())
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # По товарам
            by_prod = errors.groupby("PRODUCT").agg(CNT=("RES","count")).reset_index().sort_values("CNT",ascending=False)
            by_prod = by_prod[by_prod["PRODUCT"].notna()]
            if not by_prod.empty:
                fig = px.bar(by_prod.head(15), x="CNT", y="PRODUCT", orientation="h",
                    title="Problem products" if _get_lang()=="en" else "Проблемные товары", color="CNT", color_continuous_scale="Oranges", labels=_labels())
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # Таблица ошибок
            st.divider()
            res_map_full = {0:"ОК", 1:"Ошибка", 2:"Предупреждение", 3:"Таймаут"}
            disp = errors.copy()
            disp["RESULT"] = disp["RES"].map(res_map_full)
            dc = {"DATETIME":"Дата/время","PRODUCT":"Товар","MESSAGEFROMDRIVER":"Сообщение","RESULT":"Результат","REST_NAME":"Точка"}
            av = [c for c in dc if c in disp.columns]
            st.dataframe(disp[av].rename(columns={k:v for k,v in dc.items() if k in av}),
                use_container_width=True, hide_index=True, height=400)

    # ========== ПРОБЛЕМЫ С КАРТАМИ ==========
    with sub_cards:
        st.markdown("""
        **Что это:** Статусы банковских транзакций при оплате картой.
        
        **Статусы транзакций:**
        | Код | Значение | Что значит |
        |---|---|---|
        | **0** | Нет транзакции | Оплата не прошла через банковский терминал |
        | **1** | Отменена | Транзакция была начата, но отменена (отказ карты, ошибка связи) |
        | **4** | Авторизована | Успешно авторизована банком |
        | **5** | Подтверждена | Полностью завершена и подтверждена |
        | **6** | Отменена после подтверждения | Возврат средств на карту |
        """)
        st.divider()

        card_stats = load_card_errors(date_from, date_to)
        if not card_stats.empty:
            status_map = {0:"⚪ Нет транзакции", 1:"🔴 Отменена", 4:"🟡 Авторизована", 5:"🟢 Подтверждена", 6:"🔵 Возврат"}
            card_stats["STATUS"] = card_stats["TRANSACTIONSTATUS"].map(status_map).fillna("Другое")
            total = int(card_stats["CNT"].sum())
            ok_cnt = int(card_stats[card_stats["TRANSACTIONSTATUS"]==5]["CNT"].sum()) if 5 in card_stats["TRANSACTIONSTATUS"].values else 0
            cancel_cnt = int(card_stats[card_stats["TRANSACTIONSTATUS"]==1]["CNT"].sum()) if 1 in card_stats["TRANSACTIONSTATUS"].values else 0
            no_trans = int(card_stats[card_stats["TRANSACTIONSTATUS"]==0]["CNT"].sum()) if 0 in card_stats["TRANSACTIONSTATUS"].values else 0

            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric(t("total_transactions"), f"{total:,}")
            with c2: st.metric(t("successful"), f"{ok_cnt:,}")
            with c3: st.metric(t("canceled"), f"{cancel_cnt:,}")
            with c4: st.metric("⚪ Без транзакции", f"{no_trans:,}")

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(card_stats, values="CNT", names="STATUS", title="Transaction Statuses" if _get_lang()=="en" else "Статусы транзакций", hole=0.4, labels=_labels())
                fig.update_layout(height=400, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with cr:
                problem = card_stats[card_stats["TRANSACTIONSTATUS"].isin([0,1,6])]
                if not problem.empty:
                    fig = px.bar(problem, x="CNT", y="STATUS", orientation="h",
                        title="Problem transactions" if _get_lang()=="en" else "Проблемные транзакции", color="CNT", color_continuous_scale="Reds", labels=_labels())
                    fig.update_layout(height=300, coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                else:
                    st.success("Все транзакции успешны!")

            if cancel_cnt > 0 or no_trans > 0:
                st.warning(f"{cancel_cnt} canceled + {no_trans} no transaction = potential terminal issues" if _get_lang()=="en" else f"{cancel_cnt} отменённых + {no_trans} без транзакции = проблемы с терминалами")

            # --- Разбивка по столовым ---
            st.divider()
            st.markdown("### " + ("By Canteens" if _get_lang()=="en" else "По столовым"))
            card_by_rest = load_card_errors_by_restaurant(date_from, date_to)
            if not card_by_rest.empty:
                status_map_r = {0:"Нет транзакции", 1:"Отменена", 4:"Авторизована", 5:"Подтверждена", 6:"Возврат"}
                card_by_rest["STATUS"] = card_by_rest["TRANSACTIONSTATUS"].map(status_map_r).fillna("Другое")

                # Только проблемные (0, 1, 6)
                problems = card_by_rest[card_by_rest["TRANSACTIONSTATUS"].isin([0, 1, 6])].copy()
                if not problems.empty:
                    # Сводка по столовым — общее кол-во проблем
                    by_rest = problems.groupby("REST_NAME").agg(
                        PROBLEM_COUNT=("CNT", "sum"),
                        PROBLEM_SUM=("TOTAL_SUM", "sum")
                    ).reset_index().sort_values("PROBLEM_COUNT", ascending=False)

                    c1, c2 = st.columns(2)
                    with c1: st.metric(t("restaurants_with_issues"), f"{len(by_rest)}")
                    with c2: st.metric(t("total_problem"), f"{by_rest['PROBLEM_COUNT'].sum():,}")

                    # Топ столовых по проблемам
                    top_rest = by_rest.head(15)
                    fig = px.bar(top_rest, x="PROBLEM_COUNT", y="REST_NAME", orientation="h",
                        title="Restaurants with Problem Transactions" if _get_lang()=="en" else "Рестораны с проблемными транзакциями",
                        color="PROBLEM_COUNT", color_continuous_scale="Reds",
                        text="PROBLEM_COUNT",
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_rest)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # Детализация: столовая → типы проблем
                    st.divider()
                    st.markdown("#### " + ("By Issue Type" if _get_lang()=="en" else "По типам проблем"))
                    pivot = problems.pivot_table(index="REST_NAME", columns="STATUS",
                        values="CNT", aggfunc="sum", fill_value=0).reset_index()
                    pivot["Всего"] = pivot.select_dtypes(include="number").sum(axis=1)
                    pivot = pivot.sort_values("Total" if _get_lang()=="en" else "Всего", ascending=False)
                    pivot = pivot.rename(columns={"REST_NAME": "Столовая"})
                    st.dataframe(pivot, use_container_width=True, hide_index=True)

                    # Суммы
                    if "TOTAL_SUM" in problems.columns and problems["TOTAL_SUM"].notna().sum() > 0:
                        st.divider()
                        by_rest_sum = by_rest[by_rest["PROBLEM_SUM"] > 0].head(15)
                        if not by_rest_sum.empty:
                            fig2 = px.bar(by_rest_sum, x="PROBLEM_SUM", y="REST_NAME", orientation="h",
                                title="Problem Transaction Amounts" if _get_lang()=="en" else "Суммы проблемных транзакций",
                                color="PROBLEM_SUM", color_continuous_scale="YlOrRd",
                                text=by_rest_sum["PROBLEM_SUM"].apply(lambda x: f"{x:,.0f}₽"),
                                labels=_labels())
                            fig2.update_traces(textposition="auto")
                            fig2.update_layout(height=max(400, len(by_rest_sum)*30),
                                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                            fix_bar_hover(fig2)

                            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                else:
                    st.success("Нет проблемных транзакций по столовым!")

                # Полная разбивка (включая успешные)
                with st.expander("Все статусы по столовым"):
                    all_pivot = card_by_rest.pivot_table(index="REST_NAME", columns="STATUS",
                        values="CNT", aggfunc="sum", fill_value=0).reset_index()
                    all_pivot = all_pivot.rename(columns={"REST_NAME": "Столовая"})
                    st.dataframe(all_pivot, use_container_width=True, hide_index=True)
            else:
                st.info(t("no_restaurant_data"))
        else:
            st.info(t("no_data"))

    # ========== ФИСКАЛИЗАЦИЯ ==========
    with sub_fiscal:
        st.markdown("""
        **Что это:** Статус пробития чеков на ККТ. Показывает фискализированные, нефискализированные, удалённые чеки и ошибки.
        """)
        st.divider()

        fiscal_sum = load_fiscal_summary(date_from, date_to)
        if not fiscal_sum.empty:
            fs = fiscal_sum.iloc[0]
            total = int(fs["TOTAL"]) if fs["TOTAL"] else 0
            active = int(fs["ACTIVE"]) if fs["ACTIVE"] else 0
            deleted = int(fs["DELETED"]) if fs["DELETED"] else 0
            active_sum = int(fs["ACTIVE_SUM"]) if fs["ACTIVE_SUM"] else 0
            corrections = int(fs["CORRECTIONS"]) if fs["CORRECTIONS"] else 0
            bill_errors = int(fs["BILL_ERRORS"]) if fs["BILL_ERRORS"] else 0
            not_fiscal = int(fs["NOT_FISCAL"]) if fs["NOT_FISCAL"] else 0
            fiscal_ok = int(fs["FISCAL_OK"]) if fs["FISCAL_OK"] else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.metric(t("total_checks_count"), f"{total:,}")
            with c2: st.metric("✅ Фискализировано", f"{fiscal_ok:,}")
            with c3: st.metric(t("not_fiscal"), f"{not_fiscal:,}")
            with c4: st.metric("🗑 Удалённых", f"{deleted:,}")
            with c5: st.metric(t("amount_label"), f"{active_sum:,} ₽")

            # Предупреждения
            if not_fiscal > 0:
                st.error(f"{not_fiscal} checks NOT fiscalized!" if _get_lang()=="en" else f"{not_fiscal} чеков НЕ фискализированы!")
            if bill_errors > 0:
                st.warning(f"{bill_errors} checks with print errors (BILLERROR)")
            if corrections > 0:
                st.info(f"{corrections} correction checks" if _get_lang()=="en" else f"{corrections} чеков коррекции")
            if not_fiscal == 0 and bill_errors == 0 and deleted == 0:
                st.success(f"✅ Все {fiscal_ok:,} чеков успешно фискализированы")

            # По столовым
            st.divider()
            st.markdown("### " + ("By Location & Register" if _get_lang()=="en" else "По столовым и кассам"))
            fiscal_detail = load_fiscal_checks(date_from, date_to)
            if not fiscal_detail.empty:
                # Сводка по столовым
                by_rest = fiscal_detail.groupby("REST_NAME").agg(
                    TOTAL=("TOTAL_CHECKS", "sum"),
                    ACTIVE=("ACTIVE_CHECKS", "sum"),
                    DELETED=("DELETED_CHECKS", "sum"),
                    ERRORS=("BILL_ERRORS", "sum"),
                    NOT_FISCAL=("NOT_FISCAL", "sum"),
                    CORRECTIONS=("CORRECTIONS", "sum"),
                    SUM=("ACTIVE_SUM", "sum"),
                ).reset_index().sort_values("TOTAL", ascending=False)

                # Проблемные столовые
                problems = by_rest[(by_rest["NOT_FISCAL"] > 0) | (by_rest["ERRORS"] > 0) | (by_rest["DELETED"] > 0)]
                if not problems.empty:
                    st.markdown("#### " + ("Restaurants with Issues" if _get_lang()=="en" else "Рестораны с проблемами"))
                    fig = px.bar(problems, x="NOT_FISCAL", y="REST_NAME", orientation="h",
                        title="Non-fiscalized Checks" if _get_lang()=="en" else "Нефискализированные чеки",
                        color="NOT_FISCAL", color_continuous_scale="Reds",
                        text="NOT_FISCAL",
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(300, len(problems)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Полная таблица
                disp = by_rest.rename(columns={
                    "REST_NAME": "Столовая", "TOTAL": "Всего", "ACTIVE": "Активных",
                    "DELETED": "Удалённых", "ERRORS": "Ошибок", "NOT_FISCAL": "Не фискал.",
                    "CORRECTIONS": "Коррекций", "SUM": "Сумма ₽"})
                st.dataframe(disp, use_container_width=True, hide_index=True)

                # Детализация по кассам
                with st.expander("Детализация по кассам"):
                    disp_full = fiscal_detail.rename(columns={
                        "REST_NAME": "Столовая", "CASH_NAME": "Касса",
                        "TOTAL_CHECKS": "Всего", "ACTIVE_CHECKS": "Активных",
                        "DELETED_CHECKS": "Удалённых", "ACTIVE_SUM": "Сумма ₽",
                        "CORRECTIONS": "Коррекций", "BILL_ERRORS": "Ошибок",
                        "NOT_FISCAL": "Не фискал."})
                    st.dataframe(disp_full, use_container_width=True, hide_index=True)
        else:
            st.info(t("no_data_period"))

# --- ОТКАЗЫ И ОТМЕНЫ ---
if page == "Удаление":
    page_header("Удаление")

    sub_voids, sub_checks, sub_ops, sub_payments = st.tabs([
        "Удаления блюд", "Отмены чеков", "Операции отмен", "Оплаты по типам"
    ])

    # ========== ОТКАЗЫ БЛЮД ==========
    with sub_voids:
        voids = load_voids(date_from, date_to)
        if not voids.empty:
            vs = float(voids["PRLISTSUM"].sum())
            ords = load_orders(date_from, date_to)
            rv = float(ords["TOPAYSUM"].sum()) if not ords.empty else 0

            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric(t("total_voids"), f"{len(voids)}")
            with c2: st.metric("💸 Сумма", f"{vs:,.0f} ₽")
            with c3: st.metric("📉 % " + ("of revenue" if _get_lang()=="en" else "от выручки"), f"{vs/rv*100 if rv else 0:.1f}%")
            with c4: st.metric(t("avg_void"), f"{vs/len(voids):,.0f} ₽" if len(voids) else "—")
            st.caption("Voided items — removed before payment. No revenue impact but important for control." if _get_lang()=="en" else "Удаление блюд — позиции, удалённые до оплаты. Важны для контроля.")
            st.divider()

            view_mode = st.selectbox("View:" if _get_lang()=="en" else "Разрез:", ["By reason","By cashier","By location","By dish","Table"] if _get_lang()=="en" else ["По причинам","По кассирам","По точкам","По блюдам","Таблица"], key="void_view")

            if view_mode == "По причинам" and "VOID_REASON" in voids.columns:
                vr = voids.groupby("VOID_REASON").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                vr["VOID_REASON"] = vr["VOID_REASON"].replace("","Без причины")
                if not vr.empty:
                    cl,cr = st.columns(2)
                    with cl:
                        fig = px.bar(vr.head(10),x="SUM",y="VOID_REASON",orientation="h",title="💸 По сумме",color="SUM",color_continuous_scale="Reds", labels=_labels())
                        fig.update_layout(height=400,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
                    with cr:
                        fig = px.pie(vr.head(8),values="COUNT",names="VOID_REASON",title="By Count" if _get_lang()=="en" else "По количеству",hole=0.4, labels=_labels())
                        fig.update_layout(height=400,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})

            elif view_mode == "По кассирам":
                if "CREATOR_NAME" in voids.columns:
                    by_cr = voids.groupby("CREATOR_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                    by_cr = by_cr[by_cr["CREATOR_NAME"].notna()]
                    if not by_cr.empty:
                        st.markdown("#### " + ("Who creates voids" if _get_lang()=="en" else "Кто создаёт удаления"))
                        fig = px.bar(by_cr.head(15),x="SUM",y="CREATOR_NAME",orientation="h",title="💸 Сумма удалений по кассирам",color="COUNT",color_continuous_scale="Reds", labels=_labels())
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
                if "AUTHOR_NAME" in voids.columns:
                    by_au = voids.groupby("AUTHOR_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                    by_au = by_au[by_au["AUTHOR_NAME"].notna()]
                    if not by_au.empty:
                        st.markdown("#### " + ("Who approves voids (manager)" if _get_lang()=="en" else "Кто подтверждает удаления"))
                        fig = px.bar(by_au.head(15),x="SUM",y="AUTHOR_NAME",orientation="h",title="✅ По менеджерам",color="COUNT",color_continuous_scale="Oranges", labels=_labels())
                        fig.update_layout(height=400,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})

            elif view_mode == "По точкам" and "REST_NAME" in voids.columns:
                by_rest = voids.groupby("REST_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                by_rest = by_rest[by_rest["REST_NAME"].notna()]
                if not by_rest.empty:
                    cl,cr = st.columns(2)
                    with cl:
                        fig = px.bar(by_rest.head(15),x="SUM",y="REST_NAME",orientation="h",title="By Amount" if _get_lang()=="en" else "По сумме",color="SUM",color_continuous_scale="Reds", labels=_labels())
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})
                    with cr:
                        fig = px.bar(by_rest.head(15),x="COUNT",y="REST_NAME",orientation="h",title="By Count" if _get_lang()=="en" else "По количеству",color="COUNT",color_continuous_scale="Oranges", labels=_labels())
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})

            elif view_mode == "По блюдам" and "DISH_NAME" in voids.columns:
                by_dish = voids.groupby("DISH_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                by_dish = by_dish[by_dish["DISH_NAME"].notna()]
                if not by_dish.empty:
                    fig = px.bar(by_dish.head(15),x="SUM",y="DISH_NAME",orientation="h",title="Most Voided Dishes" if _get_lang()=="en" else "Какие блюда удаляют из чека",color="COUNT",color_continuous_scale="Reds", labels=_labels())
                    fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"scrollZoom":False})

            elif view_mode == "Таблица":
                dc = {"DATETIME":"Дата","REST_NAME":"Точка","DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","VOID_REASON":"Причина",
                      "QUANTITY":"Кол-во","PRLISTSUM":"Сумма","CREATOR_NAME":"Кассир","AUTHOR_NAME":"Менеджер"}
                av = [c for c in dc if c in voids.columns]
                vd = voids[av].rename(columns={k:v for k,v in dc.items() if k in av})
                st.dataframe(vd,use_container_width=True,hide_index=True,height=500)
                st.download_button("CSV",vd.to_csv(index=False).encode("utf-8"),"voids.csv","text/csv",use_container_width=True)
        else:
            st.info(t("no_data_period"))

    # ========== ОТМЕНЫ ЧЕКОВ ==========
    with sub_checks:
        st.markdown("""
        **Что это:** Чек, который был закрыт (оплачен), а потом **отменён задним числом**.
        Это серьёзная операция, которая может указывать на:
        - 🔴 **Злоупотребления** — кассир пробил заказ, взял деньги и отменил чек
        - 🟡 **Ошибки** — пробили не тому гостю, двойная оплата
        - 🟢 **Возврат** — гость вернул блюдо после оплаты
        """)
        st.divider()

        st.markdown("""
        **Описание полей:**
        | Поле | Что значит |
        |---|---|
        | **Дата отмены** | Когда чек был отменён (после того как уже был оплачен) |
        | **Дата закрытия** | Когда чек был изначально оплачен |
        | **Сумма** | Сколько денег было в чеке на момент отмены |
        | **Кассир** | Кто изначально закрыл (оплатил) чек |
        | **Кто отменил** | Кто инициировал отмену чека |
        | **Менеджер отмены** | Кто подтвердил отмену (обычно нужна авторизация менеджера) |
        | **Транзакции отменены** | Были ли отменены банковские/карточные транзакции при отмене |
        | **Причина** | Текст причины отмены (если кассир указал) |
        """)
        st.divider()

        del_checks = load_deleted_checks(date_from, date_to)
        if not del_checks.empty:
            total = len(del_checks)
            total_sum = float(del_checks["TOPAYSUM"].sum()) if "TOPAYSUM" in del_checks.columns else 0

            c1,c2,c3 = st.columns(3)
            with c1: st.metric(t("canceled_checks"), f"{total}")
            with c2: st.metric("💸 Сумма", f"{total_sum:,.0f} ₽")
            with c3:
                ords = load_orders(date_from, date_to)
                rv = float(ords["TOPAYSUM"].sum()) if not ords.empty else 0
                st.metric("📉 % " + ("of revenue" if _get_lang()=="en" else "от выручки"), f"{total_sum/rv*100 if rv else 0:.2f}%")

            # По кто отменил
            if "DELETE_PERSON_NAME" in del_checks.columns:
                by_person = del_checks.groupby("DELETE_PERSON_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_person = by_person[by_person["DELETE_PERSON_NAME"].notna()]
                if not by_person.empty:
                    fig = px.bar(by_person, x="SUM", y="DELETE_PERSON_NAME", orientation="h",
                        title="Who Initiates Cancellations" if _get_lang()=="en" else "Кто инициирует отмены", color="COUNT", color_continuous_scale="Reds", labels=_labels())
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # По менеджеру
            if "DELETE_MANAGER_NAME" in del_checks.columns:
                by_mgr = del_checks.groupby("DELETE_MANAGER_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_mgr = by_mgr[by_mgr["DELETE_MANAGER_NAME"].notna()]
                if not by_mgr.empty:
                    fig = px.bar(by_mgr, x="SUM", y="DELETE_MANAGER_NAME", orientation="h",
                        title="✅ Кто подтверждает отмены (менеджер)", color="COUNT", color_continuous_scale="Oranges", labels=_labels())
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # По точкам
            if "REST_NAME" in del_checks.columns:
                by_rest = del_checks.groupby("REST_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_rest = by_rest[by_rest["REST_NAME"].notna()]
                if not by_rest.empty:
                    fig = px.bar(by_rest, x="SUM", y="REST_NAME", orientation="h",
                        title="Cancellations by Location" if _get_lang()=="en" else "Отмены чеков по точкам", color="COUNT", color_continuous_scale="Reds", labels=_labels())
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # Таблица
            st.divider()
            dc = {
                "DELETEDATETIME":"Дата отмены",
                "CLOSEDATETIME":"Дата оплаты",
                "TOPAYSUM":"Сумма чека",
                "CREATOR_NAME":"Кассир",
                "DELETE_PERSON_NAME":"Кто отменил",
                "DELETE_MANAGER_NAME":"Менеджер отмены",
                "UNDOTRANSACTIONS":"Транзакции отменены",
                "OPENVOIDNAME":"Причина",
                "REST_NAME":"Точка",
                "GUESTCNT":"Гостей",
            }
            av = [c for c in dc if c in del_checks.columns]
            disp = del_checks[av].rename(columns={k:v for k,v in dc.items() if k in av})
            if "Транзакции отменены" in disp.columns:
                disp["Транзакции отменены"] = disp["Транзакции отменены"].map({1:"✅ Да", 0:"Нет"}).fillna("—")
            st.dataframe(disp, use_container_width=True, hide_index=True)
            st.download_button("CSV", disp.to_csv(index=False).encode("utf-8"),
                "deleted_checks.csv", "text/csv", use_container_width=True)
        else:
            st.success("✅ Отменённых чеков за период нет — это хорошо!")
            st.caption("Cancelled checks should be investigated: who, why, and is it systematic." if _get_lang()=="en" else "Отменённые чеки — повод проверить: кто, зачем, систематически ли.")

    # ========== ОПЕРАЦИИ ОТМЕН ==========
    with sub_ops:
        ops = load_cancel_operations(date_from, date_to)
        if not ops.empty:
            c1,c2 = st.columns(2)
            with c1: st.metric(t("cancel_ops"), f"{len(ops)}")
            total_diff = float(ops["DIFF"].sum()) if "DIFF" in ops.columns else 0
            with c2: st.metric("💸 Сумма разниц", f"{total_diff:,.0f} ₽")
            st.divider()

            # По типам операций
            if "OPERATION" in ops.columns:
                by_op = ops.groupby("OPERATION").agg(COUNT=("OPERATION","count")).reset_index().sort_values("COUNT",ascending=False)
                if not by_op.empty:
                    fig = px.bar(by_op, x="COUNT", y="OPERATION", orientation="h",
                        title="Cancellation Types" if _get_lang()=="en" else "Типы отмен", color="COUNT", color_continuous_scale="Oranges", labels=_labels())
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            cl,cr = st.columns(2)
            # По кассирам
            with cl:
                if "OPERATOR_NAME" in ops.columns:
                    by_emp = ops.groupby("OPERATOR_NAME").agg(COUNT=("OPERATOR_NAME","count")).reset_index().sort_values("COUNT",ascending=False)
                    by_emp = by_emp[by_emp["OPERATOR_NAME"].notna()]
                    if not by_emp.empty:
                        fig = px.bar(by_emp.head(15), x="COUNT", y="OPERATOR_NAME", orientation="h",
                            title="Cancellations by Cashier" if _get_lang()=="en" else "Отмены по кассирам", color="COUNT", color_continuous_scale="Reds", labels=_labels())
                        fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # По точкам
            with cr:
                if "REST_NAME" in ops.columns:
                    by_rest = ops.groupby("REST_NAME").agg(COUNT=("REST_NAME","count")).reset_index().sort_values("COUNT",ascending=False)
                    by_rest = by_rest[by_rest["REST_NAME"].notna()]
                    if not by_rest.empty:
                        fig = px.bar(by_rest.head(15), x="COUNT", y="REST_NAME", orientation="h",
                            title="Cancellations by Location" if _get_lang()=="en" else "Отмены по точкам", color="COUNT", color_continuous_scale="Oranges", labels=_labels())
                        fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # Таблица
            st.divider()
            disp_ops = ops.copy()
            cols_rename = {"DATETIME":"Дата/время","OPERATION":"Операция","OPERATOR_NAME":"Кассир",
                "MANAGER_NAME":"Менеджер","REST_NAME":"Точка","ORDERSUMBEFORE":"Сумма до","ORDERSUMAFTER":"Сумма после","DIFF":"Разница"}
            avail = [c for c in cols_rename if c in disp_ops.columns]
            st.dataframe(disp_ops[avail].rename(columns={k:v for k,v in cols_rename.items() if k in avail}),
                use_container_width=True, hide_index=True, height=400)
        else:
            st.success("✅ Операций отмен нет!")

    # ========== ОПЛАТЫ ПО ТИПАМ ==========
    with sub_payments:
        pay_br = load_payments_by_type(date_from, date_to)
        if not pay_br.empty:
            pay_map = {0:"💵 Наличные",1:"Банк. карта",2:"🏨 Карта отеля",3:"🎫 Плат. карта",
                       4:"📤 Искл. из доходов",5:"🏦 Безнал",6:"🎟️ Купон",7:"Выкуп"}
            pay_br["PAY_TYPE"] = pay_br["PAYLINETYPE"].map(pay_map).fillna("Другое")

            # Общие метрики по типам
            by_type = pay_br.groupby("PAY_TYPE").agg(
                COUNT=("PAY_COUNT","sum"), SUM=("TOTAL_SUM","sum")).reset_index().sort_values("SUM",ascending=False)
            total_pay = float(by_type["SUM"].sum())

            c1,c2,c3 = st.columns(3)
            with c1: st.metric(t("total_payments"), f'{int(by_type["COUNT"].sum()):,}')
            with c2: st.metric(t("total_amount"), f"{total_pay:,.0f} ₽")
            with c3:
                card_sum = float(by_type[by_type["PAY_TYPE"]=="Банк. карта"]["SUM"].sum()) if "Банк. карта" in by_type["PAY_TYPE"].values else 0
                st.metric(t("card_share"), f"{card_sum/total_pay*100:.1f}%" if total_pay else "—")
            st.divider()

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(by_type, values="SUM", names="PAY_TYPE", title="Payment Structure" if _get_lang()=="en" else "Структура оплат", hole=0.4, labels=_labels())
                fig.update_layout(height=400, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with cr:
                fig = px.bar(by_type, x="SUM", y="PAY_TYPE", orientation="h",
                    title="Amounts by Payment Type" if _get_lang()=="en" else "Суммы по типам оплат", color="SUM", color_continuous_scale="Tealgrn", labels=_labels())
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            # По кассирам и типам
            st.divider()
            st.markdown("#### " + ("Cashiers by Payment Type" if _get_lang()=="en" else "Кассиры по типам оплат"))
            selected_type = st.selectbox("Тип оплаты:", ["Все"] + by_type["PAY_TYPE"].tolist())
            if selected_type == "Все":
                cashier_data = pay_br.groupby("CASHIER_NAME").agg(
                    COUNT=("PAY_COUNT","sum"), SUM=("TOTAL_SUM","sum")).reset_index().sort_values("SUM",ascending=False)
            else:
                filtered = pay_br[pay_br["PAY_TYPE"]==selected_type]
                cashier_data = filtered.groupby("CASHIER_NAME").agg(
                    COUNT=("PAY_COUNT","sum"), SUM=("TOTAL_SUM","sum")).reset_index().sort_values("SUM",ascending=False)
            cashier_data = cashier_data[cashier_data["CASHIER_NAME"].notna()]

            if not cashier_data.empty:
                fig = px.bar(cashier_data.head(20), x="SUM", y="CASHIER_NAME", orientation="h",
                    title=f"Кассиры: {selected_type}", color="COUNT", color_continuous_scale="Viridis",
                    hover_data={"COUNT":True,"SUM":":.0f"}, labels=_labels())
                fig.update_layout(height=max(400,min(len(cashier_data),20)*30), yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                st.dataframe(cashier_data.rename(columns={"CASHIER_NAME":"Кассир","COUNT":"Операций","SUM":"Сумма"}),
                    use_container_width=True, hide_index=True)
        else:
            st.info(t("no_payment_data"))

# --- ЗАКАЗЫ ---
if page == "Заказы":
    page_header("Заказы")
    ot=load_orders(date_from,date_to)
    if not ot.empty:
        cm={"OPENTIME":"Открыт","ENDSERVICE":"Закрыт","TABLENAME":"Стол","GUESTSCOUNT":"Гости","TOPAYSUM":"К оплате","PAIDSUM":"Оплачено","DISCOUNTSUM":"Скидка","TOTALDISHPIECES":"Блюд"}
        av=[c for c in cm if c in ot.columns]
        dp=ot[av].rename(columns={k:v for k,v in cm.items() if k in av})
        st.markdown(f"### Заказы ({len(dp)})")
        st.dataframe(dp,use_container_width=True,hide_index=True,height=500)
        st.download_button("CSV",dp.to_csv(index=False).encode("utf-8"),"orders.csv","text/csv",use_container_width=True)

# ============================================================
# STOREHOUSE СТРАНИЦЫ (REST API)
# ============================================================

# --- ОСТАТКИ ---
if page == "Склад":
    page_header("Склад")

    # === DEMO MODE ===
    if IS_DEMO and _DEMO_DB:
        st.caption(f"Демо-данные · {date_from} — {date_to}")
        tab_goods, tab_invoices, tab_stock = st.tabs(["Товары", "Накладные", "Остатки"])

        with tab_goods:
            goods = pd.read_sql("SELECT g.RID, g.NAME, g.UNIT, g.PRICE, gg.NAME as GROUP_NAME FROM GOODS g LEFT JOIN GOOD_GROUPS gg ON g.GROUPRID=gg.RID ORDER BY g.NAME", _DEMO_DB)
            st.metric(t("products"), f"{len(goods)}")
            st.dataframe(goods.rename(columns={"RID":"ID","NAME":"Товар","UNIT":"Ед.","PRICE":"Цена закупки","GROUP_NAME":"Группа"}),
                use_container_width=True, hide_index=True)

        with tab_invoices:
            inv = pd.read_sql("""SELECT i.RID, i.INVOICEDATE as ДАТА, i.INVOICESTRING as НОМЕР,
                s.NAME as ПОСТАВЩИК, d.NAME as СКЛАД, i.PAYSUMNOTAX as СУММА
                FROM STAT_SH4_SHIFTS_INVOICES i
                LEFT JOIN SUPPLIERS s ON i.RIDSHIPPER=s.RID
                LEFT JOIN DEPARTS d ON i.RIDDESTINATION=d.RID
                WHERE i.INVOICEDATE >= ? AND i.INVOICEDATE <= ?
                ORDER BY i.INVOICEDATE DESC""", _DEMO_DB, params=(str(date_from), str(date_to)))
            c1, c2 = st.columns(2)
            with c1: st.metric(t("invoices_count"), f"{len(inv)}")
            with c2: st.metric(t("amount_label"), f"{inv['СУММА'].sum():,.0f} ₽" if not inv.empty else "0")
            if not inv.empty:
                fig = px.bar(inv.groupby("ПОСТАВЩИК")["СУММА"].sum().reset_index().sort_values("СУММА", ascending=False),
                    x="СУММА", y="ПОСТАВЩИК", orientation="h", labels=_labels())
                fig.update_layout(height=300, yaxis=dict(autorange="reversed"), **CHART_THEME)
                fix_bar_hover(fig)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            st.dataframe(inv, use_container_width=True, hide_index=True)

        with tab_stock:
            stock = pd.read_sql("""SELECT g.NAME as ТОВАР, gg.NAME as ГРУППА,
                SUM(id.QUANTITY) as ПРИХОД, SUM(id.STAMPSUMNOTAX) as СУММА
                FROM STAT_SH4_SHIFTS_INVOICES_DETAIL id
                JOIN STAT_SH4_SHIFTS_INVOICES i ON id.RID=i.RID
                JOIN GOODS g ON id.GOODSRID=g.RID
                LEFT JOIN GOOD_GROUPS gg ON g.GROUPRID=gg.RID
                WHERE i.INVOICEDATE >= ? AND i.INVOICEDATE <= ?
                GROUP BY g.NAME, gg.NAME ORDER BY СУММА DESC""", _DEMO_DB, params=(str(date_from), str(date_to)))
            if not stock.empty:
                fig = px.bar(stock.head(15), x="СУММА", y="ТОВАР", orientation="h",
                    color="СУММА", color_continuous_scale="Tealgrn")
                fig.update_layout(height=max(300, len(stock.head(15))*28), yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            st.dataframe(stock, use_container_width=True, hide_index=True)

        st.stop()

    st.caption(f"StoreHouse API: {SH_API['url']} · Период: {date_from} — {date_to}")

    d1_str = str(date_from)
    d2_str = str(date_to)

    sub_overview, sub_stock, sub_incoming, sub_transfers, sub_goods_tab, sub_structure, sub_debug = st.tabs([
        "Обзор", "Остатки", "Приход", "Перемещения",
        "Товары", "🏗️ Структура", "🔧 Отладка"])

    # ==================== ОБЗОР ====================
    with sub_overview:
        st.markdown("### " + ("Warehouse System Overview" if _get_lang()=="en" else "Обзор складской системы"))

        # Загружаем базовые справочники
        divs_o, _ = sh_load_divisions()
        deps_o, _ = sh_load_departs()
        cats_o, _ = sh_load_goods_categories()
        goods_o, _ = sh_load_goods()

        n_divs = len(divs_o) if not divs_o.empty else 0
        n_deps = len(deps_o) if not deps_o.empty else 0
        n_cats = len(cats_o) if not cats_o.empty else 0
        n_goods = len(goods_o) if not goods_o.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric(t("divisions"), f"{n_divs}")
        with c2: st.metric(t("warehouses"), f"{n_deps}")
        with c3: st.metric(t("products"), f"{n_goods:,}")
        with c4: st.metric(t("categories_count"), f"{n_cats}")

        st.divider()

        # Пробуем загрузить сводку по приходным за период
        st.markdown(f"#### Приходные накладные за {d1_str} — {d2_str}")
        list_df, list_err = sh_load_gdoc0_ext_list(d1_str, d2_str)
        if list_err:
            st.warning(f"GDoc0ExtList: {list_err}")
        elif not list_df.empty:
            st.success(f"✅ {len(list_df)} приходных накладных за период")
        else:
            st.info(t("no_data_period"))

        # Пробуем перемещения
        st.markdown(f"#### Перемещения за {d1_str} — {d2_str}")
        tr_list, tr_err = sh_load_gdoc1_5_list(d1_str, d2_str, doc_type=4)
        if tr_err:
            st.caption("⚠️ Transfers unavailable — no SH API access" if _get_lang()=="en" else "⚠️ Перемещения недоступны — нет прав в SH API")
        elif not tr_list.empty:
            st.success(f"✅ {len(tr_list)} перемещений за период")
        else:
            st.info(t("no_data_period"))

        st.divider()

        # Treemap подразделения → склады
        if not deps_o.empty:
            dep_vent_col = None
            dep_name_col = None
            for c in deps_o.columns:
                cl = c.lower()
                if "venture" in cl and "name" in cl or c == "103\\3":
                    dep_vent_col = c
                if c == "3" or (cl == "name" and dep_name_col is None) or c == "Name":
                    dep_name_col = c
            if not dep_vent_col:
                for c in deps_o.columns:
                    if "подразделение" in c.lower() or "venture" in c.lower():
                        dep_vent_col = c
                        break
            if not dep_name_col:
                for c in deps_o.columns:
                    if "название" in c.lower() or "name" in c.lower():
                        dep_name_col = c
                        break
            if dep_vent_col and dep_name_col and dep_vent_col in deps_o.columns:
                tree_data = deps_o[[dep_vent_col, dep_name_col]].copy()
                tree_data.columns = ["Подразделение", "Склад"]
                tree_data = tree_data.dropna(subset=["Подразделение", "Склад"])
                tree_data["count"] = 1
                if not tree_data.empty:
                    fig = px.treemap(tree_data, path=["Подразделение", "Склад"], values="count",
                        title="Map: Divisions → Warehouses" if _get_lang()=="en" else "Карта: подразделения → склады",
                        color_discrete_sequence=px.colors.qualitative.Set3, labels=_labels())
                    fig.update_layout(height=500, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # Статус API
        st.divider()
        st.markdown("#### " + ("Data Status" if _get_lang()=="en" else "Статус данных"))
        status_rows = [
            {"Источник": "Приходные (GDoc0ExtList)", "Статус": "✅" if not list_err else "⚠️", "Деталь": f"{len(list_df)} документов" if not list_err and not list_df.empty else str(list_err or "нет данных")},
            {"Источник": "Перемещения (GDoc1_5LstDocs type=4)", "Статус": "✅" if not tr_err else "🔒", "Деталь": f"{len(tr_list)} документов" if not tr_err and not tr_list.empty else "Нет прав в SH API"},
            {"Источник": "Товары (GoodsTree)", "Статус": "✅" if n_goods > 0 else "❌", "Деталь": f"{n_goods:,} товаров"},
            {"Источник": "Склады (Departs)", "Статус": "✅" if n_deps > 0 else "❌", "Деталь": f"{n_deps} складов"},
            {"Источник": "Подразделения (Divisions)", "Статус": "✅" if n_divs > 0 else "❌", "Деталь": f"{n_divs} подразделений"},
            {"Источник": "Остатки (GRemns)", "Статус": "✅", "Деталь": "Работает! dept=0 общие, dept=1 по складам"},
            {"Источник": "📑 Расходные (type=1)", "Статус": "🔒", "Деталь": "Ошибка 5 — нет прав"},
            {"Источник": "Инвентаризации (type=5)", "Статус": "🔒", "Деталь": "Ошибка 5 — нет прав"},
        ]
        st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    # ==================== ОСТАТКИ ====================
    with sub_stock:
        st.markdown(f"### Остатки товаров на {d2_str}")
        st.caption("GRemns API → current stock by warehouse")

        stock_df, stock_err = sh_load_stock(d2_str, by_depart=True)

        if stock_err:
            st.warning(f"{stock_err}")
        elif not stock_df.empty:
            n_items = len(stock_df)
            total_amount = stock_df["AMOUNT"].sum() if "AMOUNT" in stock_df.columns else 0
            total_qty = stock_df["QTY"].sum() if "QTY" in stock_df.columns else 0
            n_products = stock_df["PRODUCT_NAME"].nunique() if "PRODUCT_NAME" in stock_df.columns else 0
            n_departs = stock_df["DEPART"].nunique() if "DEPART" in stock_df.columns else 0

            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric(t("warehouses"), f"{n_departs}")
            with c2: st.metric(t("products"), f"{n_products:,}")
            with c3: st.metric(t("stock_value"), f"{total_amount:,.0f} ₽")
            with c4: st.metric(t("positions"), f"{n_items:,}")

            # --- Динамика остатков ---
            _dyn_d1 = date_from
            _dyn_d2 = date_to
            # Минимум 7 дней для графика
            if (date_to - date_from).days < 7:
                _dyn_d1 = date_to - timedelta(6)
            _dyn_key = f"_stock_dyn_{_dyn_d1}_{_dyn_d2}"
            if _dyn_key not in st.session_state:
                with st.spinner(f"Загружаю динамику остатков ({_dyn_d1} — {_dyn_d2})..."):
                    st.session_state[_dyn_key] = sh_load_stock_dynamics(str(_dyn_d1), str(_dyn_d2))
            _dyn_df = st.session_state[_dyn_key]

            if not _dyn_df.empty and "DATE" in _dyn_df.columns and "AMOUNT" in _dyn_df.columns:
                _all_deps = sorted(_dyn_df["DEPART"].unique().tolist()) if "DEPART" in _dyn_df.columns else []
                _sel_deps = st.multiselect("Фильтр по складам:", _all_deps, default=[], key="stock_dyn_filter",
                                            placeholder=_all_wh)
                _filtered = _dyn_df[_dyn_df["DEPART"].isin(_sel_deps)] if _sel_deps else _dyn_df

                # Агрегируем по дате (и по складу если фильтр)
                if _sel_deps and len(_sel_deps) <= 5:
                    # Раздельные линии по складам
                    _plot = _filtered.copy()
                    _plot["DATE"] = pd.to_datetime(_plot["DATE"])
                    fig_dyn = px.line(_plot, x="DATE", y="AMOUNT", color="DEPART",
                        labels=_labels(),
                        markers=True)
                else:
                    # Суммарная линия
                    _by_date = _filtered.groupby("DATE")["AMOUNT"].sum().reset_index()
                    _by_date["DATE"] = pd.to_datetime(_by_date["DATE"])
                    _by_date = _by_date.sort_values("DATE")
                    accent = "#00ff6a" if not IS_LIGHT else "#00b847"
                    fig_dyn = go.Figure()
                    fig_dyn.add_trace(go.Scatter(x=_by_date["DATE"], y=_by_date["AMOUNT"],
                        mode="lines+markers", fill="tozeroy",
                        line=dict(color=accent, width=2.5),
                        marker=dict(size=6, color=accent),
                        text=_by_date["AMOUNT"].apply(lambda x: f"{x:,.0f} ₽"),
                        hovertemplate="%{x|%d.%m} — %{text}<extra></extra>"))
                fig_dyn.update_layout(
                    height=320, yaxis_title="₽", xaxis_title="",
                    margin=dict(l=60, r=20, t=30, b=30),
                    **CHART_THEME)
                fix_bar_hover(fig_dyn)
                st.plotly_chart(fig_dyn, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                st.caption(f"Динамика за {_dyn_d1} — {_dyn_d2} ({(_dyn_d2 - _dyn_d1).days + 1} дн.)")

            st.divider()

            # Переключатель отчётов
            _all_wh = "All warehouses" if _get_lang()=="en" else "Все склады"
            stock_mode = st.radio("Report:" if _get_lang()=="en" else "Отчёт:", [
                "По складам",
                "По товарам",
                "Товары на складе"
            ], horizontal=True, key="stock_mode")

            st.divider()

            # ======= ПО СКЛАДАМ =======
            if stock_mode == "По складам" and "DEPART" in stock_df.columns:
                by_dep = stock_df.groupby("DEPART").agg(
                    PRODUCTS=("PRODUCT_NAME", "nunique"),
                    TOTAL_QTY=("QTY", "sum"),
                    TOTAL_AMOUNT=("AMOUNT", "sum"),
                ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)

                fig = px.bar(by_dep, x="TOTAL_AMOUNT", y="DEPART", orientation="h",
                    title="Stock Value by Warehouse" if _get_lang()=="en" else "Стоимость остатков по складам",
                    color="TOTAL_AMOUNT", color_continuous_scale="Viridis",
                    text=by_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels=_labels())
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, len(by_dep)*32),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                cl, cr = st.columns(2)
                with cl:
                    fig2 = px.pie(by_dep, values="TOTAL_AMOUNT", names="DEPART",
                        title="Stock Share by Warehouse" if _get_lang()=="en" else "Доля остатков по складам", hole=0.4)
                    fig2.update_layout(height=400, **CHART_THEME)
                    fix_bar_hover(fig2)

                    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                with cr:
                    fig3 = px.bar(by_dep, x="PRODUCTS", y="DEPART", orientation="h",
                        title="Products per Warehouse" if _get_lang()=="en" else "Кол-во товаров на складе",
                        color="PRODUCTS", color_continuous_scale="Tealgrn",
                        text="PRODUCTS",
                        labels=_labels())
                    fig3.update_traces(textposition="auto")
                    fig3.update_layout(height=max(400, len(by_dep)*32),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig3)

                    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar":False,"scrollZoom":False})

                # Таблица
                st.divider()
                disp = by_dep.copy()
                disp.columns = ["Склад", "Товаров", "Кол-во", "Сумма ₽"]
                disp["Ø на товар ₽"] = (disp["Сумма ₽"] / disp["Товаров"]).round(0)
                st.dataframe(disp, use_container_width=True, hide_index=True)

            # ======= ПО ТОВАРАМ =======
            elif stock_mode == "По товарам" and "PRODUCT_NAME" in stock_df.columns:
                by_prod = stock_df.groupby("PRODUCT_NAME").agg(
                    TOTAL_QTY=("QTY", "sum"),
                    TOTAL_AMOUNT=("AMOUNT", "sum"),
                    STORES=("DEPART", "nunique") if "DEPART" in stock_df.columns else ("QTY", "count"),
                ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)
                if "UNIT" in stock_df.columns:
                    units = stock_df.groupby("PRODUCT_NAME")["UNIT"].first()
                    by_prod = by_prod.merge(units.rename("UNIT"), on="PRODUCT_NAME", how="left")

                top30 = by_prod.head(30)
                fig = px.bar(top30, x="TOTAL_AMOUNT", y="PRODUCT_NAME", orientation="h",
                    title="Top-30 by Stock Value" if _get_lang()=="en" else "Топ-30 по стоимости остатков",
                    color="TOTAL_AMOUNT", color_continuous_scale="YlOrRd",
                    text=top30["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels=_labels())
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(500, len(top30)*25),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                st.divider()
                show_cols_s = [c for c in ["PRODUCT_NAME","TOTAL_QTY","TOTAL_AMOUNT","STORES","UNIT"] if c in by_prod.columns]
                disp_p = by_prod[show_cols_s].copy()
                col_names = {"PRODUCT_NAME":"Товар","TOTAL_QTY":"Кол-во","TOTAL_AMOUNT":"Сумма ₽","STORES":"Складов","UNIT":"Ед."}
                disp_p = disp_p.rename(columns=col_names)
                st.dataframe(disp_p, use_container_width=True, hide_index=True, height=500)
                st.download_button("CSV остатков", by_prod.to_csv(index=False).encode("utf-8"),
                    "stock.csv", "text/csv", use_container_width=True)

            # ======= ТОВАРЫ НА СКЛАДЕ =======
            elif stock_mode == "Товары на складе" and "DEPART" in stock_df.columns:
                departs_list = sorted(stock_df["DEPART"].unique().tolist())
                selected_store = st.selectbox("Select warehouse:" if _get_lang()=="en" else "Выберите склад:", departs_list, key="stock_store_sel")
                filtered_s = stock_df[stock_df["DEPART"] == selected_store].copy()

                if not filtered_s.empty:
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric(t("products"), f"{len(filtered_s):,}")
                    with c2: st.metric(t("amount_label"), f"{filtered_s['AMOUNT'].sum():,.0f} ₽")
                    with c3: st.metric(t("quantity"), f"{filtered_s['QTY'].sum():,.1f}")

                    filtered_s = filtered_s.sort_values("AMOUNT", ascending=False)
                    top_s = filtered_s.head(20)
                    fig = px.bar(top_s, x="AMOUNT", y="PRODUCT_NAME", orientation="h",
                        title=f"Остатки — {selected_store}",
                        color="AMOUNT", color_continuous_scale="Viridis",
                        text=top_s["AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_s)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    st.divider()
                    show_s = [c for c in ["PRODUCT_NAME","QTY","AMOUNT","UNIT"] if c in filtered_s.columns]
                    disp_fs = filtered_s[show_s].copy()
                    disp_fs = disp_fs.rename(columns={"PRODUCT_NAME":"Товар","QTY":"Кол-во","AMOUNT":"Сумма ₽","UNIT":"Ед."})
                    st.dataframe(disp_fs, use_container_width=True, hide_index=True, height=500)
                    st.download_button("CSV", filtered_s.to_csv(index=False).encode("utf-8"),
                        f"stock_{selected_store.replace(' ','_')}.csv", "text/csv", use_container_width=True)
                else:
                    st.info(f"No stock at {selected_store}" if _get_lang()=="en" else f"Нет остатков на складе «{selected_store}»")
        else:
            st.info("No stock data. GRemns unavailable." if _get_lang()=="en" else "Нет данных об остатках.")

    # ==================== ПРИХОД ====================
    with sub_incoming:
        st.markdown(f"### Приходные накладные ({d1_str} — {d2_str})")
        st.caption("One load → three reports: by product, by warehouse, by product per warehouse" if _get_lang()=="en" else "Одна загрузка → три отчёта")

        progress_ph = st.empty()
        if st.button("🚀 Load Invoices" if _get_lang()=="en" else "🚀 Загрузить приход", key="load_incoming_full", use_container_width=True):
            with st.spinner("Загружаю приходные накладные..."):
                items_df, docs_df, load_err = sh_load_incoming_full(
                    d1_str, d2_str, _progress_container=progress_ph, max_rids=200)
                st.session_state["_inc_items"] = items_df
                st.session_state["_inc_docs"] = docs_df
                st.session_state["_inc_err"] = load_err

        items_df = st.session_state.get("_inc_items", pd.DataFrame())
        docs_df = st.session_state.get("_inc_docs", pd.DataFrame())
        inc_err = st.session_state.get("_inc_err", None)

        if inc_err:
            st.warning(f"{inc_err}")
        elif not items_df.empty or not docs_df.empty:
            n_items = len(items_df)
            n_docs = len(docs_df)
            total_sum = docs_df["TOTAL_AMOUNT"].sum() if not docs_df.empty and "TOTAL_AMOUNT" in docs_df.columns else 0
            n_products = items_df["PRODUCT_NAME"].nunique() if not items_df.empty else 0
            n_departs = docs_df["DEPART"].nunique() if not docs_df.empty and "DEPART" in docs_df.columns else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.metric(t("invoices_count"), f"{n_docs}")
            with c2: st.metric(t("positions"), f"{n_items:,}")
            with c3: st.metric(t("products"), f"{n_products:,}")
            with c4: st.metric(t("warehouses"), f"{n_departs}")
            with c5: st.metric(t("amount_label"), f"{total_sum:,.0f} ₽")

            # Диагностика колонок GDoc0
            gdoc0_cols = st.session_state.get("_gdoc0_all_cols", [])
            if gdoc0_cols:
                with st.expander("🔧 Колонки GDoc0 (диагностика)"):
                    st.caption(f"{len(gdoc0_cols)} колонок: {gdoc0_cols}")

            # ---- Три отчёта внутри одного таба ----
            rpt_mode = st.radio("Report:" if _get_lang()=="en" else "Отчёт:", [
                "По товарам (закупочные цены)",
                "По складам (суммы прихода)",
                "Товары в каждом складе"
            ], horizontal=True, key="inc_report_mode")

            st.divider()

            # ======================== ПО ТОВАРАМ ========================
            if rpt_mode == "По товарам (закупочные цены)" and not items_df.empty:
                # Группируем: товар → средневзвешенная цена, суммы
                grouped = items_df.groupby("PRODUCT_NAME").agg(
                    TOTAL_QTY=("QTY", "sum"),
                    TOTAL_AMOUNT=("AMOUNT", "sum"),
                    DOC_COUNT=("DOC_RID", "nunique"),
                    ENTRY_COUNT=("QTY", "count"),
                ).reset_index()
                grouped["AVG_PURCHASE_PRICE"] = (grouped["TOTAL_AMOUNT"] / grouped["TOTAL_QTY"]).round(2)
                if "UNIT" in items_df.columns:
                    units = items_df.groupby("PRODUCT_NAME")["UNIT"].agg(
                        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "")
                    grouped = grouped.merge(units.rename("UNIT"), on="PRODUCT_NAME", how="left")
                grouped = grouped.sort_values("TOTAL_AMOUNT", ascending=False).reset_index(drop=True)

                # Топ-20
                top20 = grouped.head(20)
                fig = px.bar(top20, x="TOTAL_AMOUNT", y="PRODUCT_NAME", orientation="h",
                    title="Top-20 by Purchase Amount" if _get_lang()=="en" else "Топ-20 по сумме закупок",
                    color="AVG_PURCHASE_PRICE", color_continuous_scale="YlOrRd",
                    text=top20["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels=_labels())
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(450, len(top20)*28),
                    yaxis=dict(autorange="reversed"), **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Распределение
                cl, cr = st.columns(2)
                with cl:
                    fig = px.histogram(grouped, x="AVG_PURCHASE_PRICE", nbins=40,
                        title="Purchase Price Distribution" if _get_lang()=="en" else "Распределение закупочных цен",
                        labels=_labels(),
                        color_discrete_sequence=["#00ff6a"])
                    fig.update_layout(height=350, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                with cr:
                    fig = px.scatter(grouped.head(100), x="AVG_PURCHASE_PRICE", y="TOTAL_AMOUNT",
                        hover_name="PRODUCT_NAME", size="TOTAL_QTY",
                        title="Price vs Purchase Volume" if _get_lang()=="en" else "Цена vs Объём закупок",
                        labels=_labels(),
                        color_discrete_sequence=["#00e5ff"])
                    fig.update_layout(height=350, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Таблица
                st.divider()
                show_cols = [c for c in ["PRODUCT_NAME","AVG_PURCHASE_PRICE","TOTAL_QTY","TOTAL_AMOUNT","DOC_COUNT","UNIT"] if c in grouped.columns]
                display_gp = grouped[show_cols].copy()
                display_gp.columns = ["Товар","Ø Цена ₽","Общ. кол-во","Общ. сумма ₽","Накладных","Ед."][:len(show_cols)]
                st.dataframe(display_gp, use_container_width=True, hide_index=True, height=500)
                st.download_button("CSV закупок", grouped.to_csv(index=False).encode("utf-8"),
                    "purchases.csv", "text/csv", use_container_width=True)

            # ======================== ПО СКЛАДАМ ========================
            elif rpt_mode == "По складам (суммы прихода)" and not docs_df.empty and "DEPART" in docs_df.columns:
                by_dep = docs_df.groupby("DEPART").agg(
                    DOC_COUNT=("DOC_RID", "count"),
                    TOTAL_AMOUNT=("TOTAL_AMOUNT", "sum"),
                    TOTAL_ITEMS=("ITEMS_COUNT", "sum"),
                ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)

                # Топ складов
                fig = px.bar(by_dep, x="TOTAL_AMOUNT", y="DEPART", orientation="h",
                    title="Incoming Amount by Warehouse" if _get_lang()=="en" else "Сумма прихода по складам",
                    color="TOTAL_AMOUNT", color_continuous_scale="Viridis",
                    text=by_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels=_labels())
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, len(by_dep)*30),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                cl, cr = st.columns(2)
                with cl:
                    fig = px.bar(by_dep, x="DOC_COUNT", y="DEPART", orientation="h",
                        title="Invoices per Warehouse" if _get_lang()=="en" else "Кол-во накладных по складам",
                        color="DOC_COUNT", color_continuous_scale="Tealgrn",
                        text="DOC_COUNT",
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(350, len(by_dep)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                with cr:
                    fig = px.pie(by_dep, values="TOTAL_AMOUNT", names="DEPART",
                        title="Incoming Share by Warehouse" if _get_lang()=="en" else "Доля прихода по складам", hole=0.4, labels=_labels())
                    fig.update_layout(height=max(350, len(by_dep)*28), **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Поставщики
                if "SUPPLIER" in docs_df.columns and docs_df["SUPPLIER"].str.strip().ne("").sum() > 0:
                    st.divider()
                    st.markdown("#### " + ("By Suppliers" if _get_lang()=="en" else "По поставщикам"))
                    sup_data = docs_df[docs_df["SUPPLIER"].str.strip().ne("")]
                    by_sup = sup_data.groupby("SUPPLIER").agg(
                        DOC_COUNT=("DOC_RID", "count"),
                        TOTAL_AMOUNT=("TOTAL_AMOUNT", "sum"),
                    ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)
                    top_sup = by_sup.head(20)
                    fig = px.bar(top_sup, x="TOTAL_AMOUNT", y="SUPPLIER", orientation="h",
                        title="Top-20 Suppliers by Amount" if _get_lang()=="en" else "Топ-20 поставщиков по сумме",
                        color="TOTAL_AMOUNT", color_continuous_scale="YlOrRd",
                        text=top_sup["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_sup)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Таблица по складам
                st.divider()
                disp_dep = by_dep.copy()
                disp_dep.columns = ["Склад", "Накладных", "Сумма ₽", "Позиций"]
                disp_dep["Ø на накладную ₽"] = (disp_dep["Сумма ₽"] / disp_dep["Накладных"]).round(0)
                st.dataframe(disp_dep, use_container_width=True, hide_index=True)

                # Полная таблица накладных
                st.divider()
                dep_show = docs_df.copy()
                col_rename = {"DOC_RID": "RID", "DEPART": "Склад", "SUPPLIER": "Поставщик",
                              "DOC_DATE": "Дата", "ITEMS_COUNT": "Позиций", "TOTAL_AMOUNT": "Сумма ₽"}
                dep_show = dep_show.rename(columns={k:v for k,v in col_rename.items() if k in dep_show.columns})
                st.dataframe(dep_show, use_container_width=True, hide_index=True, height=400)
                st.download_button("CSV по складам", docs_df.to_csv(index=False).encode("utf-8"),
                    "incoming_by_depart.csv", "text/csv", use_container_width=True)

            # ======================== ТОВАРЫ В КАЖДОМ СКЛАДЕ ========================
            elif rpt_mode == "Товары в каждом складе" and not items_df.empty and "DEPART" in items_df.columns:
                departs = sorted(items_df["DEPART"].dropna().unique().tolist())
                departs = [d for d in departs if d.strip()]
                if departs:
                    _all_wh = "All warehouses" if _get_lang()=="en" else "Все склады"
                    selected_dep = st.selectbox("Select warehouse:" if _get_lang()=="en" else "Выберите склад:", [_all_wh] + departs, key="dep_sel")
                    if selected_dep == _all_wh:
                        filtered = items_df
                    else:
                        filtered = items_df[items_df["DEPART"] == selected_dep]

                    # Группируем по товарам внутри склада
                    by_prod = filtered.groupby("PRODUCT_NAME").agg(
                        TOTAL_QTY=("QTY", "sum"),
                        TOTAL_AMOUNT=("AMOUNT", "sum"),
                        DOC_COUNT=("DOC_RID", "nunique"),
                    ).reset_index()
                    by_prod["AVG_PRICE"] = (by_prod["TOTAL_AMOUNT"] / by_prod["TOTAL_QTY"]).round(2)
                    by_prod = by_prod.sort_values("TOTAL_AMOUNT", ascending=False).reset_index(drop=True)

                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric(t("products"), f"{len(by_prod):,}")
                    with c2: st.metric(t("amount_label"), f"{by_prod['TOTAL_AMOUNT'].sum():,.0f} ₽")
                    with c3: st.metric(t("invoices_count"), f"{by_prod['DOC_COUNT'].sum()}")

                    # Топ-20 товаров на этом складе
                    top_dep = by_prod.head(20)
                    title_txt = f"Top-20 — {selected_dep}" if _get_lang()=="en" else f"Топ-20 товаров — {selected_dep}"
                    fig = px.bar(top_dep, x="TOTAL_AMOUNT", y="PRODUCT_NAME", orientation="h",
                        title=title_txt,
                        color="AVG_PRICE", color_continuous_scale="YlOrRd",
                        text=top_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_dep)*28),
                        yaxis=dict(autorange="reversed"), **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # Сравнение складов по этому товару
                    if selected_dep != _all_wh:
                        st.divider()
                        st.markdown(f"#### Уникальные товары на складе «{selected_dep}»")
                        # Товары, которые есть только на этом складе
                        all_prods = set(items_df["PRODUCT_NAME"].unique())
                        dep_prods = set(filtered["PRODUCT_NAME"].unique())
                        other_prods = set(items_df[items_df["DEPART"] != selected_dep]["PRODUCT_NAME"].unique())
                        unique_here = dep_prods - other_prods
                        if unique_here:
                            st.success(f"✅ {len(unique_here)} товаров только на этом складе")
                        else:
                            st.info("All products of this warehouse also appear in other warehouses" if _get_lang()=="en" else "Все товары этого склада встречаются и на других складах")

                    # Таблица
                    st.divider()
                    disp_prod = by_prod.copy()
                    disp_prod.columns = ["Товар", "Кол-во", "Сумма ₽", "Накладных", "Ø Цена ₽"]
                    st.dataframe(disp_prod, use_container_width=True, hide_index=True, height=500)
                    csv_name = f"products_{selected_dep.replace(' ','_')}.csv" if selected_dep != _all_wh else "products_all.csv"
                    st.download_button("CSV", by_prod.to_csv(index=False).encode("utf-8"),
                        csv_name, "text/csv", use_container_width=True)
                else:
                    st.warning("Warehouses not defined in invoice headers" if _get_lang()=="en" else "Склады не определены в заголовках накладных")
            else:
                if items_df.empty and not docs_df.empty:
                    st.info("Item positions not parsed." if _get_lang()=="en" else "Позиции товаров не распарсены.")
                else:
                    st.info(t("no_data"))
        else:
            st.info("Click Load to fetch data" if _get_lang()=="en" else "Нажмите «Загрузить приход»")

    # ==================== ПЕРЕМЕЩЕНИЯ ====================
    with sub_transfers:
        st.markdown(f"### Внутренние перемещения ({d1_str} — {d2_str})")
        st.caption("GDoc1_5LstDocs type=4 → transfers → GDoc4 → items + warehouses")

        progress_tr = st.empty()
        if st.button("🚀 Load Transfers" if _get_lang()=="en" else "🚀 Загрузить перемещения", key="load_transfers"):
            with st.spinner("Загружаю перемещения..."):
                tr_l, tr_i, tr_e = sh_load_transfers(d1_str, d2_str, _progress_container=progress_tr, max_rids=100)
                st.session_state["_tr_list"] = tr_l
                st.session_state["_tr_items"] = tr_i
                st.session_state["_tr_err"] = tr_e

        tr_list_r = st.session_state.get("_tr_list", pd.DataFrame())
        tr_items_r = st.session_state.get("_tr_items", pd.DataFrame())
        tr_err_r = st.session_state.get("_tr_err", None)

        if tr_err_r:
            if "MSDSET" in str(tr_err_r) or "errId" in str(tr_err_r) or "Ошибка 5" in str(tr_err_r):
                st.info("Transfers unavailable — no API access" if _get_lang()=="en" else "Перемещения недоступны — нет прав в SH API")
            else:
                st.warning(f"{tr_err_r}")
        elif not tr_list_r.empty:
            st.success(f"✅ {len(tr_list_r)} перемещений, {len(tr_items_r)} позиций товаров")

            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Documents" if _get_lang()=="en" else "Документов", f"{len(tr_list_r)}")
            with c2: st.metric(t("positions"), f"{len(tr_items_r):,}" if not tr_items_r.empty else "0")
            with c3:
                if not tr_items_r.empty and "_DEPART_FROM" in tr_items_r.columns:
                    n_stores = tr_items_r["_DEPART_FROM"].nunique()
                    st.metric("Source warehouses" if _get_lang()=="en" else "Складов-отправителей", f"{n_stores}")
                else:
                    st.metric(t("warehouses"), "—")

            if not tr_items_r.empty:
                st.divider()

                # Ищем название товара
                name_col_tr = None
                for candidate in ["210\\3", "210/3"]:
                    if candidate in tr_items_r.columns:
                        name_col_tr = candidate
                        break
                if not name_col_tr:
                    for c in tr_items_r.columns:
                        if "210" in str(c) and str(c).endswith("3") and len(str(c)) <= 6:
                            name_col_tr = c
                            break
                if not name_col_tr:
                    for c in tr_items_r.columns:
                        try:
                            strs = tr_items_r[c].dropna().astype(str)
                            if len(strs) > 0 and strs.str.len().mean() > 5 and not strs.str.match(r'^[\d\.\-]+$').all():
                                name_col_tr = c
                                break
                        except Exception:
                            continue

                if name_col_tr:
                    # Топ перемещаемых товаров
                    by_product = tr_items_r[name_col_tr].value_counts().head(20).reset_index()
                    by_product.columns = ["Товар", "Перемещений"]
                    fig = px.bar(by_product, x="Перемещений", y="Товар", orientation="h",
                        title="Top-20 by Transfer Frequency" if _get_lang()=="en" else "Топ-20 по частоте перемещений",
                        color="Перемещений", color_continuous_scale="Viridis",
                        text="Перемещений", labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(by_product)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                # Матрица перемещений между складами
                if "_DEPART_FROM" in tr_items_r.columns and "_DEPART_TO" in tr_items_r.columns:
                    st.divider()
                    st.markdown("#### " + ("Transfer Matrix" if _get_lang()=="en" else "Матрица перемещений"))
                    matrix = tr_items_r.groupby(["_DEPART_FROM", "_DEPART_TO"]).size().reset_index(name="COUNT")
                    matrix.columns = ["Откуда", "Куда", "Кол-во"]
                    if len(matrix) > 0:
                        fig = px.density_heatmap(matrix, x="Куда", y="Откуда", z="Кол-во",
                            title="Transfer Intensity" if _get_lang()=="en" else "Интенсивность перемещений",
                            color_continuous_scale="Viridis", labels=_labels())
                        fig.update_layout(height=max(400, matrix["Откуда"].nunique()*40), **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                    st.dataframe(matrix.sort_values(t("quantity"), ascending=False), use_container_width=True, hide_index=True)

                # Полная таблица перемещений
                st.divider()
                st.markdown("#### " + ("All Transfer Items" if _get_lang()=="en" else "Все позиции перемещений"))
                clean_tr = sh_clean_df(tr_items_r)
                st.dataframe(clean_tr, use_container_width=True, hide_index=True, height=500)
                st.download_button("CSV перемещений", tr_items_r.to_csv(index=False).encode("utf-8"),
                    "transfers.csv", "text/csv", use_container_width=True)
        else:
            st.info("Click Load to analyze transfers" if _get_lang()=="en" else "Нажмите «Загрузить перемещения»")

    # ==================== ТОВАРЫ ====================
    with sub_goods_tab:
        goods, goods_err = sh_load_goods()
        if goods_err:
            st.warning(f"{goods_err}")
        elif not goods.empty:
            st.success(f"✅ {len(goods):,} товаров из GoodsTree")
            clean = sh_clean_df(goods)
            useful_keywords = ["название", "группа", "ед.", "цена", "белки", "жиры",
                "углевод", "калорийн", "поставщик", "тип товара", "комплект",
                "наценка", "срок", "алкогол", "производ", "выход", "ндс"]
            skip_keywords = ["id ", "guid", "флаг", "маска", "маршрут", "маркиров", "код"]
            useful_cols = []
            for c in clean.columns:
                cl_c = c.lower()
                if any(k in cl_c for k in useful_keywords) and not any(k in cl_c for k in skip_keywords):
                    useful_cols.append(c)
            if not useful_cols:
                useful_cols = clean.columns[:10].tolist()

            grp_col = next((c for c in useful_cols if "группа" in c.lower()), None)
            c1, c2, c3 = st.columns(3)
            with c1: st.metric(t("products"), f"{len(clean):,}")
            with c2:
                if grp_col: st.metric(t("group"), f"{clean[grp_col].nunique()}")
            with c3:
                name_col_g = next((c for c in useful_cols if "название" in c.lower()), None)
                if name_col_g:
                    non_empty = clean[name_col_g].dropna()
                    st.metric("📝 С названием", f"{len(non_empty):,}")
            st.divider()

            if grp_col and grp_col in clean.columns:
                by_grp = clean[grp_col].value_counts().head(20).reset_index()
                by_grp.columns = ["Группа", "Кол-во товаров"]
                fig = px.bar(by_grp, x="Кол-во товаров", y="Группа", orientation="h",
                    title="Top-20 Groups by Product Count" if _get_lang()=="en" else "Топ-20 групп по кол-ву товаров",
                    color="Кол-во товаров", color_continuous_scale="Viridis", text="Кол-во товаров", labels=_labels())
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, min(20, len(by_grp))*30),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            st.divider()
            st.dataframe(clean[useful_cols], use_container_width=True, hide_index=True, height=500)
            with st.expander("Все колонки (включая технические)"):
                st.dataframe(clean, use_container_width=True, hide_index=True, height=400)
            st.download_button("CSV", clean[useful_cols].to_csv(index=False).encode("utf-8"),
                "sh_goods.csv", "text/csv", use_container_width=True)
        else:
            st.info("GoodsTree returned empty result")

    # ==================== СТРУКТУРА ====================
    with sub_structure:
        st.markdown("### 🏗️ " + ("Warehouse Structure" if _get_lang()=="en" else "Структура складов"))

        divs, divs_err = sh_load_divisions()
        deps, deps_err = sh_load_departs()
        cats, cats_err = sh_load_goods_categories()

        sub_struct_tab = st.selectbox("Show:" if _get_lang()=="en" else "Показать:", ["Divisions","Warehouses","Categories"] if _get_lang()=="en" else ["Подразделения","Склады","Категории"], key="struct_sel")

        if sub_struct_tab == "Подразделения":
            if divs_err:
                st.error(f"Divisions: {divs_err}")
            elif not divs.empty:
                st.success(f"✅ {len(divs)} подразделений (столовые, буфеты, цеха)")
                st.dataframe(sh_clean_df(divs), use_container_width=True, hide_index=True, height=500)
            else:
                st.info("Divisions returned empty result")

        elif sub_struct_tab == "Склады":
            if deps_err:
                st.error(f"Departs: {deps_err}")
            elif not deps.empty:
                st.success(f"✅ {len(deps)} складов/подразделений")

                dep_vent_col = None
                dep_name_col = None
                for c in deps.columns:
                    cl_d = c.lower()
                    if "venture" in cl_d and "name" in cl_d or c == "103\\3":
                        dep_vent_col = c
                    if c == "3" or (cl_d == "name" and dep_name_col is None) or c == "Name":
                        dep_name_col = c
                if not dep_vent_col:
                    for c in deps.columns:
                        if "подразделение" in c.lower() or "venture" in c.lower():
                            dep_vent_col = c
                            break
                if not dep_name_col:
                    for c in deps.columns:
                        if "название" in c.lower() or "name" in c.lower():
                            dep_name_col = c
                            break
                if dep_vent_col and dep_name_col:
                    by_venture = deps.groupby(dep_vent_col).agg(
                        STORE_COUNT=(dep_name_col, "count"),
                        STORES=(dep_name_col, lambda x: ", ".join(x.dropna().astype(str)))
                    ).reset_index().sort_values("STORE_COUNT", ascending=False)
                    by_venture.columns = ["Подразделение", "Кол-во складов", "Склады"]

                    fig = px.bar(by_venture, x="Кол-во складов", y="Подразделение", orientation="h",
                        title="Warehouses per Division" if _get_lang()=="en" else "Складов в подразделении",
                        color="Кол-во складов", color_continuous_scale="Viridis",
                        text="Кол-во складов", labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(by_venture)*35),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    st.divider()
                    st.dataframe(by_venture, use_container_width=True, hide_index=True)

                st.divider()
                st.dataframe(sh_clean_df(deps), use_container_width=True, hide_index=True, height=500)
            else:
                st.info("Departs returned empty result")

        elif sub_struct_tab == "Категории":
            if cats_err:
                st.error(f"GoodsCategories: {cats_err}")
            elif not cats.empty:
                st.success(f"✅ {len(cats)} категорий товаров")
                st.dataframe(sh_clean_df(cats), use_container_width=True, hide_index=True)
            else:
                st.info("GoodsCategories returned empty result")

    # ==================== ОТЛАДКА ====================
    with sub_debug:
        st.markdown("### 🔧 " + ("API Debug" if _get_lang()=="en" else "Отладка API"))
        st.markdown("**Сырой ответ API** — для диагностики парсинга")
        debug_proc = st.selectbox("Procedure:" if _get_lang()=="en" else "Процедура:",
            ["Goods", "GoodsCategories", "Departs", "Divisions",
             "GDoc0ExtList", "GDoc1_5LstDocs", "GDoc4", "DocAccSums",
             "GAbcRpt", "FifoDtl"], key="debug_proc")
        if st.button("Show raw JSON" if _get_lang()=="en" else "Показать сырой JSON", key="debug_btn"):
            raw_data, raw_err = sh_exec_raw(debug_proc)
            if raw_err:
                st.error(f"Ошибка: {raw_err}")
            else:
                st.json(raw_data)
                tables = raw_data.get("shTable", [])
                st.caption(f"shTable содержит {len(tables)} таблиц")
                for i, tbl in enumerate(tables):
                    st.markdown(f"**Таблица {i}:** head=`{tbl.get('head')}`, "
                        f"fields={len(tbl.get('fields',[]))}, "
                        f"original={len(tbl.get('original',[]))}, "
                        f"values={len(tbl.get('values',[]))} колонок"
                        f" × {len(tbl['values'][0]) if tbl.get('values') else 0} строк")

# --- НАКЛАДНЫЕ ---
if page == "Накладные":
    page_header("Накладные")

    # === DEMO MODE ===
    if IS_DEMO and _DEMO_DB:
        st.caption("Demo data: invoices with details" if _get_lang()=="en" else "Демо-данные: приходные накладные с детализацией")

        inv = pd.read_sql("""SELECT i.*, s.NAME as SUPPLIER, d.NAME as DEPART
            FROM STAT_SH4_SHIFTS_INVOICES i
            LEFT JOIN SUPPLIERS s ON i.RIDSHIPPER=s.RID
            LEFT JOIN DEPARTS d ON i.RIDDESTINATION=d.RID
            WHERE i.INVOICEDATE >= ? AND i.INVOICEDATE <= ?
            ORDER BY i.INVOICEDATE DESC""", _DEMO_DB, params=(str(date_from), str(date_to)))

        detail = pd.read_sql("""SELECT id.*, g.NAME as PRODUCT, g.UNIT
            FROM STAT_SH4_SHIFTS_INVOICES_DETAIL id
            JOIN GOODS g ON id.GOODSRID=g.RID""", _DEMO_DB)

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric(t("invoices_count"), f"{len(inv)}")
        with c2: st.metric(t("positions"), f"{len(detail):,}")
        with c3: st.metric(t("amount_label"), f"{inv['PAYSUMNOTAX'].sum():,.0f} ₽" if not inv.empty else "0")
        with c4: st.metric(t("no_data") if False else "Suppliers", f"{inv['SUPPLIER'].nunique()}" if not inv.empty else "0")

        if not inv.empty:
            st.divider()
            tab_list, tab_by_sup, tab_by_prod = st.tabs(["Список", "По поставщикам", "По товарам"])

            with tab_list:
                st.dataframe(inv[["INVOICEDATE","INVOICESTRING","SUPPLIER","DEPART","PAYSUMNOTAX","TAXSUM"]].rename(
                    columns={"INVOICEDATE":"Дата","INVOICESTRING":"Номер","SUPPLIER":"Поставщик","DEPART":"Склад","PAYSUMNOTAX":"Сумма","TAXSUM":"НДС"}),
                    use_container_width=True, hide_index=True)

            with tab_by_sup:
                by_sup = inv.groupby("SUPPLIER")["PAYSUMNOTAX"].sum().reset_index().sort_values("PAYSUMNOTAX", ascending=False)
                fig = px.bar(by_sup, x="PAYSUMNOTAX", y="SUPPLIER", orientation="h",
                    text=by_sup["PAYSUMNOTAX"].apply(lambda x: f"{x:,.0f} ₽"),
                    labels=_labels())
                fig.update_layout(height=max(250, len(by_sup)*40), yaxis=dict(autorange="reversed"), **CHART_THEME)
                fix_bar_hover(fig)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            with tab_by_prod:
                inv_rids = inv["RID"].tolist()
                if inv_rids:
                    prod_detail = detail[detail["RID"].isin(inv_rids)]
                    by_prod = prod_detail.groupby("PRODUCT").agg(
                        QTY=("QUANTITY","sum"), SUM=("STAMPSUMNOTAX","sum")).reset_index().sort_values("SUM", ascending=False)
                    fig = px.bar(by_prod.head(20), x="SUM", y="PRODUCT", orientation="h",
                        color="SUM", color_continuous_scale="Tealgrn",
                        labels=_labels())
                    fig.update_layout(height=max(300, len(by_prod.head(20))*28), yaxis=dict(autorange="reversed"),
                        coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        st.stop()

    st.caption("SQL → document RIDs → SH API → item details")

    sub_pipeline, sub_corr, sub_sql = st.tabs([
        "Документы (SQL+API)", "Поставщики", "🗄️ SQL-статус"])

    with sub_pipeline:
        with st.spinner("Загружаю документы из SQL → детали из API..."):
            headers_df, items_df = sh_load_all_docs_from_sql()

        if not headers_df.empty:
            st.success(f"✅ {len(headers_df)} документов, {len(items_df)} позиций товаров")

            # Метрики
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Documents" if _get_lang()=="en" else "Документов", f"{len(headers_df)}")
            with c2: st.metric("Item positions" if _get_lang()=="en" else "Позиций товаров", f"{len(items_df):,}" if not items_df.empty else "0")
            with c3:
                types = headers_df["Тип"].nunique() if "Тип" in headers_df.columns else 0
                st.metric("Doc types" if _get_lang()=="en" else "Типов документов", f"{types}")
            st.divider()

            # Список документов
            st.markdown("### " + ("Documents" if _get_lang()=="en" else "Документы"))
            st.dataframe(headers_df, use_container_width=True, hide_index=True)

            # Позиции товаров
            if not items_df.empty:
                st.divider()
                st.markdown("### " + ("Items in Documents" if _get_lang()=="en" else "Товары в документах"))

                # Топ товаров по частоте
                name_col = next((c for c in items_df.columns if "товар" in c.lower() or c == "210\\3"), None)
                if name_col:
                    by_product = items_df[name_col].value_counts().head(20).reset_index()
                    by_product.columns = ["Товар", "Кол-во документов"]
                    fig = px.bar(by_product, x="Кол-во документов", y="Товар", orientation="h",
                        title="Top-20 by Document Frequency" if _get_lang()=="en" else "Топ-20 по частоте в документах",
                        color="Кол-во документов", color_continuous_scale="Viridis",
                        text="Кол-во документов", labels=_labels())
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, min(20, len(by_product))*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                st.divider()
                st.dataframe(items_df, use_container_width=True, hide_index=True, height=500)
                st.download_button("CSV позиций", items_df.to_csv(index=False).encode("utf-8"),
                    "doc_items.csv", "text/csv", use_container_width=True)
            else:
                st.info("Item positions empty" if _get_lang()=="en" else "Позиции товаров пусты")
        else:
            st.warning("No documents in SQL database" if _get_lang()=="en" else "Нет документов в SQL-базе")
            st.markdown("""
            **Как это работает:**
            1. SQL-база `RK7_STAT_SH4_SHIFTS_FOODCOST` → таблица `INVOICES` → список RID документов
            2. По каждому RID + тип → вызов GDoc API (GDoc0, GDoc1, ..., GDoc12)
            3. API возвращает заголовок документа + список товаров с ценами

            **Доступные типы документов:** приходные, расходные, перемещения, инвентаризации, списания, реализации, возвраты
            """)

    with sub_corr:
        corr = sh_stat_corr()
        if not corr.empty:
            type_map = {1: "Физлицо", 2: "📤 Реализация", 3: "Поставщик"}
            corr_clean = corr.copy()
            corr_clean["Тип"] = corr_clean["TYPECORR"].map(type_map).fillna("Другое")
            st.success(f"✅ {len(corr_clean)} контрагентов")

            c1, c2 = st.columns(2)
            with c1:
                by_type = corr_clean["Тип"].value_counts().reset_index()
                by_type.columns = ["Тип", "Кол-во"]
                fig = px.pie(by_type, values="Кол-во", names="Тип",
                    title="By Counterparty Type" if _get_lang()=="en" else "По типам контрагентов", hole=0.4, labels=_labels())
                fig.update_layout(height=350, **CHART_THEME)
                fix_bar_hover(fig)

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            with c2:
                suppliers = corr_clean[corr_clean["TYPECORR"] == 3]
                st.metric(t("no_data") if False else "Suppliers", f"{len(suppliers)}")
                st.metric("Individuals" if _get_lang()=="en" else "Физлиц", f"{len(corr_clean[corr_clean['TYPECORR']==1])}")
                st.metric("📤 Реализация", f"{len(corr_clean[corr_clean['TYPECORR']==2])}")

            st.divider()
            st.dataframe(corr_clean[["NAME","Тип","CODE"]].rename(
                columns={"NAME":"Название","CODE":"Код"}),
                use_container_width=True, hide_index=True, height=400)
        else:
            st.info(t("no_data"))

    with sub_sql:
        st.markdown("### 🗄️ " + ("SQL Database Status" if _get_lang()=="en" else "Состояние SQL-базы"))
        counts = sh_stat_table_counts()
        if not counts.empty:
            filled = counts[counts["Записей"] > 0]
            c1, c2 = st.columns(2)
            with c1: st.metric("✅ Таблиц с данными", f"{len(filled)}")
            with c2: st.metric("⬚ Пустых", f"{len(counts) - len(filled)}")
            st.dataframe(counts, use_container_width=True, hide_index=True)

            # Группы товаров из SQL
            groups = sh_stat_goodgroups()
            if not groups.empty:
                st.divider()
                st.markdown(f"### Группы товаров ({len(groups)})")
                groups_clean = groups[["RID","PARENTRID","NAME"]].rename(
                    columns={"RID":"ID","PARENTRID":"Родитель","NAME":"Название"})
                st.dataframe(groups_clean, use_container_width=True, hide_index=True, height=300)

# --- ФУДКОСТ ---
if page == "Фудкост":
    page_header("Фудкост")
    st.caption(f"Источник: STAT_SH4_SHIFTS_SELLING (экспорт StoreHouse → SQL)")

    # Загрузка данных с названиями
    selling = run_query_cached("""
        SELECT CAST(s.SELLINGDATE AS DATE) as SELL_DATE,
            s.GOODRID, s.GROUPRID, s.RKSIFR,
            mi.NAME as DISH_NAME,
            SUM(s.QUANTITY) as QTY,
            CAST(SUM(s.PURCHASESUMNOTAX) AS DECIMAL(18,2)) as PURCHASE,
            CAST(SUM(s.WHOLESALESUMNOTAX) AS DECIMAL(18,2)) as SELLING
        FROM STAT_SH4_SHIFTS_SELLING s
        LEFT JOIN MENUITEMS mi ON s.RKSIFR = mi.SIFR
        WHERE s.SELLINGDATE >= %s AND s.SELLINGDATE < DATEADD(DAY,1,%s)
        GROUP BY CAST(s.SELLINGDATE AS DATE), s.GOODRID, s.GROUPRID, s.RKSIFR, mi.NAME""",
        (str(date_from), str(date_to)))
    # Справочник точек продажи
    _locs = run_query_cached("SELECT RID, NAME FROM STAT_SH4_SALELOCATIONS WHERE NAME IS NOT NULL")
    _loc_map = dict(zip(_locs["RID"], _locs["NAME"])) if not _locs.empty else {}
    if not selling.empty:
        selling["DISH_NAME"] = selling["DISH_NAME"].fillna("Товар #" + selling["GOODRID"].astype(str))

    if selling.empty:
        st.warning("No data in STAT_SH4_SHIFTS_SELLING" if _get_lang()=="en" else "Нет данных в STAT_SH4_SHIFTS_SELLING за период")
        st.info("Data loaded from SH for last 30 days" if _get_lang()=="en" else "Данные загружаются из SH за 30 дней")
    else:
        total_purchase = float(selling["PURCHASE"].sum())
        total_selling = float(selling["SELLING"].sum())
        total_margin = total_selling - total_purchase
        avg_foodcost = (total_purchase / total_selling * 100) if total_selling > 0 else 0
        n_days = selling["SELL_DATE"].nunique()
        n_goods = selling["GOODRID"].nunique()

        # === СВОДНЫЕ МЕТРИКИ ===
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("💵 Продажи", f"{total_selling:,.0f} ₽")
        with c2: st.metric(t("cost_label"), f"{total_purchase:,.0f} ₽")
        with c3:
            st.metric(t("margin"), f"{total_margin:,.0f} ₽",
                delta=f"{total_margin/max(1,total_selling)*100:.1f}%")
        with c4:
            fc_color = "normal" if 25 <= avg_foodcost <= 35 else "inverse"
            st.metric(t("foodcost"), f"{avg_foodcost:.1f}%",
                delta=t("norm") if 25 <= avg_foodcost <= 35 else (t("above") if avg_foodcost > 35 else t("below")),
                delta_color=fc_color)

        st.divider()

        # === ТАБЫ ===
        tab_daily, tab_groups, tab_detail, tab_table = st.tabs([
            "По дням", "По столовым", "По товарам", "Таблица"])

        # ---- ПО ДНЯМ ----
        with tab_daily:
            by_day = selling.groupby("SELL_DATE").agg(
                PURCHASE=("PURCHASE", "sum"), SELLING=("SELLING", "sum"),
                GOODS=("GOODRID", "nunique"), QTY=("QTY", "sum")
            ).reset_index().sort_values("SELL_DATE")
            by_day["FC_PCT"] = (by_day["PURCHASE"] / by_day["SELLING"].replace(0, 1) * 100).round(1)
            by_day["MARGIN"] = by_day["SELLING"] - by_day["PURCHASE"]

            fig = go.Figure()
            fig.add_trace(go.Bar(x=by_day["SELL_DATE"], y=by_day["SELLING"],
                name=("Sales" if _get_lang()=="en" else "Продажи"), marker_color="#00ff6a"))
            fig.add_trace(go.Bar(x=by_day["SELL_DATE"], y=by_day["PURCHASE"],
                name=t("cost_label"), marker_color="#ff6b9d"))
            fig.add_trace(go.Scatter(x=by_day["SELL_DATE"], y=by_day["FC_PCT"],
                name=(t("foodcost") + " %"), yaxis="y2", mode="lines+markers",
                line=dict(color="#ffea00", width=3)))
            fig.update_layout(barmode="group",
                title="Sales vs Cost by Day" if _get_lang()=="en" else "Продажи vs Себестоимость по дням",
                yaxis=dict(title="₽"),
                yaxis2=dict(title=t("foodcost") + " %", side="right", overlaying="y", range=[0, 50]),
                height=400, legend=dict(orientation="h", y=1.1), **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            disp_day = by_day.copy()
            disp_day.columns = ["Дата", "Себестоимость", "Продажи", "Товаров", "Кол-во", "Фудкост %", "Маржа"]
            st.dataframe(disp_day, use_container_width=True, hide_index=True)

        # ---- ПО СТОЛОВЫМ (GROUPRID) ----
        with tab_groups:
            by_group = selling.groupby("GROUPRID").agg(
                PURCHASE=("PURCHASE", "sum"), SELLING=("SELLING", "sum"),
                GOODS=("GOODRID", "nunique"), QTY=("QTY", "sum")
            ).reset_index().sort_values("SELLING", ascending=False)
            by_group["FC_PCT"] = (by_group["PURCHASE"] / by_group["SELLING"].replace(0, 1) * 100).round(1)
            # Справочник товарных групп
            _gg = run_query_cached("SELECT RID, NAME FROM STAT_SH4_SHIFTS_GOODGROUPS WHERE NAME IS NOT NULL")
            _gg_map = dict(zip(_gg["RID"], _gg["NAME"])) if not _gg.empty else {}
            # Названия: SALELOCATIONS (0-39) или GOODGROUPS
            def _resolve_group(rid):
                if rid in _loc_map:
                    return _loc_map[rid]
                if rid in _gg_map:
                    return _gg_map[rid]
                return f"Группа #{rid}"
            by_group["LOCATION"] = by_group["GROUPRID"].apply(_resolve_group)

            fig2 = px.bar(by_group.head(20), x="SELLING", y="LOCATION", orientation="h",
                title="Sales by Location" if _get_lang()=="en" else "Продажи по точкам", color="FC_PCT", color_continuous_scale="RdYlGn_r",
                text=by_group.head(20)["SELLING"].apply(lambda x: f"{x:,.0f}₽"),
                labels={"SELLING": "Sales ₽" if _get_lang()=="en" else "Продажи ₽", "LOCATION": _labels().get("REST_NAME","Location"), "FC_PCT": t("foodcost")+" %"})
            fig2.update_traces(textposition="auto")
            fig2.update_layout(height=max(400, len(by_group.head(20))*30),
                yaxis=dict(autorange="reversed"), **CHART_THEME)
            fix_bar_hover(fig2)

            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            disp_gr = by_group[["LOCATION","SELLING","PURCHASE","FC_PCT","GOODS","QTY"]].copy()
            disp_gr.columns = ["Точка","Продажи ₽","Себестоимость ₽","Фудкост %","Товаров","Кол-во"]
            st.dataframe(disp_gr, use_container_width=True, hide_index=True)

        # ---- ПО ТОВАРАМ ----
        with tab_detail:
            by_good = selling.groupby(["GOODRID","DISH_NAME"]).agg(
                PURCHASE=("PURCHASE", "sum"), SELLING=("SELLING", "sum"),
                QTY=("QTY", "sum")
            ).reset_index().sort_values("SELLING", ascending=False)
            by_good["FC_PCT"] = (by_good["PURCHASE"] / by_good["SELLING"].replace(0, 1) * 100).round(1)
            by_good["MARGIN"] = by_good["SELLING"] - by_good["PURCHASE"]

            # Топ-20 по продажам
            top = by_good.head(20)
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(y=top["DISH_NAME"],
                x=top["SELLING"], name=("Sales" if _get_lang()=="en" else "Продажи"), orientation="h", marker_color="#00ff6a"))
            fig3.add_trace(go.Bar(y=top["DISH_NAME"],
                x=top["PURCHASE"], name=t("cost_label"), orientation="h", marker_color="#ff6b9d"))
            fig3.update_layout(barmode="group", title="Топ-20 блюд: продажи vs себестоимость",
                height=max(400, len(top)*28), yaxis=dict(autorange="reversed"), **CHART_THEME)
            fix_bar_hover(fig3)

            st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar":False,"scrollZoom":False})

            # Проблемные товары (фудкост > 40%)
            high_fc = by_good[(by_good["FC_PCT"] > 40) & (by_good["SELLING"] > 1000)].sort_values("FC_PCT", ascending=False)
            if not high_fc.empty:
                st.warning(f"{len(high_fc)} items with food cost > 40%")
                st.dataframe(high_fc.head(20).rename(columns={
                    "DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","GOODRID":"ID SH","PURCHASE":"Себестоим.",
                    "SELLING":"Продажи","QTY":"Кол-во","FC_PCT":"Фудкост %","MARGIN":"Маржа"}),
                    use_container_width=True, hide_index=True)

            st.divider()
            st.dataframe(by_good.rename(columns={
                "DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","GOODRID":"ID SH","PURCHASE":"Себестоим.",
                "SELLING":"Продажи","QTY":"Кол-во","FC_PCT":"Фудкост %","MARGIN":"Маржа"}),
                use_container_width=True, hide_index=True, height=500)

        # ---- ТАБЛИЦА (сырые данные) ----
        with tab_table:
            st.markdown(f"**{len(selling):,}** строк за {date_from} — {date_to}")
            st.dataframe(selling, use_container_width=True, hide_index=True, height=500)
            st.download_button("CSV фудкост", selling.to_csv(index=False).encode("utf-8"),
                "foodcost_selling.csv", "text/csv", use_container_width=True)

# --- ФУДКОСТ (РАСЧЁТ) ---
if page == "Фудкост (расчёт)":
    page_header("Фудкост (расчёт)")

    # === DEMO MODE: show foodcost from STAT_SH4_SHIFTS_SELLING ===
    if IS_DEMO and _DEMO_DB:
        st.markdown("**Фудкост** = закупочная стоимость ÷ цена продажи × 100%. Норма: **25–35%**.")
        st.divider()

        selling = pd.read_sql("""
            SELECT s.RKSIFR, m.NAME as DISH_NAME, s.GROUPRID,
                SUM(s.PURCHASESUMNOTAX) as PURCHASE, SUM(s.WHOLESALESUMNOTAX) as SELLING,
                SUM(s.QUANTITY) as QTY
            FROM STAT_SH4_SHIFTS_SELLING s
            LEFT JOIN SH_MENUITEMS m ON s.RKSIFR = m.SIFR
            WHERE s.SHIFTDATE >= ? AND s.SHIFTDATE <= ?
            GROUP BY s.RKSIFR, m.NAME, s.GROUPRID
            ORDER BY SELLING DESC
        """, _DEMO_DB, params=(str(date_from), str(date_to)))

        if not selling.empty:
            selling["FC"] = (selling["PURCHASE"] / selling["SELLING"] * 100).round(1)
            selling["MARGIN"] = selling["SELLING"] - selling["PURCHASE"]
            total_purchase = selling["PURCHASE"].sum()
            total_selling = selling["SELLING"].sum()
            avg_fc = total_purchase / total_selling * 100 if total_selling > 0 else 0

            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric(t("purchases_sh"), f"{total_purchase:,.0f} ₽")
            with c2: st.metric(t("revenue"), f"{total_selling:,.0f} ₽")
            with c3: st.metric(t("foodcost"), f"{avg_fc:.1f}%")
            with c4: st.metric(t("margin"), f"{total_selling - total_purchase:,.0f} ₽")

            st.divider()

            tab1, tab2, tab3 = st.tabs(["По товарам", "По группам", "Таблица"])

            with tab1:
                top = selling.head(20).copy()
                fig = px.bar(top, x="FC", y="DISH_NAME", orientation="h",
                    color="FC", color_continuous_scale="RdYlGn_r",
                    text=top["FC"].apply(lambda x: f"{x:.1f}%"),
                    labels={"DISH_NAME": _labels()["DISH_NAME"], "FC": t("foodcost")+" %"})
                fig.update_layout(height=max(400, len(top)*28), yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False, **CHART_THEME)
                fix_bar_hover(fig)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

            with tab2:
                groups = pd.read_sql("SELECT RID, NAME FROM GOOD_GROUPS", _DEMO_DB)
                by_group = selling.groupby("GROUPRID").agg(
                    PURCHASE=("PURCHASE", "sum"), SELLING=("SELLING", "sum")).reset_index()
                by_group = by_group.merge(groups, left_on="GROUPRID", right_on="RID", how="left")
                by_group["FC"] = (by_group["PURCHASE"] / by_group["SELLING"] * 100).round(1)
                by_group = by_group.sort_values("SELLING", ascending=False)

                fig = px.bar(by_group, x="NAME", y=["PURCHASE", "SELLING"], barmode="group",
                    labels={"NAME": t("group"), "value": "₽", "variable": ""},
                    color_discrete_map={"PURCHASE": "#ff6b9d", "SELLING": "#00ff6a" if not IS_LIGHT else "#00b847"})
                fig.update_layout(height=400, **CHART_THEME)
                fix_bar_hover(fig)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                st.dataframe(by_group[["NAME","PURCHASE","SELLING","FC"]].rename(
                    columns={"NAME":"Группа","PURCHASE":"Закупка","SELLING":"Продажа","FC":"FC%" if _get_lang()=="en" else "ФК%"}),
                    use_container_width=True, hide_index=True)

            with tab3:
                st.dataframe(selling[["DISH_NAME","QTY","PURCHASE","SELLING","FC","MARGIN"]].rename(
                    columns={"DISH_NAME":"Товар","QTY":"Кол-во","PURCHASE":"Закупка","SELLING":"Продажа","FC":"FC%" if _get_lang()=="en" else "ФК%","MARGIN":"Маржа"}),
                    use_container_width=True, hide_index=True)
        else:
            st.warning(t("no_data_period"))

        st.stop()  # Don't run the real SH pipeline below

    st.markdown("""
    **Реальный фудкост** = себестоимость порции (из Актов нарезки SH, по рецептуре) ÷ цена продажи (RK) × 100%  
    Норма для столовых: **25–35%**. Пайплайн: `GoodsTree` → комплекты → `FindLinksToCmp` → `GDoc12` → себестоимость.
    """)

    d1_str, d2_str = str(date_from), str(date_to)

    # ---- Загрузка себестоимости из рецептур (GDoc12) ----
    rc_key = "recipe_costs"
    rc_err_key = "recipe_costs_err"
    _fc_started_key = "_fc_calc_started"

    if rc_key not in st.session_state:
        st.session_state[rc_key] = pd.DataFrame()
        st.session_state[rc_err_key] = None

    recipe_costs = st.session_state[rc_key]
    rc_err = st.session_state[rc_err_key]

    # Если ещё не запускали расчёт — показать кнопку
    if not st.session_state.get(_fc_started_key, False) and recipe_costs.empty:
        st.divider()
        st.info(t("select_period_info"))
        if st.button(f"📊 {t("calculate_foodcost")}", type="primary", use_container_width=True, key="fc_start_calc"):
            st.session_state[_fc_started_key] = True
            st.rerun()
        st.stop()

    # ---- Авто-загрузка рецептур (после нажатия кнопки или повторного входа) ----
    if recipe_costs.empty:
        with st.spinner("Загружаю себестоимость из рецептур (акты нарезки)..."):
            result, result_err = sh_load_recipe_foodcost(max_complects=50, max_docs=50)
            st.session_state[rc_key] = result
            st.session_state[rc_err_key] = result_err
            if not result.empty:
                recipe_costs = result
                rc_err = result_err
            elif result_err:
                st.warning(f"Failed to load recipes: {result_err}" if _get_lang()=="en" else f"Не удалось загрузить рецептуры: {result_err}")

    if not recipe_costs.empty:
        _rc_col, _rc_btn = st.columns([4, 1])
        with _rc_col:
            st.caption(f"📋 Себестоимость из рецептур: {len(recipe_costs)} блюд")
        with _rc_btn:
            if st.button(f"🔄 {t('load_more')}", key="rc_reload_more", help="Load 200 recipes" if _get_lang()=="en" else "Загрузить до 200 рецептур"):
                with st.spinner("Загружаю расширенный набор рецептур..."):
                    result, result_err = sh_load_recipe_foodcost(max_complects=200, max_docs=200)
                    st.session_state[rc_key] = result
                    st.session_state[rc_err_key] = result_err
                    st.rerun()
    else:
        st.warning("No recipe cost data" if _get_lang()=="en" else "Нет данных о себестоимости из рецептур")

    # ---- Загрузка базовых данных ----
    with st.spinner("Загружаю товары из StoreHouse..."):
        sh_prices = load_sh_goods_prices()
    with st.spinner("Загрузка блюд..."):
        rk_prices = load_rk_dish_prices(date_from, date_to)

    # ---- Статус данных ----
    _fc_purch_key_check = "sh_purchases_30d"
    _has_api_pp = _fc_purch_key_check in st.session_state and not st.session_state[_fc_purch_key_check].empty
    if not _has_api_pp:
        # Авто-загрузка закупочных цен из SH API (если не были загружены на Пульсе)
        with st.spinner("Загружаю закупочные цены из накладных SH..."):
            _pp_d1 = (datetime.now().date() - timedelta(30)).isoformat()
            _pp_d2 = datetime.now().date().isoformat()
            try:
                _pp_data, _pp_err = sh_load_purchase_prices(_pp_d1, _pp_d2, max_rids=50)
                if not _pp_err and not _pp_data.empty:
                    st.session_state[_fc_purch_key_check] = _pp_data
                    _has_api_pp = True
            except: pass
    if _has_api_pp:
        _n_api = len(st.session_state[_fc_purch_key_check])
        _cap_col, _btn_col = st.columns([4, 1])
        with _cap_col:
            st.caption(f"📥 Закупочные цены из накладных SH (за 30 дней): {_n_api} товаров")
        with _btn_col:
            if st.button(f"🔄 {t('load_more')}", key="fc_reload_more_pp", help="Load 200 invoices" if _get_lang()=="en" else "Загрузить 200 накладных"):
                with st.spinner("Загружаю расширенный набор накладных..."):
                    _pp_d1 = (datetime.now().date() - timedelta(60)).isoformat()
                    _pp_d2 = datetime.now().date().isoformat()
                    try:
                        _pp_data, _pp_err = sh_load_purchase_prices(_pp_d1, _pp_d2, max_rids=200)
                        if not _pp_err and not _pp_data.empty:
                            st.session_state[_fc_purch_key_check] = _pp_data
                            st.rerun()
                    except: pass
    elif recipe_costs.empty:
        st.caption("⏳ " + ("Failed to load invoices from SH" if _get_lang()=="en" else "Не удалось загрузить накладные из SH"))

    # ---- Метрики ----
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric(f"{t('products')} SH", f"{len(sh_prices):,}" if not sh_prices.empty else "0")
    with c2: st.metric(f"{t('dishes')} RK", f"{len(rk_prices):,}" if not rk_prices.empty else "0")
    with c3: st.metric(f"{t('dishes')} + {t('cost')}", f"{len(recipe_costs):,}" if not recipe_costs.empty else "0")
    with c4:
        if not recipe_costs.empty:
            avg_cost = recipe_costs["COST_PER_PORTION"].mean()
            st.metric(f"Ø {t('cost')}", f"{avg_cost:.1f} ₽")
        else:
            st.metric(f"Ø {t('cost')}", "—")

    if sh_prices.empty:
        st.error("Failed to load products from SH" if _get_lang()=="en" else "Не удалось загрузить товары из SH")
    elif rk_prices.empty:
        st.warning(t("no_data_period"))
    else:
        # Сопоставление: recipe_costs (себестоимость) + sh_prices + rk_prices
        # Если есть закупочные цены из API (загружены на Пульсе) — дополняем рецептуры
        rc_arg = recipe_costs if not recipe_costs.empty else None
        _fc_purch_key = "sh_purchases_30d"
        if _fc_purch_key in st.session_state and not st.session_state[_fc_purch_key].empty:
            api_pp = st.session_state[_fc_purch_key]
            if rc_arg is not None:
                # Merge: recipe costs + API purchase prices for items NOT in recipes
                _rc_names = set(rc_arg["DISH_NAME"].str.strip().str.lower())
                _api_as_rc = api_pp[["PRODUCT_NAME", "AVG_PURCHASE_PRICE"]].rename(
                    columns={"PRODUCT_NAME": "DISH_NAME", "AVG_PURCHASE_PRICE": "COST_PER_PORTION"})
                _api_new = _api_as_rc[~_api_as_rc["DISH_NAME"].str.strip().str.lower().isin(_rc_names)]
                if not _api_new.empty:
                    rc_arg = pd.concat([rc_arg, _api_new], ignore_index=True)
            else:
                # No recipes — use API purchase prices as cost source
                rc_arg = api_pp[["PRODUCT_NAME", "AVG_PURCHASE_PRICE"]].rename(
                    columns={"PRODUCT_NAME": "DISH_NAME", "AVG_PURCHASE_PRICE": "COST_PER_PORTION"})

        with st.spinner("Сопоставляю названия и рассчитываю фудкост..."):
            fc = match_foodcost(rk_prices, sh_prices, purchase_prices=rc_arg)

        if not fc.empty:
            has_price_diff = "PRICE_DIFF" in fc.columns
            has_foodcost = "FOODCOST_PCT" in fc.columns and fc["FOODCOST_PCT"].notna().any()

            st.divider()
            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric(f"🔗 {t("matched")}", f"{len(fc)}")
            with c2: st.metric(t("rk_coverage"), f"{len(fc)/max(1,len(rk_prices))*100:.0f}%")
            if has_foodcost:
                fc_with_cost = fc[fc["FOODCOST_PCT"].notna() & (fc["FOODCOST_PCT"] > 0) & (fc["FOODCOST_PCT"] < 300)]
                avg_fc = fc_with_cost["FOODCOST_PCT"].mean() if not fc_with_cost.empty else 0
                with c3:
                    color = "normal" if 25 <= avg_fc <= 35 else "inverse"
                    st.metric(f"Ø {t('foodcost')}", f"{avg_fc:.1f}%",
                              delta=t("norm") if 25 <= avg_fc <= 35 else (t("above") if avg_fc > 35 else t("below")),
                              delta_color=color)
                with c4: st.metric(f"{t('dishes')} + {t('foodcost')}", f"{len(fc_with_cost)}")
            st.divider()

            # ==== ТАБЫ ====
            tab_names = ["Фудкост" if has_foodcost else "Фудкост (нет данных)",
                         "Сравнение цен", "Себестоимость по рецептурам", "Таблица"]
            sub_fc_tab, sub_prices, sub_recipe, sub_table = st.tabs(tab_names)

            # ============ ТАБ: ФУДКОСТ ============
            with sub_fc_tab:
                if has_foodcost:
                    fc_fc = fc[fc["FOODCOST_PCT"].notna() & (fc["FOODCOST_PCT"] > 0) & (fc["FOODCOST_PCT"] < 300)].copy()

                    # ---- СВОДНЫЕ МЕТРИКИ ЗА ПЕРИОД ----
                    # Считаем общую себестоимость = COST_PRICE * TOTAL_QTY
                    fc_fc["_TOTAL_COST"] = fc_fc["COST_PRICE"] * fc_fc["TOTAL_QTY"]
                    total_revenue = fc_fc["TOTAL_SUM"].sum()
                    total_cost = fc_fc["_TOTAL_COST"].sum()
                    total_margin = total_revenue - total_cost
                    avg_foodcost = (total_cost / total_revenue * 100) if total_revenue > 0 else 0
                    st.session_state["_last_avg_foodcost"] = avg_foodcost

                    # ---- ПОЛНЫЕ ДАННЫЕ ----
                    full_orders = load_orders(date_from, date_to)
                    full_revenue = float(full_orders["TOPAYSUM"].sum()) if not full_orders.empty else 0

                    # Товары БЕЗ рецептур: всё что продано но НЕ покрыто рецептурами
                    # Их себестоимость = закупочная цена из накладных SH
                    fc_no_recipe = fc[(fc["FOODCOST_PCT"].isna()) | (fc["FOODCOST_PCT"] <= 0)].copy()
                    no_recipe_revenue = float(fc_no_recipe["TOTAL_SUM"].sum()) if not fc_no_recipe.empty else 0
                    no_recipe_cost = 0
                    if not fc_no_recipe.empty:
                        # Сначала пробуем COST_PRICE (уже сопоставлена)
                        if "COST_PRICE" in fc_no_recipe.columns:
                            fc_no_recipe["_COST"] = fc_no_recipe["COST_PRICE"].fillna(0) * fc_no_recipe["TOTAL_QTY"]
                        else:
                            fc_no_recipe["_COST"] = 0.0
                        # Для товаров, где _COST == 0, пробуем подтянуть из закупочных накладных
                        _fc_pp_key = "sh_purchases_30d"
                        if _fc_pp_key in st.session_state and not st.session_state[_fc_pp_key].empty:
                            _pp = st.session_state[_fc_pp_key]
                            _pp_dict = dict(zip(_pp["PRODUCT_NAME"].str.strip().str.lower(), _pp["AVG_PURCHASE_PRICE"]))
                            _zero_mask = fc_no_recipe["_COST"] <= 0
                            if _zero_mask.any() and "DISH_NAME" in fc_no_recipe.columns:
                                for idx in fc_no_recipe[_zero_mask].index:
                                    _dn = str(fc_no_recipe.at[idx, "DISH_NAME"]).strip()
                                    _pp_price = _find_best_purchase_price(_dn, _pp_dict)
                                    if _pp_price and _pp_price > 0:
                                        fc_no_recipe.at[idx, "_COST"] = _pp_price * fc_no_recipe.at[idx, "TOTAL_QTY"]
                        no_recipe_cost = float(fc_no_recipe["_COST"].sum())

                    # Непокрытая выручка (не сопоставлена ни с чем)
                    uncovered_revenue = max(0, full_revenue - total_revenue - no_recipe_revenue)

                    # ИТОГО
                    total_all_revenue = full_revenue
                    total_all_cost = total_cost + no_recipe_cost
                    total_all_margin = total_all_revenue - total_all_cost
                    total_all_fc = (total_all_cost / total_all_revenue * 100) if total_all_revenue > 0 else 0

                    st.markdown(f"### {t("summary_period")}: {date_from} — {date_to}")

                    # --- РЯД 1: ИТОГО (главное) ---
                    r0c1, r0c2, r0c3, r0c4 = st.columns(4)
                    with r0c1: st.metric(t("revenue"), f"{total_all_revenue:,.0f} ₽")
                    with r0c2: st.metric(t("cost_label"), f"{total_all_cost:,.0f} ₽")
                    with r0c3: st.metric(t("margin"), f"{total_all_margin:,.0f} ₽",
                                  delta=f"{total_all_margin/max(1,total_all_revenue)*100:.1f}%")
                    with r0c4:
                        _all_fc_color = "normal" if 25 <= total_all_fc <= 35 else "inverse"
                        st.metric(t("foodcost"), f"{total_all_fc:.1f}%",
                                  delta=t("norm") if 25 <= total_all_fc <= 35 else (t("above") if total_all_fc > 35 else t("below")),
                                  delta_color=_all_fc_color)
                    _coverage_pct = (total_revenue+no_recipe_revenue)/max(1,full_revenue)*100
                    if _coverage_pct < 100:
                        st.caption(f"Покрыто себестоимостью: {_coverage_pct:.0f}% выручки. Не сопоставлено: {uncovered_revenue:,.0f} ₽")
                    st.divider()

                    # --- Детализация ---
                    # Товары С рецептурами (переработка)
                    st.caption(f"📋 {t("with_recipes")}")
                    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
                    with r1c1: st.metric(t("revenue"), f"{total_revenue:,.0f} ₽")
                    with r1c2: st.metric(t("cost_label"), f"{total_cost:,.0f} ₽")
                    with r1c3: st.metric(t("margin"), f"{total_margin:,.0f} ₽",
                                  delta=f"{total_margin/max(1,total_revenue)*100:.1f}%")
                    with r1c4:
                        fc_color = "normal" if 25 <= avg_foodcost <= 35 else "inverse"
                        st.metric(t("foodcost"), f"{avg_foodcost:.1f}%",
                                  delta=t("norm") if 25 <= avg_foodcost <= 35 else (t("above") if avg_foodcost > 35 else t("below")),
                                  delta_color=fc_color)

                    # Товары БЕЗ переработки (купили → продали)
                    if no_recipe_revenue > 0:
                        st.divider()
                        st.caption(f"📦 {t("without_processing")}")
                        no_recipe_margin = no_recipe_revenue - no_recipe_cost
                        no_recipe_fc = (no_recipe_cost / no_recipe_revenue * 100) if no_recipe_revenue > 0 else 0
                        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
                        with r2c1: st.metric(t("revenue"), f"{no_recipe_revenue:,.0f} ₽")
                        with r2c2: st.metric(t("cost_label"), f"{no_recipe_cost:,.0f} ₽",
                                      delta=(t("from_invoices") if no_recipe_cost > 0 else t("no_data")))
                        with r2c3: st.metric(t("margin"), f"{no_recipe_margin:,.0f} ₽")
                        with r2c4: st.metric(t("foodcost"), f"{no_recipe_fc:.1f}%" if no_recipe_cost > 0 else "—")
                        # Товары без закупочной цены — предложить загрузить ещё накладных
                        if not fc_no_recipe.empty and "DISH_NAME" in fc_no_recipe.columns:
                            _zero_items = fc_no_recipe[fc_no_recipe["_COST"] <= 0].copy()
                            if not _zero_items.empty:
                                _zero_rev = float(_zero_items["TOTAL_SUM"].sum())
                                with st.expander(f"⚠️ {len(_zero_items)} товаров без закупочной цены ({_zero_rev:,.0f} ₽ выручки)"):
                                    _show = _zero_items[["DISH_NAME", "TOTAL_QTY", "TOTAL_SUM"]].copy()
                                    _show.columns = ["Товар", "Кол-во", "Выручка ₽"]
                                    _show = _show.sort_values(t("revenue") + " ₽", ascending=False).reset_index(drop=True)
                                    st.dataframe(_show, use_container_width=True, hide_index=True)
                                    st.caption("These items not found in loaded invoices. Try loading more." if _get_lang()=="en" else "Этих товаров нет в загруженных накладных. Попробуйте загрузить больше.")
                                    if st.button(f"🔍 {t('load_more')}", key="fc_load_more_invoices"):
                                        with st.spinner("Ищу закупочные цены в накладных SH..."):
                                            _pp_d1 = (datetime.now().date() - timedelta(60)).isoformat()
                                            _pp_d2 = datetime.now().date().isoformat()
                                            try:
                                                _pp_data, _pp_err = sh_load_purchase_prices(_pp_d1, _pp_d2, max_rids=200)
                                                if not _pp_err and not _pp_data.empty:
                                                    st.session_state["sh_purchases_30d"] = _pp_data
                                                    st.success(f"Загружено {len(_pp_data)} товаров из накладных за 60 дней")
                                                    st.rerun()
                                                elif _pp_err:
                                                    st.warning(f"Ошибка: {_pp_err}")
                                            except Exception as _e:
                                                st.warning(f"Не удалось: {_e}")

                    # Покрытие выручки рецептурами
                    if full_revenue > 0:
                        coverage_pct = total_revenue / full_revenue * 100
                        uncovered = full_revenue - total_revenue
                        cov_color = "#00ff6a" if coverage_pct >= 80 else "#ffe600" if coverage_pct >= 60 else "#ff4757"
                        st.markdown(f"""
                        <div style="background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 18px;margin-top:8px;">
                            <div style="display:flex;justify-content:space-between;align-items:center;">
                                <div>
                                    <span style="color:var(--t3);font-size:.7rem;text-transform:uppercase;letter-spacing:.1em;font-weight:700;">Покрытие выручки рецептурами</span>
                                    <div style="display:flex;align-items:baseline;gap:6px;margin-top:4px;">
                                        <span style="font-size:1.3rem;font-weight:800;color:{cov_color};">{coverage_pct:.1f}%</span>
                                        <span style="color:var(--t3);font-size:.8rem;">{total_revenue:,.0f} из {full_revenue:,.0f} ₽</span>
                                    </div>
                                </div>
                                <div style="text-align:right;">
                                    <span style="color:var(--t3);font-size:.7rem;">Без рецептур</span>
                                    <div style="color:#ff9500;font-weight:700;font-size:.9rem;">{uncovered:,.0f} ₽ ({100-coverage_pct:.1f}%)</div>
                                </div>
                            </div>
                            <div style="margin-top:10px;height:4px;background:rgba(255,255,255,.03);border-radius:2px;overflow:hidden;">
                                <div style="width:{min(coverage_pct,100):.1f}%;height:100%;background:linear-gradient(90deg,{cov_color},{'#b8ff00' if coverage_pct>=60 else '#ff9500'});border-radius:2px;"></div>
                            </div>
                            <div style="color:var(--t3);font-size:.65rem;margin-top:6px;">
                                ℹ️ Блюда без рецептур: напитки, покупные товары (шоколадки, снеки), блюда с несовпавшими названиями между RK и SH
                            </div>
                        </div>""", unsafe_allow_html=True)

                    st.divider()

                    # ---- ПОМЕСЯЧНАЯ АНАЛИТИКА ----
                    st.markdown("### " + ("Monthly Dynamics" if _get_lang()=="en" else "Помесячная динамика"))
                    st.caption("Select period for monthly food cost analysis" if _get_lang()=="en" else "Выберите период для анализа фудкоста по месяцам")

                    mc1, mc2 = st.columns(2)
                    with mc1:
                        monthly_from = st.date_input("С:", date_from - timedelta(90), key="fc_monthly_from")
                    with mc2:
                        monthly_to = st.date_input("По:", date_to, key="fc_monthly_to")

                    with st.spinner("Загрузка помесячных продаж..."):
                        monthly_sales = load_rk_monthly_sales(monthly_from, monthly_to)

                    if not monthly_sales.empty and not recipe_costs.empty:
                        # Сопоставляем продажи с себестоимостью
                        rc = recipe_costs.copy()
                        rc["_norm"] = rc["DISH_NAME"].str.strip().str.lower()
                        cost_dict = dict(zip(rc["_norm"], rc["COST_PER_PORTION"]))

                        ms = monthly_sales.copy()
                        ms["_norm"] = ms["DISH_NAME"].str.strip().str.lower()
                        ms["COST_PRICE"] = ms["_norm"].map(cost_dict)
                        ms_valid = ms[ms["COST_PRICE"].notna() & (ms["COST_PRICE"] > 0)].copy()

                        if not ms_valid.empty:
                            ms_valid["ITEM_COST"] = ms_valid["COST_PRICE"] * ms_valid["TOTAL_QTY"]
                            ms_valid["ITEM_MARGIN"] = ms_valid["TOTAL_SUM"] - ms_valid["ITEM_COST"]

                            # Группируем по месяцу
                            by_month = ms_valid.groupby("MONTH").agg(
                                REVENUE=("TOTAL_SUM", "sum"),
                                COST=("ITEM_COST", "sum"),
                                MARGIN=("ITEM_MARGIN", "sum"),
                                DISHES=("DISH_NAME", "nunique"),
                                SOLD=("TOTAL_QTY", "sum"),
                            ).reset_index()
                            by_month["FOODCOST_PCT"] = (by_month["COST"] / by_month["REVENUE"] * 100).round(1)
                            by_month["MARGIN_PCT"] = (by_month["MARGIN"] / by_month["REVENUE"] * 100).round(1)
                            by_month = by_month.sort_values("MONTH")

                            # График 1: Выручка, Себестоимость, Маржа
                            fig = go.Figure()
                            fig.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["REVENUE"],
                                name=t("revenue"), marker_color="#00ff6a", text=by_month["REVENUE"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["COST"],
                                name=t("cost_label"), marker_color="#ef4444", text=by_month["COST"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["MARGIN"],
                                name=t("margin"), marker_color="#10b981", text=by_month["MARGIN"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.update_layout(title="Выручка, себестоимость и маржа по месяцам",
                                barmode="group", height=420, xaxis_title="Month" if _get_lang()=="en" else "Месяц", yaxis_title="₽",
                                legend=dict(orientation="h", y=1.1), **CHART_THEME)
                            fix_bar_hover(fig)

                            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                            # График 2: Фудкост % по месяцам
                            colors_fc_monthly = by_month["FOODCOST_PCT"].apply(
                                lambda x: "#ef4444" if x > 35 else ("#10b981" if x < 25 else "#f59e0b"))
                            fig2 = go.Figure()
                            fig2.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["FOODCOST_PCT"],
                                marker_color=colors_fc_monthly.tolist(),
                                text=by_month["FOODCOST_PCT"].apply(lambda x: f"{x:.1f}%"),
                                textposition="auto", name=(t("foodcost") + " %")))
                            fig2.add_hline(y=25, line_dash="dash", line_color="#10b981", annotation_text="25%")
                            fig2.add_hline(y=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                            fig2.update_layout(title="Фудкост % по месяцам",
                                height=350, xaxis_title="Month" if _get_lang()=="en" else "Месяц", yaxis_title="Food cost %" if _get_lang()=="en" else "Фудкост %", **CHART_THEME)
                            fix_bar_hover(fig2)

                            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                            # Таблица по месяцам
                            with st.expander("Таблица по месяцам"):
                                month_disp = by_month.rename(columns={
                                    "MONTH":"Месяц","REVENUE":"Выручка ₽","COST":"Себестоимость ₽",
                                    "MARGIN":"Margin ₽" if _get_lang()=="en" else "Маржа ₽","FOODCOST_PCT":"Фудкост %","MARGIN_PCT":"Маржа %",
                                    "DISHES":"Блюд","SOLD":"Продано порций"})
                                st.dataframe(month_disp, use_container_width=True, hide_index=True)
                        else:
                            st.warning(t("no_data_period"))
                    elif monthly_sales.empty:
                        st.warning(t("no_data_period"))
                    else:
                        st.info(t("open_foodcost_calc"))

                    st.divider()

                    # ---- РАСПРЕДЕЛЕНИЕ ПО ЗОНАМ (существующие графики) ----
                    st.markdown("### " + ("Distribution by Zones" if _get_lang()=="en" else "Распределение по зонам"))

                    fc_fc["FC_ZONE"] = fc_fc["FOODCOST_PCT"].apply(
                        lambda x: "🟢 Норма (25–35%)" if 25 <= x <= 35
                        else ("🔴 Высокий (>35%)" if x > 35 else "🟡 Низкий (<25%)"))
                    zones = fc_fc["FC_ZONE"].value_counts().reset_index()
                    zones.columns = ["Зона", "Кол-во"]

                    cl, cr = st.columns(2)
                    with cl:
                        fig = px.pie(zones, values="Кол-во", names="Зона", hole=0.45,
                            title="Food Cost Zone Distribution" if _get_lang()=="en" else "Распределение по зонам фудкоста",
                            color="Зона", color_discrete_map={
                                "🟢 Норма (25–35%)": "#10b981", "🔴 Высокий (>35%)": "#ef4444", "🟡 Низкий (<25%)": "#f59e0b"})
                        fig.update_layout(height=380, **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                    with cr:
                        fig = px.histogram(fc_fc, x="FOODCOST_PCT", nbins=40,
                            title="Food Cost Distribution %" if _get_lang()=="en" else "Распределение фудкоста %",
                            labels={"FOODCOST_PCT": t("foodcost")+" %"}, color_discrete_sequence=["#00ff6a"])
                        fig.add_vline(x=25, line_dash="dash", line_color="#10b981", annotation_text="25%")
                        fig.add_vline(x=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                        fig.update_layout(height=380, **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    st.divider()
                    # Топ-30 высокий фудкост
                    top_fc = fc_fc.sort_values("FOODCOST_PCT", ascending=False).head(30)
                    colors_fc = top_fc["FOODCOST_PCT"].apply(
                        lambda x: "#ef4444" if x > 35 else ("#10b981" if x < 25 else "#f59e0b"))
                    fig = go.Figure(go.Bar(
                        x=top_fc["FOODCOST_PCT"], y=top_fc["DISH_NAME"],
                        orientation="h", marker_color=colors_fc.tolist(),
                        text=top_fc["FOODCOST_PCT"].apply(lambda x: f"{x:.1f}%"),
                        customdata=top_fc[["COST_PRICE","SALE_PRICE","MARGIN"]].values if "MARGIN" in top_fc.columns else None,
                        hovertemplate="<b>%{y}</b><br>ФК: %{x:.1f}%<br>Себест: %{customdata[0]:.0f}₽<br>Продажа: %{customdata[1]:.0f}₽<br>Маржа: %{customdata[2]:.0f}₽<extra></extra>" if "MARGIN" in top_fc.columns else None))
                    fig.update_traces(textposition="auto")
                    fig.update_layout(title="Топ-30: самый высокий фудкост",
                        height=max(500, len(top_fc)*25),
                        yaxis=dict(autorange="reversed"), xaxis_title=t("foodcost") + " %", **CHART_THEME)
                    fig.add_vline(x=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                    fig.add_vline(x=25, line_dash="dash", line_color="#10b981")
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # Маржинальные vs убыточные
                    if "MARGIN" in fc_fc.columns:
                        st.divider()
                        cl, cr = st.columns(2)
                        with cl:
                            st.markdown("#### " + ("Highest Margin" if _get_lang()=="en" else "Самые маржинальные"))
                            best = fc_fc.sort_values("MARGIN", ascending=False).head(15)
                            fig = px.bar(best, x="MARGIN", y="DISH_NAME", orientation="h",
                                color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                                title="Top-15 by Margin" if _get_lang()=="en" else "Топ-15 по марже", labels={"MARGIN":"Margin ₽" if _get_lang()=="en" else "Маржа ₽","DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","FOODCOST_PCT":"FC%" if _get_lang()=="en" else "ФК%"})
                            fig.update_layout(height=450, yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="FC%" if _get_lang()=="en" else "ФК%", **CHART_THEME)
                            fix_bar_hover(fig)

                            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                        with cr:
                            st.markdown("#### " + ("Lowest Margin" if _get_lang()=="en" else "Минимальная маржа"))
                            worst = fc_fc.sort_values("MARGIN", ascending=True).head(15)
                            fig = px.bar(worst, x="MARGIN", y="DISH_NAME", orientation="h",
                                color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                                title="Top-15 Lowest Margin" if _get_lang()=="en" else "Топ-15 с мин. маржой", labels={"MARGIN":"Margin ₽" if _get_lang()=="en" else "Маржа ₽","DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","FOODCOST_PCT":"FC%" if _get_lang()=="en" else "ФК%"})
                            fig.update_layout(height=450, yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="FC%" if _get_lang()=="en" else "ФК%", **CHART_THEME)
                            fix_bar_hover(fig)

                            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # Scatter
                    st.divider()
                    fig = px.scatter(fc_fc, x="COST_PRICE", y="SALE_PRICE",
                        hover_name="DISH_NAME", size="TOTAL_SUM",
                        color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                        title="Cost vs Sale Price (size = revenue)" if _get_lang()=="en" else "Себестоимость vs Цена продажи",
                        labels={"COST_PRICE": t("cost")+" ₽", "SALE_PRICE": ("Sale price" if _get_lang()=="en" else "Цена продажи")+" ₽", "FOODCOST_PCT": "FC%"})
                    max_p = max(fc_fc["COST_PRICE"].max(), fc_fc["SALE_PRICE"].max()) * 1.1 if len(fc_fc) else 100
                    fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p],
                        mode="lines", line=dict(dash="dash", color="rgba(255,255,255,0.3)", width=1), name="Себест.=Цена"))
                    fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p / 0.35],
                        mode="lines", line=dict(dash="dot", color="#ef4444", width=1), name="ФК=35%"))
                    fig.update_layout(height=500, coloraxis_colorbar_title="FC%" if _get_lang()=="en" else "ФК%", **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # По группам
                    if "SH_GROUP" in fc_fc.columns:
                        st.divider()
                        by_grp = fc_fc.groupby("SH_GROUP").agg(
                            AVG_FC=("FOODCOST_PCT", "mean"), COUNT=("DISH_NAME", "count"),
                            AVG_MARGIN=("MARGIN", "mean") if "MARGIN" in fc_fc.columns else ("FOODCOST_PCT", "count")
                        ).reset_index()
                        by_grp = by_grp[by_grp["COUNT"] >= 2].sort_values("AVG_FC", ascending=False)
                        if not by_grp.empty:
                            colors_g = by_grp["AVG_FC"].apply(
                                lambda x: "#ef4444" if x > 35 else ("#10b981" if x < 25 else "#f59e0b"))
                            fig = go.Figure(go.Bar(x=by_grp["AVG_FC"], y=by_grp["SH_GROUP"],
                                orientation="h", marker_color=colors_g.tolist(),
                                text=by_grp["AVG_FC"].apply(lambda x: f"{x:.1f}%")))
                            fig.update_traces(textposition="auto")
                            fig.update_layout(title="Средний фудкост по группам SH",
                                height=max(400, len(by_grp)*28),
                                yaxis=dict(autorange="reversed"), xaxis_title=t("foodcost") + " %", **CHART_THEME)
                            fig.add_vline(x=35, line_dash="dash", line_color="#ef4444")
                            fig.add_vline(x=25, line_dash="dash", line_color="#10b981")
                            fix_bar_hover(fig)

                            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    # Таблица фудкоста
                    st.divider()
                    disp_cols = ["DISH_NAME","COST_PRICE","SALE_PRICE","FOODCOST_PCT","MARGIN","TOTAL_QTY","TOTAL_SUM","_match"]
                    if "MARKUP_PCT" in fc_fc.columns:
                        disp_cols.insert(5, "MARKUP_PCT")
                    avail = [c for c in disp_cols if c in fc_fc.columns]
                    fc_disp = fc_fc[avail].rename(columns={"DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","COST_PRICE":"Себест. ₽","SALE_PRICE":"Продажа ₽",
                        "FOODCOST_PCT":"Фудкост %","MARGIN":"Margin ₽" if _get_lang()=="en" else "Маржа ₽","MARKUP_PCT":"Наценка %",
                        "TOTAL_QTY":"Продано","TOTAL_SUM":"Выручка ₽","_match":"Совпадение"})
                    sort_fc = st.selectbox("Sort:" if _get_lang()=="en" else "Сортировать:", ["Food cost % (↓)","Margin ₽ (↑)","Revenue ₽ (↓)"] if _get_lang()=="en" else ["Фудкост % (↓)","Маржа ₽ (↑)","Выручка ₽ (↓)"], key="fc_sort_main")
                    s_col = sort_fc.split(" (")[0]
                    s_asc = "↑" in sort_fc
                    if s_col in fc_disp.columns:
                        fc_disp = fc_disp.sort_values(s_col, ascending=s_asc)
                    st.dataframe(fc_disp, use_container_width=True, hide_index=True, height=500)
                    st.download_button("Фудкост CSV", fc_disp.to_csv(index=False).encode("utf-8"),
                        "foodcost_recipe.csv", "text/csv", use_container_width=True)

                    # ---- ДЕТАЛИЗАЦИЯ: клик по блюду → показать Акт нарезки ----
                    st.divider()
                    st.markdown("#### " + ("Detail: cost check" if _get_lang()=="en" else "Детализация: проверка себестоимости"))
                    st.caption("Select a dish to see the cutting act (GDoc12) with recipe and cost calculation" if _get_lang()=="en" else "Выберите блюдо чтобы увидеть Акт нарезки с рецептурой и расчётом себестоимости")

                    # Список блюд для выбора
                    dish_options = fc_fc.sort_values("FOODCOST_PCT", ascending=False)["DISH_NAME"].tolist()
                    selected_dish = st.selectbox("Dish:" if _get_lang()=="en" else "Блюдо:", dish_options, key="drilldown_dish",
                        index=0 if dish_options else None)

                    if selected_dish:
                        dish_row = fc_fc[fc_fc["DISH_NAME"] == selected_dish].iloc[0]

                        # Метрики выбранного блюда
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: st.metric(t("cost_label"), f"{dish_row.get('COST_PRICE', 0):.2f} ₽")
                        with c2: st.metric("Sale price" if _get_lang()=="en" else "Цена продажи", f"{dish_row.get('SALE_PRICE', 0):.2f} ₽")
                        with c3: st.metric(t("foodcost"), f"{dish_row.get('FOODCOST_PCT', 0):.1f}%")
                        with c4: st.metric(t("margin"), f"{dish_row.get('MARGIN', 0):.2f} ₽")

                        # Ищем DOC_RID для этого блюда в recipe_costs
                        doc_rid = None
                        if not recipe_costs.empty and "LAST_DOC_RID" in recipe_costs.columns:
                            match = recipe_costs[recipe_costs["DISH_NAME"].str.strip().str.lower() == selected_dish.strip().lower()]
                            if not match.empty:
                                doc_rid = int(match.iloc[0]["LAST_DOC_RID"])

                        if doc_rid:
                            with st.spinner(f"Загружаю Акт нарезки (RID={doc_rid})..."):
                                header, items, detail_err = sh_load_gdoc12_detail(doc_rid)

                            if detail_err:
                                st.error(f"Ошибка загрузки: {detail_err}")
                            else:
                                # Заголовок документа
                                if header:
                                    st.markdown(f"##### Акт нарезки №{header.get('Номер', '?')} от {header.get('Дата', '?')}")
                                    header_cols = st.columns(len(header))
                                    for i, (k, v) in enumerate(header.items()):
                                        with header_cols[min(i, len(header_cols)-1)]:
                                            st.caption(f"**{k}:** {v}")

                                # Все позиции документа — ВСЕ блюда из этого акта
                                if not items.empty:
                                    st.markdown(f"##### Все позиции акта ({len(items)} блюд)")
                                    st.caption("Each row — a dish prepared by recipe. Cost calculated by SH from ingredient purchase prices." if _get_lang()=="en" else "Каждая строка — блюдо по рецептуре. Себестоимость из закупочных цен ингредиентов.")

                                    # Подсветим выбранное блюдо
                                    display_items = items.copy()
                                    if "Dish" if _get_lang()=="en" else "Блюдо" in display_items.columns:
                                        display_items["⭐"] = display_items["Dish" if _get_lang()=="en" else "Блюдо"].str.strip().str.lower().apply(
                                            lambda x: "→" if selected_dish.strip().lower() in x or x in selected_dish.strip().lower() else "")
                                        # Переставляем маркер в начало
                                        cols = ["⭐"] + [c for c in display_items.columns if c != "⭐"]
                                        display_items = display_items[cols]

                                    st.dataframe(display_items, use_container_width=True, hide_index=True, height=min(600, max(200, len(items)*35+50)))

                                    # Инфо о комплекте
                                    if "Комплект" in items.columns or "RID комплекта" in items.columns:
                                        sel_items = items.copy()
                                        if "Dish" if _get_lang()=="en" else "Блюдо" in sel_items.columns:
                                            sel_items = sel_items[sel_items["Dish" if _get_lang()=="en" else "Блюдо"].str.strip().str.lower().apply(
                                                lambda x: selected_dish.strip().lower() in x or x in selected_dish.strip().lower())]
                                        if not sel_items.empty:
                                            cmp_name = sel_items.iloc[0].get("Комплект", "?")
                                            cmp_rid = sel_items.iloc[0].get("RID комплекта", "?")
                                            st.info(f"🔗 Комплект (рецептура): **{cmp_name}** (RID: {cmp_rid})")
                                else:
                                    st.warning("Document items empty" if _get_lang()=="en" else "Позиции документа пусты")
                        else:
                            st.caption("Document RID not found. Try reloading data." if _get_lang()=="en" else "Не найден RID документа. Попробуйте перезагрузить.")
                else:
                    st.info("⏳ " + ("Press the button above to load recipe costs" if _get_lang()=="en" else "Нажмите кнопку выше чтобы загрузить рецептуры"))
                    if rc_err:
                        st.caption(f"Причина: {rc_err}")

            # ============ ТАБ: СРАВНЕНИЕ ЦЕН ============
            with sub_prices:
                if has_price_diff:
                    fc_valid = fc[fc["PRICE_DIFF"].notna() & fc["SH_SALE_PRICE"].notna()].copy()
                    fc_valid["ABS_DIFF"] = fc_valid["PRICE_DIFF"].abs()
                    cl, cr = st.columns(2)
                    with cl:
                        top_diff = fc_valid.sort_values("ABS_DIFF", ascending=False).head(30)
                        colors = top_diff["PRICE_DIFF"].apply(lambda x: "#ef4444" if x > 0 else "#10b981")
                        fig = go.Figure(go.Bar(x=top_diff["PRICE_DIFF"], y=top_diff["DISH_NAME"],
                            orientation="h", marker_color=colors.tolist(),
                            text=top_diff["PRICE_DIFF"].apply(lambda x: f"+{x:.0f}" if x>0 else f"{x:.0f}")))
                        fig.update_traces(textposition="auto")
                        fig.update_layout(title="Топ-30: разница цен (RK − SH), ₽",
                            height=max(500, min(30, len(top_diff))*25),
                            yaxis=dict(autorange="reversed"), xaxis_title="₽", **CHART_THEME)
                        fig.add_vline(x=0, line_color="white", line_width=1)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                    with cr:
                        fig = px.scatter(fc_valid, x="SH_SALE_PRICE", y="SALE_PRICE",
                            hover_name="DISH_NAME", size="TOTAL_SUM",
                            color="PRICE_DIFF_PCT", color_continuous_scale="RdYlGn_r",
                            title="Price SH vs Price RK" if _get_lang()=="en" else "Цена SH vs Цена RK", labels={"SH_SALE_PRICE": "SH ₽", "SALE_PRICE": "RK ₽"})
                        max_p = max(fc_valid["SH_SALE_PRICE"].max(), fc_valid["SALE_PRICE"].max()) * 1.1
                        fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p],
                            mode="lines", line=dict(dash="dash", color="white", width=1), name="Равны"))
                        fig.update_layout(height=450, coloraxis_colorbar_title="Разн.%", **CHART_THEME)
                        fix_bar_hover(fig)

                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
                else:
                    st.info(t("no_data"))

            # ============ ТАБ: СЕБЕСТОИМОСТЬ ============
            with sub_recipe:
                if not recipe_costs.empty:
                    st.success(f"✅ {len(recipe_costs)} блюд с себестоимостью из Актов нарезки (GDoc12)")
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric(t("dishes"), f"{len(recipe_costs):,}")
                    with c2: st.metric(f"Ø {t('cost')}", f"{recipe_costs['COST_PER_PORTION'].mean():.1f} ₽")
                    with c3: st.metric("Cutting acts" if _get_lang()=="en" else "Актов нарезки", f"{recipe_costs['DOC_COUNT'].sum():,.0f}")

                    top = recipe_costs.head(30)
                    fig = px.bar(top, x="COST_PER_PORTION", y="DISH_NAME", orientation="h",
                        color="COST_PER_PORTION", color_continuous_scale="Viridis",
                        title="Top-30 by Portion Cost" if _get_lang()=="en" else "Топ-30 по себестоимости порции",
                        labels={"COST_PER_PORTION": t("cost")+" ₽", "DISH_NAME": _labels()["DISH_NAME"]},
                        text="COST_PER_PORTION")
                    fig.update_traces(texttemplate="%{text:.1f} ₽", textposition="auto")
                    fig.update_layout(height=max(500, len(top)*25),
                        yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="₽", **CHART_THEME)
                    fix_bar_hover(fig)

                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

                    rc_disp = recipe_costs.rename(columns={"DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","COST_PER_PORTION":"Себест. ₽/порц",
                        "TOTAL_PORTIONS":"Порций","TOTAL_COST":"Общая себест. ₽","DOC_COUNT":"Актов","UNIT":"Ед."})
                    st.dataframe(rc_disp, use_container_width=True, hide_index=True, height=500)
                    st.download_button("Себестоимость CSV", rc_disp.to_csv(index=False).encode("utf-8"),
                        "recipe_costs.csv", "text/csv", use_container_width=True)
                else:
                    st.info("Cost not loaded" if _get_lang()=="en" else "Себестоимость не загружена")

            # ============ ТАБ: ТАБЛИЦА ============
            with sub_table:
                disp_cols = ["DISH_NAME", "SH_SALE_PRICE", "SALE_PRICE", "PRICE_DIFF", "PRICE_DIFF_PCT",
                             "TOTAL_QTY", "TOTAL_SUM", "SH_GROUP", "_match"]
                if has_foodcost:
                    disp_cols = ["DISH_NAME", "COST_PRICE", "SH_SALE_PRICE", "SALE_PRICE",
                                 "FOODCOST_PCT", "MARGIN", "PRICE_DIFF", "TOTAL_QTY", "TOTAL_SUM", "SH_GROUP", "_match"]
                avail = [c for c in disp_cols if c in fc.columns]
                disp_names = {"DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","COST_PRICE":"Себест. ₽","SH_SALE_PRICE":"Цена SH ₽",
                    "SALE_PRICE":"Цена RK ₽","PRICE_DIFF":"Разн. ₽","PRICE_DIFF_PCT":"Разн. %",
                    "FOODCOST_PCT":"Фудкост %","MARGIN":"Margin ₽" if _get_lang()=="en" else "Маржа ₽","TOTAL_QTY":"Продано","TOTAL_SUM":"Выручка ₽",
                    "SH_GROUP":"Группа","_match":"Совпадение"}
                disp = fc[avail].rename(columns={c: disp_names.get(c,c) for c in avail})
                sort_opts = ["Фудкост % (↓)","Маржа ₽ (↑)","Выручка ₽ (↓)"] if has_foodcost else ["Разн. ₽ (↓)","Выручка ₽ (↓)"]
                sort_by = st.selectbox("Sort:" if _get_lang()=="en" else "Сортировать:", sort_opts, key="fc_sort_tbl")
                s_col = sort_by.split(" (")[0]
                s_asc = "↑" in sort_by
                if s_col in disp.columns:
                    disp = disp.sort_values(s_col, ascending=s_asc)
                st.dataframe(disp, use_container_width=True, hide_index=True, height=600)
                st.download_button("CSV", disp.to_csv(index=False).encode("utf-8"),
                    "foodcost_full.csv", "text/csv", use_container_width=True)

                unmatched = rk_prices[~rk_prices["DISH_NAME"].str.strip().str.lower().isin(
                    fc["DISH_NAME"].str.strip().str.lower())]
                if not unmatched.empty:
                    with st.expander(f"Не сопоставлено: {len(unmatched)} блюд из RK"):
                        st.dataframe(unmatched.sort_values("TOTAL_SUM", ascending=False).head(30)[
                            ["DISH_NAME","SALE_PRICE","TOTAL_QTY","TOTAL_SUM"]].rename(
                            columns={"DISH_NAME":"Dish" if _get_lang()=="en" else "Блюдо","SALE_PRICE":"Цена","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка"}),
                            use_container_width=True, hide_index=True)
        else:
            st.warning("Could not match dishes by name" if _get_lang()=="en" else "Не удалось сопоставить блюда")
            with st.expander("Примеры названий"):
                cl, cr = st.columns(2)
                with cl:
                    st.markdown("**R-Keeper:**")
                    for n in rk_prices["DISH_NAME"].head(20): st.text(n)
                with cr:
                    st.markdown("**StoreHouse:**")
                    for n in sh_prices["SH_NAME"].head(20): st.text(n)

# --- СКЛАД: API ---
if page == "Склад: Схема":
    page_header("StoreHouse API")
    if IS_DEMO:
        st.info("API Explorer unavailable in demo mode" if _get_lang()=="en" else "API Explorer недоступен в демо-режиме")
        st.stop()
    st.caption(f"REST API: {SH_API['url']} · Пользователь: {SH_API['user']}")

    # Информация о сервере
    with st.expander("🖥️ Информация о сервере", expanded=True):
        info, info_err = sh_info()
        if info_err:
            st.error(f"Сервер недоступен: {info_err}")
        else:
            cols = st.columns(3)
            with cols[0]: st.metric("API Version" if _get_lang()=="en" else "Версия API", info.get("Version", "?"))
            with cols[1]: st.metric("Connection" if _get_lang()=="en" else "Подключение", info.get("LinkDisp", "?"))
            db = info.get("DB", {})
            with cols[2]:
                if db:
                    st.metric("DB Size" if _get_lang()=="en" else "Размер БД", db.get("Size", "?"))
                    st.caption(f"ID: {db.get('Ident','')} · v{db.get('Version','')}")
                else:
                    st.metric("DB" if _get_lang()=="en" else "БД", "—")

    st.divider()

    # Проверка прав
    st.markdown("### " + ("Procedure Access" if _get_lang()=="en" else "Права на процедуры"))
    all_procs = [
        "Divisions", "Goods", "GoodsCategories", "GoodsTree", "Departs",
        "Remains", "Selling", "FoodCost", "Invoices",
        "TrialBalance", "Documents"
    ]
    rights, rights_err = sh_check_rights(all_procs)
    if rights_err:
        st.error(f"Ошибка проверки прав: {rights_err}")
    else:
        for proc, allowed in rights.items():
            icon = "✅" if allowed else "🔒"
            st.markdown(f"{icon} **{proc}** — {'доступна' if allowed else 'нет прав'}")
        available = [p for p, a in rights.items() if a]
        locked = [p for p, a in rights.items() if not a]
        if locked:
            st.divider()
            st.warning(f"🔒 Нет прав на: **{', '.join(locked)}**. Попросите администратора SH выдать доступ.")

    st.divider()

    # Структура процедур
    st.markdown("### " + ("Procedure Structure" if _get_lang()=="en" else "Структура процедуры"))
    proc_to_explore = st.selectbox("Procedure:" if _get_lang()=="en" else "Процедура:", all_procs)
    if st.button("📖 Show Structure" if _get_lang()=="en" else "📖 Показать структуру", key="sh_struct"):
        struct_data, struct_err = sh_struct(proc_to_explore)
        if struct_err:
            st.error(f"Ошибка: {struct_err}")
        else:
            tables = struct_data.get("shTable", [])
            if tables:
                for i, tbl in enumerate(tables):
                    head = tbl.get("head", f"Table_{i}")
                    single = "📌 Однострочный" if tbl.get("SingleRow") else "Многострочный"
                    st.markdown(f"**Датасет `{head}`** ({single})")
                    fields = tbl.get("fields", [])
                    if fields:
                        fd = pd.DataFrame(fields)
                        st.dataframe(fd, use_container_width=True, hide_index=True)
            else:
                st.info("Structure empty" if _get_lang()=="en" else "Структура пуста")

    st.divider()

    # Выполнение процедуры
    st.markdown("### ▶️ " + ("Execute Procedure" if _get_lang()=="en" else "Выполнить процедуру"))
    exec_proc = st.selectbox("Procedure:" if _get_lang()=="en" else "Процедура:", all_procs, key="exec_proc")
    exec_mode = st.radio("Mode:" if _get_lang()=="en" else "Режим:", ["🧠 Smart (auto)", "Simple"] if _get_lang()=="en" else ["🧠 Умный (auto)", "Простой"], horizontal=True)
    if st.button("▶️ Execute" if _get_lang()=="en" else "▶️ Выполнить", key="sh_exec_btn"):
        with st.spinner(f"Выполняю {exec_proc}..."):
            if exec_mode.startswith("🧠"):
                df, exec_err = sh_exec_smart(exec_proc)
                if exec_err:
                    st.error(f"Ошибка: {exec_err}")
                elif not df.empty:
                    st.success(f"✅ {len(df)} строк, {len(df.columns)} столбцов")
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.download_button(f"CSV", df.to_csv(index=False).encode("utf-8"),
                        f"sh_{exec_proc}.csv", "text/csv", use_container_width=True)
                else:
                    st.info("Empty result" if _get_lang()=="en" else "Пустой результат")
            else:
                all_tables, exec_err = sh_exec_all_tables(exec_proc)
        if exec_err:
            st.error(f"Ошибка: {exec_err}")
        else:
            for i, df in enumerate(all_tables):
                if not df.empty:
                    st.markdown(f"**Таблица {i+1}** ({len(df)} строк, {len(df.columns)} столбцов)")
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.download_button(f"CSV таблицы {i+1}", df.to_csv(index=False).encode("utf-8"),
                        f"sh_{exec_proc}_{i}.csv", "text/csv", key=f"dl_{exec_proc}_{i}",
                        use_container_width=True)
            if not all_tables:
                st.info("Procedure returned empty result" if _get_lang()=="en" else "Процедура вернула пустой результат")

    # SQL статистика StoreHouse
    st.divider()
    st.markdown("### 🗄️ " + ("SH Statistics SQL" if _get_lang()=="en" else "SQL-база статистики StoreHouse"))
    st.caption(f"База: {SH_STAT_DB} · Сервер: {DB_CONFIG['server']}:{DB_CONFIG['port']}")
    counts = sh_stat_table_counts()
    if not counts.empty:
        filled = counts[counts["Записей"] > 0]
        empty = counts[counts["Записей"] == 0]
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Total tables" if _get_lang()=="en" else "Всего таблиц", f"{len(counts)}")
        with c2: st.metric("✅ С данными", f"{len(filled)}")
        with c3: st.metric("⬚ Пустых", f"{len(empty)}")
        st.dataframe(counts, use_container_width=True, hide_index=True)

        # Показываем наполненные таблицы
        if not filled.empty:
            fig = px.bar(filled, x="Записей", y="Таблица", orientation="h",
                title="Statistics SQL Database" if _get_lang()=="en" else "Наполнение SQL-базы статистики",
                color="Записей", color_continuous_scale="Viridis", text="Записей", labels=_labels())
            fig.update_traces(textposition="auto")
            fig.update_layout(height=max(300, len(filled)*40),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            fix_bar_hover(fig)

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
    else:
        st.warning("Could not connect to statistics SQL database" if _get_lang()=="en" else "Не удалось подключиться к SQL-базе статистики")


# --- ДОХОД/РАСХОД ---
if page == "Доход/Расход":
    page_header("Доход / Расход")
    orders = load_orders(date_from, date_to)

    # === РУЧНЫЕ РАСХОДЫ (сохраняются в session_state) ===
    st.markdown(f"### {t("fixed_costs_monthly")}")
    st.caption(t("fixed_costs_desc"))

    if "_fixed_costs" not in st.session_state:
        saved = load_user_setting(CURRENT_USER["username"], "fixed_costs", None)
        st.session_state["_fixed_costs"] = saved if saved else {"staff": 0, "rent": 0, "utilities": 0, "marketing": 0, "other": 0}
    fc = st.session_state["_fixed_costs"]

    with st.expander(t("fixed_costs_settings"), expanded=not any(v > 0 for v in fc.values())):
        fc_cols = st.columns(5)
        with fc_cols[0]:
            fc["staff"] = st.number_input("Персонал ₽/мес", min_value=0, value=fc["staff"], step=10000, key="_fc_staff", help="ФОТ, зарплаты, налоги на персонал")
        with fc_cols[1]:
            fc["rent"] = st.number_input("Аренда ₽/мес", min_value=0, value=fc["rent"], step=10000, key="_fc_rent", help="Аренда помещений")
        with fc_cols[2]:
            fc["utilities"] = st.number_input("Комм.услуги ₽/мес", min_value=0, value=fc["utilities"], step=5000, key="_fc_util", help="Электричество, вода, интернет")
        with fc_cols[3]:
            fc["marketing"] = st.number_input("Маркетинг ₽/мес", min_value=0, value=fc["marketing"], step=5000, key="_fc_mkt", help="Реклама, продвижение")
        with fc_cols[4]:
            fc["other"] = st.number_input("Прочие ₽/мес", min_value=0, value=fc["other"], step=5000, key="_fc_other", help="Все остальные постоянные расходы")
        st.session_state["_fixed_costs"] = fc
        # Автосохранение в БД
        save_user_setting(CURRENT_USER["username"], "fixed_costs", fc)

    fixed_monthly = sum(fc.values())
    period_days = max((date_to - date_from).days, 1)

    st.divider()

    # === ДАННЫЕ ИЗ СИСТЕМ ===
    if orders.empty:
        st.warning(t("no_data_period"))
    else:
        paid = orders[orders["PAID"]==1] if "PAID" in orders.columns else orders
        revenue = float(paid["TOPAYSUM"].sum())
        discounts = float(paid["DISCOUNTSUM"].sum()) if "DISCOUNTSUM" in paid.columns else 0

        voids_sum = 0
        try:
            vd = load_voids(date_from, date_to)
            if not vd.empty and "PRLISTSUM" in vd.columns:
                voids_sum = float(vd["PRLISTSUM"].sum())
        except: pass

        staff_meals = 0
        try:
            sm_df = run_query_cached("""SELECT SUM(p.BASICSUM) as TOTAL
                FROM PAYMENTS p
                JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
                WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
                  AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1)
                  AND p.PAYLINETYPE = 3""", (str(date_from), str(date_to)))
            if not sm_df.empty and sm_df.iloc[0]["TOTAL"] is not None:
                staff_meals = float(sm_df.iloc[0]["TOTAL"])
        except: pass

        purchases = 0
        try:
            _dr_inv_key = f"_qc_dr_inv_{date_from}_{date_to}"
            if _dr_inv_key not in st.session_state:
                inv = sh_stat_query("""SELECT SUM(PAYSUMNOTAX) as S FROM STAT_SH4_SHIFTS_INVOICES
                    WHERE INVOICEDATE BETWEEN %s AND %s""", (str(date_from), str(date_to)))
                st.session_state[_dr_inv_key] = inv
            inv = st.session_state[_dr_inv_key]
            if not inv.empty and inv.iloc[0]["S"] is not None:
                purchases = float(inv.iloc[0]["S"])
        except: pass
        # Fallback: cached API purchases from Пульс
        if purchases == 0:
            _dr_purch_key = "sh_purchases_30d"
            if _dr_purch_key in st.session_state:
                _dr_cached = st.session_state[_dr_purch_key]
                if not _dr_cached.empty and "TOTAL_AMOUNT" in _dr_cached.columns:
                    purchases = float(_dr_cached["TOTAL_AMOUNT"].sum())

        variable_costs = discounts + purchases
        # Питание сотрудников: вычитаем себестоимость, а не сумму по меню
        _dr_sm_cost, _dr_sm_fc, _dr_sm_exact = _staff_meal_cost(staff_meals)
        variable_costs += _dr_sm_cost
        # НДС (налоги)
        tax_total = 0
        try:
            import re as _re_dr
            _tax_dr = load_tax_breakdown(date_from, date_to)
            if not _tax_dr.empty and "REVENUE" in _tax_dr.columns:
                for _, _tr in _tax_dr.iterrows():
                    _lbl = str(_tr.get("TAX_NAME", ""))
                    _m = _re_dr.search(r'(\d+)', _lbl)
                    _r = int(_m.group(1)) if _m else 0
                    if _r > 0:
                        tax_total += float(_tr["REVENUE"]) * _r / (100 + _r)
        except: pass
        fixed_period = _fixed_cost_for_period(fixed_monthly, date_from, date_to)
        total_costs = variable_costs + tax_total + fixed_period
        net_profit = revenue - total_costs
        margin_pct = (net_profit / revenue * 100) if revenue > 0 else 0

        daily_revenue = revenue / period_days
        daily_variable = variable_costs / period_days
        daily_net = net_profit / period_days
        monthly_net = daily_net * 30

        # === МЕТРИКИ ===
        st.markdown(f"### {t('summary_period')}")
        accent = "#00ff6a" if not IS_LIGHT else "#00b847"
        red = "#ff4d6a" if not IS_LIGHT else "#e53e3e"

        m1, m2, m3, m4 = st.columns(4)
        with m1: st.metric(t("income_revenue"), f"{revenue:,.0f} ₽")
        with m2: st.metric(t("expenses_total"), f"{total_costs:,.0f} ₽")
        with m3: st.metric(t("net_income"), f"{net_profit:,.0f} ₽", delta=f"{margin_pct:.1f}% {t('margin')}")
        with m4: st.metric(t("per_day"), f"{daily_net:,.0f} ₽", delta=f"~{monthly_net:,.0f} ₽/мес")
        st.caption("Variable = discounts + staff meal cost + purchases. Staff meals at cost (food cost %), not menu price. Tax by rates. Fixed = payroll + rent + utilities + marketing + other. Margin = net income / revenue." if _get_lang()=="en" else "Перем. = скидки + себестоимость питания сотр. + закупки. НДС по ставкам. Пост. = ФОТ + аренда + комм. + маркетинг + прочие. Маржа = чистый доход / выручка.")
        st.divider()

        # === СТРУКТУРА И WATERFALL ===
        cl, cr = st.columns(2)
        with cl:
            st.markdown(f"### {t("expense_structure")}")
            cost_items = []
            if purchases > 0: cost_items.append({"Статья": t("purchases_sh"), "Сумма": purchases})
            if tax_total > 0: cost_items.append({"Статья": t("taxes_nds"), "Сумма": tax_total})
            if fc["staff"] > 0: cost_items.append({"Статья": "Персонал", "Сумма": _fixed_cost_for_period(fc["staff"], date_from, date_to)})
            if fc["rent"] > 0: cost_items.append({"Статья": "Аренда", "Сумма": _fixed_cost_for_period(fc["rent"], date_from, date_to)})
            if fc["utilities"] > 0: cost_items.append({"Статья": "Комм.услуги", "Сумма": _fixed_cost_for_period(fc["utilities"], date_from, date_to)})
            if fc["marketing"] > 0: cost_items.append({"Статья": "Маркетинг", "Сумма": _fixed_cost_for_period(fc["marketing"], date_from, date_to)})
            if fc["other"] > 0: cost_items.append({"Статья": "Прочие", "Сумма": _fixed_cost_for_period(fc["other"], date_from, date_to)})
            if discounts > 0: cost_items.append({"Статья": "Скидки", "Сумма": discounts})
            if _dr_sm_cost > 0: cost_items.append({"Статья": f"Питание сотр. (себест.{'~' if not _dr_sm_exact else ''})", "Сумма": _dr_sm_cost})

            if cost_items:
                df_costs = pd.DataFrame(cost_items)
                fig_pie = go.Figure(go.Pie(labels=df_costs["Статья"], values=df_costs["Сумма"], hole=0.5, textinfo="label+percent",
                    marker=dict(colors=["#ef4444","#f59e0b","#3b82f6","#8b5cf6","#10b981","#ec4899","#6b7280","#f97316","#14b8a6"])))
                fig_pie.update_layout(height=350, showlegend=False, margin=dict(l=20,r=20,t=20,b=20), **CHART_THEME)
                st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            else:
                st.info(t("fill_fixed_costs"))

        with cr:
            st.markdown("### " + ("P&L Waterfall" if _get_lang()=="en" else "Водопад P&L"))
            wf = pd.DataFrame([
                {"Статья": "Выручка", "Сумма": revenue, "m": "absolute"},
                {"Статья": "Закупки", "Сумма": -purchases, "m": "relative"},
                {"Статья": t("taxes_nds"), "Сумма": -tax_total, "m": "relative"},
                {"Статья": "Пост.расходы", "Сумма": -fixed_period, "m": "relative"},
                {"Статья": "Скидки", "Сумма": -discounts, "m": "relative"},
                {"Статья": "Пит.сотр.", "Сумма": -staff_meals, "m": "relative"},
                {"Статья": "Чистый доход", "Сумма": net_profit, "m": "total"},
            ])
            fig_wf = go.Figure(go.Waterfall(x=wf["Статья"], y=wf["Сумма"], measure=wf["m"].tolist(),
                increasing=dict(marker_color=accent), decreasing=dict(marker_color=red),
                totals=dict(marker_color="#3b82f6"), textposition="outside",
                text=[f"{v:,.0f}" for v in wf["Сумма"]]))
            fig_wf.update_layout(height=350, title="", showlegend=False, margin=dict(l=40,r=20,t=20,b=40), **CHART_THEME)
            fix_bar_hover(fig_wf)
            st.plotly_chart(fig_wf, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # === ПО ДНЯМ ===
        st.divider()
        st.markdown(f"### {t("revenue_vs_fixed")}")
        daily = load_daily(date_from, date_to)
        if not daily.empty and "REVENUE" in daily.columns:
            fig_d = go.Figure()
            fig_d.add_trace(go.Bar(x=daily["DAY"], y=daily["REVENUE"], name=t("revenue"), marker_color=accent))
            _fixed_daily_avg = fixed_period / period_days if period_days > 0 else 0
            if _fixed_daily_avg > 0:
                fig_d.add_hline(y=_fixed_daily_avg, line_dash="dash", line_color=red, opacity=0.6,
                    annotation_text=f"Пост. расходы: {_fixed_daily_avg:,.0f} ₽/день", annotation_position="top left")
            fig_d.update_layout(height=280, showlegend=True, legend=dict(orientation="h", y=1.1), margin=dict(l=40,r=20,t=40,b=30), **CHART_THEME)
            fix_bar_hover(fig_d)
            st.plotly_chart(fig_d, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # === ИИ ===
        st.divider()
        st.markdown("### " + ("AI Strategy" if _get_lang()=="en" else "Стратегия ИИ"))
        ai_fk = f"ai_fin_{date_from}_{date_to}_{fixed_monthly}"
        if ai_fk not in st.session_state: st.session_state[ai_fk] = None

        if st.button(t("get_ai_recommendations"), key="ai_fin_btn", use_container_width=True):
            with st.spinner("Gemini анализирует финансы..."):
                ctx = f"Период: {date_from}—{date_to} ({period_days}д). Выручка: {revenue:,.0f}₽ ({daily_revenue:,.0f}₽/день). Перем.расходы: {variable_costs:,.0f}₽ (закупки {purchases:,.0f}, скидки {discounts:,.0f}, пит.сотр {staff_meals:,.0f}). НДС: {tax_total:,.0f}₽. Пост.расходы: {fixed_monthly:,.0f}₽/мес (ФОТ {fc['staff']:,}, аренда {fc['rent']:,}, комм {fc['utilities']:,}, маркетинг {fc['marketing']:,}, прочие {fc['other']:,}). Чистый доход: {net_profit:,.0f}₽ ({margin_pct:.1f}%). В день: {daily_net:,.0f}₽. Месяц: ~{monthly_net:,.0f}₽."
                prompt = f"""Финансовый AI-консультант столовых. 4 рекомендации по P&L.\n\nДАННЫЕ:\n{ctx}\n\nОтвет ТОЛЬКО JSON: [{{"cat":"МАРЖА","title":"до 8 слов","text":"2-3 предложения с числами","color":"#hex"}}]\nКатегории: ДОХОД, РАСХОД, МАРЖА, ОПТИМИЗАЦИЯ, РИСК, ПРОГНОЗ\nЦвета: #00ff6a позитив, #0ea5e9 оптимизация, #f59e0b предупреждение, #ef4444 риск. Русский, рубли 1 234 ₽."""
                st.session_state[ai_fk] = ask_gemini(prompt)

        recs = st.session_state.get(ai_fk)
        if recs:
            try:
                import json as _j
                clean = recs.strip()
                if clean.startswith("```"): clean = clean.split("\n",1)[1]
                if clean.endswith("```"): clean = clean.rsplit("```",1)[0]
                cards = _j.loads(clean.strip())
                cbg = "#0e0e16" if not IS_LIGHT else "#ffffff"
                cbr = "rgba(255,255,255,0.06)" if not IS_LIGHT else "rgba(0,0,0,0.08)"
                tc = "#e0e0e8" if not IS_LIGHT else "#1a1a2e"
                sc = "#7a7a92" if not IS_LIGHT else "#5a5a70"
                cols = st.columns(len(cards))
                for i, card in enumerate(cards):
                    with cols[i]:
                        cc = card.get("color","#00ff6a")
                        st.markdown(f"""<div style="background:{cbg};border:1px solid {cbr};border-radius:14px;padding:20px;height:100%;border-top:3px solid {cc};">
                            <div style="font-size:.6rem;font-weight:800;letter-spacing:.12em;color:{cc};margin-bottom:8px;">{card.get("cat","")}</div>
                            <div style="font-size:.92rem;font-weight:700;color:{tc};margin-bottom:10px;line-height:1.3;">{card.get("title","")}</div>
                            <div style="font-size:.78rem;color:{sc};line-height:1.5;">{card.get("text","")}</div>
                        </div>""", unsafe_allow_html=True)
            except: st.markdown(recs)


# --- ЛИЧНЫЙ КАБИНЕТ ---
if page == "Личный кабинет":
    page_header("Личный кабинет", show_period=False)

    user_login = CURRENT_USER["username"]
    user_name = CURRENT_USER["name"]
    user_role = CURRENT_USER["role"]

    # === ПРОФИЛЬ ===
    st.markdown("### " + ("Profile" if _get_lang()=="en" else "Профиль"))
    p1, p2, p3 = st.columns(3)
    with p1: st.text_input("Имя", value=user_name, disabled=True, key="_lk_name")
    with p2: st.text_input("Логин", value=user_login, disabled=True, key="_lk_login")
    with p3: st.text_input("Роль", value="Администратор" if user_role == "admin" else "Пользователь", disabled=True, key="_lk_role")

    # === СМЕНА ПАРОЛЯ ===
    st.divider()
    st.markdown("### " + ("Change Password" if _get_lang()=="en" else "Смена пароля"))
    with st.expander("Сменить пароль"):
        old_pw = st.text_input("Текущий пароль", type="password", key="_lk_old_pw")
        new_pw = st.text_input("Новый пароль", type="password", key="_lk_new_pw")
        new_pw2 = st.text_input("Повторите новый пароль", type="password", key="_lk_new_pw2")
        if st.button("Save password" if _get_lang()=="en" else "Сохранить пароль", key="_lk_save_pw"):
            if not old_pw or not new_pw:
                st.warning("Fill all fields" if _get_lang()=="en" else "Заполните все поля")
            elif new_pw != new_pw2:
                st.error("Пароли не совпадают")
            elif len(new_pw) < 4:
                st.error("Пароль слишком короткий (мин. 4 символа)")
            else:
                old_hash = hashlib.sha256(old_pw.encode()).hexdigest()
                # Check old password
                if user_login not in AUTH_USERS or AUTH_USERS[user_login]["hash"] != old_hash:
                    st.error("Неверный текущий пароль")
                else:
                    new_hash = hashlib.sha256(new_pw.encode()).hexdigest()
                    save_user_setting(user_login, "password_hash", new_hash)
                    st.success("Пароль сохранён!")
                    st.info("Passwords stored in code. DB integration pending." if _get_lang()=="en" else "Пароли хранятся в коде. Интеграция с БД в разработке.")

    # === ПОСТОЯННЫЕ РАСХОДЫ ===
    st.divider()
    st.markdown(f"### {t('fixed_costs_monthly')}")
    st.caption("These values are used on the Income/Expense page for net income calculation." if _get_lang()=="en" else "Эти значения используются на странице «Доход/Расход» для расчёта чистого дохода.")

    saved_fc = load_user_setting(user_login, "fixed_costs", {"staff": 0, "rent": 0, "utilities": 0, "marketing": 0, "other": 0})

    fc_cols = st.columns(5)
    with fc_cols[0]:
        saved_fc["staff"] = st.number_input("Персонал ₽/мес", min_value=0, value=saved_fc.get("staff", 0), step=10000, key="_lk_fc_staff")
    with fc_cols[1]:
        saved_fc["rent"] = st.number_input("Аренда ₽/мес", min_value=0, value=saved_fc.get("rent", 0), step=10000, key="_lk_fc_rent")
    with fc_cols[2]:
        saved_fc["utilities"] = st.number_input("Комм.услуги ₽/мес", min_value=0, value=saved_fc.get("utilities", 0), step=5000, key="_lk_fc_util")
    with fc_cols[3]:
        saved_fc["marketing"] = st.number_input("Маркетинг ₽/мес", min_value=0, value=saved_fc.get("marketing", 0), step=5000, key="_lk_fc_mkt")
    with fc_cols[4]:
        saved_fc["other"] = st.number_input("Прочие ₽/мес", min_value=0, value=saved_fc.get("other", 0), step=5000, key="_lk_fc_other")

    total_fc = sum(saved_fc.values())
    st.metric(t("fixed_costs_monthly"), f"{total_fc:,.0f} ₽/мес", delta=f"{total_fc/30:,.0f} ₽/день")

    if st.button("Save expenses" if _get_lang()=="en" else "Сохранить расходы", key="_lk_save_fc", use_container_width=True):
        save_user_setting(user_login, "fixed_costs", saved_fc)
        st.session_state["_fixed_costs"] = saved_fc
        st.success("Расходы сохранены!")

    # === НАСТРОЙКИ ИНТЕРФЕЙСА ===
    st.divider()
    st.markdown("### " + ("UI Settings" if _get_lang()=="en" else "Настройки интерфейса"))

    ui_prefs = load_user_setting(user_login, "ui_prefs", {"default_period": "7 дней", "default_page": "Пульс"})

    uc1, uc2 = st.columns(2)
    with uc1:
        periods = ["Сегодня", "Вчера", "7 дней", "30 дней", "90 дней"]
        idx = periods.index(ui_prefs.get("default_period", "7 дней")) if ui_prefs.get("default_period", "7 дней") in periods else 2
        ui_prefs["default_period"] = st.selectbox("Default period" if _get_lang()=="en" else "Период по умолчанию", periods, index=idx, key="_lk_def_period")
    with uc2:
        ui_prefs["default_page"] = st.selectbox("Start page" if _get_lang()=="en" else "Стартовая страница", PAGES, index=0, key="_lk_def_page")

    if st.button("Save settings" if _get_lang()=="en" else "Сохранить настройки", key="_lk_save_ui", use_container_width=True):
        save_user_setting(user_login, "ui_prefs", ui_prefs)
        st.success("Настройки сохранены!")

    # === ВСЕ НАСТРОЙКИ (DEBUG) ===
    if user_role == "admin":
        st.divider()
        st.markdown("### " + ("All Saved Settings" if _get_lang()=="en" else "Все сохранённые настройки"))
        all_settings = load_all_user_settings(user_login)
        if all_settings:
            for k, v in all_settings.items():
                st.json({k: v})
        else:
            st.info("No saved settings" if _get_lang()=="en" else "Нет сохранённых настроек")

st.divider()
st.caption(f"{date_from} — {date_to} | {datetime.now().strftime('%H:%M:%S')} | {len(load_restaurants())} точек | v9.41")
