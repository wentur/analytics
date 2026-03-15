"""
🍽️ R-Keeper Analytics Dashboard + AI Chat
Расширенная версия: рестораны, категории, персонал, касса
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymssql
import requests
import re
import time
import hashlib

# ============================================================
# НАСТРОЙКИ
# ============================================================
DB_CONFIG = {
    "server": "saturn.carbis.ru", "port": "7473",
    "user": "readonly", "password": "ai3nPG7rwtrJRw",
    "database": "RK7", "login_timeout": 15, "timeout": 60,
}
SH_API = {
    "url": "http://saturn.carbis.ru:7477/api",
    "user": "readonly",
    "password": "60iNr1uy",
}

GEMINI_API_KEY = "AIzaSyCVlZ88pdunZZ_JwbshJGPaKbIoDRhIMjE"
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

def show_login_page():
    """Показать форму входа. Вызывается если check_auth() == None."""
    # Минимальный CSS для страницы входа
    st.markdown("""<style>
        .stApp { background: #08080e !important; }
        [data-testid="stForm"] {
            background: #0e0e16;
            border: 1px solid rgba(0,255,106,0.1);
            border-radius: 16px; padding: 36px;
            max-width: 400px; margin: 70px auto;
        }
    </style>""", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align:center; margin-bottom: 10px;">
            <span style="font-size: 3rem;">🍽️</span>
            <h2 style="color: #e0e0e8; margin: 10px 0 5px;">R-Keeper AI</h2>
            <p style="color: #7070888; font-size: 0.9rem;">Аналитика сети столовых МГУ</p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            username = st.text_input("Логин", placeholder="Введите логин")
            password = st.text_input("Пароль", type="password", placeholder="Введите пароль")
            submitted = st.form_submit_button("🔐 Войти", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("Введите логин и пароль")
                else:
                    user = _verify_user(username, password)
                    if user:
                        st.session_state["_auth_user"] = user
                        st.rerun()
                    else:
                        st.error("❌ Неверный логин или пароль")

        st.markdown("""
        <div style="text-align:center; margin-top: 20px; color: #555; font-size: 0.75rem;">
            Доступ ограничен · Carbis · 2026
        </div>
        """, unsafe_allow_html=True)

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
        st.error(f"❌ Ошибка подключения: {e}")
        return None

def run_query(query, params=None):
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
        st.error(f"Ошибка запроса: {e}")
        return pd.DataFrame()

def run_query_safe(query):
    q = query.strip().rstrip(";").strip()
    forbidden = ["INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","TRUNCATE","EXEC","EXECUTE","GRANT","REVOKE"]
    first_word = q.split()[0].upper() if q else ""
    if first_word not in ("SELECT","WITH"):
        return None, "⛔ Разрешены только SELECT"
    for w in forbidden:
        if w in q.upper().split(): return None, f"⛔ Запрещено: {w}"
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
st.set_page_config(page_title="R-Keeper AI", page_icon="🍽️", layout="wide", initial_sidebar_state="expanded")

# --- AUTH GATE ---
if check_auth() is None:
    show_login_page()
    st.stop()

# Текущий пользователь (доступен везде)
CURRENT_USER = check_auth()

# Тема: светлая/тёмная
if "_theme" not in st.session_state:
    st.session_state["_theme"] = "dark"
IS_LIGHT = st.session_state["_theme"] == "light"

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

:root {
    --bg:#08080e; --bg2:#0a0a12; --card:#0e0e16;
    --border:rgba(255,255,255,0.04); --border-h:rgba(0,255,106,0.2);
    --t1:#fff; --t2:#7a7a92; --t3:#44445a;
    --green:#00ff6a; --lime:#c8ff00; --yellow:#ffe600; --pink:#ff0090; --purple:#8b5cf6;
}

/* === BASE === */
.stApp { background: var(--bg) !important; color: var(--t1); font-family: 'Inter', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.block-container { padding: 1.2rem 1.5rem 1.5rem; max-width: 100%; }

/* === SIDEBAR === */
section[data-testid="stSidebar"] { background: var(--bg2); border-right: 1px solid var(--border); }
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

/* === METRIC CARDS (JUICE style) === */
[data-testid="stMetric"] {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 14px; padding: 14px 14px;
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
    font-size: 1.35rem !important; font-weight: 800; color: #fff;
    letter-spacing: -.03em; font-variant-numeric: tabular-nums; line-height: 1.1;
    white-space: nowrap; overflow: visible;
}
[data-testid="stMetricLabel"] {
    font-size: .55rem; color: var(--t3);
    text-transform: uppercase; letter-spacing: .1em; font-weight: 700; margin-bottom: 4px;
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
h1 { color:#fff !important; font-weight:800 !important; font-size:1.35rem !important; letter-spacing:-.03em !important; margin-bottom:0 !important; }
h2 { color:#fff !important; font-weight:700 !important; font-size:1.15rem !important; letter-spacing:-.02em !important; }
h3 { color:#fff !important; font-weight:600 !important; font-size:.95rem !important; }
h4 { color:var(--t2) !important; font-weight:500 !important; font-size:.85rem !important; }
p, li { color: var(--t2); font-size: .82rem; line-height: 1.5; }
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

/* === INPUTS === */
.stSelectbox > div > div, .stMultiSelect > div > div { background:var(--card) !important; border-color:var(--border) !important; border-radius:10px !important; }
.stTextInput > div > div > input, .stTextArea > div > div > textarea {
    background:var(--card) !important; border-color:var(--border) !important; border-radius:10px !important; color:#fff !important;
}
.stTextInput > div > div > input:focus { border-color:var(--green) !important; box-shadow:0 0 0 1px var(--green),0 0 15px rgba(0,255,106,.08) !important; }

/* === RADIO (horizontal) === */
.stRadio > div { gap:6px; }
.stRadio label { border-radius:8px; padding:6px 12px; transition:all .15s; border:1px solid transparent; color:var(--t2); font-size:.82rem; }
.stRadio label:hover { background:rgba(0,255,106,.03); }
.stRadio label[data-checked="true"] { background:rgba(0,255,106,.06); border-color:rgba(0,255,106,.2); color:var(--green); }

/* === CHAT === */
.stChatMessage { background:var(--card) !important; border:1px solid var(--border); border-radius:12px; }
.stChatInputContainer > div { background:var(--card); border-color:var(--border); border-radius:12px; }
.stChatInputContainer > div:focus-within { border-color:var(--green); box-shadow:0 0 15px rgba(0,255,106,.06); }

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

/* === RUSSIAN LOADING TEXT === */
[data-testid="stStatusWidget"] div { visibility: hidden; position: relative; }
[data-testid="stStatusWidget"] div::after {
    content: "⏳ Загрузка данных..."; visibility: visible;
    position: absolute; top: 0; left: 0;
    font-size: .8rem; color: var(--t2); font-weight: 500;
}
</style>""", unsafe_allow_html=True)

# === LIGHT THEME OVERRIDES ===
if IS_LIGHT:
    st.markdown("""<style>
    :root { --bg:#f5f5f8; --bg2:#eeeef2; --card:#ffffff; --border:rgba(0,0,0,0.08);
        --border-h:rgba(0,180,80,0.3); --t1:#1a1a2e; --t2:#5a5a70; --t3:#9a9ab0;
        --green:#00b847; --lime:#7ab800; --yellow:#cc9900; --pink:#cc0070; --purple:#6b3fc6; }
    .stApp { background: var(--bg) !important; color: var(--t1); }
    section[data-testid="stSidebar"] { background: var(--bg2); border-right: 1px solid var(--border); }
    section[data-testid="stSidebar"] .stRadio label { color: var(--t2); }
    section[data-testid="stSidebar"] .stRadio label:hover { background: rgba(0,180,80,.05); color: var(--t1); }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"] {
        background: rgba(0,180,80,.08); color: var(--green); border-left-color: var(--green); }
    [data-testid="stMetric"] { background: var(--card); border-color: var(--border); box-shadow: 0 1px 3px rgba(0,0,0,.06); }
    [data-testid="stMetric"]:hover { border-color: var(--border-h); box-shadow: 0 4px 12px rgba(0,0,0,.08); }
    [data-testid="stMetric"]::after { background: linear-gradient(90deg, transparent 10%, rgba(0,180,80,.3) 50%, transparent 90%); }
    [data-testid="stMetricValue"] { color: var(--t1); }
    [data-testid="stMetricLabel"] { color: var(--t3); }
    [data-testid="stMetricDelta"] { background: rgba(0,180,80,.08); }
    h1,h2,h3 { color: var(--t1) !important; }
    h4 { color: var(--t2) !important; }
    p, li { color: var(--t2); }
    strong { color: var(--t1); }
    .stTabs [data-baseweb="tab-list"] { background: var(--card); border-color: var(--border); }
    .stTabs [data-baseweb="tab"] { color: var(--t3); }
    .stTabs [aria-selected="true"] { color: var(--green) !important; background: rgba(0,180,80,.06) !important; }
    .stDataFrame { border-color: var(--border); }
    .stButton > button { background: var(--card); border-color: var(--border); color: var(--t1); }
    .stButton > button:hover { border-color: var(--border-h); }
    div[data-testid="stExpander"] { background: var(--card); border-color: var(--border); }
    .stSelectbox > div > div { background: var(--card) !important; border-color: var(--border) !important; }
    .stTextInput > div > div > input { background: var(--card) !important; border-color: var(--border) !important; color: var(--t1) !important; }
    hr { background: var(--border) !important; }
    .stCaption { color: var(--t3) !important; }
    .stChatMessage { background: var(--card) !important; border-color: var(--border); }
    table th { background: rgba(0,180,80,.04); color: var(--t1); }
    table td,th { border-color: var(--border); }
    a { color: var(--green); }
    code { background: rgba(0,180,80,.06); color: var(--green); }
    ::-webkit-scrollbar-thumb { background: rgba(0,180,80,.15); }
    .glass-card { background: var(--card); border-color: var(--border); }
    .stRadio label[data-checked="true"] { background: rgba(0,180,80,.06); border-color: rgba(0,180,80,.2); color: var(--green); }
    </style>""", unsafe_allow_html=True)

with st.sidebar:
    # --- Пользователь + тема ---
    u_col, theme_col, logout_col = st.columns([3, 1, 1])
    with u_col:
        st.markdown(f"👤 **{CURRENT_USER['name']}**")
    with theme_col:
        theme_icon = "☀️" if IS_LIGHT else "🌙"
        if st.button(theme_icon, key="theme_btn", help="Светлая/тёмная тема"):
            st.session_state["_theme"] = "light" if st.session_state["_theme"] == "dark" else "dark"
            st.rerun()
    with logout_col:
        if st.button("🚪", key="logout_btn", help="Выйти"):
            st.session_state.pop("_auth_user", None)
            st.rerun()
    st.markdown("## 🍽️ R-Keeper AI")
    st.caption("Аналитика сети столовых МГУ")
    st.divider()
    period = st.selectbox("📅 Период", ["Сегодня","Вчера","7 дней","30 дней","90 дней","Произвольный"], index=2)
    today = datetime.now().date()
    if period == "Сегодня": date_from = date_to = today
    elif period == "Вчера": date_from = date_to = today - timedelta(1)
    elif period == "7 дней": date_from, date_to = today - timedelta(7), today
    elif period == "30 дней": date_from, date_to = today - timedelta(30), today
    elif period == "90 дней": date_from, date_to = today - timedelta(90), today
    else:
        c1,c2 = st.columns(2)
        with c1: date_from = st.date_input("С", today-timedelta(30))
        with c2: date_to = st.date_input("По", today)
    st.caption(f"📅 {date_from} → {date_to}")
    st.divider()

st.markdown("# 🍽️ Дашборд сети столовых МГУ")
st.caption(f"Период: {date_from} — {date_to}")
conn = get_connection()
if conn is None: st.stop()

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
@st.cache_data(ttl=120)
def load_orders(d1, d2):
    return run_query("""
        SELECT VISIT, MIDSERVER, IDENTINVISIT, OPENTIME, ENDSERVICE,
            GUESTSCOUNT, PRICELISTSUM, TOPAYSUM, PAIDSUM, DISCOUNTSUM,
            TOTALDISHPIECES, TABLENAME, MAINWAITER, PAID, ICOMMONSHIFT, DURATION
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) ORDER BY OPENTIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
def load_payments(d1, d2):
    return run_query("""
        SELECT p.PAYLINETYPE, SUM(p.BASICSUM) as TOTAL_SUM, COUNT(*) as PAY_COUNT
        FROM PAYMENTS p JOIN ORDERS o ON p.VISIT=o.VISIT AND p.MIDSERVER=o.MIDSERVER AND p.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (p.DBSTATUS IS NULL OR p.DBSTATUS!=-1) AND p.STATE=6
        GROUP BY p.PAYLINETYPE""", (str(d1),str(d2)))

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
def load_hourly(d1, d2):
    return run_query("""
        SELECT DATEPART(HOUR,OPENTIME) as HOUR,
            COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDER_COUNT,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
        GROUP BY DATEPART(HOUR,OPENTIME) ORDER BY HOUR""", (str(d1),str(d2)))

@st.cache_data(ttl=120)
def load_daily(d1, d2):
    return run_query("""
        SELECT CAST(OPENTIME AS DATE) as DAY,
            COUNT(DISTINCT CONCAT(VISIT,'-',IDENTINVISIT)) as ORDER_COUNT,
            SUM(TOPAYSUM) as REVENUE, SUM(GUESTSCOUNT) as GUESTS, AVG(TOPAYSUM) as AVG_CHECK
        FROM ORDERS WHERE OPENTIME >= %s AND OPENTIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1) AND PAID=1
        GROUP BY CAST(OPENTIME AS DATE) ORDER BY DAY""", (str(d1),str(d2)))

# --- НОВЫЕ ЗАПРОСЫ ---

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
def load_cashinout(d1, d2):
    return run_query("""
        SELECT DATETIME, ISDEPOSIT, ORIGINALSUM, KIND, MIDSERVER,
            ICASHIER, OPENREASONNAME
        FROM CASHINOUT
        WHERE DATETIME >= %s AND DATETIME < DATEADD(DAY,1,%s)
          AND (DBSTATUS IS NULL OR DBSTATUS!=-1)
        ORDER BY DATETIME DESC""", (str(d1),str(d2)))

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
def load_checkmark_stats(d1, d2):
    return run_query("""
        SELECT cm.RES, COUNT(*) as CNT
        FROM CHECKMARKRESULTS cm
        WHERE cm.DATETIME >= %s AND cm.DATETIME < DATEADD(DAY,1,%s)
        GROUP BY cm.RES""", (str(d1),str(d2)))

@st.cache_data(ttl=300)
def load_card_errors(d1, d2):
    return run_query("""
        SELECT pe.TRANSACTIONSTATUS, COUNT(*) as CNT
        FROM PAYMENTSEXTRA pe
        JOIN ORDERS o ON pe.VISIT=o.VISIT AND pe.MIDSERVER=o.MIDSERVER AND pe.ORDERIDENT=o.IDENTINVISIT
        WHERE o.OPENTIME >= %s AND o.OPENTIME < DATEADD(DAY,1,%s)
          AND (pe.DBSTATUS IS NULL OR pe.DBSTATUS!=-1)
        GROUP BY pe.TRANSACTIONSTATUS""", (str(d1),str(d2)))

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=600)
def sh_stat_goodgroups():
    """Группы товаров из SQL (89 групп, иерархия)"""
    return sh_stat_query("""
        SELECT RID, PARENTRID, NAME, CODESTR, EXTCODE
        FROM STAT_SH4_SHIFTS_GOODGROUPS
        ORDER BY PARENTRID, NAME""")

@st.cache_data(ttl=600)
def sh_stat_corr():
    """Контрагенты/поставщики из SQL (512 записей)"""
    return sh_stat_query("""
        SELECT RID, NAME, TYPECORR, CODE, GROUPRID
        FROM STAT_SH4_SHIFTS_CORR
        ORDER BY TYPECORR, NAME""")

@st.cache_data(ttl=300)
def sh_stat_invoices():
    """Накладные из SQL"""
    return sh_stat_query("""
        SELECT RID, INVOICEDATE, INVOICESTRING, INVOICENUMBER,
            TYPEINVOICE, RIDSHIPPER, RIDDESTINATION,
            PAYSUMNOTAX, TAXSUM
        FROM STAT_SH4_SHIFTS_INVOICES
        ORDER BY INVOICEDATE DESC""")

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=600)
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
        for sn in sh_norms:
            if rk_name in sn or (len(rk_name) > 5 and rk_name[:12] in sn):
                row = {**rk_row.to_dict(), **sh_dict[sn], "_match": "частичное"}
                partial_rows.append(row)
                break
    if partial_rows:
        merged = pd.concat([merged, pd.DataFrame(partial_rows)], ignore_index=True)
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

@st.cache_data(ttl=300)
def sh_load_remains():
    """Остатки на складах (Remains) — требует права"""
    df, err = sh_exec("Remains")
    return df, err

@st.cache_data(ttl=300)
def sh_load_remains_depart(depart_rid):
    """Остатки по конкретному складу"""
    params = [{"head": "111", "original": ["Rid"], "values": [[depart_rid]]}]
    df, err = sh_exec("Remains", params)
    return df, err

@st.cache_data(ttl=300)
def sh_load_selling(date_from_str, date_to_str):
    """Продажи (Selling) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Selling", params)
    return df, err

@st.cache_data(ttl=300)
def sh_load_foodcost_api(date_from_str, date_to_str):
    """Фудкост (FoodCost) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("FoodCost", params)
    return df, err

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=300)
def sh_load_invoices(date_from_str, date_to_str):
    """Накладные (Invoices) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Invoices", params)
    return df, err

@st.cache_data(ttl=300)
def sh_load_trial_balance(date_from_str, date_to_str):
    """Оборотная ведомость (TrialBalance) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("TrialBalance", params)
    return df, err

@st.cache_data(ttl=300)
def sh_load_documents_api(date_from_str, date_to_str):
    """Документы (Documents) — требует права"""
    params = [{"head": "111", "original": ["DateFrom", "DateTo"],
               "values": [[date_from_str], [date_to_str]]}]
    df, err = sh_exec("Documents", params)
    return df, err

# ============================================================
# ЗАКУПОЧНЫЕ ЦЕНЫ ИЗ ПРИХОДНЫХ НАКЛАДНЫХ (РЕАЛЬНЫЙ ФУДКОСТ)
# ============================================================

@st.cache_data(ttl=600)
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
            st_text.caption(f"📄 Накладная {i+1}/{len(rids)} (RID={rid}) · {len(all_items)} позиций · {errors} ошибок")
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
        return pd.DataFrame(), f"Нет позиций в {len(rids)} накладных ({errors} ошибок)"

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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
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
            st_text.caption(f"📄 Накладная {i+1}/{len(rids)} (RID={rid}) · {len(all_items)} позиций · {errors} ошибок")

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
            st_text.caption(f"🔍 Комплект {i+1}/{len(cmp_rids)} (RID={cmp_rid}) · {len(all_doc_rids)} документов найдено")

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
            st_text.caption(f"📄 Акт нарезки {i+1}/{len(doc_rids)} (RID={rid}) · {len(all_items)} блюд")

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
        "210\\3": "Блюдо",
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
        progress_container.caption("📦 Загрузка списка комплектов из GoodsTree...")

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
        progress_container.caption(f"🔍 Поиск Актов нарезки для {len(cmp_sample)} комплектов...")

    # Шаг 2: найти GDoc12 RID'ы
    sub_progress = progress_container if progress_container else None
    doc_rids, find_errors = sh_find_gdoc12_rids(cmp_sample, max_docs=max_docs, progress_container=sub_progress)

    if not doc_rids:
        return pd.DataFrame(), f"Не найдено Актов нарезки для {len(cmp_sample)} комплектов ({find_errors} ошибок)"

    if progress_container:
        progress_container.caption(f"📄 Загрузка {len(doc_rids)} Актов нарезки...")

    # Шаг 3: загрузить себестоимость
    costs_df, costs_err = sh_load_gdoc12_costs(doc_rids, progress_container=sub_progress)
    if costs_err:
        return pd.DataFrame(), costs_err

    return costs_df, None


# ============================================================
# НАВИГАЦИЯ (ленивая загрузка — грузится только выбранная страница)
# ============================================================
PAGES_ALL = [
    "🤖 ИИ-чат", "🔔 Проактив", "📈 Выручка", "📅 Сезонность", "🍕 Блюда", "🏢 Столовые", "🗂️ Категории",
    "👨‍🍳 Персонал", "💰 Касса", "📊 Цены", "🔤 ABC", "⏱️ Скорость",
    "🕐 Смены", "⚠️ Проблемы", "❌ Отказы", "📋 Заказы",
    "📦 Склад", "📄 Накладные", "🍳 Фудкост", "🔀 Фудкост (расчёт)", "🔍 Склад: Схема"
]

# Страницы доступные только admin
ADMIN_ONLY_PAGES = {"🤖 ИИ-чат", "🔔 Проактив"}

# Фильтруем по роли текущего пользователя
if CURRENT_USER["role"] == "admin":
    PAGES = PAGES_ALL
else:
    PAGES = [p for p in PAGES_ALL if p not in ADMIN_ONLY_PAGES]

with st.sidebar:
    st.divider()
    page = st.radio("📑 Раздел", PAGES, label_visibility="collapsed")
    st.divider()
    st.caption(f"Gemini AI · {len(load_restaurants())} точек · 📦 SH")

if IS_LIGHT:
    CHART_THEME = dict(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#5a5a70", size=11),
        title_font=dict(color="#1a1a2e", size=13, family="Inter"),
        hoverlabel=dict(bgcolor="#ffffff", bordercolor="#e0e0e8", font_size=12, font_family="Inter"),
        colorway=["#00b847","#4caf50","#7ab800","#cc9900","#ff9500","#e65100","#cc0070","#6b3fc6"],
    )
else:
    CHART_THEME = dict(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#7a7a92", size=11),
        title_font=dict(color="#ffffff", size=13, family="Inter"),
        hoverlabel=dict(bgcolor="#0e0e16", bordercolor="#1a1a28", font_size=12, font_family="Inter"),
        colorway=["#00ff6a","#5aff8a","#9fff5a","#d4ff00","#ffea00","#ffaa00","#ff6b9d","#ff00aa"],
    )

# Русские подписи для осей и подсказок plotly
RU = {
    "TOTAL_SUM":"Выручка, ₽", "TOTAL_QTY":"Кол-во", "ORDER_COUNT":"Заказов",
    "DISH_NAME":"Блюдо", "DISH_ID":"ID", "REVENUE":"Выручка, ₽",
    "AVG_CHECK":"Ср. чек, ₽", "GUESTS":"Гостей", "ORDERS":"Заказов",
    "REST_NAME":"Точка", "REST_ID":"ID", "EMP_NAME":"Сотрудник", "EMP_ID":"ID",
    "CATEGORY":"Категория", "CAT_ID":"ID", "COUNT":"Кол-во", "SUM":"Сумма, ₽",
    "CNT":"Кол-во", "AVG_SEC":"Ср. время, сек", "MIN_SEC":"Мин, сек", "MAX_SEC":"Макс, сек",
    "CASHIER":"Кассир", "CASHIER_NAME":"Кассир", "VOID_REASON":"Причина",
    "PRLISTSUM":"Сумма, ₽", "QUANTITY":"Кол-во", "HOUR":"Час",
    "DAY":"Дата", "PRICE":"Цена, ₽", "AVG_PRICE":"Ср. цена, ₽",
    "DISH_NAME":"Блюдо", "TIME_RANGE":"Диапазон", "LABEL":"Время",
    "PAY_TYPE":"Тип оплаты", "PAYLINETYPE":"Тип оплаты",
    "OPERATION":"Операция", "OPERATOR_NAME":"Кассир", "MANAGER_NAME":"Менеджер",
    "DIFF":"Разница, ₽", "ORDERSUMBEFORE":"Сумма до", "ORDERSUMAFTER":"Сумма после",
    "LATE_COUNT":"Опозданий", "SHIFT_COUNT":"Смен", "AVG_HOURS":"Ср. часов",
    "DURATION_MIN":"Длительность", "SHIFTS":"Смен",
    "AVG_DISHES":"Ср. блюд", "PRICE_DIFF":"Разница, ₽", "PRICE_VARIANTS":"Вариантов цен",
    "MIN_PRICE":"Мин. цена", "MAX_PRICE":"Макс. цена", "PRICE_DIFF":"Разница, ₽",
    "STATUS":"Статус", "PRODUCT":"Товар", "MESSAGEFROMDRIVER":"Сообщение",
    "CREATOR_NAME":"Кассир", "AUTHOR_NAME":"Менеджер",
    "DELETE_PERSON_NAME":"Кто отменил", "DELETE_MANAGER_NAME":"Менеджер отмены",
    "TOPAYSUM":"Сумма, ₽", "BASICSUM":"Сумма, ₽",
}

# Умное кэширование — данные грузятся один раз, обновляются только по кнопке
def page_header(title, icon=""):
    """Заголовок страницы с кнопкой обновления"""
    cl, cr = st.columns([6, 1])
    with cl:
        st.markdown(f"### {icon} {title}")
    with cr:
        refresh = st.button("🔄 Обновить", key=f"refresh_{title}", use_container_width=True)
    if refresh:
        st.cache_data.clear()
        st.rerun()
    return refresh

# --- ИИ ЧАТ ---
if page == "🤖 ИИ-чат":
    page_header("ИИ-чат", "🤖")
    st.caption("*«Топ-5 блюд в Столовой 1»  •  «Выручка по столовым за неделю»  •  «Кто больше всех опаздывает?»*")
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
    user_input = st.chat_input("Задайте вопрос...")
    question = sel_q or user_input
    if question:
        st.session_state.chat_history.append({"role":"user","content":question})
        with st.spinner("🔍 Генерирую SQL..."):
            raw = generate_sql(question).strip()
        if raw.startswith("Ошибка Gemini") or raw.startswith("Gemini перегружен"):
            st.session_state.chat_history.append({"role":"assistant","content":f"⚠️ {raw}","sql":None,"dataframe":None})
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
            st.session_state.chat_history.append({"role":"assistant","content":"Не связано с данными ресторана. Данные по складу/остаткам/фудкосту — на страницах 📦📄🍳 дашборда.","sql":None,"dataframe":None})
        else:
            with st.spinner("⚡ Запрос к базе..."):
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
        if st.button("🗑️ Очистить чат", use_container_width=True):
            st.session_state.chat_history = []; st.rerun()

# --- ПРОАКТИВНЫЙ АНАЛИЗ ---
if page == "🔔 Проактив":
    page_header("Проактивный анализ", "🔔")
    days_in_period = (date_to - date_from).days + 1
    prev_end = date_from - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days_in_period-1)
    st.caption(f"Сравнение: **{date_from} — {date_to}** vs **{prev_start} — {prev_end}**")

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
        with c1: st.metric("💰 Выручка", f"{c_rev:,.0f} ₽", delta=delta_str(pct_rev), delta_color="normal")
        with c2: st.metric("🧾 Заказов", f"{c_ord:,.0f}", delta=delta_str(pct_ord), delta_color="normal")
        with c3: st.metric("📊 Ср. чек", f"{c_avg:,.0f} ₽", delta=delta_str(pct_avg), delta_color="normal")
        with c4: st.metric("👥 Гостей", f"{c_gst:,.0f}", delta=delta_str(pct_gst), delta_color="normal")

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
                anomalies.append(f"{'🔴' if cv>pv else '🟢'} Отказы: {pv:.0f} → {cv:.0f} ({delta_str(pct(cv,pv))}), сумма {cvs:,.0f} ₽")

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
        st.markdown("### 🚨 Обнаруженные аномалии")
        if anomalies:
            for a in anomalies:
                st.markdown(f"**{a}**")

            # Графики сравнения
            if not cur_rest.empty and not prev_rest.empty:
                st.divider()
                merged_chart = cur_rest.merge(prev_rest, on="REST_NAME", suffixes=(" (сейчас)"," (до)"), how="outer").fillna(0)
                merged_chart = merged_chart.sort_values("REVENUE (сейчас)", ascending=True).tail(15)
                fig = go.Figure()
                fig.add_trace(go.Bar(y=merged_chart["REST_NAME"], x=merged_chart["REVENUE (до)"],
                    name="Пред. период", orientation="h", marker_color="rgba(99,102,241,0.4)"))
                fig.add_trace(go.Bar(y=merged_chart["REST_NAME"], x=merged_chart["REVENUE (сейчас)"],
                    name="Текущий период", orientation="h", marker_color="#00ff6a"))
                fig.update_layout(title="📊 Сравнение выручки по точкам", barmode="overlay",
                    height=500, **CHART_THEME, legend=dict(orientation="h",y=1.1))
                st.plotly_chart(fig, use_container_width=True)

            # ИИ-рекомендации
            st.divider()
            st.markdown("### 💡 Рекомендации ИИ")
            anomalies_text = "\n".join(anomalies)
            if st.button("🧠 Получить рекомендации от ИИ", use_container_width=True, type="primary"):
                with st.spinner("🤔 Анализирую и формирую рекомендации..."):
                    recs = generate_proactive_insights(anomalies_text)
                st.markdown(recs)
        else:
            st.success("✅ Всё в норме! Значительных отклонений от предыдущего периода не обнаружено.")
    else:
        st.warning("Недостаточно данных для сравнения. Выберите период больше 1 дня.")

# --- ВЫРУЧКА ---
if page == "📈 Выручка":
    page_header("Выручка", "📈")
    orders = load_orders(date_from, date_to)
    if orders.empty:
        st.warning("⚠️ Нет данных за период")
    else:
        paid = orders[orders["PAID"]==1] if "PAID" in orders.columns else orders
        rev=float(paid["TOPAYSUM"].sum()); n=len(paid)
        avg_c=rev/n if n else 0; guests=int(paid["GUESTSCOUNT"].sum())
        dishes_n=int(paid["TOTALDISHPIECES"].sum()); disc=float(paid["DISCOUNTSUM"].sum())
        # Кол-во чеков (ближе к ОФД)
        checks_df = load_check_count(date_from, date_to)
        n_checks = int(checks_df.iloc[0]["CHECKS"]) if not checks_df.empty else n
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        with c1: st.metric("💰 Выручка",f"{rev:,.0f} ₽")
        with c2: st.metric("🧾 Заказов",f"{n:,}")
        with c3: st.metric("🧾 Чеков",f"{n_checks:,}")
        with c4: st.metric("📊 Ср. чек",f"{avg_c:,.0f} ₽")
        with c5: st.metric("👥 Гостей",f"{guests:,}")
        with c6: st.metric("🍽️ Блюд",f"{dishes_n:,}")
        st.divider()
        cl,cr = st.columns([2,1])
        with cl:
            if (date_to-date_from).days<=1:
                h=load_hourly(date_from,date_to)
                if not h.empty:
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=h["HOUR"],y=h["REVENUE"],name="Выручка",marker_color="#00ff6a"))
                    fig.add_trace(go.Scatter(x=h["HOUR"],y=h["ORDER_COUNT"],name="Заказы",yaxis="y2",mode="lines+markers",line=dict(color="#f59e0b",width=3)))
                    fig.update_layout(title="По часам",yaxis=dict(title="₽"),yaxis2=dict(title="Шт",side="right",overlaying="y"),height=400,legend=dict(orientation="h",y=1.1),**CHART_THEME)
                    st.plotly_chart(fig,use_container_width=True)
            else:
                d=load_daily(date_from,date_to)
                if not d.empty:
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=d["DAY"],y=d["REVENUE"],name="Выручка",marker_color="#00ff6a"))
                    fig.add_trace(go.Scatter(x=d["DAY"],y=d["AVG_CHECK"],name="Ср.чек",yaxis="y2",mode="lines+markers",line=dict(color="#10b981",width=3)))
                    fig.update_layout(title="По дням",yaxis=dict(title="₽"),yaxis2=dict(title="Ср.чек",side="right",overlaying="y"),height=400,legend=dict(orientation="h",y=1.1),**CHART_THEME)
                    st.plotly_chart(fig,use_container_width=True)
        with cr:
            if guests>0:
                st.markdown("### 👥 На гостя")
                st.metric("Ср. чек/гость",f"{rev/guests:,.0f} ₽")
                st.metric("Гостей/заказ",f"{guests/n:.1f}" if n else "—")
                st.metric("Блюд/заказ",f"{dishes_n/n:.1f}" if n else "—")

        # --- Питание сотрудников ---
        staff_meals = run_query("""
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
            st.divider()
            st.markdown("### 🍴 Питание сотрудников")
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1: st.metric("🍴 Сумма", f"{sm_total:,.0f} ₽")
            with sc2: st.metric("🧾 Транзакций", f"{sm_cnt:,}")
            with sc3: st.metric("📊 % от выручки", f"{sm_pct:.1f}%")
            with sc4: st.metric("Ø Чек", f"{sm_total/sm_cnt:,.0f} ₽" if sm_cnt > 0 else "—")

            # Детализация по сотрудникам и столовым
            with st.expander("📋 Подробнее — кто, где и сколько", expanded=False):
                staff_detail = run_query("""
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
                    st.markdown("#### 👤 По сотрудникам")
                    by_emp = staff_detail.groupby("EMPLOYEE").agg(
                        MEALS=("MEALS", "sum"),
                        TOTAL_SUM=("TOTAL_SUM", "sum"),
                        RESTAURANTS=("RESTAURANT", lambda x: ", ".join(sorted(x.unique()))),
                        REST_COUNT=("RESTAURANT", "nunique"),
                    ).reset_index().sort_values("TOTAL_SUM", ascending=False)
                    by_emp["AVG_CHECK"] = (by_emp["TOTAL_SUM"] / by_emp["MEALS"]).round(0).astype(int)

                    top_emp = by_emp.head(20)
                    fig = px.bar(top_emp, x="TOTAL_SUM", y="EMPLOYEE", orientation="h",
                        title="💰 Топ-20 по сумме питания",
                        color="MEALS", color_continuous_scale="YlOrRd",
                        text=top_emp["TOTAL_SUM"].apply(lambda x: f"{x:,}₽"),
                        labels={"TOTAL_SUM": "Сумма, ₽", "EMPLOYEE": "Сотрудник", "MEALS": "Раз"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_emp)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    disp_emp = by_emp.rename(columns={
                        "EMPLOYEE": "Сотрудник", "MEALS": "Раз",
                        "TOTAL_SUM": "Сумма ₽", "AVG_CHECK": "Ø Чек ₽",
                        "REST_COUNT": "Столовых", "RESTAURANTS": "Где ел(а)"})
                    st.dataframe(disp_emp, use_container_width=True, hide_index=True, height=400)

                    # Сводка по столовым
                    st.divider()
                    st.markdown("#### 🏢 По столовым")
                    by_rest = staff_detail.groupby("RESTAURANT").agg(
                        MEALS=("MEALS", "sum"),
                        TOTAL_SUM=("TOTAL_SUM", "sum"),
                        EMPLOYEES=("EMPLOYEE", "nunique"),
                    ).reset_index().sort_values("TOTAL_SUM", ascending=False)

                    fig2 = px.bar(by_rest, x="TOTAL_SUM", y="RESTAURANT", orientation="h",
                        title="🏢 Питание сотрудников по столовым",
                        color="EMPLOYEES", color_continuous_scale="Viridis",
                        text=by_rest["TOTAL_SUM"].apply(lambda x: f"{x:,}₽"),
                        labels={"TOTAL_SUM": "Сумма, ₽", "RESTAURANT": "Столовая", "EMPLOYEES": "Сотрудников"})
                    fig2.update_traces(textposition="auto")
                    fig2.update_layout(height=max(350, len(by_rest)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig2, use_container_width=True)

                    disp_rest = by_rest.rename(columns={
                        "RESTAURANT": "Столовая", "MEALS": "Раз",
                        "TOTAL_SUM": "Сумма ₽", "EMPLOYEES": "Сотрудников"})
                    st.dataframe(disp_rest, use_container_width=True, hide_index=True)

                    # Полная детализация
                    st.divider()
                    st.markdown("#### 📋 Полная таблица")
                    disp_full = staff_detail.rename(columns={
                        "EMPLOYEE": "Сотрудник", "RESTAURANT": "Столовая",
                        "MEALS": "Раз", "TOTAL_SUM": "Сумма ₽", "AVG_CHECK": "Ø Чек ₽",
                        "FIRST_MEAL": "Первый", "LAST_MEAL": "Последний"})
                    st.dataframe(disp_full, use_container_width=True, hide_index=True, height=400)
                    st.download_button("📥 CSV питание сотрудников", staff_detail.to_csv(index=False).encode("utf-8"),
                        "staff_meals.csv", "text/csv", use_container_width=True)
                else:
                    st.info("Нет детализации по сотрудникам")

# --- СЕЗОННОСТЬ ---
if page == "📅 Сезонность":
    page_header("Сезонность: год к году", "📅")
    st.caption("Помесячное сравнение выручки — текущий год vs прошлый")

    yoy = load_monthly_revenue_yoy()
    if yoy.empty:
        st.warning("⚠️ Нет данных по выручке")
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
                cur_year = st.selectbox("Текущий год:", sorted(years, reverse=True), index=0, key="season_cur")
            with cr:
                prev_options = [y for y in years if y < cur_year]
                if prev_options:
                    prev_year = st.selectbox("Сравнить с:", sorted(prev_options, reverse=True), index=0, key="season_prev")
                else:
                    prev_year = None
                    st.info("Нет данных за предыдущие годы")

        cur_data = yoy[yoy["Y"] == cur_year].set_index("M")
        prev_data = yoy[yoy["Y"] == prev_year].set_index("M") if prev_year else pd.DataFrame()

        # Сводные метрики за год
        cur_total = cur_data["REVENUE"].sum() if not cur_data.empty else 0
        prev_total = prev_data["REVENUE"].sum() if not prev_data.empty else 0
        yoy_diff_pct = ((cur_total / prev_total - 1) * 100) if prev_total > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric(f"💰 {cur_year}", f"{cur_total:,.0f} ₽")
        with c2:
            if prev_year:
                st.metric(f"💰 {prev_year}", f"{prev_total:,.0f} ₽")
        with c3:
            if prev_year and prev_total > 0:
                diff = cur_total - prev_total
                st.metric("📊 Разница", f"{diff:+,.0f} ₽", f"{yoy_diff_pct:+.1f}%")
        with c4:
            n_months_cur = len(cur_data)
            st.metric("📅 Месяцев", f"{n_months_cur}")

        st.divider()

        # =============== ПЛАШКИ ПО МЕСЯЦАМ ===============
        st.markdown(f"### 📅 Помесячно: {cur_year} vs {prev_year or '—'}")

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
        st.markdown("### 📊 Динамика по месяцам")

        # Подготовка данных для графика
        chart_rows = []
        for m in range(1, max_month_cur + 1 if cur_year == now.year else 13):
            cur_rev = float(cur_data.loc[m, "REVENUE"]) if m in cur_data.index else 0
            prev_rev = float(prev_data.loc[m, "REVENUE"]) if not prev_data.empty and m in prev_data.index else 0
            chart_rows.append({"Месяц": MONTH_SHORT[m], "M": m,
                               str(cur_year): cur_rev, str(prev_year): prev_rev if prev_year else 0})
        chart_df = pd.DataFrame(chart_rows)

        # Bar chart — рядом
        fig = go.Figure()
        if prev_year:
            fig.add_trace(go.Bar(name=str(prev_year), x=chart_df["Месяц"],
                y=chart_df[str(prev_year)], marker_color="rgba(0,255,106,0.3)",
                text=chart_df[str(prev_year)].apply(lambda x: f"{x/1e6:.1f}М" if x > 0 else ""),
                textposition="auto"))
        fig.add_trace(go.Bar(name=str(cur_year), x=chart_df["Месяц"],
            y=chart_df[str(cur_year)], marker_color="#00ff6a",
            text=chart_df[str(cur_year)].apply(lambda x: f"{x/1e6:.1f}М" if x > 0 else ""),
            textposition="auto"))
        fig.update_layout(barmode="group", title=f"📊 Выручка по месяцам: {cur_year} vs {prev_year or '—'}",
            height=450, **CHART_THEME)
        st.plotly_chart(fig, use_container_width=True)

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
                colors = ["#4caf50" if v > 0 else "#f44336" for v in pct_df["Изменение %"]]
                fig2 = go.Figure(go.Bar(x=pct_df["Месяц"], y=pct_df["Изменение %"],
                    marker_color=colors,
                    text=pct_df["Изменение %"].apply(lambda x: f"{x:+.1f}%"),
                    textposition="auto"))
                fig2.add_hline(y=0, line_dash="dot", line_color="#555")
                fig2.update_layout(title=f"📈 Изменение выручки {cur_year} vs {prev_year} (%)",
                    height=350, **CHART_THEME,
                    yaxis_title="Изменение, %")
                st.plotly_chart(fig2, use_container_width=True)

        # Таблица
        st.divider()
        st.markdown("### 📋 Таблица")
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
if page == "🍕 Блюда":
    page_header("Топ блюд", "🍕")
    ds=load_dishes(date_from,date_to)
    if not ds.empty:
        cl,cr=st.columns(2)
        with cl:
            fig=px.bar(ds.head(15),x="TOTAL_SUM",y="DISH_NAME",orientation="h",title="🏆 По выручке",color="TOTAL_SUM",color_continuous_scale="Viridis", labels=RU)
            fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
            st.plotly_chart(fig,use_container_width=True)
        with cr:
            tq=ds.sort_values("TOTAL_QTY",ascending=False).head(15)
            fig=px.bar(tq,x="TOTAL_QTY",y="DISH_NAME",orientation="h",title="🔥 По количеству",color="TOTAL_QTY",color_continuous_scale="Inferno", labels=RU)
            fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
            st.plotly_chart(fig,use_container_width=True)
        st.dataframe(ds.rename(columns={"DISH_ID":"ID","DISH_NAME":"Блюдо","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка","ORDER_COUNT":"Заказов"}),use_container_width=True,hide_index=True)
    else: st.info("Нет данных")

# --- СТОЛОВЫЕ ---
if page == "🏢 Столовые":
    page_header("Столовые", "🏢")
    rest_data = load_revenue_by_restaurant(date_from, date_to)
    if not rest_data.empty:
        total_rev = float(rest_data["REVENUE"].sum())
        total_ord = int(rest_data["ORDER_COUNT"].sum())
        total_guests = int(rest_data["GUESTS"].sum())
        active_rest = len(rest_data)

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric("🏢 Точек работает", f"{active_rest}")
        with c2: st.metric("💰 Общая выручка", f"{total_rev:,.0f} ₽")
        with c3: st.metric("🧾 Заказов", f"{total_ord:,}")
        with c4: st.metric("👥 Гостей", f"{total_guests:,}")
        st.divider()

        # Выручка по столовым
        fig = px.bar(rest_data, x="REVENUE", y="REST_NAME", orientation="h",
            title="💰 Выручка по точкам", color="REVENUE", color_continuous_scale="Viridis",
            hover_data={"ORDER_COUNT":True, "AVG_CHECK":":.0f", "GUESTS":True}, labels=RU)
        fig.update_layout(height=max(400, len(rest_data)*35), yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
        st.plotly_chart(fig, use_container_width=True)

        # Средний чек по столовым
        cl,cr = st.columns(2)
        with cl:
            fig = px.bar(rest_data.sort_values("AVG_CHECK",ascending=False), x="AVG_CHECK", y="REST_NAME",
                orientation="h", title="📊 Средний чек по точкам", color="AVG_CHECK", color_continuous_scale="Tealgrn", labels=RU)
            fig.update_layout(height=max(400,len(rest_data)*30), yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
        with cr:
            fig = px.pie(rest_data.head(10), values="REVENUE", names="REST_NAME", title="🥧 Доля выручки (топ-10)", hole=0.4, labels=RU)
            fig.update_layout(height=400, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # Динамика по столовым
        if (date_to-date_from).days > 1:
            daily_rest = load_daily_by_restaurant(date_from, date_to)
            if not daily_rest.empty:
                top5 = rest_data.head(5)["REST_NAME"].tolist()
                dr_top = daily_rest[daily_rest["REST_NAME"].isin(top5)]
                fig = px.line(dr_top, x="DAY", y="REVENUE", color="REST_NAME",
                    title="📈 Динамика выручки (топ-5 точек)", markers=True, labels=RU)
                fig.update_layout(height=400, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

        st.dataframe(rest_data.rename(columns={"REST_ID":"ID","REST_NAME":"Точка","ORDER_COUNT":"Заказов",
            "REVENUE":"Выручка","GUESTS":"Гостей","AVG_CHECK":"Ср.чек","DISHES":"Блюд"}),
            use_container_width=True, hide_index=True)
    else: st.info("Нет данных по столовым")

# --- КАТЕГОРИИ ---
if page == "🗂️ Категории":
    page_header("Категории", "🗂️")
    cat_data = load_revenue_by_category(date_from, date_to)
    if not cat_data.empty:
        cl,cr = st.columns(2)
        with cl:
            fig = px.bar(cat_data.head(15), x="TOTAL_SUM", y="CATEGORY", orientation="h",
                title="💰 Выручка по категориям", color="TOTAL_SUM", color_continuous_scale="Sunset", labels=RU)
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
        with cr:
            fig = px.pie(cat_data.head(10), values="TOTAL_SUM", names="CATEGORY",
                title="🥧 Доля категорий", hole=0.35, labels=RU)
            fig.update_layout(height=500, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cat_data.rename(columns={"CAT_ID":"ID","CATEGORY":"Категория","TOTAL_QTY":"Кол-во",
            "TOTAL_SUM":"Выручка","ORDER_COUNT":"Заказов"}), use_container_width=True, hide_index=True)

        # === Drill-down: топ блюд в категории ===
        st.divider()
        st.markdown("### 🔍 Топ блюд в категории")
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
            with c2: st.metric("💰 Выручка", f"{dishes_in_cat['TOTAL_SUM'].sum():,.0f} ₽")
            with c3: st.metric("📦 Продано шт.", f"{dishes_in_cat['TOTAL_QTY'].sum():,.0f}")

            fig_d = px.bar(dishes_in_cat.head(20), x="TOTAL_SUM", y="DISH_NAME", orientation="h",
                title=f"🏆 Топ блюд — {selected_cat_name}",
                color="TOTAL_SUM", color_continuous_scale="Viridis",
                text=dishes_in_cat.head(20)["TOTAL_SUM"].apply(lambda x: f"{x:,.0f}₽"),
                labels={"TOTAL_SUM": "Выручка, ₽", "DISH_NAME": "Блюдо"})
            fig_d.update_traces(textposition="auto")
            fig_d.update_layout(height=max(400, len(dishes_in_cat.head(20))*25),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig_d, use_container_width=True)

            disp_d = dishes_in_cat.rename(columns={
                "DISH_NAME":"Блюдо","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка ₽",
                "AVG_PRICE":"Ø Цена ₽","ORDER_COUNT":"Заказов"})
            st.dataframe(disp_d, use_container_width=True, hide_index=True)
        else:
            st.info(f"Нет блюд в категории «{selected_cat_name}»")
    else: st.info("Нет данных по категориям")

# --- ПЕРСОНАЛ ---
if page == "👨‍🍳 Персонал":
    page_header("Персонал", "👨‍🍳")
    sub1,sub2 = st.tabs(["📊 Выручка кассиров","⏰ Рабочее время"])

    with sub1:
        emp = load_top_employees(date_from, date_to)
        if not emp.empty:
            c1,c2,c3 = st.columns(3)
            with c1: st.metric("👥 Работало", f"{len(emp)}")
            with c2: st.metric("🏆 Лучший", emp.iloc[0]["EMP_NAME"] if emp.iloc[0]["EMP_NAME"] else "—")
            with c3: st.metric("💰 Макс. выручка", f'{float(emp.iloc[0]["REVENUE"]):,.0f} ₽')

            fig = px.bar(emp.head(15), x="REVENUE", y="EMP_NAME", orientation="h",
                title="🏆 Топ кассиров по выручке", color="REVENUE", color_continuous_scale="Viridis",
                hover_data={"ORDER_COUNT":True,"AVG_CHECK":":.0f"}, labels=RU)
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(emp.rename(columns={"EMP_ID":"ID","EMP_NAME":"Сотрудник","ORDER_COUNT":"Заказов",
                "REVENUE":"Выручка","AVG_CHECK":"Ср.чек","GUESTS":"Гостей"}), use_container_width=True, hide_index=True)
        else: st.info("Нет данных")

    with sub2:
        clock = load_clockrecs(date_from, date_to)
        if not clock.empty:
            total_shifts = int(clock["SHIFT_COUNT"].sum())
            total_late = int(clock["LATE_COUNT"].sum())
            late_pct = total_late / total_shifts * 100 if total_shifts else 0

            c1,c2,c3 = st.columns(3)
            with c1: st.metric("📅 Смен всего", f"{total_shifts}")
            with c2: st.metric("⏰ Опозданий", f"{total_late}")
            with c3: st.metric("📉 % опозданий", f"{late_pct:.1f}%")

            # Опоздания
            late_emp = clock[clock["LATE_COUNT"]>0].sort_values("LATE_COUNT", ascending=False).head(15)
            if not late_emp.empty:
                fig = px.bar(late_emp, x="LATE_COUNT", y="EMP_NAME", orientation="h",
                    title="⏰ Топ по опозданиям", color="LATE_COUNT", color_continuous_scale="Reds", labels=RU)
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

            st.dataframe(clock.rename(columns={"EMPID":"ID","EMP_NAME":"Сотрудник","SHIFT_COUNT":"Смен",
                "AVG_HOURS":"Ср.часов","LATE_COUNT":"Опозданий"}), use_container_width=True, hide_index=True)
        else: st.info("Нет данных по рабочему времени")

# --- КАССА ---
if page == "💰 Касса":
    page_header("Касса", "💰")
    cash = load_cashinout(date_from, date_to)
    if not cash.empty:
        deposits = cash[cash["ISDEPOSIT"]==1]
        collections = cash[cash["ISDEPOSIT"]==0]
        dep_sum = float(deposits["ORIGINALSUM"].sum()) if not deposits.empty else 0
        col_sum = float(collections["ORIGINALSUM"].abs().sum()) if not collections.empty else 0

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric("📥 Внесений", f"{len(deposits)}")
        with c2: st.metric("💵 Сумма внесений", f"{dep_sum:,.0f} ₽")
        with c3: st.metric("📤 Изъятий", f"{len(collections)}")
        with c4: st.metric("💸 Сумма изъятий", f"{col_sum:,.0f} ₽")

        # По дням
        cash_copy = cash.copy()
        cash_copy["DAY"] = pd.to_datetime(cash_copy["DATETIME"]).dt.date
        cash_copy["ABS_SUM"] = cash_copy["ORIGINALSUM"].abs()
        cash_copy["TYPE"] = cash_copy["ISDEPOSIT"].map({1:"Внесение",0:"Изъятие"})
        daily_cash = cash_copy.groupby(["DAY","TYPE"]).agg(SUM=("ABS_SUM","sum"),COUNT=("ORIGINALSUM","count")).reset_index()

        if not daily_cash.empty:
            fig = px.bar(daily_cash, x="DAY", y="SUM", color="TYPE", barmode="group",
                title="📊 Внесения и изъятия по дням",
                color_discrete_map={"Внесение":"#10b981","Изъятие":"#ef4444"}, labels=RU)
            fig.update_layout(height=400, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # По причинам
        kind_map = {0:"Вручную",1:"Программой",3:"Закрытие смены",2:"Чаевые",4:"Закр.общей смены",5:"Откр.общей смены",6:"Пополнение карты"}
        cash_copy["KIND_NAME"] = cash_copy["KIND"].map(kind_map).fillna("Другое")
        by_kind = cash_copy.groupby("KIND_NAME").agg(SUM=("ABS_SUM","sum"),COUNT=("ORIGINALSUM","count")).reset_index().sort_values("SUM",ascending=False)
        if not by_kind.empty:
            fig = px.pie(by_kind, values="SUM", names="KIND_NAME", title="По типам операций", hole=0.4, labels=RU)
            fig.update_layout(height=400, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
    else: st.info("Нет кассовых операций")

# --- ЦЕНЫ ---
if page == "📊 Цены":
    page_header("Анализ цен", "📊")
    st.markdown("### 📊 Анализ изменения цен")
    st.caption("Показаны только блюда, у которых цена менялась за период")
    prices = load_current_prices(date_from, date_to)
    if not prices.empty:
        c1,c2,c3 = st.columns(3)
        with c1: st.metric("🔄 Блюд с изменённой ценой", f"{len(prices)}")
        with c2: st.metric("📈 Макс. разница", f'{float(prices["PRICE_DIFF"].max()):,.0f} ₽')
        with c3: st.metric("📊 Ср. разница", f'{float(prices["PRICE_DIFF"].mean()):,.0f} ₽')
        st.divider()

        # Топ по разнице цен
        cl,cr = st.columns(2)
        with cl:
            top_diff = prices.head(15)
            fig = px.bar(top_diff, x="PRICE_DIFF", y="DISH_NAME", orientation="h",
                title="📈 Наибольший рост цены", color="PRICE_DIFF", color_continuous_scale="Reds",
                hover_data={"MIN_PRICE":True,"MAX_PRICE":True,"PRICE_VARIANTS":True}, labels=RU)
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
        with cr:
            fig = px.bar(top_diff, x="PRICE_VARIANTS", y="DISH_NAME", orientation="h",
                title="🔢 Количество вариантов цен", color="PRICE_VARIANTS", color_continuous_scale="Viridis",
                hover_data={"MIN_PRICE":True,"MAX_PRICE":True}, labels=RU)
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # Min vs Max (диапазон)
        fig = go.Figure()
        top20 = prices.head(20)
        fig.add_trace(go.Bar(x=top20["DISH_NAME"], y=top20["MIN_PRICE"], name="Мин. цена", marker_color="#00ff6a"))
        fig.add_trace(go.Bar(x=top20["DISH_NAME"], y=top20["PRICE_DIFF"], name="Разница", marker_color="#ef4444"))
        fig.update_layout(title="📊 Диапазон цен (мин + разница = макс)", barmode="stack",
            height=400, xaxis_tickangle=-45, **CHART_THEME)
        st.plotly_chart(fig, use_container_width=True)

        # История цен
        if (date_to - date_from).days > 1:
            st.divider()
            st.markdown("#### 📈 История цен по блюдам")
            ph = load_price_history(date_from, date_to)
            if not ph.empty:
                top_dishes = prices.head(10)["DISH_NAME"].tolist()
                dish_select = st.multiselect("Выберите блюда:", top_dishes, default=top_dishes[:3])
                if dish_select:
                    ph_filt = ph[ph["DISH_NAME"].isin(dish_select)]
                    if not ph_filt.empty:
                        fig = px.line(ph_filt, x="DAY", y="AVG_PRICE", color="DISH_NAME",
                            title="Динамика цен", markers=True, labels={"AVG_PRICE":"Цена, ₽","DAY":"Дата"})
                        fig.update_layout(height=400, **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        disp = prices.copy()
        disp.columns = ["Блюдо","Ср.цена","Мин","Макс","Вариантов цен","Кол-во","Выручка","Разница"]
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else: st.info("Нет данных по ценам")

# --- ABC ---
if page == "🔤 ABC":
    page_header("ABC-анализ", "🔤")
    st.markdown("### 🔤 ABC-анализ блюд")
    st.caption("A = 80% выручки (звёзды), B = 15% (середнячки), C = 5% (кандидаты на вылет)")
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

        c1,c2,c3 = st.columns(3)
        with c1:
            st.metric("🅰️ Группа A", f"{a_count} блюд")
            st.caption(f"{a_rev:,.0f} ₽ ({a_rev/total_rev*100:.0f}% выручки)")
        with c2:
            st.metric("🅱️ Группа B", f"{b_count} блюд")
            st.caption(f"{b_rev:,.0f} ₽ ({b_rev/total_rev*100:.0f}% выручки)")
        with c3:
            st.metric("🅲 Группа C", f"{c_count} блюд")
            st.caption(f"{c_rev:,.0f} ₽ ({c_rev/total_rev*100:.0f}% выручки)")
        st.divider()

        # ABC визуализация
        cl, cr = st.columns(2)
        with cl:
            # Пирог A/B/C
            pie_data = pd.DataFrame({
                "Группа": [f"A — {a_count} блюд", f"B — {b_count} блюд", f"C — {c_count} блюд"],
                "Выручка": [a_rev, b_rev, c_rev]
            })
            fig = px.pie(pie_data, values="Выручка", names="Группа",
                title="Доля выручки по группам ABC", hole=0.4,
                color="Группа", color_discrete_map={
                    f"A — {a_count} блюд": "#10b981",
                    f"B — {b_count} блюд": "#f59e0b",
                    f"C — {c_count} блюд": "#ef4444"}, labels=RU)
            fig.update_layout(height=400, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        with cr:
            # Сводка по группам
            summary = pd.DataFrame({
                "Группа": ["A — звёзды", "B — середнячки", "C — на вылет"],
                "Блюд": [a_count, b_count, c_count],
                "Выручка": [f"{a_rev:,.0f} ₽", f"{b_rev:,.0f} ₽", f"{c_rev:,.0f} ₽"],
                "Доля": [f"{a_rev/total_rev*100:.0f}%", f"{b_rev/total_rev*100:.0f}%", f"{c_rev/total_rev*100:.0f}%"]
            })
            st.dataframe(summary, use_container_width=True, hide_index=True)

        # Переключатель групп — на полную ширину
        st.divider()
        abc_grp_view = st.radio("Блюда по группам:", ["A — звёзды","B — середнячки","C — кандидаты на вылет"],
            horizontal=True, key="abc_grp_radio")
        grp_letter = abc_grp_view[0]
        grp_data = abc[abc["ABC"]==grp_letter].copy()
        grp_rev = float(grp_data["TOTAL_SUM"].sum())
        if not grp_data.empty:
            show = grp_data.head(30)
            fig = px.bar(show, x="TOTAL_SUM", y="DISH_NAME", orientation="h",
                title=f"Группа {grp_letter}: {len(grp_data)} блюд · {grp_rev:,.0f} ₽ · {grp_rev/total_rev*100:.0f}% выручки",
                color="TOTAL_SUM", color_continuous_scale={
                    "A":"Tealgrn","B":"YlOrBr","C":"OrRd"}[grp_letter],
                text=show["TOTAL_SUM"].apply(lambda x: f"{x:,.0f} ₽"), labels=RU)
            fig.update_traces(textposition="auto")
            fig.update_layout(height=max(400, min(30, len(show))*28),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"Нет блюд в группе {grp_letter}")

        # Распределение по категориям
        if "CATEGORY" in abc.columns:
            abc_cat = abc.groupby(["CATEGORY","ABC"]).agg(
                COUNT=("DISH_NAME","count"), SUM=("TOTAL_SUM","sum")).reset_index()
            abc_cat = abc_cat[abc_cat["CATEGORY"].notna()]
            if not abc_cat.empty:
                fig = px.bar(abc_cat, x="CATEGORY", y="COUNT", color="ABC",
                    title="ABC по категориям",
                    color_discrete_map={"A":"#10b981","B":"#f59e0b","C":"#ef4444"},
                    barmode="stack", labels=RU)
                fig.update_layout(height=400, xaxis_tickangle=-45, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

        # Фильтр по группам
        st.divider()
        abc_filter = st.selectbox("Показать группу:", ["Все","A","B","C"])
        abc_disp = abc if abc_filter=="Все" else abc[abc["ABC"]==abc_filter]
        disp = abc_disp[["DISH_NAME","CATEGORY","TOTAL_QTY","TOTAL_SUM","AVG_PRICE","ABC","CUM_PCT"]].copy()
        disp.columns = ["Блюдо","Категория","Кол-во","Выручка","Ср.цена","Группа","Накоп.%"]
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else: st.info("Нет данных для ABC-анализа")

# --- СКОРОСТЬ ---
if page == "⏱️ Скорость":
    page_header("Скорость обслуживания", "⏱️")
    st.caption("Время от создания заказа до закрытия после оплаты")

    speed = load_cashier_speed(date_from, date_to)
    dist = load_speed_distribution(date_from, date_to)

    if not speed.empty:
        avg_all = float(speed["AVG_SEC"].mean())
        fastest = speed.iloc[0]["CASHIER"] if speed.iloc[0]["CASHIER"] else "—"
        fastest_sec = float(speed.iloc[0]["AVG_SEC"])
        slowest = speed.iloc[-1]["CASHIER"] if speed.iloc[-1]["CASHIER"] else "—"
        slowest_sec = float(speed.iloc[-1]["AVG_SEC"])

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric("⏱️ Среднее время", f"{avg_all:.0f} сек")
        with c2: st.metric("🏆 Самый быстрый", f"{fastest}", delta=f"{fastest_sec:.0f} сек")
        with c3: st.metric("🐢 Самый медленный", f"{slowest}", delta=f"{slowest_sec:.0f} сек", delta_color="inverse")
        with c4: st.metric("👥 Кассиров", f"{len(speed)}")
        st.divider()

        # Распределение времени
        if not dist.empty:
            dist_clean = dist.copy()
            dist_clean["LABEL"] = dist_clean["TIME_RANGE"].str[4:]  # убираем 01. 02. ...
            fig = px.bar(dist_clean, x="LABEL", y="ORDER_COUNT",
                title="📊 Распределение заказов по времени обслуживания",
                color="ORDER_COUNT", color_continuous_scale="RdYlGn_r",
                text="ORDER_COUNT", labels={"LABEL":"Время","ORDER_COUNT":"Заказов"})
            fig.update_traces(textposition="auto")
            fig.update_layout(height=400, coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # Кассиры
        cl, cr = st.columns(2)
        with cl:
            fig = px.bar(speed, x="AVG_SEC", y="CASHIER", orientation="h",
                title="⏱️ Среднее время по кассирам (сек)",
                color="AVG_SEC", color_continuous_scale="RdYlGn_r",
                hover_data={"ORDERS":True, "AVG_DISHES":":.1f", "AVG_CHECK":":.0f"}, labels=RU)
            fig.update_layout(height=max(400, len(speed)*30), yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        with cr:
            # Скорость vs Выручка (scatter)
            fig = px.scatter(speed, x="AVG_SEC", y="REVENUE", size="ORDERS",
                hover_name="CASHIER", title="💡 Скорость vs Выручка",
                labels={"AVG_SEC":"Ср. время, сек","REVENUE":"Выручка, ₽"},
                color="AVG_DISHES", color_continuous_scale="Viridis")
            fig.update_layout(height=max(400, len(speed)*30), coloraxis_colorbar_title="Ср.блюд", **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # По часам
        speed_hour = load_speed_by_hour(date_from, date_to)
        if not speed_hour.empty:
            st.divider()
            fig = go.Figure()
            fig.add_trace(go.Bar(x=speed_hour["HOUR"], y=speed_hour["AVG_SEC"],
                name="Ср. время (сек)", marker_color="#f59e0b"))
            fig.add_trace(go.Scatter(x=speed_hour["HOUR"], y=speed_hour["ORDERS"],
                name="Заказов", yaxis="y2", mode="lines+markers",
                line=dict(color="#00ff6a", width=3)))
            fig.update_layout(title="⏰ Скорость по часам дня",
                yaxis=dict(title="Ср. время, сек"), yaxis2=dict(title="Заказов", side="right", overlaying="y"),
                height=400, legend=dict(orientation="h", y=1.1), **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # По столовым
        speed_rest = load_speed_by_restaurant(date_from, date_to)
        if not speed_rest.empty:
            st.divider()
            fig = px.bar(speed_rest, x="AVG_SEC", y="REST_NAME", orientation="h",
                title="🏢 Скорость по точкам", color="AVG_SEC", color_continuous_scale="RdYlGn_r",
                hover_data={"ORDERS":True, "AVG_DISHES":":.1f"}, labels=RU)
            fig.update_layout(height=max(400, len(speed_rest)*30),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

        # Таблица
        st.divider()
        disp = speed.copy()
        disp["AVG_MIN"] = disp["AVG_SEC"].apply(lambda x: f"{x/60:.1f} мин" if x >= 60 else f"{x:.0f} сек")
        disp_show = disp[["CASHIER","ORDERS","AVG_MIN","AVG_SEC","MIN_SEC","MAX_SEC","AVG_DISHES","REVENUE","AVG_CHECK"]].copy()
        disp_show.columns = ["Кассир","Заказов","Ср.время","Сек","Мин.сек","Макс.сек","Ср.блюд","Выручка","Ср.чек"]
        st.dataframe(disp_show, use_container_width=True, hide_index=True)
    else:
        st.info("Нет данных по скорости")

# --- СМЕНЫ ---
if page == "🕐 Смены":
    page_header("Смены по столовым", "🕐")
    shifts = load_shifts(date_from, date_to)
    if not shifts.empty:
        total_shifts = len(shifts)
        open_now = len(shifts[shifts["CLOSED"]==0])
        avg_hours = float(shifts[shifts["DURATION_MIN"]>0]["DURATION_MIN"].mean())/60 if len(shifts[shifts["DURATION_MIN"]>0]) else 0

        c1,c2,c3 = st.columns(3)
        with c1: st.metric("📅 Смен за период", f"{total_shifts}")
        with c2: st.metric("🟢 Открыто сейчас", f"{open_now}")
        with c3: st.metric("⏱️ Ср. длительность", f"{avg_hours:.1f} ч")
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
            title="📊 Количество смен по точкам", color="AVG_HOURS",
            color_continuous_scale="RdYlGn", hover_data={"AVG_HOURS":":.1f"}, labels=RU)
        fig.update_layout(height=max(400,len(by_rest)*28), yaxis=dict(autorange="reversed"),
            coloraxis_colorbar_title="Ср.часов", **CHART_THEME)
        st.plotly_chart(fig, use_container_width=True)

        # Таблица смен
        st.divider()
        disp = shifts.copy()
        disp["СТАТУС"] = disp["CLOSED"].map({0:"🟢 Открыта", 1:"🔴 Закрыта"})
        disp["ЧАСОВ"] = (disp["DURATION_MIN"] / 60).round(1)
        disp_show = disp[["SHIFTDATE","CREATETIME","CLOSETIME","MANAGER","REST_NAME","СТАТУС","ЧАСОВ"]].copy()
        disp_show.columns = ["Дата смены","Открытие","Закрытие","Менеджер","Точка","Статус","Часов"]
        st.dataframe(disp_show, use_container_width=True, hide_index=True, height=500)
    else:
        st.info("Нет данных по сменам")

# --- ПРОБЛЕМЫ ---
if page == "⚠️ Проблемы":
    page_header("Проблемы: карты, маркировка, чеки", "⚠️")
    
    sub_mark, sub_cards, sub_fiscal = st.tabs(["🏷️ Честный знак", "💳 Проблемы с картами", "🧾 Фискализация"])

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
        | ⏱️ **3** | Таймаут (сервер не ответил вовремя) | Проблема с интернетом или нагрузкой сервиса |
        """)
        st.divider()

        stats = load_checkmark_stats(date_from, date_to)
        if not stats.empty:
            res_map = {0:"🟢 ОК", 1:"🔴 Ошибка", 2:"🟡 Предупреждение", 3:"⏱️ Таймаут"}
            stats["STATUS"] = stats["RES"].map(res_map).fillna("Неизвестно")
            total = int(stats["CNT"].sum())
            ok_cnt = int(stats[stats["RES"]==0]["CNT"].sum()) if 0 in stats["RES"].values else 0
            err_cnt = int(stats[stats["RES"]==1]["CNT"].sum()) if 1 in stats["RES"].values else 0
            timeout_cnt = int(stats[stats["RES"]==3]["CNT"].sum()) if 3 in stats["RES"].values else 0
            err_pct = (total - ok_cnt) / total * 100 if total else 0

            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric("📊 Всего проверок", f"{total:,}")
            with c2: st.metric("🟢 Успешных", f"{ok_cnt:,}")
            with c3: st.metric("🔴 Ошибок", f"{err_cnt:,}")
            with c4: st.metric("⏱️ Таймаутов", f"{timeout_cnt:,}")

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(stats, values="CNT", names="STATUS", title="Результаты проверок",
                    color_discrete_map={"🟢 ОК":"#10b981","🔴 Ошибка":"#ef4444","🟡 Предупреждение":"#f59e0b","⏱️ Таймаут":"#00ff6a"}, hole=0.4, labels=RU)
                fig.update_layout(height=400, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)
            with cr:
                fig = px.bar(stats[stats["RES"]!=0], x="CNT", y="STATUS", orientation="h",
                    title="Проблемные проверки", color="STATUS",
                    color_discrete_map={"🔴 Ошибка":"#ef4444","🟡 Предупреждение":"#f59e0b","⏱️ Таймаут":"#00ff6a"}, labels=RU)
                fig.update_layout(height=400, showlegend=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

        # Детали ошибок
        errors = load_checkmark_errors(date_from, date_to)
        if not errors.empty:
            st.divider()
            st.markdown("#### 🔍 Детали ошибок")

            # По типам ошибок
            by_msg = errors.groupby("MESSAGEFROMDRIVER").agg(CNT=("RES","count")).reset_index().sort_values("CNT",ascending=False)
            if not by_msg.empty:
                fig = px.bar(by_msg.head(10), x="CNT", y="MESSAGEFROMDRIVER", orientation="h",
                    title="Типы ошибок", color="CNT", color_continuous_scale="Reds", labels=RU)
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

            # По товарам
            by_prod = errors.groupby("PRODUCT").agg(CNT=("RES","count")).reset_index().sort_values("CNT",ascending=False)
            by_prod = by_prod[by_prod["PRODUCT"].notna()]
            if not by_prod.empty:
                fig = px.bar(by_prod.head(15), x="CNT", y="PRODUCT", orientation="h",
                    title="Проблемные товары", color="CNT", color_continuous_scale="Oranges", labels=RU)
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

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
            with c1: st.metric("💳 Всего транзакций", f"{total:,}")
            with c2: st.metric("🟢 Успешных", f"{ok_cnt:,}")
            with c3: st.metric("🔴 Отменённых", f"{cancel_cnt:,}")
            with c4: st.metric("⚪ Без транзакции", f"{no_trans:,}")

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(card_stats, values="CNT", names="STATUS", title="Статусы транзакций", hole=0.4, labels=RU)
                fig.update_layout(height=400, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)
            with cr:
                problem = card_stats[card_stats["TRANSACTIONSTATUS"].isin([0,1,6])]
                if not problem.empty:
                    fig = px.bar(problem, x="CNT", y="STATUS", orientation="h",
                        title="⚠️ Проблемные транзакции", color="CNT", color_continuous_scale="Reds", labels=RU)
                    fig.update_layout(height=300, coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.success("Все транзакции успешны!")

            if cancel_cnt > 0 or no_trans > 0:
                st.warning(f"⚠️ {cancel_cnt} отменённых + {no_trans} без транзакции = потенциальные проблемы с терминалами. Рекомендуется проверить оборудование.")

            # --- Разбивка по столовым ---
            st.divider()
            st.markdown("### 🏢 Проблемы по столовым")
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
                    with c1: st.metric("🏢 Столовых с проблемами", f"{len(by_rest)}")
                    with c2: st.metric("⚠️ Всего проблемных", f"{by_rest['PROBLEM_COUNT'].sum():,}")

                    # Топ столовых по проблемам
                    top_rest = by_rest.head(15)
                    fig = px.bar(top_rest, x="PROBLEM_COUNT", y="REST_NAME", orientation="h",
                        title="⚠️ Столовые с проблемными транзакциями",
                        color="PROBLEM_COUNT", color_continuous_scale="Reds",
                        text="PROBLEM_COUNT",
                        labels={"PROBLEM_COUNT": "Проблемных", "REST_NAME": "Столовая"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_rest)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    # Детализация: столовая → типы проблем
                    st.divider()
                    st.markdown("#### 📋 Детализация по типам проблем")
                    pivot = problems.pivot_table(index="REST_NAME", columns="STATUS",
                        values="CNT", aggfunc="sum", fill_value=0).reset_index()
                    pivot["Всего"] = pivot.select_dtypes(include="number").sum(axis=1)
                    pivot = pivot.sort_values("Всего", ascending=False)
                    pivot = pivot.rename(columns={"REST_NAME": "Столовая"})
                    st.dataframe(pivot, use_container_width=True, hide_index=True)

                    # Суммы
                    if "TOTAL_SUM" in problems.columns and problems["TOTAL_SUM"].notna().sum() > 0:
                        st.divider()
                        by_rest_sum = by_rest[by_rest["PROBLEM_SUM"] > 0].head(15)
                        if not by_rest_sum.empty:
                            fig2 = px.bar(by_rest_sum, x="PROBLEM_SUM", y="REST_NAME", orientation="h",
                                title="💰 Суммы проблемных транзакций по столовым",
                                color="PROBLEM_SUM", color_continuous_scale="YlOrRd",
                                text=by_rest_sum["PROBLEM_SUM"].apply(lambda x: f"{x:,.0f}₽"),
                                labels={"PROBLEM_SUM": "Сумма, ₽", "REST_NAME": "Столовая"})
                            fig2.update_traces(textposition="auto")
                            fig2.update_layout(height=max(400, len(by_rest_sum)*30),
                                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                            st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.success("🟢 Нет проблемных транзакций по столовым!")

                # Полная разбивка (включая успешные)
                with st.expander("📊 Все статусы по столовым"):
                    all_pivot = card_by_rest.pivot_table(index="REST_NAME", columns="STATUS",
                        values="CNT", aggfunc="sum", fill_value=0).reset_index()
                    all_pivot = all_pivot.rename(columns={"REST_NAME": "Столовая"})
                    st.dataframe(all_pivot, use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по столовым")
        else:
            st.info("Нет данных по картам")

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
            with c1: st.metric("🧾 Всего чеков", f"{total:,}")
            with c2: st.metric("✅ Фискализировано", f"{fiscal_ok:,}")
            with c3: st.metric("❌ Не фискализировано", f"{not_fiscal:,}")
            with c4: st.metric("🗑 Удалённых", f"{deleted:,}")
            with c5: st.metric("💰 Сумма", f"{active_sum:,} ₽")

            # Предупреждения
            if not_fiscal > 0:
                st.error(f"🚨 {not_fiscal} чеков НЕ фискализированы! Они не отправлены в ОФД/налоговую.")
            if bill_errors > 0:
                st.warning(f"⚠️ {bill_errors} чеков с ошибками печати (BILLERROR)")
            if corrections > 0:
                st.info(f"📋 {corrections} чеков коррекции")
            if not_fiscal == 0 and bill_errors == 0 and deleted == 0:
                st.success(f"✅ Все {fiscal_ok:,} чеков успешно фискализированы")

            # По столовым
            st.divider()
            st.markdown("### 🏢 По столовым и кассам")
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
                    st.markdown("#### ⚠️ Столовые с проблемами")
                    fig = px.bar(problems, x="NOT_FISCAL", y="REST_NAME", orientation="h",
                        title="❌ Нефискализированные чеки по столовым",
                        color="NOT_FISCAL", color_continuous_scale="Reds",
                        text="NOT_FISCAL",
                        labels={"NOT_FISCAL": "Не фискализировано", "REST_NAME": "Столовая"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(300, len(problems)*30),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                # Полная таблица
                disp = by_rest.rename(columns={
                    "REST_NAME": "Столовая", "TOTAL": "Всего", "ACTIVE": "Активных",
                    "DELETED": "Удалённых", "ERRORS": "Ошибок", "NOT_FISCAL": "Не фискал.",
                    "CORRECTIONS": "Коррекций", "SUM": "Сумма ₽"})
                st.dataframe(disp, use_container_width=True, hide_index=True)

                # Детализация по кассам
                with st.expander("📋 Детализация по кассам"):
                    disp_full = fiscal_detail.rename(columns={
                        "REST_NAME": "Столовая", "CASH_NAME": "Касса",
                        "TOTAL_CHECKS": "Всего", "ACTIVE_CHECKS": "Активных",
                        "DELETED_CHECKS": "Удалённых", "ACTIVE_SUM": "Сумма ₽",
                        "CORRECTIONS": "Коррекций", "BILL_ERRORS": "Ошибок",
                        "NOT_FISCAL": "Не фискал."})
                    st.dataframe(disp_full, use_container_width=True, hide_index=True)
        else:
            st.info("Нет данных по фискальным чекам за период")

# --- ОТКАЗЫ И ОТМЕНЫ ---
if page == "❌ Отказы":
    page_header("Отказы и отмены", "❌")

    sub_voids, sub_checks, sub_ops, sub_payments = st.tabs([
        "🗑️ Удаления блюд", "🧾 Отмены чеков", "⚡ Операции отмен", "💳 Оплаты по типам"
    ])

    # ========== ОТКАЗЫ БЛЮД ==========
    with sub_voids:
        voids = load_voids(date_from, date_to)
        if not voids.empty:
            vs = float(voids["PRLISTSUM"].sum())
            ords = load_orders(date_from, date_to)
            rv = float(ords["TOPAYSUM"].sum()) if not ords.empty else 0

            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric("❌ Всего отказов", f"{len(voids)}")
            with c2: st.metric("💸 Сумма", f"{vs:,.0f} ₽")
            with c3: st.metric("📉 % от выручки", f"{vs/rv*100 if rv else 0:.1f}%")
            with c4: st.metric("🍽️ Ср. сумма отказа", f"{vs/len(voids):,.0f} ₽" if len(voids) else "—")
            st.divider()

            view_mode = st.selectbox("Разрез:", ["По причинам","По кассирам","По точкам","По блюдам","Таблица"], key="void_view")

            if view_mode == "По причинам" and "VOID_REASON" in voids.columns:
                vr = voids.groupby("VOID_REASON").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                vr["VOID_REASON"] = vr["VOID_REASON"].replace("","Без причины")
                if not vr.empty:
                    cl,cr = st.columns(2)
                    with cl:
                        fig = px.bar(vr.head(10),x="SUM",y="VOID_REASON",orientation="h",title="💸 По сумме",color="SUM",color_continuous_scale="Reds", labels=RU)
                        fig.update_layout(height=400,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)
                    with cr:
                        fig = px.pie(vr.head(8),values="COUNT",names="VOID_REASON",title="📊 По количеству",hole=0.4, labels=RU)
                        fig.update_layout(height=400,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)

            elif view_mode == "По кассирам":
                if "CREATOR_NAME" in voids.columns:
                    by_cr = voids.groupby("CREATOR_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                    by_cr = by_cr[by_cr["CREATOR_NAME"].notna()]
                    if not by_cr.empty:
                        st.markdown("#### Кто создаёт отказы")
                        fig = px.bar(by_cr.head(15),x="SUM",y="CREATOR_NAME",orientation="h",title="💸 Сумма отказов по кассирам",color="COUNT",color_continuous_scale="Reds", labels=RU)
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)
                if "AUTHOR_NAME" in voids.columns:
                    by_au = voids.groupby("AUTHOR_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                    by_au = by_au[by_au["AUTHOR_NAME"].notna()]
                    if not by_au.empty:
                        st.markdown("#### Кто подтверждает отказы (менеджер)")
                        fig = px.bar(by_au.head(15),x="SUM",y="AUTHOR_NAME",orientation="h",title="✅ По менеджерам",color="COUNT",color_continuous_scale="Oranges", labels=RU)
                        fig.update_layout(height=400,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)

            elif view_mode == "По точкам" and "REST_NAME" in voids.columns:
                by_rest = voids.groupby("REST_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                by_rest = by_rest[by_rest["REST_NAME"].notna()]
                if not by_rest.empty:
                    cl,cr = st.columns(2)
                    with cl:
                        fig = px.bar(by_rest.head(15),x="SUM",y="REST_NAME",orientation="h",title="🏢 По сумме",color="SUM",color_continuous_scale="Reds", labels=RU)
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)
                    with cr:
                        fig = px.bar(by_rest.head(15),x="COUNT",y="REST_NAME",orientation="h",title="🏢 По количеству",color="COUNT",color_continuous_scale="Oranges", labels=RU)
                        fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                        st.plotly_chart(fig,use_container_width=True)

            elif view_mode == "По блюдам" and "DISH_NAME" in voids.columns:
                by_dish = voids.groupby("DISH_NAME").agg(COUNT=("DISHUNI","count"),SUM=("PRLISTSUM","sum")).reset_index().sort_values("SUM",ascending=False)
                by_dish = by_dish[by_dish["DISH_NAME"].notna()]
                if not by_dish.empty:
                    fig = px.bar(by_dish.head(15),x="SUM",y="DISH_NAME",orientation="h",title="🍕 Какие блюда отказывают",color="COUNT",color_continuous_scale="Reds", labels=RU)
                    fig.update_layout(height=500,yaxis=dict(autorange="reversed"),coloraxis_showscale=False,**CHART_THEME)
                    st.plotly_chart(fig,use_container_width=True)

            elif view_mode == "Таблица":
                dc = {"DATETIME":"Дата","REST_NAME":"Точка","DISH_NAME":"Блюдо","VOID_REASON":"Причина",
                      "QUANTITY":"Кол-во","PRLISTSUM":"Сумма","CREATOR_NAME":"Кассир","AUTHOR_NAME":"Менеджер"}
                av = [c for c in dc if c in voids.columns]
                vd = voids[av].rename(columns={k:v for k,v in dc.items() if k in av})
                st.dataframe(vd,use_container_width=True,hide_index=True,height=500)
                st.download_button("📥 CSV",vd.to_csv(index=False).encode("utf-8"),"voids.csv","text/csv",use_container_width=True)
        else:
            st.info("Отказов блюд нет за период")

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
            with c1: st.metric("🧾 Отменённых чеков", f"{total}")
            with c2: st.metric("💸 Сумма", f"{total_sum:,.0f} ₽")
            with c3:
                ords = load_orders(date_from, date_to)
                rv = float(ords["TOPAYSUM"].sum()) if not ords.empty else 0
                st.metric("📉 % от выручки", f"{total_sum/rv*100 if rv else 0:.2f}%")

            # По кто отменил
            if "DELETE_PERSON_NAME" in del_checks.columns:
                by_person = del_checks.groupby("DELETE_PERSON_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_person = by_person[by_person["DELETE_PERSON_NAME"].notna()]
                if not by_person.empty:
                    fig = px.bar(by_person, x="SUM", y="DELETE_PERSON_NAME", orientation="h",
                        title="👤 Кто инициирует отмены чеков", color="COUNT", color_continuous_scale="Reds", labels=RU)
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

            # По менеджеру
            if "DELETE_MANAGER_NAME" in del_checks.columns:
                by_mgr = del_checks.groupby("DELETE_MANAGER_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_mgr = by_mgr[by_mgr["DELETE_MANAGER_NAME"].notna()]
                if not by_mgr.empty:
                    fig = px.bar(by_mgr, x="SUM", y="DELETE_MANAGER_NAME", orientation="h",
                        title="✅ Кто подтверждает отмены (менеджер)", color="COUNT", color_continuous_scale="Oranges", labels=RU)
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

            # По точкам
            if "REST_NAME" in del_checks.columns:
                by_rest = del_checks.groupby("REST_NAME").agg(
                    COUNT=("TOPAYSUM","count"), SUM=("TOPAYSUM","sum")
                ).reset_index().sort_values("SUM", ascending=False)
                by_rest = by_rest[by_rest["REST_NAME"].notna()]
                if not by_rest.empty:
                    fig = px.bar(by_rest, x="SUM", y="REST_NAME", orientation="h",
                        title="🏢 Отмены чеков по точкам", color="COUNT", color_continuous_scale="Reds", labels=RU)
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

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
                disp["Транзакции отменены"] = disp["Транзакции отменены"].map({1:"✅ Да", 0:"❌ Нет"}).fillna("—")
            st.dataframe(disp, use_container_width=True, hide_index=True)
            st.download_button("📥 CSV", disp.to_csv(index=False).encode("utf-8"),
                "deleted_checks.csv", "text/csv", use_container_width=True)
        else:
            st.success("✅ Отменённых чеков за период нет — это хорошо!")
            st.caption("Если отменённые чеки появятся — это повод проверить: кто отменил, зачем, и не повторяется ли это систематически.")

    # ========== ОПЕРАЦИИ ОТМЕН ==========
    with sub_ops:
        ops = load_cancel_operations(date_from, date_to)
        if not ops.empty:
            c1,c2 = st.columns(2)
            with c1: st.metric("⚡ Операций отмен", f"{len(ops)}")
            total_diff = float(ops["DIFF"].sum()) if "DIFF" in ops.columns else 0
            with c2: st.metric("💸 Сумма разниц", f"{total_diff:,.0f} ₽")
            st.divider()

            # По типам операций
            if "OPERATION" in ops.columns:
                by_op = ops.groupby("OPERATION").agg(COUNT=("OPERATION","count")).reset_index().sort_values("COUNT",ascending=False)
                if not by_op.empty:
                    fig = px.bar(by_op, x="COUNT", y="OPERATION", orientation="h",
                        title="📊 Типы отмен", color="COUNT", color_continuous_scale="Oranges", labels=RU)
                    fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

            cl,cr = st.columns(2)
            # По кассирам
            with cl:
                if "OPERATOR_NAME" in ops.columns:
                    by_emp = ops.groupby("OPERATOR_NAME").agg(COUNT=("OPERATOR_NAME","count")).reset_index().sort_values("COUNT",ascending=False)
                    by_emp = by_emp[by_emp["OPERATOR_NAME"].notna()]
                    if not by_emp.empty:
                        fig = px.bar(by_emp.head(15), x="COUNT", y="OPERATOR_NAME", orientation="h",
                            title="👤 Отмены по кассирам", color="COUNT", color_continuous_scale="Reds", labels=RU)
                        fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)

            # По точкам
            with cr:
                if "REST_NAME" in ops.columns:
                    by_rest = ops.groupby("REST_NAME").agg(COUNT=("REST_NAME","count")).reset_index().sort_values("COUNT",ascending=False)
                    by_rest = by_rest[by_rest["REST_NAME"].notna()]
                    if not by_rest.empty:
                        fig = px.bar(by_rest.head(15), x="COUNT", y="REST_NAME", orientation="h",
                            title="🏢 Отмены по точкам", color="COUNT", color_continuous_scale="Oranges", labels=RU)
                        fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)

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
            pay_map = {0:"💵 Наличные",1:"💳 Банк. карта",2:"🏨 Карта отеля",3:"🎫 Плат. карта",
                       4:"📤 Искл. из доходов",5:"🏦 Безнал",6:"🎟️ Купон",7:"🔄 Выкуп"}
            pay_br["PAY_TYPE"] = pay_br["PAYLINETYPE"].map(pay_map).fillna("Другое")

            # Общие метрики по типам
            by_type = pay_br.groupby("PAY_TYPE").agg(
                COUNT=("PAY_COUNT","sum"), SUM=("TOTAL_SUM","sum")).reset_index().sort_values("SUM",ascending=False)
            total_pay = float(by_type["SUM"].sum())

            c1,c2,c3 = st.columns(3)
            with c1: st.metric("💳 Всего оплат", f'{int(by_type["COUNT"].sum()):,}')
            with c2: st.metric("💰 Общая сумма", f"{total_pay:,.0f} ₽")
            with c3:
                card_sum = float(by_type[by_type["PAY_TYPE"]=="💳 Банк. карта"]["SUM"].sum()) if "💳 Банк. карта" in by_type["PAY_TYPE"].values else 0
                st.metric("💳 Доля карт", f"{card_sum/total_pay*100:.1f}%" if total_pay else "—")
            st.divider()

            cl,cr = st.columns(2)
            with cl:
                fig = px.pie(by_type, values="SUM", names="PAY_TYPE", title="📊 Структура оплат", hole=0.4, labels=RU)
                fig.update_layout(height=400, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)
            with cr:
                fig = px.bar(by_type, x="SUM", y="PAY_TYPE", orientation="h",
                    title="💰 Суммы по типам оплат", color="SUM", color_continuous_scale="Tealgrn", labels=RU)
                fig.update_layout(height=400, yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

            # По кассирам и типам
            st.divider()
            st.markdown("#### 👤 Кассиры по типам оплат")
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
                    title=f"💰 Кассиры: {selected_type}", color="COUNT", color_continuous_scale="Viridis",
                    hover_data={"COUNT":True,"SUM":":.0f"}, labels=RU)
                fig.update_layout(height=max(400,min(len(cashier_data),20)*30), yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

                st.dataframe(cashier_data.rename(columns={"CASHIER_NAME":"Кассир","COUNT":"Операций","SUM":"Сумма"}),
                    use_container_width=True, hide_index=True)
        else:
            st.info("Нет данных по оплатам")

# --- ЗАКАЗЫ ---
if page == "📋 Заказы":
    page_header("Заказы", "📋")
    ot=load_orders(date_from,date_to)
    if not ot.empty:
        cm={"OPENTIME":"Открыт","ENDSERVICE":"Закрыт","TABLENAME":"Стол","GUESTSCOUNT":"Гости","TOPAYSUM":"К оплате","PAIDSUM":"Оплачено","DISCOUNTSUM":"Скидка","TOTALDISHPIECES":"Блюд"}
        av=[c for c in cm if c in ot.columns]
        dp=ot[av].rename(columns={k:v for k,v in cm.items() if k in av})
        st.markdown(f"### Заказы ({len(dp)})")
        st.dataframe(dp,use_container_width=True,hide_index=True,height=500)
        st.download_button("📥 CSV",dp.to_csv(index=False).encode("utf-8"),"orders.csv","text/csv",use_container_width=True)

# ============================================================
# STOREHOUSE СТРАНИЦЫ (REST API)
# ============================================================

# --- ОСТАТКИ ---
if page == "📦 Склад":
    page_header("Склад", "📦")
    st.caption(f"StoreHouse API: {SH_API['url']} · Период: {date_from} — {date_to}")

    d1_str = str(date_from)
    d2_str = str(date_to)

    sub_overview, sub_stock, sub_incoming, sub_transfers, sub_goods_tab, sub_structure, sub_debug = st.tabs([
        "🗺️ Обзор", "📊 Остатки", "📥 Приход", "🔀 Перемещения",
        "📦 Товары", "🏗️ Структура", "🔧 Отладка"])

    # ==================== ОБЗОР ====================
    with sub_overview:
        st.markdown("### 🗺️ Обзор складской системы")

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
        with c1: st.metric("🏢 Подразделений", f"{n_divs}")
        with c2: st.metric("🏪 Складов", f"{n_deps}")
        with c3: st.metric("📦 Товаров", f"{n_goods:,}")
        with c4: st.metric("🗂️ Категорий", f"{n_cats}")

        st.divider()

        # Пробуем загрузить сводку по приходным за период
        st.markdown(f"#### 📥 Приходные накладные за {d1_str} — {d2_str}")
        list_df, list_err = sh_load_gdoc0_ext_list(d1_str, d2_str)
        if list_err:
            st.warning(f"⚠️ GDoc0ExtList: {list_err}")
        elif not list_df.empty:
            st.success(f"✅ {len(list_df)} приходных накладных за период")
        else:
            st.info("Нет приходных накладных за период")

        # Пробуем перемещения
        st.markdown(f"#### 🔀 Перемещения за {d1_str} — {d2_str}")
        tr_list, tr_err = sh_load_gdoc1_5_list(d1_str, d2_str, doc_type=4)
        if tr_err:
            st.warning(f"⚠️ GDoc1_5LstDocs type=4: {tr_err}")
        elif not tr_list.empty:
            st.success(f"✅ {len(tr_list)} перемещений за период")
        else:
            st.info("Нет перемещений за период")

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
                        title="🗺️ Карта: подразделения → склады",
                        color_discrete_sequence=px.colors.qualitative.Set3, labels=RU)
                    fig.update_layout(height=500, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

        # Статус API
        st.divider()
        st.markdown("#### 🔌 Статус доступных данных")
        status_rows = [
            {"Источник": "📥 Приходные (GDoc0ExtList)", "Статус": "✅" if not list_err else "⚠️", "Деталь": f"{len(list_df)} документов" if not list_err and not list_df.empty else str(list_err or "нет данных")},
            {"Источник": "🔀 Перемещения (GDoc1_5LstDocs type=4)", "Статус": "✅" if not tr_err else "⚠️", "Деталь": f"{len(tr_list)} документов" if not tr_err and not tr_list.empty else str(tr_err or "нет данных")},
            {"Источник": "📦 Товары (GoodsTree)", "Статус": "✅" if n_goods > 0 else "❌", "Деталь": f"{n_goods:,} товаров"},
            {"Источник": "🏪 Склады (Departs)", "Статус": "✅" if n_deps > 0 else "❌", "Деталь": f"{n_deps} складов"},
            {"Источник": "🏢 Подразделения (Divisions)", "Статус": "✅" if n_divs > 0 else "❌", "Деталь": f"{n_divs} подразделений"},
            {"Источник": "📊 Остатки (GRemns)", "Статус": "✅", "Деталь": "Работает! dept=0 общие, dept=1 по складам"},
            {"Источник": "📑 Расходные (type=1)", "Статус": "🔒", "Деталь": "Ошибка 5 — нет прав"},
            {"Источник": "📋 Инвентаризации (type=5)", "Статус": "🔒", "Деталь": "Ошибка 5 — нет прав"},
        ]
        st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    # ==================== ОСТАТКИ ====================
    with sub_stock:
        st.markdown(f"### 📊 Остатки товаров на {d2_str}")
        st.caption("GRemns API → текущие остатки по складам с суммами")

        stock_df, stock_err = sh_load_stock(d2_str, by_depart=True)

        if stock_err:
            st.warning(f"⚠️ {stock_err}")
        elif not stock_df.empty:
            n_items = len(stock_df)
            total_amount = stock_df["AMOUNT"].sum() if "AMOUNT" in stock_df.columns else 0
            total_qty = stock_df["QTY"].sum() if "QTY" in stock_df.columns else 0
            n_products = stock_df["PRODUCT_NAME"].nunique() if "PRODUCT_NAME" in stock_df.columns else 0
            n_departs = stock_df["DEPART"].nunique() if "DEPART" in stock_df.columns else 0

            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("🏪 Складов", f"{n_departs}")
            with c2: st.metric("🏷️ Товаров", f"{n_products:,}")
            with c3: st.metric("💰 Стоимость", f"{total_amount:,.0f} ₽")
            with c4: st.metric("📦 Позиций", f"{n_items:,}")

            st.divider()

            # Переключатель отчётов
            stock_mode = st.radio("📊 Отчёт:", [
                "🏪 По складам",
                "🏷️ По товарам",
                "📋 Товары на складе"
            ], horizontal=True, key="stock_mode")

            st.divider()

            # ======= ПО СКЛАДАМ =======
            if stock_mode.startswith("🏪") and "DEPART" in stock_df.columns:
                by_dep = stock_df.groupby("DEPART").agg(
                    PRODUCTS=("PRODUCT_NAME", "nunique"),
                    TOTAL_QTY=("QTY", "sum"),
                    TOTAL_AMOUNT=("AMOUNT", "sum"),
                ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)

                fig = px.bar(by_dep, x="TOTAL_AMOUNT", y="DEPART", orientation="h",
                    title="💰 Стоимость остатков по складам",
                    color="TOTAL_AMOUNT", color_continuous_scale="Viridis",
                    text=by_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels={"TOTAL_AMOUNT": "Сумма, ₽", "DEPART": "Склад"})
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, len(by_dep)*32),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

                cl, cr = st.columns(2)
                with cl:
                    fig2 = px.pie(by_dep, values="TOTAL_AMOUNT", names="DEPART",
                        title="🥧 Доля остатков по складам", hole=0.4)
                    fig2.update_layout(height=400, **CHART_THEME)
                    st.plotly_chart(fig2, use_container_width=True)
                with cr:
                    fig3 = px.bar(by_dep, x="PRODUCTS", y="DEPART", orientation="h",
                        title="📦 Кол-во товаров на складе",
                        color="PRODUCTS", color_continuous_scale="Tealgrn",
                        text="PRODUCTS",
                        labels={"PRODUCTS": "Товаров", "DEPART": "Склад"})
                    fig3.update_traces(textposition="auto")
                    fig3.update_layout(height=max(400, len(by_dep)*32),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig3, use_container_width=True)

                # Таблица
                st.divider()
                disp = by_dep.copy()
                disp.columns = ["Склад", "Товаров", "Кол-во", "Сумма ₽"]
                disp["Ø на товар ₽"] = (disp["Сумма ₽"] / disp["Товаров"]).round(0)
                st.dataframe(disp, use_container_width=True, hide_index=True)

            # ======= ПО ТОВАРАМ =======
            elif stock_mode.startswith("🏷️") and "PRODUCT_NAME" in stock_df.columns:
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
                    title="💰 Топ-30 товаров по стоимости остатков",
                    color="TOTAL_AMOUNT", color_continuous_scale="YlOrRd",
                    text=top30["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels={"TOTAL_AMOUNT": "Сумма, ₽", "PRODUCT_NAME": "Товар"})
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(500, len(top30)*25),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

                st.divider()
                show_cols_s = [c for c in ["PRODUCT_NAME","TOTAL_QTY","TOTAL_AMOUNT","STORES","UNIT"] if c in by_prod.columns]
                disp_p = by_prod[show_cols_s].copy()
                col_names = {"PRODUCT_NAME":"Товар","TOTAL_QTY":"Кол-во","TOTAL_AMOUNT":"Сумма ₽","STORES":"Складов","UNIT":"Ед."}
                disp_p = disp_p.rename(columns=col_names)
                st.dataframe(disp_p, use_container_width=True, hide_index=True, height=500)
                st.download_button("📥 CSV остатков", by_prod.to_csv(index=False).encode("utf-8"),
                    "stock.csv", "text/csv", use_container_width=True)

            # ======= ТОВАРЫ НА СКЛАДЕ =======
            elif stock_mode.startswith("📋") and "DEPART" in stock_df.columns:
                departs_list = sorted(stock_df["DEPART"].unique().tolist())
                selected_store = st.selectbox("🏪 Выберите склад:", departs_list, key="stock_store_sel")
                filtered_s = stock_df[stock_df["DEPART"] == selected_store].copy()

                if not filtered_s.empty:
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric("🏷️ Товаров", f"{len(filtered_s):,}")
                    with c2: st.metric("💰 Сумма", f"{filtered_s['AMOUNT'].sum():,.0f} ₽")
                    with c3: st.metric("📦 Кол-во", f"{filtered_s['QTY'].sum():,.1f}")

                    filtered_s = filtered_s.sort_values("AMOUNT", ascending=False)
                    top_s = filtered_s.head(20)
                    fig = px.bar(top_s, x="AMOUNT", y="PRODUCT_NAME", orientation="h",
                        title=f"💰 Остатки — {selected_store}",
                        color="AMOUNT", color_continuous_scale="Viridis",
                        text=top_s["AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels={"AMOUNT": "Сумма, ₽", "PRODUCT_NAME": "Товар"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_s)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    st.divider()
                    show_s = [c for c in ["PRODUCT_NAME","QTY","AMOUNT","UNIT"] if c in filtered_s.columns]
                    disp_fs = filtered_s[show_s].copy()
                    disp_fs = disp_fs.rename(columns={"PRODUCT_NAME":"Товар","QTY":"Кол-во","AMOUNT":"Сумма ₽","UNIT":"Ед."})
                    st.dataframe(disp_fs, use_container_width=True, hide_index=True, height=500)
                    st.download_button("📥 CSV", filtered_s.to_csv(index=False).encode("utf-8"),
                        f"stock_{selected_store.replace(' ','_')}.csv", "text/csv", use_container_width=True)
                else:
                    st.info(f"Нет остатков на складе «{selected_store}»")
        else:
            st.info("Нет данных об остатках. Процедура GRemns недоступна или вернула пустой результат.")

    # ==================== ПРИХОД ====================
    with sub_incoming:
        st.markdown(f"### 📥 Приходные накладные ({d1_str} — {d2_str})")
        st.caption("Одна загрузка → три отчёта: по товарам, по складам, по товарам в каждом складе")

        progress_ph = st.empty()
        if st.button("🚀 Загрузить приход", key="load_incoming_full", use_container_width=True):
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
            st.warning(f"⚠️ {inc_err}")
        elif not items_df.empty or not docs_df.empty:
            n_items = len(items_df)
            n_docs = len(docs_df)
            total_sum = docs_df["TOTAL_AMOUNT"].sum() if not docs_df.empty and "TOTAL_AMOUNT" in docs_df.columns else 0
            n_products = items_df["PRODUCT_NAME"].nunique() if not items_df.empty else 0
            n_departs = docs_df["DEPART"].nunique() if not docs_df.empty and "DEPART" in docs_df.columns else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.metric("📄 Накладных", f"{n_docs}")
            with c2: st.metric("📦 Позиций", f"{n_items:,}")
            with c3: st.metric("🏷️ Товаров", f"{n_products:,}")
            with c4: st.metric("🏪 Складов", f"{n_departs}")
            with c5: st.metric("💰 Сумма", f"{total_sum:,.0f} ₽")

            # Диагностика колонок GDoc0
            gdoc0_cols = st.session_state.get("_gdoc0_all_cols", [])
            if gdoc0_cols:
                with st.expander("🔧 Колонки GDoc0 (диагностика)"):
                    st.caption(f"{len(gdoc0_cols)} колонок: {gdoc0_cols}")

            # ---- Три отчёта внутри одного таба ----
            rpt_mode = st.radio("📊 Отчёт:", [
                "🏷️ По товарам (закупочные цены)",
                "🏪 По складам (суммы прихода)",
                "📋 Товары в каждом складе"
            ], horizontal=True, key="inc_report_mode")

            st.divider()

            # ======================== ПО ТОВАРАМ ========================
            if rpt_mode.startswith("🏷️") and not items_df.empty:
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
                    title="📊 Топ-20 товаров по сумме закупок",
                    color="AVG_PURCHASE_PRICE", color_continuous_scale="YlOrRd",
                    text=top20["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels={"TOTAL_AMOUNT": "Сумма, ₽", "PRODUCT_NAME": "Товар", "AVG_PURCHASE_PRICE": "Ø Цена"})
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(450, len(top20)*28),
                    yaxis=dict(autorange="reversed"), **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

                # Распределение
                cl, cr = st.columns(2)
                with cl:
                    fig = px.histogram(grouped, x="AVG_PURCHASE_PRICE", nbins=40,
                        title="📊 Распределение закупочных цен",
                        labels={"AVG_PURCHASE_PRICE": "Средняя цена, ₽"},
                        color_discrete_sequence=["#00ff6a"])
                    fig.update_layout(height=350, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)
                with cr:
                    fig = px.scatter(grouped.head(100), x="AVG_PURCHASE_PRICE", y="TOTAL_AMOUNT",
                        hover_name="PRODUCT_NAME", size="TOTAL_QTY",
                        title="💰 Цена vs Объём закупок",
                        labels={"AVG_PURCHASE_PRICE": "Ø Цена, ₽", "TOTAL_AMOUNT": "Сумма, ₽", "TOTAL_QTY": "Кол-во"},
                        color_discrete_sequence=["#00e5ff"])
                    fig.update_layout(height=350, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                # Таблица
                st.divider()
                show_cols = [c for c in ["PRODUCT_NAME","AVG_PURCHASE_PRICE","TOTAL_QTY","TOTAL_AMOUNT","DOC_COUNT","UNIT"] if c in grouped.columns]
                display_gp = grouped[show_cols].copy()
                display_gp.columns = ["Товар","Ø Цена ₽","Общ. кол-во","Общ. сумма ₽","Накладных","Ед."][:len(show_cols)]
                st.dataframe(display_gp, use_container_width=True, hide_index=True, height=500)
                st.download_button("📥 CSV закупок", grouped.to_csv(index=False).encode("utf-8"),
                    "purchases.csv", "text/csv", use_container_width=True)

            # ======================== ПО СКЛАДАМ ========================
            elif rpt_mode.startswith("🏪") and not docs_df.empty and "DEPART" in docs_df.columns:
                by_dep = docs_df.groupby("DEPART").agg(
                    DOC_COUNT=("DOC_RID", "count"),
                    TOTAL_AMOUNT=("TOTAL_AMOUNT", "sum"),
                    TOTAL_ITEMS=("ITEMS_COUNT", "sum"),
                ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)

                # Топ складов
                fig = px.bar(by_dep, x="TOTAL_AMOUNT", y="DEPART", orientation="h",
                    title="💰 Сумма прихода по складам",
                    color="TOTAL_AMOUNT", color_continuous_scale="Viridis",
                    text=by_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                    labels={"TOTAL_AMOUNT": "Сумма, ₽", "DEPART": "Склад"})
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, len(by_dep)*30),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

                cl, cr = st.columns(2)
                with cl:
                    fig = px.bar(by_dep, x="DOC_COUNT", y="DEPART", orientation="h",
                        title="📄 Кол-во накладных по складам",
                        color="DOC_COUNT", color_continuous_scale="Tealgrn",
                        text="DOC_COUNT",
                        labels={"DOC_COUNT": "Накладных", "DEPART": "Склад"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(350, len(by_dep)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)
                with cr:
                    fig = px.pie(by_dep, values="TOTAL_AMOUNT", names="DEPART",
                        title="🥧 Доля прихода по складам", hole=0.4, labels=RU)
                    fig.update_layout(height=max(350, len(by_dep)*28), **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                # Поставщики
                if "SUPPLIER" in docs_df.columns and docs_df["SUPPLIER"].str.strip().ne("").sum() > 0:
                    st.divider()
                    st.markdown("#### 🤝 Приход по поставщикам")
                    sup_data = docs_df[docs_df["SUPPLIER"].str.strip().ne("")]
                    by_sup = sup_data.groupby("SUPPLIER").agg(
                        DOC_COUNT=("DOC_RID", "count"),
                        TOTAL_AMOUNT=("TOTAL_AMOUNT", "sum"),
                    ).reset_index().sort_values("TOTAL_AMOUNT", ascending=False)
                    top_sup = by_sup.head(20)
                    fig = px.bar(top_sup, x="TOTAL_AMOUNT", y="SUPPLIER", orientation="h",
                        title="💰 Топ-20 поставщиков по сумме",
                        color="TOTAL_AMOUNT", color_continuous_scale="YlOrRd",
                        text=top_sup["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels={"TOTAL_AMOUNT": "Сумма, ₽", "SUPPLIER": "Поставщик"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_sup)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

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
                st.download_button("📥 CSV по складам", docs_df.to_csv(index=False).encode("utf-8"),
                    "incoming_by_depart.csv", "text/csv", use_container_width=True)

            # ======================== ТОВАРЫ В КАЖДОМ СКЛАДЕ ========================
            elif rpt_mode.startswith("📋") and not items_df.empty and "DEPART" in items_df.columns:
                departs = sorted(items_df["DEPART"].dropna().unique().tolist())
                departs = [d for d in departs if d.strip()]
                if departs:
                    selected_dep = st.selectbox("🏪 Выберите склад:", ["Все склады"] + departs, key="dep_sel")
                    if selected_dep == "Все склады":
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
                    with c1: st.metric("🏷️ Товаров", f"{len(by_prod):,}")
                    with c2: st.metric("💰 Сумма", f"{by_prod['TOTAL_AMOUNT'].sum():,.0f} ₽")
                    with c3: st.metric("📄 Накладных", f"{by_prod['DOC_COUNT'].sum()}")

                    # Топ-20 товаров на этом складе
                    top_dep = by_prod.head(20)
                    title_txt = f"📊 Топ-20 товаров — {selected_dep}" if selected_dep != "Все склады" else "📊 Топ-20 товаров (все склады)"
                    fig = px.bar(top_dep, x="TOTAL_AMOUNT", y="PRODUCT_NAME", orientation="h",
                        title=title_txt,
                        color="AVG_PRICE", color_continuous_scale="YlOrRd",
                        text=top_dep["TOTAL_AMOUNT"].apply(lambda x: f"{x:,.0f}₽"),
                        labels={"TOTAL_AMOUNT": "Сумма, ₽", "PRODUCT_NAME": "Товар", "AVG_PRICE": "Ø Цена"})
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(top_dep)*28),
                        yaxis=dict(autorange="reversed"), **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    # Сравнение складов по этому товару
                    if selected_dep != "Все склады":
                        st.divider()
                        st.markdown(f"#### 🔍 Уникальные товары на складе «{selected_dep}»")
                        # Товары, которые есть только на этом складе
                        all_prods = set(items_df["PRODUCT_NAME"].unique())
                        dep_prods = set(filtered["PRODUCT_NAME"].unique())
                        other_prods = set(items_df[items_df["DEPART"] != selected_dep]["PRODUCT_NAME"].unique())
                        unique_here = dep_prods - other_prods
                        if unique_here:
                            st.success(f"✅ {len(unique_here)} товаров только на этом складе")
                        else:
                            st.info("Все товары этого склада встречаются и на других складах")

                    # Таблица
                    st.divider()
                    disp_prod = by_prod.copy()
                    disp_prod.columns = ["Товар", "Кол-во", "Сумма ₽", "Накладных", "Ø Цена ₽"]
                    st.dataframe(disp_prod, use_container_width=True, hide_index=True, height=500)
                    csv_name = f"products_{selected_dep.replace(' ','_')}.csv" if selected_dep != "Все склады" else "products_all.csv"
                    st.download_button("📥 CSV", by_prod.to_csv(index=False).encode("utf-8"),
                        csv_name, "text/csv", use_container_width=True)
                else:
                    st.warning("Склады не определены в заголовках накладных")
            else:
                if items_df.empty and not docs_df.empty:
                    st.info("Позиции товаров не распарсены. Доступен только отчёт «По складам».")
                else:
                    st.info("Нет данных для этого отчёта")
        else:
            st.info("Нажмите «🚀 Загрузить приход» — одна загрузка для всех трёх отчётов")

    # ==================== ПЕРЕМЕЩЕНИЯ ====================
    with sub_transfers:
        st.markdown(f"### 🔀 Внутренние перемещения ({d1_str} — {d2_str})")
        st.caption("GDoc1_5LstDocs type=4 → список перемещений → GDoc4 → товары + склады")

        progress_tr = st.empty()
        if st.button("🚀 Загрузить перемещения", key="load_transfers"):
            with st.spinner("Загружаю перемещения..."):
                tr_l, tr_i, tr_e = sh_load_transfers(d1_str, d2_str, _progress_container=progress_tr, max_rids=100)
                st.session_state["_tr_list"] = tr_l
                st.session_state["_tr_items"] = tr_i
                st.session_state["_tr_err"] = tr_e

        tr_list_r = st.session_state.get("_tr_list", pd.DataFrame())
        tr_items_r = st.session_state.get("_tr_items", pd.DataFrame())
        tr_err_r = st.session_state.get("_tr_err", None)

        if tr_err_r:
            st.warning(f"⚠️ {tr_err_r}")
        elif not tr_list_r.empty:
            st.success(f"✅ {len(tr_list_r)} перемещений, {len(tr_items_r)} позиций товаров")

            c1, c2, c3 = st.columns(3)
            with c1: st.metric("📄 Документов", f"{len(tr_list_r)}")
            with c2: st.metric("📦 Позиций", f"{len(tr_items_r):,}" if not tr_items_r.empty else "0")
            with c3:
                if not tr_items_r.empty and "_DEPART_FROM" in tr_items_r.columns:
                    n_stores = tr_items_r["_DEPART_FROM"].nunique()
                    st.metric("🏪 Складов-отправителей", f"{n_stores}")
                else:
                    st.metric("🏪 Складов", "—")

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
                        title="📊 Топ-20 товаров по частоте перемещений",
                        color="Перемещений", color_continuous_scale="Viridis",
                        text="Перемещений", labels=RU)
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(by_product)*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                # Матрица перемещений между складами
                if "_DEPART_FROM" in tr_items_r.columns and "_DEPART_TO" in tr_items_r.columns:
                    st.divider()
                    st.markdown("#### 🔄 Матрица перемещений (склад → склад)")
                    matrix = tr_items_r.groupby(["_DEPART_FROM", "_DEPART_TO"]).size().reset_index(name="COUNT")
                    matrix.columns = ["Откуда", "Куда", "Кол-во"]
                    if len(matrix) > 0:
                        fig = px.density_heatmap(matrix, x="Куда", y="Откуда", z="Кол-во",
                            title="Интенсивность перемещений",
                            color_continuous_scale="Viridis", labels=RU)
                        fig.update_layout(height=max(400, matrix["Откуда"].nunique()*40), **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(matrix.sort_values("Кол-во", ascending=False), use_container_width=True, hide_index=True)

                # Полная таблица перемещений
                st.divider()
                st.markdown("#### 📋 Все позиции перемещений")
                clean_tr = sh_clean_df(tr_items_r)
                st.dataframe(clean_tr, use_container_width=True, hide_index=True, height=500)
                st.download_button("📥 CSV перемещений", tr_items_r.to_csv(index=False).encode("utf-8"),
                    "transfers.csv", "text/csv", use_container_width=True)
        else:
            st.info("Нажмите «🚀 Загрузить перемещения» для анализа внутренних перемещений")

    # ==================== ТОВАРЫ ====================
    with sub_goods_tab:
        goods, goods_err = sh_load_goods()
        if goods_err:
            st.warning(f"⚠️ {goods_err}")
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
            with c1: st.metric("📦 Всего товаров", f"{len(clean):,}")
            with c2:
                if grp_col: st.metric("🗂️ Групп", f"{clean[grp_col].nunique()}")
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
                    title="📊 Топ-20 групп по количеству товаров",
                    color="Кол-во товаров", color_continuous_scale="Viridis", text="Кол-во товаров", labels=RU)
                fig.update_traces(textposition="auto")
                fig.update_layout(height=max(400, min(20, len(by_grp))*30),
                    yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.dataframe(clean[useful_cols], use_container_width=True, hide_index=True, height=500)
            with st.expander("📋 Все колонки (включая технические)"):
                st.dataframe(clean, use_container_width=True, hide_index=True, height=400)
            st.download_button("📥 CSV", clean[useful_cols].to_csv(index=False).encode("utf-8"),
                "sh_goods.csv", "text/csv", use_container_width=True)
        else:
            st.info("GoodsTree вернула пустой результат")

    # ==================== СТРУКТУРА ====================
    with sub_structure:
        st.markdown("### 🏗️ Структура складов и подразделений")

        divs, divs_err = sh_load_divisions()
        deps, deps_err = sh_load_departs()
        cats, cats_err = sh_load_goods_categories()

        sub_struct_tab = st.selectbox("Показать:", ["🏢 Подразделения", "🏪 Склады", "🗂️ Категории"], key="struct_sel")

        if sub_struct_tab == "🏢 Подразделения":
            if divs_err:
                st.error(f"❌ Divisions: {divs_err}")
            elif not divs.empty:
                st.success(f"✅ {len(divs)} подразделений (столовые, буфеты, цеха)")
                st.dataframe(sh_clean_df(divs), use_container_width=True, hide_index=True, height=500)
            else:
                st.info("Divisions вернула пустой результат")

        elif sub_struct_tab == "🏪 Склады":
            if deps_err:
                st.error(f"❌ Departs: {deps_err}")
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
                        title="🏪 Складов в каждом подразделении",
                        color="Кол-во складов", color_continuous_scale="Viridis",
                        text="Кол-во складов", labels=RU)
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, len(by_venture)*35),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    st.divider()
                    st.dataframe(by_venture, use_container_width=True, hide_index=True)

                st.divider()
                st.dataframe(sh_clean_df(deps), use_container_width=True, hide_index=True, height=500)
            else:
                st.info("Departs вернула пустой результат")

        elif sub_struct_tab == "🗂️ Категории":
            if cats_err:
                st.error(f"❌ GoodsCategories: {cats_err}")
            elif not cats.empty:
                st.success(f"✅ {len(cats)} категорий товаров")
                st.dataframe(sh_clean_df(cats), use_container_width=True, hide_index=True)
            else:
                st.info("GoodsCategories вернула пустой результат")

    # ==================== ОТЛАДКА ====================
    with sub_debug:
        st.markdown("### 🔧 Отладка API")
        st.markdown("**Сырой ответ API** — для диагностики парсинга")
        debug_proc = st.selectbox("Процедура:",
            ["Goods", "GoodsCategories", "Departs", "Divisions",
             "GDoc0ExtList", "GDoc1_5LstDocs", "GDoc4", "DocAccSums",
             "GAbcRpt", "FifoDtl"], key="debug_proc")
        if st.button("🔍 Показать сырой JSON", key="debug_btn"):
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
if page == "📄 Накладные":
    page_header("Накладные и документы", "📄")
    st.caption("SQL-база → RID документов → StoreHouse API → детали с товарами")

    sub_pipeline, sub_corr, sub_sql = st.tabs([
        "📄 Документы (SQL+API)", "🤝 Поставщики", "🗄️ SQL-статус"])

    with sub_pipeline:
        with st.spinner("🔄 Загружаю документы из SQL → детали из API..."):
            headers_df, items_df = sh_load_all_docs_from_sql()

        if not headers_df.empty:
            st.success(f"✅ {len(headers_df)} документов, {len(items_df)} позиций товаров")

            # Метрики
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("📄 Документов", f"{len(headers_df)}")
            with c2: st.metric("📦 Позиций товаров", f"{len(items_df):,}" if not items_df.empty else "0")
            with c3:
                types = headers_df["Тип"].nunique() if "Тип" in headers_df.columns else 0
                st.metric("📋 Типов документов", f"{types}")
            st.divider()

            # Список документов
            st.markdown("### 📄 Документы")
            st.dataframe(headers_df, use_container_width=True, hide_index=True)

            # Позиции товаров
            if not items_df.empty:
                st.divider()
                st.markdown("### 📦 Товары в документах")

                # Топ товаров по частоте
                name_col = next((c for c in items_df.columns if "товар" in c.lower() or c == "210\\3"), None)
                if name_col:
                    by_product = items_df[name_col].value_counts().head(20).reset_index()
                    by_product.columns = ["Товар", "Кол-во документов"]
                    fig = px.bar(by_product, x="Кол-во документов", y="Товар", orientation="h",
                        title="📊 Топ-20 товаров по частоте в документах",
                        color="Кол-во документов", color_continuous_scale="Viridis",
                        text="Кол-во документов", labels=RU)
                    fig.update_traces(textposition="auto")
                    fig.update_layout(height=max(400, min(20, len(by_product))*28),
                        yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                st.divider()
                st.dataframe(items_df, use_container_width=True, hide_index=True, height=500)
                st.download_button("📥 CSV позиций", items_df.to_csv(index=False).encode("utf-8"),
                    "doc_items.csv", "text/csv", use_container_width=True)
            else:
                st.info("Позиции товаров пусты — возможно документы без детализации")
        else:
            st.warning("⚠️ Нет документов в SQL-базе. Инженер ещё заливает данные.")
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
            type_map = {1: "👤 Физлицо", 2: "📤 Реализация", 3: "🏢 Поставщик"}
            corr_clean = corr.copy()
            corr_clean["Тип"] = corr_clean["TYPECORR"].map(type_map).fillna("Другое")
            st.success(f"✅ {len(corr_clean)} контрагентов")

            c1, c2 = st.columns(2)
            with c1:
                by_type = corr_clean["Тип"].value_counts().reset_index()
                by_type.columns = ["Тип", "Кол-во"]
                fig = px.pie(by_type, values="Кол-во", names="Тип",
                    title="По типам контрагентов", hole=0.4, labels=RU)
                fig.update_layout(height=350, **CHART_THEME)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                suppliers = corr_clean[corr_clean["TYPECORR"] == 3]
                st.metric("🏢 Поставщиков", f"{len(suppliers)}")
                st.metric("👤 Физлиц", f"{len(corr_clean[corr_clean['TYPECORR']==1])}")
                st.metric("📤 Реализация", f"{len(corr_clean[corr_clean['TYPECORR']==2])}")

            st.divider()
            st.dataframe(corr_clean[["NAME","Тип","CODE"]].rename(
                columns={"NAME":"Название","CODE":"Код"}),
                use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Нет данных по контрагентам")

    with sub_sql:
        st.markdown("### 🗄️ Состояние SQL-базы")
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
                st.markdown(f"### 🗂️ Группы товаров ({len(groups)})")
                groups_clean = groups[["RID","PARENTRID","NAME"]].rename(
                    columns={"RID":"ID","PARENTRID":"Родитель","NAME":"Название"})
                st.dataframe(groups_clean, use_container_width=True, hide_index=True, height=300)

# --- ФУДКОСТ ---
if page == "🍳 Фудкост":
    page_header("Фудкост", "🍳")
    st.caption(f"Источник: STAT_SH4_SHIFTS_SELLING (экспорт StoreHouse → SQL)")

    # Загрузка данных с названиями
    selling = run_query("""
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
    _locs = run_query("SELECT RID, NAME FROM STAT_SH4_SALELOCATIONS WHERE NAME IS NOT NULL")
    _loc_map = dict(zip(_locs["RID"], _locs["NAME"])) if not _locs.empty else {}
    if not selling.empty:
        selling["DISH_NAME"] = selling["DISH_NAME"].fillna("Товар #" + selling["GOODRID"].astype(str))

    if selling.empty:
        st.warning("⚠️ Нет данных в STAT_SH4_SHIFTS_SELLING за выбранный период. Экспорт SH → SQL ещё не выполнен или период не покрыт.")
        st.info("Данные загружаются из StoreHouse за последние 30 дней. Проверьте что экспорт настроен.")
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
        with c2: st.metric("📦 Себестоимость", f"{total_purchase:,.0f} ₽")
        with c3:
            st.metric("💰 Маржа", f"{total_margin:,.0f} ₽",
                delta=f"{total_margin/max(1,total_selling)*100:.1f}% от продаж")
        with c4:
            fc_color = "normal" if 25 <= avg_foodcost <= 35 else "inverse"
            st.metric("🍳 Фудкост", f"{avg_foodcost:.1f}%",
                delta="норма" if 25 <= avg_foodcost <= 35 else ("выше нормы" if avg_foodcost > 35 else "ниже нормы"),
                delta_color=fc_color)

        st.divider()

        # === ТАБЫ ===
        tab_daily, tab_groups, tab_detail, tab_table = st.tabs([
            "📅 По дням", "🏢 По столовым", "🏷️ По товарам", "📋 Таблица"])

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
                name="Продажи", marker_color="#00ff6a"))
            fig.add_trace(go.Bar(x=by_day["SELL_DATE"], y=by_day["PURCHASE"],
                name="Себестоимость", marker_color="#ff6b9d"))
            fig.add_trace(go.Scatter(x=by_day["SELL_DATE"], y=by_day["FC_PCT"],
                name="Фудкост %", yaxis="y2", mode="lines+markers",
                line=dict(color="#ffea00", width=3)))
            fig.update_layout(barmode="group",
                title="📅 Продажи vs Себестоимость по дням",
                yaxis=dict(title="₽"),
                yaxis2=dict(title="Фудкост %", side="right", overlaying="y", range=[0, 50]),
                height=400, legend=dict(orientation="h", y=1.1), **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

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
            _gg = run_query("SELECT RID, NAME FROM STAT_SH4_SHIFTS_GOODGROUPS WHERE NAME IS NOT NULL")
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
                title="🏢 Продажи по точкам", color="FC_PCT", color_continuous_scale="RdYlGn_r",
                text=by_group.head(20)["SELLING"].apply(lambda x: f"{x:,.0f}₽"),
                labels={"SELLING": "Продажи ₽", "LOCATION": "Точка", "FC_PCT": "Фудкост %"})
            fig2.update_traces(textposition="auto")
            fig2.update_layout(height=max(400, len(by_group.head(20))*30),
                yaxis=dict(autorange="reversed"), **CHART_THEME)
            st.plotly_chart(fig2, use_container_width=True)

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
                x=top["SELLING"], name="Продажи", orientation="h", marker_color="#00ff6a"))
            fig3.add_trace(go.Bar(y=top["DISH_NAME"],
                x=top["PURCHASE"], name="Себестоимость", orientation="h", marker_color="#ff6b9d"))
            fig3.update_layout(barmode="group", title="🏷️ Топ-20 блюд: продажи vs себестоимость",
                height=max(400, len(top)*28), yaxis=dict(autorange="reversed"), **CHART_THEME)
            st.plotly_chart(fig3, use_container_width=True)

            # Проблемные товары (фудкост > 40%)
            high_fc = by_good[(by_good["FC_PCT"] > 40) & (by_good["SELLING"] > 1000)].sort_values("FC_PCT", ascending=False)
            if not high_fc.empty:
                st.warning(f"⚠️ {len(high_fc)} товаров с фудкостом > 40%")
                st.dataframe(high_fc.head(20).rename(columns={
                    "DISH_NAME":"Блюдо","GOODRID":"ID SH","PURCHASE":"Себестоим.",
                    "SELLING":"Продажи","QTY":"Кол-во","FC_PCT":"Фудкост %","MARGIN":"Маржа"}),
                    use_container_width=True, hide_index=True)

            st.divider()
            st.dataframe(by_good.rename(columns={
                "DISH_NAME":"Блюдо","GOODRID":"ID SH","PURCHASE":"Себестоим.",
                "SELLING":"Продажи","QTY":"Кол-во","FC_PCT":"Фудкост %","MARGIN":"Маржа"}),
                use_container_width=True, hide_index=True, height=500)

        # ---- ТАБЛИЦА (сырые данные) ----
        with tab_table:
            st.markdown(f"**{len(selling):,}** строк за {date_from} — {date_to}")
            st.dataframe(selling, use_container_width=True, hide_index=True, height=500)
            st.download_button("📥 CSV фудкост", selling.to_csv(index=False).encode("utf-8"),
                "foodcost_selling.csv", "text/csv", use_container_width=True)

# --- ФУДКОСТ (РАСЧЁТ) ---
if page == "🔀 Фудкост (расчёт)":
    page_header("Фудкост: себестоимость из рецептур", "🔀")
    st.markdown("""
    **Реальный фудкост** = себестоимость порции (из Актов нарезки SH, по рецептуре) ÷ цена продажи (RK) × 100%  
    Норма для столовых: **25–35%**. Пайплайн: `GoodsTree` → комплекты → `FindLinksToCmp` → `GDoc12` → себестоимость.
    """)
    st.divider()

    d1_str, d2_str = str(date_from), str(date_to)

    # ---- Загрузка базовых данных ----
    with st.spinner("📦 Загружаю товары из StoreHouse..."):
        sh_prices = load_sh_goods_prices()
    with st.spinner("🍽️ Загружаю блюда из R-Keeper..."):
        rk_prices = load_rk_dish_prices(date_from, date_to)

    # ---- Загрузка себестоимости из рецептур (GDoc12) ----
    rc_key = "recipe_costs"
    rc_err_key = "recipe_costs_err"

    if rc_key not in st.session_state:
        st.session_state[rc_key] = pd.DataFrame()
        st.session_state[rc_err_key] = None

    recipe_costs = st.session_state[rc_key]
    rc_err = st.session_state[rc_err_key]

    if recipe_costs.empty:
        st.info("📋 Для расчёта фудкоста нужно загрузить себестоимость блюд из Актов нарезки (GDoc12).")
        st.caption("SH считает себестоимость порции по рецептуре (комплекту) при каждом акте нарезки.")

        c1, c2 = st.columns(2)
        with c1:
            max_cmp = st.select_slider("🔗 Комплектов для поиска:", options=[30, 50, 100, 200], value=50, key="max_cmp_sl")
        with c2:
            max_doc = st.select_slider("📄 Макс. актов нарезки:", options=[20, 50, 100, 200], value=50, key="max_doc_sl")
        st.caption(f"⏱️ Оценка: ~{max(1, (max_cmp + max_doc) // 10)} сек")

        if st.button("🚀 Загрузить себестоимость из рецептур", type="primary", use_container_width=True):
            progress_area = st.container()
            result, result_err = sh_load_recipe_foodcost(
                progress_container=progress_area, max_complects=max_cmp, max_docs=max_doc)
            st.session_state[rc_key] = result
            st.session_state[rc_err_key] = result_err
            if result_err:
                st.error(f"❌ {result_err}")
            else:
                st.success(f"✅ Себестоимость загружена для **{len(result)}** блюд!")
            st.rerun()
    else:
        st.success(f"✅ Себестоимость из рецептур: **{len(recipe_costs)}** блюд")
        if st.button("🔄 Перезагрузить"):
            del st.session_state[rc_key]
            del st.session_state[rc_err_key]
            st.rerun()

    # ---- Метрики ----
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("📦 Товаров SH", f"{len(sh_prices):,}" if not sh_prices.empty else "0")
    with c2: st.metric("🍽️ Блюд RK", f"{len(rk_prices):,}" if not rk_prices.empty else "0")
    with c3: st.metric("🍳 Блюд с себестоимостью", f"{len(recipe_costs):,}" if not recipe_costs.empty else "0")
    with c4:
        if not recipe_costs.empty:
            avg_cost = recipe_costs["COST_PER_PORTION"].mean()
            st.metric("💰 Ср. себестоимость", f"{avg_cost:.1f} ₽")
        else:
            st.metric("💰 Ср. себестоимость", "—")

    if sh_prices.empty:
        st.error("❌ Не удалось загрузить товары из StoreHouse")
    elif rk_prices.empty:
        st.warning("⚠️ Нет блюд с продажами за выбранный период")
    else:
        # Сопоставление: recipe_costs (себестоимость) + sh_prices + rk_prices
        rc_arg = recipe_costs if not recipe_costs.empty else None
        with st.spinner("🔀 Сопоставляю названия и рассчитываю фудкост..."):
            fc = match_foodcost(rk_prices, sh_prices, purchase_prices=rc_arg)

        if not fc.empty:
            has_price_diff = "PRICE_DIFF" in fc.columns
            has_foodcost = "FOODCOST_PCT" in fc.columns and fc["FOODCOST_PCT"].notna().any()

            st.divider()
            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("🔗 Сопоставлено", f"{len(fc)}")
            with c2: st.metric("📊 Покрытие RK", f"{len(fc)/max(1,len(rk_prices))*100:.0f}%")
            if has_foodcost:
                fc_with_cost = fc[fc["FOODCOST_PCT"].notna() & (fc["FOODCOST_PCT"] > 0) & (fc["FOODCOST_PCT"] < 300)]
                avg_fc = fc_with_cost["FOODCOST_PCT"].mean() if not fc_with_cost.empty else 0
                with c3:
                    color = "normal" if 25 <= avg_fc <= 35 else "inverse"
                    st.metric("🍳 Ср. фудкост", f"{avg_fc:.1f}%",
                              delta="норма" if 25 <= avg_fc <= 35 else ("выше нормы" if avg_fc > 35 else "ниже нормы"),
                              delta_color=color)
                with c4: st.metric("🍳 Блюд с фудкостом", f"{len(fc_with_cost)}")
            st.divider()

            # ==== ТАБЫ ====
            tab_names = ["🍳 Фудкост" if has_foodcost else "🍳 Фудкост (нет данных)",
                         "💰 Сравнение цен", "📄 Себестоимость по рецептурам", "📋 Таблица"]
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

                    st.markdown(f"### 📊 Сводка за период: {date_from} — {date_to}")
                    c1, c2, c3, c4 = st.columns(4)
                    with c1:
                        st.metric("💵 Выручка", f"{total_revenue:,.0f} ₽")
                    with c2:
                        st.metric("📦 Себестоимость", f"{total_cost:,.0f} ₽")
                    with c3:
                        st.metric("💰 Маржа", f"{total_margin:,.0f} ₽",
                                  delta=f"{total_margin/max(1,total_revenue)*100:.1f}% от выручки")
                    with c4:
                        fc_color = "normal" if 25 <= avg_foodcost <= 35 else "inverse"
                        st.metric("🍳 Средний фудкост", f"{avg_foodcost:.1f}%",
                                  delta="норма" if 25 <= avg_foodcost <= 35 else ("выше нормы" if avg_foodcost > 35 else "ниже нормы"),
                                  delta_color=fc_color)

                    # Покрытие выручки рецептурами
                    full_revenue = float(load_orders(date_from, date_to)["TOPAYSUM"].sum()) if not load_orders(date_from, date_to).empty else 0
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
                    st.markdown("### 📅 Помесячная динамика")
                    st.caption("Выберите период для анализа фудкоста по месяцам")

                    mc1, mc2 = st.columns(2)
                    with mc1:
                        monthly_from = st.date_input("С:", date_from - timedelta(90), key="fc_monthly_from")
                    with mc2:
                        monthly_to = st.date_input("По:", date_to, key="fc_monthly_to")

                    with st.spinner("📊 Загрузка помесячных продаж..."):
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
                                name="Выручка", marker_color="#00ff6a", text=by_month["REVENUE"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["COST"],
                                name="Себестоимость", marker_color="#ef4444", text=by_month["COST"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["MARGIN"],
                                name="Маржа", marker_color="#10b981", text=by_month["MARGIN"].apply(lambda x: f"{x:,.0f}"),
                                textposition="auto"))
                            fig.update_layout(title="💰 Выручка, себестоимость и маржа по месяцам",
                                barmode="group", height=420, xaxis_title="Месяц", yaxis_title="₽",
                                legend=dict(orientation="h", y=1.1), **CHART_THEME)
                            st.plotly_chart(fig, use_container_width=True)

                            # График 2: Фудкост % по месяцам
                            colors_fc_monthly = by_month["FOODCOST_PCT"].apply(
                                lambda x: "#ef4444" if x > 35 else ("#10b981" if x < 25 else "#f59e0b"))
                            fig2 = go.Figure()
                            fig2.add_trace(go.Bar(x=by_month["MONTH"], y=by_month["FOODCOST_PCT"],
                                marker_color=colors_fc_monthly.tolist(),
                                text=by_month["FOODCOST_PCT"].apply(lambda x: f"{x:.1f}%"),
                                textposition="auto", name="Фудкост %"))
                            fig2.add_hline(y=25, line_dash="dash", line_color="#10b981", annotation_text="25%")
                            fig2.add_hline(y=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                            fig2.update_layout(title="🍳 Фудкост % по месяцам",
                                height=350, xaxis_title="Месяц", yaxis_title="Фудкост %", **CHART_THEME)
                            st.plotly_chart(fig2, use_container_width=True)

                            # Таблица по месяцам
                            with st.expander("📋 Таблица по месяцам"):
                                month_disp = by_month.rename(columns={
                                    "MONTH":"Месяц","REVENUE":"Выручка ₽","COST":"Себестоимость ₽",
                                    "MARGIN":"Маржа ₽","FOODCOST_PCT":"Фудкост %","MARGIN_PCT":"Маржа %",
                                    "DISHES":"Блюд","SOLD":"Продано порций"})
                                st.dataframe(month_disp, use_container_width=True, hide_index=True)
                        else:
                            st.warning("Нет совпадений между продажами и себестоимостью за выбранный период")
                    elif monthly_sales.empty:
                        st.warning("Нет продаж за выбранный период для помесячного анализа")
                    else:
                        st.info("Загрузите себестоимость из рецептур для помесячного анализа")

                    st.divider()

                    # ---- РАСПРЕДЕЛЕНИЕ ПО ЗОНАМ (существующие графики) ----
                    st.markdown("### 📊 Распределение по зонам")

                    fc_fc["FC_ZONE"] = fc_fc["FOODCOST_PCT"].apply(
                        lambda x: "🟢 Норма (25–35%)" if 25 <= x <= 35
                        else ("🔴 Высокий (>35%)" if x > 35 else "🟡 Низкий (<25%)"))
                    zones = fc_fc["FC_ZONE"].value_counts().reset_index()
                    zones.columns = ["Зона", "Кол-во"]

                    cl, cr = st.columns(2)
                    with cl:
                        fig = px.pie(zones, values="Кол-во", names="Зона", hole=0.45,
                            title="Распределение по зонам фудкоста",
                            color="Зона", color_discrete_map={
                                "🟢 Норма (25–35%)": "#10b981", "🔴 Высокий (>35%)": "#ef4444", "🟡 Низкий (<25%)": "#f59e0b"})
                        fig.update_layout(height=380, **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)
                    with cr:
                        fig = px.histogram(fc_fc, x="FOODCOST_PCT", nbins=40,
                            title="Распределение фудкоста, %",
                            labels={"FOODCOST_PCT": "Фудкост %"}, color_discrete_sequence=["#00ff6a"])
                        fig.add_vline(x=25, line_dash="dash", line_color="#10b981", annotation_text="25%")
                        fig.add_vline(x=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                        fig.update_layout(height=380, **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)

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
                    fig.update_layout(title="🔴 Топ-30: самый высокий фудкост",
                        height=max(500, len(top_fc)*25),
                        yaxis=dict(autorange="reversed"), xaxis_title="Фудкост %", **CHART_THEME)
                    fig.add_vline(x=35, line_dash="dash", line_color="#ef4444", annotation_text="35%")
                    fig.add_vline(x=25, line_dash="dash", line_color="#10b981")
                    st.plotly_chart(fig, use_container_width=True)

                    # Маржинальные vs убыточные
                    if "MARGIN" in fc_fc.columns:
                        st.divider()
                        cl, cr = st.columns(2)
                        with cl:
                            st.markdown("#### 🟢 Самые маржинальные")
                            best = fc_fc.sort_values("MARGIN", ascending=False).head(15)
                            fig = px.bar(best, x="MARGIN", y="DISH_NAME", orientation="h",
                                color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                                title="Топ-15 по марже (₽)", labels={"MARGIN":"Маржа ₽","DISH_NAME":"Блюдо","FOODCOST_PCT":"ФК%"})
                            fig.update_layout(height=450, yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="ФК%", **CHART_THEME)
                            st.plotly_chart(fig, use_container_width=True)
                        with cr:
                            st.markdown("#### 🔴 Минимальная маржа")
                            worst = fc_fc.sort_values("MARGIN", ascending=True).head(15)
                            fig = px.bar(worst, x="MARGIN", y="DISH_NAME", orientation="h",
                                color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                                title="Топ-15 с мин. маржой (₽)", labels={"MARGIN":"Маржа ₽","DISH_NAME":"Блюдо","FOODCOST_PCT":"ФК%"})
                            fig.update_layout(height=450, yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="ФК%", **CHART_THEME)
                            st.plotly_chart(fig, use_container_width=True)

                    # Scatter
                    st.divider()
                    fig = px.scatter(fc_fc, x="COST_PRICE", y="SALE_PRICE",
                        hover_name="DISH_NAME", size="TOTAL_SUM",
                        color="FOODCOST_PCT", color_continuous_scale="RdYlGn_r",
                        title="💡 Себестоимость vs Цена продажи (размер = выручка)",
                        labels={"COST_PRICE": "Себестоимость ₽", "SALE_PRICE": "Цена продажи ₽", "FOODCOST_PCT":"ФК%"})
                    max_p = max(fc_fc["COST_PRICE"].max(), fc_fc["SALE_PRICE"].max()) * 1.1 if len(fc_fc) else 100
                    fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p],
                        mode="lines", line=dict(dash="dash", color="rgba(255,255,255,0.3)", width=1), name="Себест.=Цена"))
                    fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p / 0.35],
                        mode="lines", line=dict(dash="dot", color="#ef4444", width=1), name="ФК=35%"))
                    fig.update_layout(height=500, coloraxis_colorbar_title="ФК%", **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

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
                            fig.update_layout(title="📊 Средний фудкост по группам SH",
                                height=max(400, len(by_grp)*28),
                                yaxis=dict(autorange="reversed"), xaxis_title="Фудкост %", **CHART_THEME)
                            fig.add_vline(x=35, line_dash="dash", line_color="#ef4444")
                            fig.add_vline(x=25, line_dash="dash", line_color="#10b981")
                            st.plotly_chart(fig, use_container_width=True)

                    # Таблица фудкоста
                    st.divider()
                    disp_cols = ["DISH_NAME","COST_PRICE","SALE_PRICE","FOODCOST_PCT","MARGIN","TOTAL_QTY","TOTAL_SUM","_match"]
                    if "MARKUP_PCT" in fc_fc.columns:
                        disp_cols.insert(5, "MARKUP_PCT")
                    avail = [c for c in disp_cols if c in fc_fc.columns]
                    fc_disp = fc_fc[avail].rename(columns={"DISH_NAME":"Блюдо","COST_PRICE":"Себест. ₽","SALE_PRICE":"Продажа ₽",
                        "FOODCOST_PCT":"Фудкост %","MARGIN":"Маржа ₽","MARKUP_PCT":"Наценка %",
                        "TOTAL_QTY":"Продано","TOTAL_SUM":"Выручка ₽","_match":"Совпадение"})
                    sort_fc = st.selectbox("Сортировать:", ["Фудкост % (↓)","Маржа ₽ (↑)","Выручка ₽ (↓)"], key="fc_sort_main")
                    s_col = sort_fc.split(" (")[0]
                    s_asc = "↑" in sort_fc
                    if s_col in fc_disp.columns:
                        fc_disp = fc_disp.sort_values(s_col, ascending=s_asc)
                    st.dataframe(fc_disp, use_container_width=True, hide_index=True, height=500)
                    st.download_button("📥 Фудкост CSV", fc_disp.to_csv(index=False).encode("utf-8"),
                        "foodcost_recipe.csv", "text/csv", use_container_width=True)

                    # ---- ДЕТАЛИЗАЦИЯ: клик по блюду → показать Акт нарезки ----
                    st.divider()
                    st.markdown("#### 🔍 Детализация: проверка себестоимости")
                    st.caption("Выберите блюдо чтобы увидеть Акт нарезки (GDoc12) с рецептурой и расчётом себестоимости")

                    # Список блюд для выбора
                    dish_options = fc_fc.sort_values("FOODCOST_PCT", ascending=False)["DISH_NAME"].tolist()
                    selected_dish = st.selectbox("🍽️ Блюдо:", dish_options, key="drilldown_dish",
                        index=0 if dish_options else None)

                    if selected_dish:
                        dish_row = fc_fc[fc_fc["DISH_NAME"] == selected_dish].iloc[0]

                        # Метрики выбранного блюда
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: st.metric("Себестоимость", f"{dish_row.get('COST_PRICE', 0):.2f} ₽")
                        with c2: st.metric("Цена продажи", f"{dish_row.get('SALE_PRICE', 0):.2f} ₽")
                        with c3: st.metric("Фудкост", f"{dish_row.get('FOODCOST_PCT', 0):.1f}%")
                        with c4: st.metric("Маржа", f"{dish_row.get('MARGIN', 0):.2f} ₽")

                        # Ищем DOC_RID для этого блюда в recipe_costs
                        doc_rid = None
                        if not recipe_costs.empty and "LAST_DOC_RID" in recipe_costs.columns:
                            match = recipe_costs[recipe_costs["DISH_NAME"].str.strip().str.lower() == selected_dish.strip().lower()]
                            if not match.empty:
                                doc_rid = int(match.iloc[0]["LAST_DOC_RID"])

                        if doc_rid:
                            with st.spinner(f"📄 Загружаю Акт нарезки (RID={doc_rid})..."):
                                header, items, detail_err = sh_load_gdoc12_detail(doc_rid)

                            if detail_err:
                                st.error(f"Ошибка загрузки: {detail_err}")
                            else:
                                # Заголовок документа
                                if header:
                                    st.markdown(f"##### 📄 Акт нарезки №{header.get('Номер', '?')} от {header.get('Дата', '?')}")
                                    header_cols = st.columns(len(header))
                                    for i, (k, v) in enumerate(header.items()):
                                        with header_cols[min(i, len(header_cols)-1)]:
                                            st.caption(f"**{k}:** {v}")

                                # Все позиции документа — ВСЕ блюда из этого акта
                                if not items.empty:
                                    st.markdown(f"##### 📋 Все позиции акта ({len(items)} блюд)")
                                    st.caption("Каждая строка — блюдо, приготовленное по рецептуре (комплекту). Себестоимость рассчитана SH по закупочным ценам ингредиентов.")

                                    # Подсветим выбранное блюдо
                                    display_items = items.copy()
                                    if "Блюдо" in display_items.columns:
                                        display_items["⭐"] = display_items["Блюдо"].str.strip().str.lower().apply(
                                            lambda x: "→" if selected_dish.strip().lower() in x or x in selected_dish.strip().lower() else "")
                                        # Переставляем маркер в начало
                                        cols = ["⭐"] + [c for c in display_items.columns if c != "⭐"]
                                        display_items = display_items[cols]

                                    st.dataframe(display_items, use_container_width=True, hide_index=True, height=min(600, max(200, len(items)*35+50)))

                                    # Инфо о комплекте
                                    if "Комплект" in items.columns or "RID комплекта" in items.columns:
                                        sel_items = items.copy()
                                        if "Блюдо" in sel_items.columns:
                                            sel_items = sel_items[sel_items["Блюдо"].str.strip().str.lower().apply(
                                                lambda x: selected_dish.strip().lower() in x or x in selected_dish.strip().lower())]
                                        if not sel_items.empty:
                                            cmp_name = sel_items.iloc[0].get("Комплект", "?")
                                            cmp_rid = sel_items.iloc[0].get("RID комплекта", "?")
                                            st.info(f"🔗 Комплект (рецептура): **{cmp_name}** (RID: {cmp_rid})")
                                else:
                                    st.warning("Позиции документа пусты")
                        else:
                            st.caption("⚠️ Не найден RID документа для этого блюда. Попробуйте перезагрузить данные.")
                else:
                    st.info("⏳ Нажмите кнопку выше чтобы загрузить себестоимость из рецептур.")
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
                        st.plotly_chart(fig, use_container_width=True)
                    with cr:
                        fig = px.scatter(fc_valid, x="SH_SALE_PRICE", y="SALE_PRICE",
                            hover_name="DISH_NAME", size="TOTAL_SUM",
                            color="PRICE_DIFF_PCT", color_continuous_scale="RdYlGn_r",
                            title="Цена SH vs Цена RK", labels={"SH_SALE_PRICE":"Цена SH ₽","SALE_PRICE":"Цена RK ₽"})
                        max_p = max(fc_valid["SH_SALE_PRICE"].max(), fc_valid["SALE_PRICE"].max()) * 1.1
                        fig.add_trace(go.Scatter(x=[0, max_p], y=[0, max_p],
                            mode="lines", line=dict(dash="dash", color="white", width=1), name="Равны"))
                        fig.update_layout(height=450, coloraxis_colorbar_title="Разн.%", **CHART_THEME)
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Нет данных для сравнения цен")

            # ============ ТАБ: СЕБЕСТОИМОСТЬ ============
            with sub_recipe:
                if not recipe_costs.empty:
                    st.success(f"✅ {len(recipe_costs)} блюд с себестоимостью из Актов нарезки (GDoc12)")
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric("📦 Блюд", f"{len(recipe_costs):,}")
                    with c2: st.metric("💰 Ср. себестоимость", f"{recipe_costs['COST_PER_PORTION'].mean():.1f} ₽")
                    with c3: st.metric("📄 Актов нарезки", f"{recipe_costs['DOC_COUNT'].sum():,.0f}")

                    top = recipe_costs.head(30)
                    fig = px.bar(top, x="COST_PER_PORTION", y="DISH_NAME", orientation="h",
                        color="COST_PER_PORTION", color_continuous_scale="Viridis",
                        title="Топ-30 по себестоимости порции",
                        labels={"COST_PER_PORTION":"Себестоимость ₽","DISH_NAME":"Блюдо"},
                        text="COST_PER_PORTION")
                    fig.update_traces(texttemplate="%{text:.1f} ₽", textposition="auto")
                    fig.update_layout(height=max(500, len(top)*25),
                        yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="₽", **CHART_THEME)
                    st.plotly_chart(fig, use_container_width=True)

                    rc_disp = recipe_costs.rename(columns={"DISH_NAME":"Блюдо","COST_PER_PORTION":"Себест. ₽/порц",
                        "TOTAL_PORTIONS":"Порций","TOTAL_COST":"Общая себест. ₽","DOC_COUNT":"Актов","UNIT":"Ед."})
                    st.dataframe(rc_disp, use_container_width=True, hide_index=True, height=500)
                    st.download_button("📥 Себестоимость CSV", rc_disp.to_csv(index=False).encode("utf-8"),
                        "recipe_costs.csv", "text/csv", use_container_width=True)
                else:
                    st.info("Себестоимость не загружена")

            # ============ ТАБ: ТАБЛИЦА ============
            with sub_table:
                disp_cols = ["DISH_NAME", "SH_SALE_PRICE", "SALE_PRICE", "PRICE_DIFF", "PRICE_DIFF_PCT",
                             "TOTAL_QTY", "TOTAL_SUM", "SH_GROUP", "_match"]
                if has_foodcost:
                    disp_cols = ["DISH_NAME", "COST_PRICE", "SH_SALE_PRICE", "SALE_PRICE",
                                 "FOODCOST_PCT", "MARGIN", "PRICE_DIFF", "TOTAL_QTY", "TOTAL_SUM", "SH_GROUP", "_match"]
                avail = [c for c in disp_cols if c in fc.columns]
                disp_names = {"DISH_NAME":"Блюдо","COST_PRICE":"Себест. ₽","SH_SALE_PRICE":"Цена SH ₽",
                    "SALE_PRICE":"Цена RK ₽","PRICE_DIFF":"Разн. ₽","PRICE_DIFF_PCT":"Разн. %",
                    "FOODCOST_PCT":"Фудкост %","MARGIN":"Маржа ₽","TOTAL_QTY":"Продано","TOTAL_SUM":"Выручка ₽",
                    "SH_GROUP":"Группа","_match":"Совпадение"}
                disp = fc[avail].rename(columns={c: disp_names.get(c,c) for c in avail})
                sort_opts = ["Фудкост % (↓)","Маржа ₽ (↑)","Выручка ₽ (↓)"] if has_foodcost else ["Разн. ₽ (↓)","Выручка ₽ (↓)"]
                sort_by = st.selectbox("Сортировать:", sort_opts, key="fc_sort_tbl")
                s_col = sort_by.split(" (")[0]
                s_asc = "↑" in sort_by
                if s_col in disp.columns:
                    disp = disp.sort_values(s_col, ascending=s_asc)
                st.dataframe(disp, use_container_width=True, hide_index=True, height=600)
                st.download_button("📥 CSV", disp.to_csv(index=False).encode("utf-8"),
                    "foodcost_full.csv", "text/csv", use_container_width=True)

                unmatched = rk_prices[~rk_prices["DISH_NAME"].str.strip().str.lower().isin(
                    fc["DISH_NAME"].str.strip().str.lower())]
                if not unmatched.empty:
                    with st.expander(f"⚠️ Не сопоставлено: {len(unmatched)} блюд из RK"):
                        st.dataframe(unmatched.sort_values("TOTAL_SUM", ascending=False).head(30)[
                            ["DISH_NAME","SALE_PRICE","TOTAL_QTY","TOTAL_SUM"]].rename(
                            columns={"DISH_NAME":"Блюдо","SALE_PRICE":"Цена","TOTAL_QTY":"Кол-во","TOTAL_SUM":"Выручка"}),
                            use_container_width=True, hide_index=True)
        else:
            st.warning("Не удалось сопоставить блюда по названию.")
            with st.expander("🔍 Примеры названий"):
                cl, cr = st.columns(2)
                with cl:
                    st.markdown("**R-Keeper:**")
                    for n in rk_prices["DISH_NAME"].head(20): st.text(n)
                with cr:
                    st.markdown("**StoreHouse:**")
                    for n in sh_prices["SH_NAME"].head(20): st.text(n)

# --- СКЛАД: API ---
if page == "🔍 Склад: Схема":
    page_header("StoreHouse API Explorer", "🔍")
    st.caption(f"REST API: {SH_API['url']} · Пользователь: {SH_API['user']}")

    # Информация о сервере
    with st.expander("🖥️ Информация о сервере", expanded=True):
        info, info_err = sh_info()
        if info_err:
            st.error(f"❌ Сервер недоступен: {info_err}")
        else:
            cols = st.columns(3)
            with cols[0]: st.metric("Версия API", info.get("Version", "?"))
            with cols[1]: st.metric("Подключение", info.get("LinkDisp", "?"))
            db = info.get("DB", {})
            with cols[2]:
                if db:
                    st.metric("Размер БД", db.get("Size", "?"))
                    st.caption(f"ID: {db.get('Ident','')} · v{db.get('Version','')}")
                else:
                    st.metric("БД", "—")

    st.divider()

    # Проверка прав
    st.markdown("### 🔐 Права на процедуры")
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
    st.markdown("### 📋 Структура процедуры")
    proc_to_explore = st.selectbox("Процедура:", all_procs)
    if st.button("📖 Показать структуру", key="sh_struct"):
        struct_data, struct_err = sh_struct(proc_to_explore)
        if struct_err:
            st.error(f"Ошибка: {struct_err}")
        else:
            tables = struct_data.get("shTable", [])
            if tables:
                for i, tbl in enumerate(tables):
                    head = tbl.get("head", f"Table_{i}")
                    single = "📌 Однострочный" if tbl.get("SingleRow") else "📋 Многострочный"
                    st.markdown(f"**Датасет `{head}`** ({single})")
                    fields = tbl.get("fields", [])
                    if fields:
                        fd = pd.DataFrame(fields)
                        st.dataframe(fd, use_container_width=True, hide_index=True)
            else:
                st.info("Структура пуста")

    st.divider()

    # Выполнение процедуры
    st.markdown("### ▶️ Выполнить процедуру")
    exec_proc = st.selectbox("Процедура для выполнения:", all_procs, key="exec_proc")
    exec_mode = st.radio("Режим:", ["🧠 Умный (auto-параметры)", "📦 Простой (без параметров)"], horizontal=True)
    if st.button("▶️ Выполнить", key="sh_exec_btn"):
        with st.spinner(f"Выполняю {exec_proc}..."):
            if exec_mode.startswith("🧠"):
                df, exec_err = sh_exec_smart(exec_proc)
                if exec_err:
                    st.error(f"Ошибка: {exec_err}")
                elif not df.empty:
                    st.success(f"✅ {len(df)} строк, {len(df.columns)} столбцов")
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.download_button(f"📥 CSV", df.to_csv(index=False).encode("utf-8"),
                        f"sh_{exec_proc}.csv", "text/csv", use_container_width=True)
                else:
                    st.info("Пустой результат")
            else:
                all_tables, exec_err = sh_exec_all_tables(exec_proc)
        if exec_err:
            st.error(f"Ошибка: {exec_err}")
        else:
            for i, df in enumerate(all_tables):
                if not df.empty:
                    st.markdown(f"**Таблица {i+1}** ({len(df)} строк, {len(df.columns)} столбцов)")
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.download_button(f"📥 CSV таблицы {i+1}", df.to_csv(index=False).encode("utf-8"),
                        f"sh_{exec_proc}_{i}.csv", "text/csv", key=f"dl_{exec_proc}_{i}",
                        use_container_width=True)
            if not all_tables:
                st.info("Процедура вернула пустой результат")

    # SQL статистика StoreHouse
    st.divider()
    st.markdown("### 🗄️ SQL-база статистики StoreHouse")
    st.caption(f"База: {SH_STAT_DB} · Сервер: {DB_CONFIG['server']}:{DB_CONFIG['port']}")
    counts = sh_stat_table_counts()
    if not counts.empty:
        filled = counts[counts["Записей"] > 0]
        empty = counts[counts["Записей"] == 0]
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("📋 Всего таблиц", f"{len(counts)}")
        with c2: st.metric("✅ С данными", f"{len(filled)}")
        with c3: st.metric("⬚ Пустых", f"{len(empty)}")
        st.dataframe(counts, use_container_width=True, hide_index=True)

        # Показываем наполненные таблицы
        if not filled.empty:
            fig = px.bar(filled, x="Записей", y="Таблица", orientation="h",
                title="📊 Наполнение SQL-базы статистики",
                color="Записей", color_continuous_scale="Viridis", text="Записей", labels=RU)
            fig.update_traces(textposition="auto")
            fig.update_layout(height=max(300, len(filled)*40),
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False, **CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Не удалось подключиться к SQL-базе статистики")

st.divider()
st.caption(f"🔄 {date_from} — {date_to} | {datetime.now().strftime('%H:%M:%S')} | Gemini AI | {len(load_restaurants())} точек | 📦 SH API: {SH_API['url']}")
