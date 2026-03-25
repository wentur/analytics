# goai.rest — Интеллект ресторана v9.11

Аналитический дашборд для сети ресторанов МГУ.
Подключается к R-Keeper 7 (SQL Server) и StoreHouse API.

## Быстрый старт

```bash
bash setup.sh
pip install -r requirements.txt
streamlit run dashboard.py
```

`setup.sh` создаст скрытые файлы (`.env`, `.gitignore`, `.streamlit/config.toml`) из шаблонов.

## Файлы

| Файл | Описание |
|------|----------|
| `dashboard.py` | Основной код (~9000 строк, Streamlit) |
| `requirements.txt` | Зависимости Python |
| `setup.sh` | Скрипт первоначальной настройки |
| `env` | Креденшалы → `setup.sh` создаст `.env` |
| `env.example` | Шаблон без паролей |
| `gitignore` | → `setup.sh` создаст `.gitignore` |
| `config.toml` | Конфиг Streamlit |
| `streamlit_config.toml` | → `setup.sh` создаст `.streamlit/config.toml` |
| `demo_data.json.gz` | Демо-данные для режима без БД |

## Учётные записи

- **admin** / Come3001 — полный доступ (ИИ-чат, Проактив)
- **alisa** / alisasuper — стандартный доступ
- **demo** / demo — демо-режим (без БД)

## Подключения

- **R-Keeper 7**: `saturn.carbis.ru:7473` (SQL Server, база RK7)
- **StoreHouse API**: `saturn.carbis.ru:7477/api` (REST API)
- **Gemini AI**: для ИИ-чата и проактивного анализа

## Страницы

Пульс · Выручка · Сезонность · Блюда · Категории · Цены · ABC ·
Рестораны · Персонал · Смены · Скорость ·
Касса · Заказы · Проблемы · Удаление блюд ·
Склад · Накладные · Фудкост · Фудкост (расчёт) · Склад: Схема ·
Доход/Расход · ИИ-чат · Проактив
