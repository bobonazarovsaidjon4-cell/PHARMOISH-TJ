"""Local development server for the PHARMOISH TJ single-file prototype and AI chat endpoint.

Configure AI_API_KEY, AI_MODEL and optionally AI_API_URL in the server environment.
Never expose these values to the browser. Bind to localhost by default.
"""

from __future__ import annotations

import json
import hashlib
import hmac
import logging
import os
import re
import secrets
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
DATABASE_PATH = ROOT / "data" / "pharmoish.sqlite3"
SESSION_COOKIE = "pharmoish_session"
SESSION_DAYS = 7
PASSWORD_ITERATIONS = 260_000
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pharmoish.ai")
MAX_BODY_BYTES = 24_000
RATE_LIMIT_REQUESTS = 12
RATE_LIMIT_WINDOW_SECONDS = 60
_rate_lock = threading.Lock()
_request_times: dict[str, deque[float]] = defaultdict(deque)

SYSTEM_PROMPT = (
    "Ты — FARMOISH AI, помощник PHARMOISH TJ, B2B-площадки для закупок и поиска поставщиков в Таджикистане. "
    "Отвечай на русском языке ясно, корректно и по делу. Помогай покупателям составлять заявки, искать "
    "поставщиков и сравнивать коммерческие предложения; помогай поставщикам находить закупки и готовить "
    "предложения; объясняй функции и навигацию платформы. Не утверждай, что можешь публиковать заявки, "
    "проверять компании или совершать сделки, если интеграция этого не поддерживает. Не придумывай факты "
    "о поставщиках, ценах, статусах или возможностях сервиса. По государственным закупкам указывай, что "
    "они должны проводиться по законодательству Республики Таджикистан и применимым официальным процедурам; "
    "не предлагай обходить эти процедуры. Не запрашивай пароли, API-ключи и избыточные персональные данные. "
    "Архитектура допускает добавление таджикского языка позднее, но сейчас отвечай по-русски."
)

MOCK_TENDERS = [
    ("PH-24018", "Промышленные насосы для производственной линии", "Global Manufacturing Co.", "Требуются насосы для производственной линии. Укажите модель, гарантию и условия доставки.", "Оборудование", "Душанбе", 85000, 125000, "2026-12-20", "Гарантия не менее 12 месяцев; паспорт изделия; монтаж желателен.", ["Техническое задание", "Спецификация"], "open"),
    ("PH-24017", "Поставка цемента для строительного объекта", "Стройинвест Душанбе", "Поставка цемента с подтверждением качества и графиком отгрузки.", "Строительство", "Худжанд", 45000, 68000, "2026-12-28", "Сертификаты качества; регулярная доставка.", ["Спецификация"], "open"),
    ("PH-24016", "Техническое обслуживание вентиляции", "Бизнес-центр Сомон", "Плановое обслуживание систем вентиляции коммерческого объекта.", "Услуги", "Душанбе", 30000, 42000, "2027-01-15", "Опыт обслуживания коммерческих объектов; гарантия на работы.", ["Перечень оборудования"], "review"),
    ("PH-24015", "Кабельная продукция и электроматериалы", "ЭнергоСнаб", "Поставка кабеля и электротехнических материалов по спецификации.", "Материалы", "Бохтар", 65000, 89500, "2027-01-30", "Нужны сертификаты соответствия и сведения о производителе.", ["Техническая спецификация"], "open"),
    ("PH-24014", "Резервный генератор для коммерческого объекта", "Фароз Групп", "Подбор и поставка резервного генератора с монтажом.", "Оборудование", "Куляб", 90000, 140000, "2027-02-10", "Пусконаладка, гарантийное обслуживание, доступность запчастей.", ["Требования к мощности"], "open"),
    ("PH-24013", "Поставка строительных смесей", "Навсоз Душанбе", "Регулярная поставка сухих смесей для отделочных работ.", "Строительство", "Душанбе", 18000, 36000, "2027-02-18", "Описание фасовки и графика поставок.", ["Перечень материалов"], "closed"),
]


def db_connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database() -> None:
    with db_connect() as database:
        database.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE COLLATE NOCASE,
                company TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('buyer', 'supplier')),
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                expires_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tenders (
                id TEXT PRIMARY KEY,
                organizer_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                organizer_name TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                region TEXT NOT NULL,
                budget_min REAL,
                budget_max REAL,
                deadline TEXT NOT NULL,
                requirements TEXT NOT NULL DEFAULT '',
                documents TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'open',
                source TEXT NOT NULL DEFAULT 'live',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id TEXT NOT NULL REFERENCES tenders(id) ON DELETE CASCADE,
                supplier_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                supplier_name TEXT NOT NULL,
                message TEXT NOT NULL,
                amount REAL NOT NULL,
                delivery_days INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'submitted',
                created_at TEXT NOT NULL,
                UNIQUE(tender_id, supplier_id)
            );
            CREATE INDEX IF NOT EXISTS idx_tenders_status_deadline ON tenders(status, deadline);
            CREATE INDEX IF NOT EXISTS idx_tenders_category_region ON tenders(category, region);
            CREATE INDEX IF NOT EXISTS idx_proposals_supplier ON proposals(supplier_id, created_at);
        """)
        existing = database.execute("SELECT 1 FROM tenders LIMIT 1").fetchone()
        if not existing:
            now = datetime.now(timezone.utc).isoformat()
            database.executemany("""
                INSERT INTO tenders (id, organizer_name, title, description, category, region,
                    budget_min, budget_max, deadline, requirements, documents, status, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'mock', ?)
            """, [
                (tender[0], tender[2], tender[1], tender[3], tender[4], tender[5], tender[6],
                 tender[7], tender[8], tender[9], json.dumps(tender[10], ensure_ascii=False), tender[11], now)
                for tender in MOCK_TENDERS
            ])


def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    salt = salt or secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS)
    return salt.hex(), derived.hex()


def public_user(row: sqlite3.Row) -> dict:
    return {"id": row["id"], "name": row["name"], "email": row["email"], "company": row["company"], "role": row["role"]}


def tender_json(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"], "organizer": row["organizer_name"], "title": row["title"],
        "description": row["description"], "category": row["category"], "region": row["region"],
        "budgetMin": row["budget_min"], "budgetMax": row["budget_max"], "deadline": row["deadline"],
        "requirements": row["requirements"], "documents": json.loads(row["documents"] or "[]"),
        "status": row["status"], "source": row["source"], "createdAt": row["created_at"],
    }


def load_ai_config() -> dict[str, str]:
    """Read non-secret defaults and server-only secrets from env or ignored .env."""
    local_values: dict[str, str] = {}
    env_file = ROOT / ".env"
    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            value = value.strip().strip("\"'")
            if name.strip() in {"AI_API_KEY", "AI_API_URL", "AI_MODEL"}:
                local_values[name.strip()] = value
    except OSError:
        pass

    def config_value(name: str, default: str = "") -> str:
        return os.environ.get(name, local_values.get(name, default)).strip()

    key = config_value("AI_API_KEY")
    if key.casefold() in {"false", "none", "null", "0", "your_api_key_here", "replace_me"}:
        key = ""
    return {
        "api_key": key,
        "api_url": config_value("AI_API_URL", "https://api.openai.com/v1/chat/completions"),
        "model": config_value("AI_MODEL", "gpt-4o-mini"),
    }


def json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def is_rate_limited(client: str) -> bool:
    now = time.monotonic()
    with _rate_lock:
        timestamps = _request_times[client]
        while timestamps and now - timestamps[0] > RATE_LIMIT_WINDOW_SECONDS:
            timestamps.popleft()
        if len(timestamps) >= RATE_LIMIT_REQUESTS:
            return True
        timestamps.append(now)
        return False


class TenderTJHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def translate_path(self, path: str) -> str:
        # The existing project is a single HTML page, so expose the requested
        # dashboard route without introducing a framework or rewriting the site.
        if urlsplit(path).path.rstrip("/") in ("", "/dashboard/ai-assistant"):
            return str(ROOT / "Untitled-1.html")
        return super().translate_path(path)

    def is_public_static_path(self) -> bool:
        requested = unquote(urlsplit(self.path).path).replace("\\", "/")
        if requested in {"", "/", "/dashboard/ai-assistant", "/Untitled-1.html"}:
            return True
        parts = requested.split("/")
        if any(part in {".", ".."} or part.startswith(".") for part in parts):
            return False
        return requested.startswith("/images/") and Path(requested).suffix.lower() in {
            ".svg", ".png", ".jpg", ".jpeg", ".webp", ".ico"
        }

    def send_json(self, status: int, payload: dict) -> None:
        body = json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if urlsplit(self.path).path == "/api/ai/health":
            configured = bool(load_ai_config()["api_key"])
            self.send_json(200, {"available": configured, "configured": configured})
            return
        if not self.is_public_static_path():
            self.send_error(404)
            return
        super().do_GET()

    def do_HEAD(self) -> None:
        if not self.is_public_static_path():
            self.send_error(404)
            return
        super().do_HEAD()

    def do_POST(self) -> None:
        if urlsplit(self.path).path != "/api/ai/chat":
            self.send_json(404, {"error": {"code": "NOT_FOUND", "message": "Маршрут не найден."}})
            return

        client = self.client_address[0]
        if is_rate_limited(client):
            self.send_json(429, {"error": {"code": "RATE_LIMITED", "message": "Слишком много запросов. Попробуйте позже."}})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_json(400, {"error": {"code": "INVALID_REQUEST", "message": "Некорректный запрос."}})
            return
        if content_length <= 0 or content_length > MAX_BODY_BYTES:
            self.send_json(413, {"error": {"code": "REQUEST_TOO_LARGE", "message": "Сообщение слишком большое."}})
            return

        try:
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.send_json(400, {"error": {"code": "INVALID_REQUEST", "message": "Не удалось прочитать сообщение."}})
            return

        messages = payload.get("messages") if isinstance(payload, dict) else None
        if not isinstance(messages, list) or not messages or len(messages) > 20:
            self.send_json(400, {"error": {"code": "INVALID_MESSAGES", "message": "Проверьте историю сообщений."}})
            return

        safe_messages = []
        total_chars = 0
        for message in messages:
            if not isinstance(message, dict) or message.get("role") not in ("user", "assistant"):
                self.send_json(400, {"error": {"code": "INVALID_MESSAGES", "message": "Формат сообщений некорректен."}})
                return
            content = message.get("content")
            if not isinstance(content, str) or not content.strip() or len(content) > 4000:
                self.send_json(400, {"error": {"code": "INVALID_MESSAGES", "message": "Проверьте текст сообщения."}})
                return
            total_chars += len(content)
            safe_messages.append({"role": message["role"], "content": content.strip()})
        if total_chars > 16_000 or safe_messages[-1]["role"] != "user":
            self.send_json(400, {"error": {"code": "INVALID_MESSAGES", "message": "История сообщений слишком большая или неполная."}})
            return

        ai_config = load_ai_config()
        if not ai_config["api_key"]:
            self.send_json(503, {"error": {"code": "AI_UNAVAILABLE", "message": "ИИ-ассистент временно недоступен."}})
            return

        request_body = json_bytes({
            "model": ai_config["model"],
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}, *safe_messages],
            "temperature": 0.3,
            "max_tokens": 900,
        })
        upstream_request = Request(
            ai_config["api_url"],
            data=request_body,
            headers={
                "Authorization": f"Bearer {ai_config['api_key']}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(upstream_request, timeout=40) as response:
                upstream = json.loads(response.read(1_000_000).decode("utf-8"))
            answer = upstream["choices"][0]["message"]["content"]
            if not isinstance(answer, str) or not answer.strip():
                raise ValueError("Empty model response")
            self.send_json(200, {"message": answer.strip()[:12_000]})
        except HTTPError as error:
            # Log only the numeric status; never log provider response bodies or credentials.
            provider_error_code = ""
            try:
                provider_payload = json.loads(error.read(16_384).decode("utf-8"))
                provider_error = provider_payload.get("error", {})
                if isinstance(provider_error, dict):
                    provider_error_code = str(provider_error.get("code") or provider_error.get("type") or "")[:80]
            except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
                pass
            logger.warning("AI provider returned HTTP %s (code=%s)", error.code, provider_error_code or "unspecified")
            safe_errors = {
                400: ("AI_REQUEST_REJECTED", "Провайдер отклонил запрос. Проверьте имя модели и настройки API."),
                401: ("AI_KEY_REJECTED", "Провайдер отклонил API-ключ. Проверьте ключ и убедитесь, что он активен."),
                403: ("AI_ACCESS_DENIED", "У ключа нет доступа к выбранной модели или API."),
                404: ("AI_MODEL_NOT_FOUND", "Модель или адрес API не найдены. Проверьте AI_MODEL и AI_API_URL."),
                429: ("AI_QUOTA_OR_RATE_LIMIT", "Лимит или баланс AI-провайдера исчерпан. Проверьте квоту и оплату аккаунта."),
            }
            code, message = safe_errors.get(
                error.code,
                ("AI_PROVIDER_ERROR", "AI-провайдер временно недоступен. Попробуйте позже."),
            )
            if error.code == 429:
                if provider_error_code in {"rate_limit_exceeded", "requests", "tokens"}:
                    code, message = "AI_RATE_LIMITED", "AI-провайдер временно ограничил частоту запросов. Подождите минуту и попробуйте снова."
                elif provider_error_code in {"insufficient_quota", "billing_hard_limit_reached", "billing_not_active", "credit_balance_exhausted"}:
                    code, message = "AI_QUOTA_EXHAUSTED", "API-ключ принят, но баланс или квота AI-аккаунта исчерпаны. Проверьте Billing, Credits и Usage в кабинете провайдера; при необходимости пополните баланс. Подписка ChatGPT Plus/API-баланс — разные вещи."
            self.send_json(503, {"error": {"code": code, "message": message}})
        except (URLError, TimeoutError) as error:
            logger.warning("AI provider connection failed: %s", type(error).__name__)
            self.send_json(503, {"error": {"code": "AI_NETWORK_ERROR", "message": "Не удалось связаться с AI-провайдером. Проверьте подключение к интернету и AI_API_URL."}})
        except (ValueError, KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
            logger.warning("AI provider response could not be processed: %s", type(error).__name__)
            self.send_json(503, {"error": {"code": "AI_BAD_RESPONSE", "message": "AI-провайдер вернул неожиданный ответ. Проверьте настройки модели."}})


def main() -> None:
    host = os.environ.get("TENDER_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8765"))
    server = ThreadingHTTPServer((host, port), TenderTJHandler)
    print(f"PHARMOISH TJ prototype: http://{host}:{port}/")
    print(f"AI endpoint: {'configured' if load_ai_config()['api_key'] else 'not configured (FARMOISH AI will show unavailable)'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping Tender TJ server...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
