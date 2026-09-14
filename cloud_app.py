"""Single-process MCP cloud app.

Run locally with: python cloud_app.py
Deploy on Render with: uvicorn cloud_app:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiosqlite
from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field, field_validator
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Mount, Route

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("cloud_app")

PORT = int(os.getenv("PORT", "8000"))
SQLITE_PATH = os.getenv("SQLITE_PATH", "learning_demo.db")
MCP_API_KEY = os.getenv("MCP_API_KEY")
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", f"http://localhost:{PORT}/mcp")
TOOL_TIMEOUT_SECONDS = float(os.getenv("TOOL_TIMEOUT_SECONDS", "10"))
MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS", "100"))
MAX_DRIVE_RESULT_ITEMS = int(os.getenv("MAX_DRIVE_RESULT_ITEMS", "50"))
MAX_DRIVE_CONTENT_CHARS = int(os.getenv("MAX_DRIVE_CONTENT_CHARS", "20000"))
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "300"))
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

START_TIME = time.monotonic()
_records: deque[tuple[float, str, float, bool, str | None]] = deque(maxlen=5000)
_http_records: deque[tuple[float, str, float, int]] = deque(maxlen=5000)
_recent_errors: deque[dict[str, Any]] = deque(maxlen=20)
_rate_hits: dict[str, list[float]] = {}
_drive_service: Any = None
_agent_runner: Any = None
_agent_session: Any = None
_agent_lock = asyncio.Lock()

TOOL_CALLS = Counter("cloud_tool_calls_total", "Total tool invocations", ["tool_name", "outcome"])
TOOL_LATENCY = Histogram("cloud_tool_latency_seconds", "Tool latency", ["tool_name"])
HTTP_REQUESTS = Counter("cloud_http_requests_total", "Total HTTP requests", ["path", "status_code"])
HTTP_LATENCY = Histogram("cloud_http_request_latency_seconds", "HTTP request latency", ["path"])


class ToolError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class RateLimiter:
    def allow(self, client_id: str) -> bool:
        now = time.monotonic()
        hits = [hit for hit in _rate_hits.get(client_id, []) if hit > now - 60]
        if len(hits) >= RATE_LIMIT_PER_MINUTE:
            _rate_hits[client_id] = hits
            return False
        hits.append(now)
        _rate_hits[client_id] = hits
        return True


rate_limiter = RateLimiter()


def record_call(tool: str, started: float, success: bool, error_code: str | None = None) -> None:
    latency = time.perf_counter() - started
    _records.append((time.time(), tool, latency, success, error_code))
    TOOL_LATENCY.labels(tool_name=tool).observe(latency)
    TOOL_CALLS.labels(tool_name=tool, outcome="success" if success else "error").inc()
    if not success:
        _recent_errors.appendleft({"time": time.strftime("%H:%M:%S"), "tool": tool, "code": error_code})


def record_http_request(path: str, started: float, status_code: int) -> None:
    latency = time.perf_counter() - started
    _http_records.append((time.time(), path, latency, status_code))
    HTTP_REQUESTS.labels(path=path, status_code=str(status_code)).inc()
    HTTP_LATENCY.labels(path=path).observe(latency)
    if status_code >= 500:
        _recent_errors.appendleft({"time": time.strftime("%H:%M:%S"), "path": path, "code": str(status_code)})


def check_access(ctx: Context | None, client_id: str = "anonymous") -> str:
    api_key = None
    if ctx is not None and hasattr(ctx, "get_http_header"):
        api_key = ctx.get_http_header("x-api-key", None)
    if MCP_API_KEY and api_key != MCP_API_KEY:
        raise ToolError("UNAUTHORIZED", "Missing or invalid API key.", 401)
    resolved_client = api_key or client_id
    if not rate_limiter.allow(resolved_client):
        raise ToolError("RATE_LIMIT_EXCEEDED", "Too many requests, slow down.", 429)
    return resolved_client


def compute_stats() -> dict[str, Any]:
    now = time.time()
    tool_records = list(_records)
    http_records = list(_http_records)
    recent = [item for item in http_records if now - item[0] <= 60]
    successful_latencies = sorted(item[2] * 1000 for item in http_records if item[3] < 400)

    def percentile(percent: float) -> float:
        if not successful_latencies:
            return 0.0
        index = round((percent / 100) * (len(successful_latencies) - 1))
        return round(successful_latencies[index], 1)

    by_tool: dict[str, dict[str, Any]] = {}
    for _, tool, latency, success, _ in tool_records:
        item = by_tool.setdefault(tool, {"tool": tool, "count": 0, "errors": 0, "total_latency": 0.0})
        item["count"] += 1
        item["total_latency"] += latency
        item["errors"] += int(not success)
    tools = []
    for item in by_tool.values():
        item["avg_latency_ms"] = round(item["total_latency"] * 1000 / item["count"], 1)
        del item["total_latency"]
        tools.append(item)

    return {
        "uptime_seconds": round(time.monotonic() - START_TIME, 1),
        "requests_last_minute": len(recent),
        "requests_total": len(http_records),
        "error_rate_percent": round(sum(item[3] >= 400 for item in recent) * 100 / len(recent), 1) if recent else 0.0,
        "p50_ms": percentile(50),
        "p95_ms": percentile(95),
        "tools": sorted(tools, key=lambda item: item["tool"]),
        "recent_errors": list(_recent_errors),
    }


async def init_database() -> None:
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            total_amount REAL NOT NULL
        )""")
        count = await db.execute_fetchall("SELECT COUNT(*) FROM orders")
        if count[0][0] == 0:
            await db.executemany(
                "INSERT INTO orders (account_id, order_date, status, total_amount) VALUES (?, ?, ?, ?)",
                [("ACC-12345", "2026-09-01", "SHIPPED", 129.99),
                 ("ACC-12345", "2026-09-05", "PROCESSING", 45.50),
                 ("ACC-12345", "2026-09-10", "DELIVERED", 89.00),
                 ("ACC-67890", "2026-08-20", "DELIVERED", 15.75)],
            )
            await db.commit()


class GetOrdersInput(BaseModel):
    account_id: str = Field(..., description="Customer account ID, for example ACC-12345")
    limit: int = Field(10, ge=1, le=MAX_RESULT_ROWS)

    @field_validator("account_id")
    @classmethod
    def valid_account_id(cls, value: str) -> str:
        if not value.replace("-", "").isalnum():
            raise ValueError("account_id must be alphanumeric; dashes are allowed")
        return value


class ListDriveFilesInput(BaseModel):
    folder_id: str | None = None
    query: str | None = None
    limit: int = Field(20, ge=1, le=MAX_DRIVE_RESULT_ITEMS)


class ReadDriveFileInput(BaseModel):
    file_id: str = Field(..., min_length=1)
    max_chars: int = Field(MAX_DRIVE_CONTENT_CHARS, ge=100, le=MAX_DRIVE_CONTENT_CHARS)


def get_drive_service() -> Any:
    global _drive_service
    if _drive_service is not None:
        return _drive_service
    try:
        if GOOGLE_SERVICE_ACCOUNT_JSON:
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=DRIVE_SCOPES
            )
        elif os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_SERVICE_ACCOUNT_FILE, scopes=DRIVE_SCOPES
            )
        else:
            raise ToolError("DRIVE_NOT_CONFIGURED", "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE.", 503)
        _drive_service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        return _drive_service
    except ToolError:
        raise
    except Exception as exc:
        logger.exception("Unable to initialize Google Drive")
        raise ToolError("DRIVE_CONFIGURATION_ERROR", "Google Drive configuration failed.", 503) from exc


def normalize_drive_folder_id(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if candidate.startswith("http://") or candidate.startswith("https://"):
        parsed = urlparse(candidate)
        query_id = parse_qs(parsed.query).get("id", [None])[0]
        if "/folders/" in parsed.path:
            candidate = parsed.path.split("/folders/", 1)[1].split("/", 1)[0]
        elif query_id:
            candidate = query_id
        else:
            raise ToolError("INVALID_DRIVE_FOLDER_ID", "Use the Google Drive folder ID or a valid folder URL.", 400)
    if not candidate or any(char in candidate for char in "/?#&="):
        raise ToolError("INVALID_DRIVE_FOLDER_ID", "Use only the Google Drive folder ID, not a sharing URL.", 400)
    return candidate


mcp = FastMCP(name="cloud-order-drive-mcp")


@mcp.tool
async def get_orders(input: GetOrdersInput, ctx: Context) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        check_access(ctx)
        async with asyncio.timeout(TOOL_TIMEOUT_SECONDS):
            async with aiosqlite.connect(SQLITE_PATH) as db:
                db.row_factory = sqlite3.Row
                cursor = await db.execute(
                    "SELECT order_id, order_date, status, total_amount FROM orders WHERE account_id = ? ORDER BY order_date DESC LIMIT ?",
                    (input.account_id, input.limit),
                )
                rows = [dict(row) for row in await cursor.fetchall()]
        result = {"account_id": input.account_id, "count": len(rows), "orders": rows}
        record_call("get_orders", started, True)
        return result
    except ToolError as exc:
        record_call("get_orders", started, False, exc.code)
        raise
    except Exception as exc:
        record_call("get_orders", started, False, "INTERNAL_ERROR")
        raise ToolError("INTERNAL_ERROR", "Database request failed.", 500) from exc


@mcp.tool
async def list_drive_files(input: ListDriveFilesInput, ctx: Context) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        check_access(ctx)
        service = get_drive_service()
        query_parts = ["trashed = false"]
        folder_id = normalize_drive_folder_id(input.folder_id or GOOGLE_DRIVE_FOLDER_ID)
        if folder_id:
            query_parts.append(f"'{folder_id}' in parents")
        if input.query:
            escaped = input.query.replace("'", "\\'")
            query_parts.append(f"name contains '{escaped}'")
        response = await asyncio.to_thread(lambda: service.files().list(
            q=" and ".join(query_parts), pageSize=input.limit,
            fields="files(id,name,mimeType,size,modifiedTime,webViewLink)",
            orderBy="modifiedTime desc",
        ).execute())
        result = {"count": len(response.get("files", [])), "files": response.get("files", [])}
        record_call("list_drive_files", started, True)
        return result
    except ToolError as exc:
        record_call("list_drive_files", started, False, exc.code)
        raise
    except Exception as exc:
        record_call("list_drive_files", started, False, "DRIVE_ERROR")
        raise ToolError("DRIVE_ERROR", "Google Drive request failed.", 502) from exc


@mcp.tool
async def read_drive_file(input: ReadDriveFileInput, ctx: Context) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        check_access(ctx)
        service = get_drive_service()
        metadata = await asyncio.to_thread(lambda: service.files().get(
            fileId=input.file_id, fields="id,name,mimeType,size,modifiedTime"
        ).execute())
        mime_type = metadata.get("mimeType", "")
        if mime_type.startswith("application/vnd.google-apps"):
            export_type = "text/plain" if mime_type == "application/vnd.google-apps.document" else "text/csv"
            request = service.files().export_media(fileId=input.file_id, mimeType=export_type)
        else:
            request = service.files().get_media(fileId=input.file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = await asyncio.to_thread(downloader.next_chunk)
        raw_content = buffer.getvalue()
        try:
            text = raw_content.decode("utf-8")
        except UnicodeDecodeError:
            text = base64.b64encode(raw_content).decode("ascii")
        result = {"file": metadata, "content": text[:input.max_chars], "truncated": len(text) > input.max_chars}
        record_call("read_drive_file", started, True)
        return result
    except ToolError as exc:
        record_call("read_drive_file", started, False, exc.code)
        raise
    except Exception as exc:
        record_call("read_drive_file", started, False, "DRIVE_ERROR")
        raise ToolError("DRIVE_ERROR", "Google Drive file read failed.", 502) from exc


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "cloud_app", "uptime_seconds": round(time.monotonic() - START_TIME, 1)})


async def stats(_: Request) -> JSONResponse:
    return JSONResponse(compute_stats())


async def metrics(_: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def chat_info(_: Request) -> JSONResponse:
    return JSONResponse(
        {"service": "google-adk-chat", "method": "POST", "message": "Send JSON with a message field to use the agent."},
        status_code=405,
        headers={"Allow": "POST"},
    )


async def chat(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "Request body must be valid JSON."}, status_code=400)
    message = str(body.get("message", "")).strip()
    if not message:
        return JSONResponse({"error": "message is required"}, status_code=400)
    try:
        from google.adk.agents import Agent
        from google.adk.runners import InMemoryRunner
        from google.adk.tools.mcp_tool import McpToolset
        from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPServerParams
        from google.genai import types

        global _agent_runner, _agent_session
        async with _agent_lock:
            if _agent_runner is None:
                toolset = McpToolset(
                    connection_params=StreamableHTTPServerParams(
                        url=MCP_SERVER_URL,
                        headers={"x-api-key": MCP_API_KEY} if MCP_API_KEY else {},
                    ),
                    header_provider=lambda _: {"x-api-key": MCP_API_KEY} if MCP_API_KEY else {},
                )
                agent = Agent(
                    name="cloud_order_drive_agent", model=os.getenv("GEMINI_MODEL", "gemini-3.6-flash"),
                    description="Answers order and Google Drive questions.",
                    instruction=(
                        "Use only tools returned by the MCP server. For orders, use get_orders and ask for an account ID when it is missing. "
                        "For Google Drive searches, use list_drive_files. To read a Drive file, use read_drive_file. "
                        "Never invent or call google_drive_search or google_drive:google_drive_search. "
                        "If a tool fails, explain the failure clearly and do not claim that files were found. "
                        "Summarize successful tool results in plain, friendly language."
                    ),
                    tools=[toolset],
                )
                _agent_runner = InMemoryRunner(agent=agent, app_name="cloud_app")
                _agent_session = await _agent_runner.session_service.create_session(app_name="cloud_app", user_id="web-user")
            runner, session = _agent_runner, _agent_session
        content = types.Content(role="user", parts=[types.Part(text=message)])
        answer = ""
        async for event in runner.run_async(user_id="web-user", session_id=session.id, new_message=content):
            if event.is_final_response() and event.content and event.content.parts:
                answer = event.content.parts[0].text or ""
        return JSONResponse({"answer": answer})
    except Exception as exc:
        logger.exception("Chat request failed")
        error_text = str(exc)
        if "503" in error_text or "UNAVAILABLE" in error_text:
            return JSONResponse(
                {"error": "The Gemini model is temporarily unavailable. Please retry in a moment.", "code": "MODEL_UNAVAILABLE"},
                status_code=503,
            )
        if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
            return JSONResponse(
                {"error": "Gemini API quota is temporarily exhausted. Please wait and retry.", "code": "QUOTA_EXHAUSTED"},
                status_code=429,
            )
        return JSONResponse({"error": "The agent could not complete the request.", "code": "AGENT_ERROR"}, status_code=502)


HTML = """<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>MCP Cloud Workspace</title>
<style>
:root{--ink:#172b32;--muted:#6c7d82;--paper:#fffdf9;--canvas:#edf3f1;--line:#d7e2de;--teal:#0d6b67;--teal-dark:#084b4a;--coral:#ef775f;--mint:#dceee8;--shadow:0 18px 50px rgba(24,58,59,.09)}
*{box-sizing:border-box}body{margin:0;color:var(--ink);background:var(--canvas);font-family:"Trebuchet MS",Verdana,sans-serif}button,textarea{font:inherit}button{cursor:pointer}a{color:var(--teal);text-decoration:none}a:hover{text-decoration:underline}
.shell{min-height:100vh;display:grid;grid-template-columns:230px minmax(0,1fr)}.rail{padding:28px 20px;background:var(--teal-dark);color:#f7fffb;display:flex;flex-direction:column}.brand{display:flex;align-items:center;gap:11px;font-weight:700;letter-spacing:.02em}.brand-mark{width:34px;height:34px;border-radius:10px;background:var(--coral);display:grid;place-items:center;color:white;font-weight:700}.rail-copy{margin:13px 0 35px;color:#b8d8d1;font-size:13px;line-height:1.55}.nav-label{margin:0 0 9px;color:#8fbeb5;font-size:11px;text-transform:uppercase;letter-spacing:.14em}.nav-link{display:flex;gap:10px;padding:11px 12px;margin:3px 0;border-radius:9px;color:#e6f5f0;font-size:14px}.nav-link.active{background:rgba(255,255,255,.13)}.rail-footer{margin-top:auto;color:#9fc5bc;font-size:12px;line-height:1.5}.pulse{display:inline-block;width:7px;height:7px;border-radius:50%;background:#7bd5a5;margin-right:6px}
.content{min-width:0;padding:32px clamp(20px,4vw,58px)}.topbar{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;max-width:1200px;margin:0 auto 25px}.eyebrow{color:var(--coral);font-size:12px;font-weight:700;letter-spacing:.14em;text-transform:uppercase}.topbar h1{margin:7px 0 5px;font:700 clamp(28px,4vw,42px)/1.05 Georgia,serif;letter-spacing:0}.lede{margin:0;color:var(--muted);font-size:14px}.status{display:flex;align-items:center;gap:8px;padding:10px 13px;background:rgba(255,255,255,.7);border:1px solid var(--line);border-radius:999px;color:var(--teal-dark);font-size:12px;white-space:nowrap}.status-dot{width:8px;height:8px;background:#2da66f;border-radius:50%;box-shadow:0 0 0 4px #d8f1e5}
.workspace{max-width:1200px;margin:0 auto;display:grid;grid-template-columns:minmax(0,1fr) 285px;gap:22px}.panel{background:var(--paper);border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow)}.chat-panel{min-height:650px;display:flex;flex-direction:column;overflow:hidden}.chat-head{display:flex;justify-content:space-between;align-items:center;padding:20px 22px;border-bottom:1px solid var(--line)}.chat-title{display:flex;align-items:center;gap:11px}.avatar{width:34px;height:34px;border-radius:10px;background:var(--mint);color:var(--teal);display:grid;place-items:center;font-weight:700}.chat-title strong{display:block;font-size:15px}.chat-title small{display:block;color:var(--muted);font-size:11px;margin-top:3px}.clear{border:0;background:transparent;color:var(--muted);font-size:12px;padding:8px}.clear:hover{color:var(--coral)}.messages{flex:1;min-height:300px;max-height:520px;overflow-y:auto;padding:24px 22px 10px;display:flex;flex-direction:column;gap:16px}.welcome{padding:20px;border:1px dashed #b9d6ce;background:#f3faf7;border-radius:12px}.welcome h2{margin:0 0 7px;font:700 23px Georgia,serif}.welcome p{margin:0;color:var(--muted);font-size:13px;line-height:1.55}.message{display:flex;gap:10px;max-width:88%;animation:rise .25s ease both}.message.user{margin-left:auto;flex-direction:row-reverse}.bubble{padding:12px 14px;border-radius:13px;background:#eef4f1;color:var(--ink);font-size:14px;line-height:1.55;white-space:pre-wrap;overflow-wrap:anywhere}.user .bubble{background:var(--teal);color:white;border-bottom-right-radius:4px}.assistant .bubble{border-bottom-left-radius:4px}.message-label{font-size:10px;color:var(--muted);margin:0 0 4px 2px}.composer{padding:15px 18px 18px;border-top:1px solid var(--line);background:#fffefa}.composer-box{display:flex;align-items:flex-end;gap:10px;padding:8px 8px 8px 13px;background:white;border:1px solid #b9ccc7;border-radius:13px;transition:border-color .2s,box-shadow .2s}.composer-box:focus-within{border-color:var(--teal);box-shadow:0 0 0 3px rgba(13,107,103,.11)}textarea{width:100%;min-height:44px;max-height:130px;resize:none;border:0;outline:0;background:transparent;color:var(--ink);line-height:1.45;padding:5px 0}textarea::placeholder{color:#94a4a5}.send{flex:0 0 auto;border:0;border-radius:10px;background:var(--coral);color:#fff;padding:11px 15px;font-weight:700}.send:hover{background:#d95f4b}.send:disabled{opacity:.55;cursor:wait}.hint{margin:8px 3px 0;color:#9aa8a8;font-size:11px}.suggestions{display:flex;flex-wrap:wrap;gap:7px;margin:0 0 15px}.suggestion{border:1px solid #c9ddd7;border-radius:999px;background:#f7fcfa;color:var(--teal-dark);padding:7px 10px;font-size:11px}.suggestion:hover{background:var(--mint)}
.side{display:flex;flex-direction:column;gap:16px}.side-panel{padding:19px}.side-panel h3{margin:0 0 15px;font:700 18px Georgia,serif}.metric-list{display:grid;gap:10px}.metric{display:flex;justify-content:space-between;align-items:baseline;padding-bottom:10px;border-bottom:1px solid #e4ece9}.metric:last-child{border:0;padding-bottom:0}.metric span{color:var(--muted);font-size:12px}.metric strong{font-size:20px;color:var(--teal-dark)}.tool-list{display:grid;gap:8px}.tool-row{display:flex;justify-content:space-between;color:var(--muted);font-size:12px}.tool-row strong{color:var(--ink)}.links{display:grid;gap:9px;font-size:13px}.links a{display:flex;justify-content:space-between}.links a::after{content:'->';color:var(--coral)}
.typing{display:flex;align-items:center;gap:4px;padding:14px}.typing i{width:5px;height:5px;background:var(--teal);border-radius:50%;animation:bounce 1s infinite}.typing i:nth-child(2){animation-delay:.15s}.typing i:nth-child(3){animation-delay:.3s}@keyframes bounce{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-4px)}}@keyframes rise{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
@media(max-width:900px){.shell{display:block}.rail{padding:15px 20px;display:block}.rail-copy,.nav-label,.nav-link,.rail-footer{display:none}.brand{justify-content:center}.content{padding:24px 16px}.workspace{grid-template-columns:1fr}.side{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}.chat-panel{min-height:600px}}
@media(max-width:560px){.topbar{display:block}.status{display:inline-flex;margin-top:17px}.side{display:flex}.chat-head{padding:16px}.messages{padding:18px 14px 8px}.composer{padding:12px}.message{max-width:94%}.send{padding:11px 12px}.suggestions{overflow-x:auto;flex-wrap:nowrap;padding-bottom:3px}.suggestion{white-space:nowrap}}
.send[aria-busy='true']{background:var(--teal);min-width:94px}.send[aria-busy='true'] span{display:inline-block;animation:working .8s ease-in-out infinite}.send[aria-busy='true'] span::after{content:''}.send[aria-busy='true']{pointer-events:none}@keyframes working{0%,100%{opacity:.45;transform:translateX(0)}50%{opacity:1;transform:translateX(3px)}}
</style>
</head>
<body>
<div class='shell'>
<aside class='rail'><div class='brand'><span class='brand-mark'>M</span><span>MCP Cloud</span></div><p class='rail-copy'>A focused workspace for orders, documents, and connected tools.</p><p class='nav-label'>Workspace</p><a class='nav-link active' href='/'>Chat workspace</a><a class='nav-link' href='/dashboard'>Performance</a><div class='rail-footer'><span class='pulse'></span>Service operational<br><br>Local development mode</div></aside>
<main class='content'>
<header class='topbar'><div><div class='eyebrow'>Connected intelligence</div><h1>MCP Cloud App</h1><p class='lede'>Ask questions across your order data and Google Drive.</p></div><div class='status'><span class='status-dot'></span><span>All systems operational</span></div></header>
<div class='workspace'>
<section class='panel chat-panel'><div class='chat-head'><div class='chat-title'><span class='avatar'>AI</span><div><strong>Cloud assistant</strong><small>Database and Drive tools available</small></div></div><button class='clear' id='clear' type='button'>Clear chat</button></div><div class='messages' id='messages'><div class='welcome' id='welcome'><h2>What would you like to find?</h2><p>Ask about customer orders or files in your connected Google Drive.</p></div></div><form class='composer' id='composer'><div class='suggestions'><button class='suggestion' type='button' data-prompt='Show the latest orders for account ACC-12345'>Latest orders</button><button class='suggestion' type='button' data-prompt='List the files in my Google Drive'>Browse Drive</button><button class='suggestion' type='button' data-prompt='Show the delivered orders for account ACC-12345'>Delivered orders</button></div><div class='composer-box'><textarea id='message' rows='1' placeholder='Ask the assistant...' aria-label='Message'></textarea><button class='send' id='send' type='submit'>Ask <span aria-hidden='true'>-></span></button></div><div class='hint'>Press Enter to send. Shift + Enter for a new line.</div></form></section>
<aside class='side'><section class='panel side-panel'><h3>Performance</h3><div class='metric-list'><div class='metric'><span>Requests / min</span><strong id='requests'>0</strong></div><div class='metric'><span>Error rate</span><strong id='errors'>0%</strong></div><div class='metric'><span>P50 latency</span><strong id='p50'>0 ms</strong></div><div class='metric'><span>P95 latency</span><strong id='p95'>0 ms</strong></div></div></section><section class='panel side-panel'><h3>Active tools</h3><div class='tool-list' id='tools'><div class='tool-row'><span>Waiting for activity</span><strong>-</strong></div></div></section><section class='panel side-panel'><h3>Resources</h3><div class='links'><a href='/dashboard'>Full dashboard</a><a href='/health'>Service health</a><a href='/metrics'>Prometheus metrics</a></div></section></aside>
</div></main></div>
<script>
const messages=document.querySelector('#messages');const input=document.querySelector('#message');const send=document.querySelector('#send');const composer=document.querySelector('#composer');let busy=false;
function addMessage(role,text){const welcome=document.querySelector('#welcome');if(welcome)welcome.remove();const item=document.createElement('div');item.className='message '+role;const bubble=document.createElement('div');bubble.className='bubble';bubble.textContent=text;item.appendChild(bubble);messages.appendChild(item);messages.scrollTop=messages.scrollHeight;return item}
function setBusy(value){busy=value;send.disabled=value;send.innerHTML=value?'Working <span aria-hidden="true">...</span>':'Ask <span aria-hidden="true">-></span>';send.setAttribute('aria-busy',String(value))}
async function ask(){const message=input.value.trim();if(!message||busy)return;addMessage('user',message);input.value='';input.style.height='auto';const pending=addMessage('assistant','');pending.querySelector('.bubble').innerHTML='<span class="typing"><i></i><i></i><i></i></span>';setBusy(true);try{const response=await fetch('/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message})});const data=await response.json();pending.querySelector('.bubble').textContent=response.ok?(data.answer||'No response received.'):(data.error||'The assistant could not complete that request.');}catch(error){pending.querySelector('.bubble').textContent='Connection error. Check that the service is running and try again.';}finally{setBusy(false);input.focus();messages.scrollTop=messages.scrollHeight}}
function resetWelcome(){messages.innerHTML='<div class="welcome" id="welcome"><h2>What would you like to find?</h2><p>Ask about customer orders or files in your connected Google Drive.</p></div>';input.value='';input.style.height='auto';input.focus()}
composer.addEventListener('submit',event=>{event.preventDefault();ask()});input.addEventListener('keydown',event=>{if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();ask()}});input.addEventListener('input',()=>{input.style.height='auto';input.style.height=Math.min(input.scrollHeight,130)+'px'});document.querySelector('#clear').addEventListener('click',resetWelcome);document.querySelectorAll('.suggestion').forEach(button=>button.addEventListener('click',()=>{input.value=button.dataset.prompt;input.focus();input.dispatchEvent(new Event('input'))}));
async function refresh(){try{const response=await fetch('/api/stats');const stats=await response.json();document.querySelector('#requests').textContent=stats.requests_last_minute;document.querySelector('#errors').textContent=stats.error_rate_percent+'%';document.querySelector('#p50').textContent=stats.p50_ms+' ms';document.querySelector('#p95').textContent=stats.p95_ms+' ms';const tools=document.querySelector('#tools');tools.innerHTML=stats.tools.length?stats.tools.map(tool=>'<div class="tool-row"><span>'+tool.tool+'</span><strong>'+tool.count+'</strong></div>').join(''):'<div class="tool-row"><span>Waiting for activity</span><strong>-</strong></div>'}catch(error){document.querySelector('.status').innerHTML='<span class="status-dot" style="background:#ef775f"></span><span>Service unavailable</span>'}}refresh();setInterval(refresh,5000);
</script></body></html>"""


async def home(_: Request) -> HTMLResponse:
    return HTMLResponse(HTML)


async def dashboard(_: Request) -> HTMLResponse:
    return HTMLResponse(HTML.replace("<h1>MCP Cloud App</h1>", "<h1>MCP Performance Dashboard</h1>"))


mcp_http_app = mcp.http_app(path="/mcp", transport="streamable-http")


@asynccontextmanager
async def lifespan(app: Starlette):
    await init_database()
    async with mcp_http_app.lifespan(app):
        yield


app = Starlette(
    routes=[
        Route("/", home), Route("/health", health), Route("/api/stats", stats),
        Route("/chat", chat_info, methods=["GET"]), Route("/chat", chat, methods=["POST"]),
        Route("/metrics", metrics), Route("/dashboard", dashboard), Mount("/", app=mcp_http_app),
    ],
    lifespan=lifespan,
)


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            if request.url.path not in {"/health", "/api/stats", "/metrics"}:
                record_http_request(request.url.path, started, 500)
            raise
        if request.url.path not in {"/health", "/api/stats", "/metrics"}:
            record_http_request(request.url.path, started, response.status_code)
        return response


app.add_middleware(RequestMetricsMiddleware)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("cloud_app:app", host="0.0.0.0", port=PORT, reload=False)
