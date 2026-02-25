# bot_supervision.py
# Requisitos:
#   pip install -U python-telegram-bot==21.6 gspread google-auth pillow
#
# Ejecutar (Windows / PowerShell):
#   $env:BOT_TOKEN="TU_TOKEN"
#   $env:SHEET_ID="TU_SHEET_ID"
#   $env:SHEET_TAB_PLANTILLAS="Plantillas"
#   $env:SHEET_TAB_SUPERVISIONES_V2="Supervisiones_v2"
#   $env:SHEET_TAB_SUPERVISORES="SUPERVISORES"
#   $env:SHEET_TAB_TECNICOS_TUFIBRA="TECNICOS_TUFIBRA"
#   $env:SHEET_TAB_CUADRILLAS_WIN="CUADRILLAS_WIN"
#   $env:SHEET_TAB_ROUTING="ROUTING"
#   $env:SHEET_TAB_PAIRING="PAIRING"
#   $env:GOOGLE_CREDS_JSON_TEXT=(Get-Content google_creds.json -Raw)   # recomendado en Railway
#   python bot_supervision.py
#
# IMPORTANTE:
# - Para que el bot reciba mensajes en grupos: @BotFather -> Group Privacy -> Turn OFF
# - En GRUPOS, Telegram NO permite request_location=True.
#   Este bot pide que envíen ubicación MANUALMENTE (clip -> ubicación).
#
# CAMBIOS IMPLEMENTADOS (resumen):
# 1) Modelo Supervisiones_v2 + ESTADO (Completado/No Completado)
# 2) Relación con Plantillas: solo Contrata/Gestor/Distrito + PlantillaUUID
# 3) Paso 2 condicionado: TU FIBRA -> técnico (lista Sheet); WIN -> cuadrilla (búsqueda + sugerencias desde CUADRILLAS_WIN)
# 4) Listas dinámicas desde Sheets (SUPERVISORES / TECNICOS_TUFIBRA / CUADRILLAS_WIN) con cache TTL
# 5) Routing & Pairing por Sheets + /config (admin-only) y vinculación por código
# 6) Ubicación estructurada: Latitud/Longitud + Link_Ubicacion (Google Maps)
# 7) UX: reducción de spam al subir múltiples fotos (mensaje único/edición)
# 8) ✅ Admin-check basado en get_chat_member() comparando strings robustos (sin ChatMemberStatus)
# 9) ✅ FIX BUG "WIN PASO 2.1": bandera expecting_codigo para que codigo_global NO capture búsquedas como código
#
# ✅ NUEVOS CAMBIOS (ESTABILIDAD):
# A) Watermark DESACTIVADO por defecto para reducir 429 (ENABLE_WATERMARK_PHOTOS default=false)
# B) Guardado en Sheets SIEMPRE se ejecuta aunque Telegram frene el envío (orden + try/except)
# C) Envió a Telegram con reintentos ante RetryAfter (flood control) para evitar que reviente el handler

import os
import re
import json
import uuid
import time
import sys
import asyncio
import logging
import secrets
import string
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from PIL import Image, ImageDraw, ImageFont  # watermark (queda instalado, pero por defecto desactivado)

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto,
    InputMediaVideo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from telegram.error import RetryAfter, TelegramError

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# =========================
# CAPTURA DE ERRORES "SILENCIOSOS" (Railway/PTB)
# =========================
def _install_global_exception_handlers() -> None:
    """
    Captura:
    - Excepciones no manejadas del proceso (sys.excepthook)
    - Excepciones no manejadas en el event loop de asyncio
    Esto ayuda a ver el "error real" que provoca que el bot se detenga.
    """
    def _excepthook(exc_type, exc, tb):
        logging.critical("UNHANDLED EXCEPTION (sys.excepthook)", exc_info=(exc_type, exc, tb))
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass

    sys.excepthook = _excepthook

    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = None

    if loop:
        def _loop_exception_handler(_loop, context):
            logging.critical("UNHANDLED ASYNCIO ERROR: %s", context.get("message", ""))
            exc = context.get("exception")
            if exc:
                logging.critical("Exception:", exc_info=exc)
            else:
                logging.critical("Context: %s", context)
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass

        loop.set_exception_handler(_loop_exception_handler)

_install_global_exception_handlers()

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB_PLANTILLAS = os.getenv("SHEET_TAB_PLANTILLAS", "Plantillas").strip()

# ✅ Nuevo: Supervisiones_v2
SHEET_TAB_SUPERVISIONES_V2 = os.getenv("SHEET_TAB_SUPERVISIONES_V2", "Supervisiones_v2").strip()

# ✅ Nuevos: listas dinámicas
SHEET_TAB_SUPERVISORES = os.getenv("SHEET_TAB_SUPERVISORES", "SUPERVISORES").strip()
SHEET_TAB_TECNICOS_TUFIBRA = os.getenv("SHEET_TAB_TECNICOS_TUFIBRA", "TECNICOS_TUFIBRA").strip()
SHEET_TAB_CUADRILLAS_WIN = os.getenv("SHEET_TAB_CUADRILLAS_WIN", "CUADRILLAS_WIN").strip()

# ✅ Nuevos: routing/pairing por sheets
SHEET_TAB_ROUTING = os.getenv("SHEET_TAB_ROUTING", "ROUTING").strip()
SHEET_TAB_PAIRING = os.getenv("SHEET_TAB_PAIRING", "PAIRING").strip()

# Cache/refresh
SUP_CACHE_TTL_SEC = int(os.getenv("SUP_CACHE_TTL_SEC", "180"))              # 3 min default
ROUTING_CACHE_TTL_SEC = int(os.getenv("ROUTING_CACHE_TTL_SEC", "180"))      # 3 min default
PAIRING_TTL_MINUTES = int(os.getenv("PAIRING_TTL_MINUTES", "10"))           # 10 min default
CUAD_CACHE_TTL_SEC = int(os.getenv("CUAD_CACHE_TTL_SEC", "180"))            # 3 min default (CUADRILLAS_WIN)

# WIN UX
WIN_SUGGEST_MAX = int(os.getenv("WIN_SUGGEST_MAX", "6"))                    # máximo 6 sugerencias (definido)
WIN_BUTTONS_MAX = 5                                                        # >5 => mostrar 5 + "Refinar búsqueda"

# En Railway: NO subas google_creds.json al repo.
# Usa GOOGLE_CREDS_JSON_TEXT (contenido JSON completo).
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "google_creds.json").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()

# ⚠️ Fallback temporal opcional para migración de ROUTING (si lo necesitas)
# Formato esperado (ejemplo):
# {"-100123":{"evidence":"-100999","summary":"-100888"}}
ROUTING_JSON = os.getenv("ROUTING_JSON", "").strip()

MAX_MEDIA_PER_BUCKET = int(os.getenv("MAX_MEDIA_PER_BUCKET", "8"))

# =========================
# Watermark (DESACTIVADO por defecto para bajar 429)
# =========================
# Antes: default true. Ahora: default false.
ENABLE_WATERMARK_PHOTOS = os.getenv("ENABLE_WATERMARK_PHOTOS", "false").lower() in ("1", "true", "yes", "y")
WM_DIR = os.getenv("WM_DIR", "wm_tmp").strip()  # Railway recomendado: /tmp/wm_tmp
WM_FONT_SIZE = int(os.getenv("WM_FONT_SIZE", "22"))

# UX anti-spam media notify
MEDIA_NOTIFY_DEBOUNCE_SEC = float(os.getenv("MEDIA_NOTIFY_DEBOUNCE_SEC", "1.0"))

# Telegram retry policy
TG_MAX_RETRIES = int(os.getenv("TG_MAX_RETRIES", "5"))
TG_RETRY_JITTER_SEC = float(os.getenv("TG_RETRY_JITTER_SEC", "0.7"))

# =========================
# TIMEZONE
# =========================
PERU_TZ = timezone(timedelta(hours=-5))

def now_peru_dt() -> datetime:
    return datetime.now(PERU_TZ)

def now_peru_str() -> str:
    return now_peru_dt().strftime("%Y-%m-%d %H:%M:%S")

def iso_peru(dt: datetime) -> str:
    # ISO-like, pero simple y legible
    return dt.astimezone(PERU_TZ).strftime("%Y-%m-%d %H:%M:%S")

# =========================
# Telegram: wrapper con reintentos (RetryAfter/429)
# =========================
async def tg_call_with_retry(coro_factory, *, what: str = "telegram_call"):
    """
    Ejecuta una llamada async a Telegram con reintentos si ocurre RetryAfter (flood control).
    - coro_factory: lambda: <awaitable>
    """
    last_exc = None
    for attempt in range(1, TG_MAX_RETRIES + 1):
        try:
            return await coro_factory()
        except RetryAfter as e:
            wait = float(getattr(e, "retry_after", 0) or 0)
            wait = max(wait, 1.0)
            # pequeño jitter para evitar sincronía
            wait = wait + (TG_RETRY_JITTER_SEC * (0.5 + (attempt / (TG_MAX_RETRIES + 1))))
            logging.warning("⏳ RetryAfter en %s (intento %s/%s). Esperando %.1fs", what, attempt, TG_MAX_RETRIES, wait)
            await asyncio.sleep(wait)
            last_exc = e
            continue
        except TelegramError as e:
            # Otros errores de Telegram (incluye 429 que no venga como RetryAfter en algún caso raro)
            logging.warning("⚠️ TelegramError en %s (intento %s/%s): %s", what, attempt, TG_MAX_RETRIES, e)
            last_exc = e
            # backoff suave
            await asyncio.sleep(1.0 + attempt * 0.5)
            continue
        except Exception as e:
            logging.warning("⚠️ Error no-Telegram en %s (intento %s/%s): %s", what, attempt, TG_MAX_RETRIES, e)
            last_exc = e
            await asyncio.sleep(1.0 + attempt * 0.5)
            continue
    # si se agotó, lanzar para que quien llame decida si ignora o aborta
    raise last_exc if last_exc else RuntimeError(f"{what}: fallo sin excepción?")

# =========================
# STATES
# =========================
(
    S_SUPERVISOR,
    S_OPERADOR,
    S_WIN_CUADRILLA,        # ✅ WIN: búsqueda/selección de cuadrilla
    S_CODIGO,
    S_TIPO,
    S_UBICACION,
    S_FACHADA_MEDIA,
    S_MENU_PRINCIPAL,
    S_MENU_CABLEADO,
    S_MENU_CUADRILLA,
    S_CARGA_MEDIA_BUCKET,
    S_ASK_OBS,
    S_WRITE_OBS,
    S_FINAL_TEXT,
    # /config flow
    S_CFG_MENU,
    S_CFG_WAIT_CODE,        # destino pega código
) = range(16)

# =========================
# MENUS / OPCIONES
# =========================
OPERADORES = ["WIN", "TU FIBRA"]

CABLEADO_ITEMS = [
    ("1. CTO", "CTO"),
    ("2. POSTE", "POSTE"),
    ("3. RUTA", "RUTA"),
    ("4. FALSO TRAMO", "FALSO_TRAMO"),
    ("5. ANCLAJE", "ANCLAJE"),
    ("6. RESERVA DOMICILIO", "RESERVA"),
    ("7. ROSETA", "ROSETA"),
    ("8. EQUIPOS", "EQUIPOS"),
    ("9. FINALIZAR EVIDENCIAS", "FIN_CABLEADO"),
]

CUADRILLA_ITEMS = [
    ("1. FOTO TECNICOS", "FOTO_TECNICOS"),
    ("2. SCTR", "SCTR"),
    ("3. ATS", "ATS"),
    ("4. LICENCIA", "LICENCIA"),
    ("5. UNIDAD", "UNIDAD"),
    ("6. SOAT", "SOAT"),
    ("7. HERRAMIENTAS", "HERRAMIENTAS"),
    ("8. KIT DE FIBRA", "KIT_FIBRA"),
    ("9. ESCALERA TELESCOPICA", "ESCALERA_TEL"),
    ("10. ESCALERA INTERNOS", "ESCALERA_INT"),
    ("11. BOTIQUIN", "BOTIQUIN"),
    ("12. FINALIZAR EVIDENCIAS", "FIN_CUADRILLA"),
]

MAIN_MENU = [
    ("🏗️EVIDENCIAS DE CABLEADO", "MENU_CABLEADO"),
    ("👷‍♂️EVIDENCIAS DE CUADRILLA", "MENU_CUADRILLA"),
    ("🚨EVIDENCIAS OPCIONALES", "MENU_OPCIONALES"),
    ("✅FINALIZAR SUPERVISION", "FINALIZAR"),
]

CABLEADO_PATTERN = r"^(CTO|POSTE|RUTA|FALSO_TRAMO|ANCLAJE|RESERVA|ROSETA|EQUIPOS|FIN_CABLEADO)$"
CUADRILLA_PATTERN = r"^(FOTO_TECNICOS|SCTR|ATS|LICENCIA|UNIDAD|SOAT|HERRAMIENTAS|KIT_FIBRA|ESCALERA_TEL|ESCALERA_INT|BOTIQUIN|FIN_CUADRILLA)$"

# =========================
# Helpers UI
# =========================
def kb_inline(options: List[Tuple[str, str]], cols: int = 2) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for label, data in options:
        row.append(InlineKeyboardButton(label, callback_data=data))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)

def evidence_controls_keyboard() -> InlineKeyboardMarkup:
    return kb_inline(
        [("➕ CARGAR MAS", "ADD_MORE"), ("✅ EVIDENCIAS COMPLETAS", "DONE_MEDIA")],
        cols=1,
    )

def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]

async def send_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    await tg_call_with_retry(
        lambda: context.application.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=reply_markup,
        ),
        what="send_message",
    )

async def safe_edit_or_send(query, text: str, reply_markup=None):
    try:
        await tg_call_with_retry(lambda: query.edit_message_text(text=text, reply_markup=reply_markup), what="edit_message_text")
    except Exception:
        await tg_call_with_retry(
            lambda: query.get_bot().send_message(chat_id=query.message.chat_id, text=text, reply_markup=reply_markup),
            what="fallback_send_message",
        )

def in_group(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.type in ("group", "supergroup")

# =========================
# Admin check (SIN ChatMemberStatus)
# =========================
# - Seguimos usando get_chat_member()
# - Pero NO importamos ChatMemberStatus: comparamos strings robustos.
# - En PTB, m.status típicamente es "creator", "administrator", "member", "restricted", "left", "kicked"
_IS_ADMIN_LOGGED_ONCE = False

def _status_str(m_status: Any) -> str:
    try:
        return str(m_status).strip().lower()
    except Exception:
        return ""

def _is_admin_status(m_status: Any) -> bool:
    s = _status_str(m_status)
    # robustez extra: algunas libs imprimen "ChatMemberStatus.ADMINISTRATOR"
    # entonces también evaluamos si contiene la palabra.
    if s in ("administrator", "creator"):
        return True
    if "administrator" in s or "creator" in s:
        return True
    return False

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Retorna True si el usuario es admin/creator del chat.
    - Se basa en get_chat_member()
    - Sin ChatMemberStatus (evita incompatibilidades de enums)
    """
    global _IS_ADMIN_LOGGED_ONCE

    if not in_group(update):
        return False
    if not update.effective_user or not update.effective_chat:
        return False
    try:
        m = await context.application.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        return _is_admin_status(getattr(m, "status", None))
    except Exception as e:
        if not _IS_ADMIN_LOGGED_ONCE:
            _IS_ADMIN_LOGGED_ONCE = True
            logging.exception("is_admin() falló al validar permisos (se retornará False). Detalle: %s", e)
        return False

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    # quitar tildes básico (sin dependencias)
    repl = str.maketrans("áéíóúüñ", "aeiouun")
    s = s.translate(repl)
    return s

# =========================
# Google Sheets helper
# =========================
_GS_CACHE: Dict[str, Any] = {"client": None, "headers": {}}

def ensure_google_creds_file() -> None:
    """
    Compat: crea el archivo GOOGLE_CREDS_JSON (default google_creds.json)
    usando el contenido de GOOGLE_CREDS_JSON_TEXT.
    """
    if GOOGLE_CREDS_JSON_TEXT and not os.path.exists(GOOGLE_CREDS_JSON):
        try:
            d = os.path.dirname(GOOGLE_CREDS_JSON)
            if d:
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass
        with open(GOOGLE_CREDS_JSON, "w", encoding="utf-8") as f:
            f.write(GOOGLE_CREDS_JSON_TEXT)

def _gs_ready() -> bool:
    if not SHEET_ID:
        return False
    if GOOGLE_CREDS_JSON_TEXT:
        return True
    return os.path.exists(GOOGLE_CREDS_JSON)

def gs_clear_cache() -> None:
    _GS_CACHE["client"] = None
    _GS_CACHE["headers"] = {}

def gs_client() -> gspread.Client:
    if _GS_CACHE["client"] is not None:
        return _GS_CACHE["client"]

    if not _gs_ready():
        raise RuntimeError("Google Sheets no está configurado (SHEET_ID o GOOGLE_CREDS_JSON_TEXT/archivo).")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if GOOGLE_CREDS_JSON_TEXT:
        info = json.loads(GOOGLE_CREDS_JSON_TEXT)
        pk = info.get("private_key", "")
        if isinstance(pk, str) and "\\n" in pk:
            info["private_key"] = pk.replace("\\n", "\n")
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        ensure_google_creds_file()
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)

    client = gspread.authorize(creds)
    _GS_CACHE["client"] = client
    return client

def gs_ws(tab_name: str):
    client = gs_client()
    sh = client.open_by_key(SHEET_ID)
    return sh.worksheet(tab_name)

def gs_headers(tab_name: str) -> List[str]:
    if tab_name in _GS_CACHE["headers"]:
        return _GS_CACHE["headers"][tab_name]

    ws = gs_ws(tab_name)
    headers = ws.row_values(1)
    headers = [h.strip() for h in headers if h is not None and str(h).strip() != ""]
    _GS_CACHE["headers"][tab_name] = headers
    return headers

def gs_append_dict(tab_name: str, data: Dict[str, Any]) -> None:
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)

    row = []
    for h in headers:
        val = data.get(h, "")
        if val is None:
            val = ""
        row.append(str(val))

    ws.append_row(row, value_input_option="USER_ENTERED")

def gs_get_all_records(tab_name: str) -> List[Dict[str, Any]]:
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    out: List[Dict[str, Any]] = []
    for r in values[1:]:
        rec: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            rec[h] = r[i] if i < len(r) else ""
        out.append(rec)
    return out

def gs_find_row_index_first(tab_name: str, criteria: Dict[str, str]) -> Optional[int]:
    """
    Busca la PRIMERA fila que coincida exactamente con criteria en columnas existentes.
    Retorna índice 1-based o None.
    """
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)
    header_to_idx = {h: i for i, h in enumerate(headers)}  # 0-based
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    for r in range(2, len(values) + 1):
        row = values[r - 1]
        ok = True
        for k, v in criteria.items():
            if k not in header_to_idx:
                ok = False
                break
            idx = header_to_idx[k]
            cell = row[idx] if idx < len(row) else ""
            if str(cell).strip() != str(v).strip():
                ok = False
                break
        if ok:
            return r
    return None

def gs_update_row_by_headers(tab_name: str, row_index: int, patch: Dict[str, Any]) -> None:
    """
    Actualiza una fila existente por headers (solo columnas que existen).
    """
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)
    header_to_col = {h: i + 1 for i, h in enumerate(headers)}  # 1-based col
    for k, v in patch.items():
        if k in header_to_col:
            ws.update_cell(row_index, header_to_col[k], str(v) if v is not None else "")

def gs_delete_row(tab_name: str, row_index: int) -> None:
    ws = gs_ws(tab_name)
    ws.delete_rows(row_index)

# =========================
# Plantillas: template + parse
# =========================
PLANTILLA_TEXT = (
    "📌 Copia/pega esta plantilla y envíala COMPLETA en un solo mensaje.\n\n"
    "Tipo de supervisión:\n"
    "Tipificación:\n"
    "Teléfono:\n"
    "DNI:\n"
    "Cliente:\n"
    "Código pedido:\n"
    "Dirección:\n"
    "Distrito:\n"
    "Plan:\n"
    "CTO1:\n"
    "Técnico:\n"
    "Contrata:\n"
    "Gestor:\n"
)

def parse_plantilla(text: str) -> Dict[str, str]:
    """
    Extrae campos básicos. No falla si faltan.
    En este bot, solo necesitamos Contrata, Distrito, Gestor + UUID (para ligar).
    """
    def pick(label: str) -> str:
        m = re.search(rf"(?im)^{re.escape(label)}\s*:\s*(.+)$", text.strip(), re.MULTILINE)
        return (m.group(1).strip() if m else "")

    return {
        "CodigoPedido": pick("Código pedido") or pick("Codigo pedido"),
        "Contrata": pick("Contrata"),
        "Distrito": pick("Distrito"),
        "Gestor": pick("Gestor"),
    }

async def cmd_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not in_group(update):
        await send_message(update, context, "Usa /plantilla dentro del grupo.")
        return
    await send_message(update, context, PLANTILLA_TEXT)

async def auto_capture_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Se ejecuta para mensajes de texto en grupos.
    Si detecta "Código pedido:", intenta guardar en Google Sheet Plantillas.
    """
    if not in_group(update):
        return

    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    if not re.search(r"(?im)c[oó]digo\s+pedido\s*:", text):
        return

    data = parse_plantilla(text)
    codigo = data.get("CodigoPedido", "").strip()
    if not codigo:
        await send_message(update, context, "⚠️ Detecté una plantilla, pero falta 'Código pedido:'. Corrige y reenvía.")
        return

    if not _gs_ready():
        await send_message(update, context, "⚠️ Google Sheets no está configurado (SHEET_ID/credenciales).")
        return

    plantilla_uuid = str(uuid.uuid4())

    row = {
        "FechaPlantilla": now_peru_str(),
        "ChatID": str(update.effective_chat.id),
        "UsuarioID": str(update.effective_user.id if update.effective_user else ""),
        "CódigoPedido": codigo,
        "Contrata": data.get("Contrata", ""),
        "Distrito": data.get("Distrito", ""),
        "Gestor": data.get("Gestor", ""),
        "PlantillaRaw": text,
        "PlantillaUUID": plantilla_uuid,
    }

    try:
        gs_append_dict(SHEET_TAB_PLANTILLAS, row)
        await send_message(update, context, f"✅ Plantilla guardada.\nCódigoPedido: {codigo}\nUUID: {plantilla_uuid}")
    except Exception as e:
        logging.exception("Error guardando plantilla")
        await send_message(update, context, f"❌ No pude guardar la plantilla en Sheets.\nDetalle: {e}")

async def cmd_cancelar_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cancelar_plantilla <CODIGO>
    Borra la PRIMERA (más antigua) o la última según criterio. Aquí: borramos la última coincidencia
    para:
      - ChatID (grupo)
      - UsuarioID (quien ejecuta)
      - CódigoPedido (argumento)
    Luego envía plantilla en blanco.
    """
    if not in_group(update):
        await send_message(update, context, "Usa /cancelar_plantilla dentro del grupo.")
        return

    args = context.args or []
    if not args:
        await send_message(update, context, "Uso: /cancelar_plantilla <CODIGO_PEDIDO>")
        return

    codigo = " ".join(args).strip()

    if not _gs_ready():
        await send_message(update, context, "⚠️ Google Sheets no está configurado (SHEET_ID/credenciales).")
        return

    chat_id = str(update.effective_chat.id)
    user_id = str(update.effective_user.id if update.effective_user else "")

    try:
        # buscamos la última coincidencia manualmente
        ws = gs_ws(SHEET_TAB_PLANTILLAS)
        headers = gs_headers(SHEET_TAB_PLANTILLAS)
        h2i = {h: i for i, h in enumerate(headers)}
        values = ws.get_all_values()
        if not values or len(values) < 2:
            await send_message(update, context, f"⚠️ No encontré una plantilla para CódigoPedido {codigo} (de tu usuario).")
            return

        idx_chat = h2i.get("ChatID")
        idx_user = h2i.get("UsuarioID")
        idx_cod = h2i.get("CódigoPedido")
        if idx_chat is None or idx_user is None or idx_cod is None:
            await send_message(update, context, "⚠️ La hoja Plantillas no tiene headers requeridos (ChatID/UsuarioID/CódigoPedido).")
            return

        last_row_idx = None
        for r in range(2, len(values) + 1):
            row = values[r - 1]
            c_chat = row[idx_chat] if idx_chat < len(row) else ""
            c_user = row[idx_user] if idx_user < len(row) else ""
            c_cod = row[idx_cod] if idx_cod < len(row) else ""
            if str(c_chat).strip() == chat_id and str(c_user).strip() == user_id and str(c_cod).strip() == str(codigo).strip():
                last_row_idx = r

        if not last_row_idx:
            await send_message(update, context, f"⚠️ No encontré una plantilla para CódigoPedido {codigo} (de tu usuario).")
            return

        gs_delete_row(SHEET_TAB_PLANTILLAS, last_row_idx)
        await send_message(update, context, f"✅ Plantilla eliminada para CódigoPedido {codigo}.\nVuelve a enviarla corregida 👇\n\n{PLANTILLA_TEXT}")
    except Exception as e:
        logging.exception("Error borrando plantilla")
        await send_message(update, context, f"❌ No pude eliminar la plantilla.\nDetalle: {e}")

async def cmd_reload_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gs_clear_cache()
    _DYN_CACHE["supervisores"] = {"ts": 0.0, "items": []}
    _DYN_CACHE["tecnicos_tufibra"] = {"ts": 0.0, "items": []}
    _DYN_CACHE["cuadrillas_win"] = {"ts": 0.0, "items": []}
    _ROUTING_CACHE["ts"] = 0.0
    _ROUTING_CACHE["routes"] = {}
    await send_message(update, context, "✅ Cache recargado (Sheets headers + listas + routing).")

# =========================
# Buscar plantilla por CódigoPedido para /inicio
# =========================
def gs_fetch_last_plantilla_for_codigo(codigo: str) -> Optional[Dict[str, str]]:
    """
    Devuelve dict con:
      Contrata, Distrito, Gestor, PlantillaUUID
    buscando la última fila en Plantillas con CódigoPedido == codigo.
    """
    ws = gs_ws(SHEET_TAB_PLANTILLAS)
    headers = gs_headers(SHEET_TAB_PLANTILLAS)
    header_to_idx = {h: i for i, h in enumerate(headers)}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    idx_cod = header_to_idx.get("CódigoPedido")
    if idx_cod is None:
        return None

    idx_contrata = header_to_idx.get("Contrata")
    idx_distrito = header_to_idx.get("Distrito")
    idx_gestor = header_to_idx.get("Gestor")
    idx_uuid = header_to_idx.get("PlantillaUUID")

    last = None
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        cell = row[idx_cod] if idx_cod < len(row) else ""
        if str(cell).strip() == str(codigo).strip():
            last = row

    if not last:
        return None

    def safe(idx: Optional[int]) -> str:
        if idx is None:
            return ""
        return last[idx].strip() if idx < len(last) else ""

    return {
        "Contrata": safe(idx_contrata),
        "Distrito": safe(idx_distrito),
        "Gestor": safe(idx_gestor),
        "PlantillaUUID": safe(idx_uuid),
    }

# =========================
# Dynamic Lists Cache (SUPERVISORES / TECNICOS_TUFIBRA / CUADRILLAS_WIN)
# =========================
_DYN_CACHE: Dict[str, Any] = {
    "supervisores": {"ts": 0.0, "items": []},
    "tecnicos_tufibra": {"ts": 0.0, "items": []},
    "cuadrillas_win": {"ts": 0.0, "items": []},
}

def _is_truthy(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "si", "sí", "on", "activo")

def fetch_dyn_list(tab: str, cache_key: str, ttl_sec: int) -> List[Dict[str, Any]]:
    """
    Lee Sheet con columnas:
      nombre, activo, orden, alias, updated_at
    Retorna lista ordenada por 'orden' asc, luego 'nombre'.
    Aplica cache TTL.
    """
    now = time.time()
    c = _DYN_CACHE.get(cache_key, {"ts": 0.0, "items": []})
    if c["items"] and (now - float(c["ts"])) < ttl_sec:
        return c["items"]

    items: List[Dict[str, Any]] = []
    try:
        records = gs_get_all_records(tab)
        for r in records:
            nombre = str(r.get("nombre", "")).strip()
            if not nombre:
                continue
            activo = _is_truthy(r.get("activo", "1"))
            if not activo:
                continue
            alias = str(r.get("alias", "")).strip()
            orden_raw = str(r.get("orden", "")).strip()
            try:
                orden = int(float(orden_raw)) if orden_raw != "" else 999999
            except Exception:
                orden = 999999
            items.append({
                "nombre": nombre,
                "alias": alias,
                "orden": orden,
            })
        items.sort(key=lambda x: (x.get("orden", 999999), _norm(x.get("nombre", ""))))
    except Exception as e:
        logging.warning(f"No se pudo cargar lista dinámica {tab}: {e}")
        # Si falla lectura, mantenemos cache anterior (si existe)
        if c["items"]:
            return c["items"]
        return []

    _DYN_CACHE[cache_key] = {"ts": now, "items": items}
    return items

def fetch_cuadrillas_win(ttl_sec: int) -> List[Dict[str, Any]]:
    """
    Lee CUADRILLAS_WIN (6 columnas o las que existan), pero requiere al menos:
      - nombre_completo (texto largo a guardar)
    Opcionales:
      - short_label (texto corto para botón)
      - activo (true/1)
      - orden (num)
      - alias (opcional)
      - updated_at (opcional)
    Retorna items ordenados por 'orden' y luego nombre_completo.
    Aplica cache TTL.
    """
    now = time.time()
    c = _DYN_CACHE.get("cuadrillas_win", {"ts": 0.0, "items": []})
    if c["items"] and (now - float(c["ts"])) < ttl_sec:
        return c["items"]

    items: List[Dict[str, Any]] = []
    try:
        records = gs_get_all_records(SHEET_TAB_CUADRILLAS_WIN)
        for r in records:
            # Soportar nombres de columnas alternas por si el usuario las nombró distinto
            nombre_completo = str(
                r.get("nombre_completo", "")
                or r.get("Nombre_Completo", "")
                or r.get("NOMBRE_COMPLETO", "")
                or r.get("nombre", "")
                or r.get("NOMBRE", "")
            ).strip()
            if not nombre_completo:
                continue

            activo = _is_truthy(r.get("activo", "1"))
            if not activo:
                continue

            short_label = str(
                r.get("short_label", "")
                or r.get("short", "")
                or r.get("alias", "")
                or r.get("ALIAS", "")
            ).strip()

            orden_raw = str(r.get("orden", "")).strip()
            try:
                orden = int(float(orden_raw)) if orden_raw != "" else 999999
            except Exception:
                orden = 999999

            if not short_label:
                # fallback: construir uno corto (ej. "P32 - ARUCUTIPA - OLMA SGI")
                # intentamos detectar "P 32" y un apellido
                nc = nombre_completo.strip()
                tokens = re.split(r"\s+", nc)
                pcode = ""
                # buscar patrón P + número
                m = re.search(r"(?i)\bP\s*[-_]*\s*(\d{1,4})\b", nc)
                if m:
                    pcode = f"P{m.group(1)}"
                # apellido probable: último token largo o el primer token de nombre propio al final
                apellido = ""
                # heurística: tomar el último token que tenga letras y longitud>=5
                for t in reversed(tokens):
                    tt = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", "", t)
                    if len(tt) >= 5:
                        apellido = tt
                        break
                # contrata: buscar "OLMA" o "SGI" etc (solo para label corto)
                contr = ""
                # tomar 2 tokens del medio si existen (heurística simple)
                mid = []
                for t in tokens:
                    if t.upper() in ("OLMA", "SGI", "WIN", "PUE", "LIMA"):
                        mid.append(t.upper())
                if mid:
                    contr = " ".join(mid[:2])
                short_parts = []
                if pcode:
                    short_parts.append(pcode)
                if apellido:
                    short_parts.append(apellido.upper())
                if contr:
                    short_parts.append(contr)
                short_label = " - ".join(short_parts) if short_parts else (tokens[0][:16] if tokens else "CUADRILLA")

            items.append({
                "nombre_completo": nombre_completo,
                "short_label": short_label,
                "orden": orden,
                "norm": _norm(nombre_completo),
            })

        items.sort(key=lambda x: (x.get("orden", 999999), x.get("norm", "")))
    except Exception as e:
        logging.warning(f"No se pudo cargar CUADRILLAS_WIN: {e}")
        if c["items"]:
            return c["items"]
        return []

    _DYN_CACHE["cuadrillas_win"] = {"ts": now, "items": items}
    return items

def build_supervisor_menu() -> InlineKeyboardMarkup:
    items = fetch_dyn_list(SHEET_TAB_SUPERVISORES, "supervisores", SUP_CACHE_TTL_SEC)
    if not items:
        # Fallback mínimo si la hoja no existe o está vacía
        options = [("⚠️ SIN SUPERVISORES (revisar Sheet)", "SUP_NONE")]
        return kb_inline(options, cols=1)

    # Callback: SUP_PICK|<index>
    opts: List[Tuple[str, str]] = []
    for i, it in enumerate(items):
        label = it["alias"] if it.get("alias") else it["nombre"]
        opts.append((label, f"SUP_PICK|{i}"))
    return kb_inline(opts, cols=2)

def pick_supervisor_by_index(i: int) -> Optional[str]:
    items = fetch_dyn_list(SHEET_TAB_SUPERVISORES, "supervisores", SUP_CACHE_TTL_SEC)
    if 0 <= i < len(items):
        return items[i]["nombre"]
    return None

def build_tecnicos_tufibra_menu() -> InlineKeyboardMarkup:
    items = fetch_dyn_list(SHEET_TAB_TECNICOS_TUFIBRA, "tecnicos_tufibra", SUP_CACHE_TTL_SEC)
    if not items:
        options = [("⚠️ SIN TÉCNICOS (revisar Sheet)", "TF_NONE")]
        return kb_inline(options, cols=1)

    opts: List[Tuple[str, str]] = []
    for i, it in enumerate(items):
        label = it["alias"] if it.get("alias") else it["nombre"]
        opts.append((label, f"TF_PICK|{i}"))
    return kb_inline(opts, cols=1)

def pick_tecnico_tufibra_by_index(i: int) -> Optional[str]:
    items = fetch_dyn_list(SHEET_TAB_TECNICOS_TUFIBRA, "tecnicos_tufibra", SUP_CACHE_TTL_SEC)
    if 0 <= i < len(items):
        return items[i]["nombre"]
    return None

def _tokenize_query(q: str) -> List[str]:
    qn = _norm(q)
    if not qn:
        return []
    # separar por espacios y quitar tokens muy cortos (pero conservar números y "p32")
    raw = re.split(r"\s+", qn)
    toks = []
    for t in raw:
        t = t.strip()
        if not t:
            continue
        if len(t) >= 2:
            toks.append(t)
    return toks

def _score_match(nc_norm: str, query_norm: str, query_tokens: List[str]) -> int:
    """
    Score simple para ordenar resultados:
      +200 si empieza por query
      +120 si contiene query
      +40 por token que aparece
      +20 si coincide "p32" o patrón de P+numero
    """
    score = 0
    if not nc_norm or not query_norm:
        return score

    if nc_norm.startswith(query_norm):
        score += 200
    if query_norm in nc_norm:
        score += 120

    for t in query_tokens:
        if t in nc_norm:
            score += 40

    # bonus por Pxx
    m = re.search(r"\bp\s*(\d{1,4})\b", query_norm)
    if m:
        pn = f"p{m.group(1)}"
        if pn in nc_norm.replace(" ", ""):
            score += 20

    return score

def win_find_matches(query: str) -> List[Dict[str, Any]]:
    """
    Devuelve matches (ordenados por score desc, luego orden asc)
    Cada item:
      {nombre_completo, short_label, score}
    """
    items = fetch_cuadrillas_win(CUAD_CACHE_TTL_SEC)
    q = (query or "").strip()
    qn = _norm(q)
    toks = _tokenize_query(q)

    if not qn or len(qn) < 2:
        return []

    ranked: List[Dict[str, Any]] = []
    for it in items:
        nc = it.get("nombre_completo", "")
        ncn = it.get("norm", _norm(nc))

        # condición de match: substring o todos los tokens presentes
        ok = False
        if qn in ncn:
            ok = True
        else:
            # todos los tokens deben aparecer
            if toks and all(t in ncn for t in toks):
                ok = True

        if not ok:
            continue

        sc = _score_match(ncn, qn, toks)
        ranked.append({
            "nombre_completo": nc,
            "short_label": it.get("short_label", "") or nc[:40],
            "score": sc,
            "orden": it.get("orden", 999999),
        })

    ranked.sort(key=lambda x: (-int(x.get("score", 0)), int(x.get("orden", 999999)), _norm(x.get("nombre_completo", ""))))
    return ranked

def win_build_buttons(matches: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    """
    - 2 a 5 coincidencias => botones para cada una
    - >5 => 5 botones + "Refinar búsqueda"
    """
    opts: List[Tuple[str, str]] = []
    take = min(len(matches), WIN_BUTTONS_MAX)
    for i in range(take):
        m = matches[i]
        label = (m.get("short_label") or m.get("nombre_completo") or "CUADRILLA").strip()
        # limitar label del botón para evitar overflow visual
        if len(label) > 40:
            label = label[:37] + "..."
        opts.append((label, f"WIN_PICK|{i}"))

    if len(matches) > WIN_BUTTONS_MAX:
        opts.append(("🔎 Refinar búsqueda", "WIN_REFINE"))

    return kb_inline(opts, cols=1)

# =========================
# ROUTING CACHE + helpers
# =========================
_ROUTING_CACHE: Dict[str, Any] = {"ts": 0.0, "routes": {}}  # origin_chat_id -> route dict

def _parse_int_chat_id(v: Any) -> Optional[int]:
    try:
        s = str(v).strip()
        if s == "":
            return None
        return int(s)
    except Exception:
        return None

def load_routing_cache(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    if (not force) and _ROUTING_CACHE["routes"] and (now - float(_ROUTING_CACHE["ts"])) < ROUTING_CACHE_TTL_SEC:
        return _ROUTING_CACHE["routes"]

    routes: Dict[str, Any] = {}
    # 1) leer desde Sheets
    if _gs_ready():
        try:
            records = gs_get_all_records(SHEET_TAB_ROUTING)
            for r in records:
                origin = str(r.get("origin_chat_id", "")).strip()
                if not origin:
                    continue
                activo = _is_truthy(r.get("activo", "1"))
                alias = str(r.get("alias", "")).strip()
                routes[origin] = {
                    "origin_chat_id": origin,
                    "evidence_chat_id": str(r.get("evidence_chat_id", "")).strip(),
                    "summary_chat_id": str(r.get("summary_chat_id", "")).strip(),
                    "alias": alias,
                    "activo": activo,
                }
            _ROUTING_CACHE["routes"] = routes
            _ROUTING_CACHE["ts"] = now
            return routes
        except Exception as e:
            logging.warning(f"No se pudo leer ROUTING desde Sheets: {e}")

    # 2) fallback opcional a ROUTING_JSON (migración)
    if ROUTING_JSON:
        try:
            j = json.loads(ROUTING_JSON)
            for origin, v in (j or {}).items():
                origin_s = str(origin).strip()
                if not origin_s:
                    continue
                ev = str(v.get("evidence", "")).strip()
                su = str(v.get("summary", "")).strip()
                routes[origin_s] = {
                    "origin_chat_id": origin_s,
                    "evidence_chat_id": ev,
                    "summary_chat_id": su,
                    "alias": "",
                    "activo": True,
                }
        except Exception as e:
            logging.warning(f"ROUTING_JSON inválido: {e}")

    _ROUTING_CACHE["routes"] = routes
    _ROUTING_CACHE["ts"] = now
    return routes

def get_route_for_chat(origin_chat_id: int) -> Optional[Dict[str, Any]]:
    routes = load_routing_cache(force=False)
    return routes.get(str(origin_chat_id))

def route_is_origin(origin_chat_id: int) -> bool:
    r = get_route_for_chat(origin_chat_id)
    return r is not None

def route_dest_evidence(origin_chat_id: int) -> Optional[int]:
    r = get_route_for_chat(origin_chat_id)
    if not r or not r.get("activo"):
        return None
    return _parse_int_chat_id(r.get("evidence_chat_id"))

def route_dest_summary(origin_chat_id: int) -> Optional[int]:
    r = get_route_for_chat(origin_chat_id)
    if not r or not r.get("activo"):
        return None
    return _parse_int_chat_id(r.get("summary_chat_id"))

# =========================
# PAIRING helpers
# =========================
def gen_pairing_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))

def pairing_expires_at_str(ttl_minutes: int) -> str:
    dt = now_peru_dt() + timedelta(minutes=ttl_minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_dt_peru(s: str) -> Optional[datetime]:
    try:
        # asume formato "%Y-%m-%d %H:%M:%S"
        dt = datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=PERU_TZ)
    except Exception:
        return None

def is_expired(expires_at: str) -> bool:
    dt = parse_dt_peru(expires_at)
    if not dt:
        return True
    return now_peru_dt() > dt

# =========================
# Session state
# =========================
def sess(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    if "s" not in context.user_data:
        context.user_data["s"] = {
            "id_supervision": str(uuid.uuid4()),
            "estado": "",  # Completado / No Completado
            "fecha_creacion": now_peru_str(),
            "fecha_cierre": "",
            "created_by": "",
            "cancelado_por": "",
            "motivo_cancelacion": "",

            "origin_chat_id": None,
            "evidence_chat_id": None,
            "summary_chat_id": None,

            "supervisor": None,
            "operador": None,
            "tecnico": None,     # TU FIBRA: técnico; WIN: cuadrilla (nombre_completo)
            "codigo": None,
            "tipo": None,
            "location": None,    # (lat, lon)
            "final_text": "",

            # ✅ FIX BUG: evita que codigo_global capture búsquedas de WIN como código
            # True SOLO cuando el bot YA mostró "PASO 3 - INGRESA CÓDIGO..."
            "expecting_codigo": False,

            # WIN UX
            "win_query_last": "",
            "win_matches": [],   # lista de matches calculados para botones (se guarda en sesión)

            # media items: {"type":"photo|video", "file_id": "...", "wm_file": "...optional local path"}
            "fachada": {"media": [], "obs": ""},
            "cableado": {},   # bucket -> {media:[], obs:""}
            "cuadrilla": {},  # bucket -> {media:[], obs:""}
            "opcionales": {"media": [], "obs": ""},
            "current_section": None,
            "current_bucket": None,

            # plantillas link
            "plantilla_uuid": "",
            "plantilla_contrata": "",
            "plantilla_distrito": "",
            "plantilla_gestor": "",

            # UX anti-spam media notify
            "media_notify_task": None,
            "media_notify_last_msg_id": None,
            "media_notify_last_text": "",
        }
    return context.user_data["s"]

def ensure_bucket(s: Dict[str, Any], section: str, bucket: Optional[str]) -> Dict[str, Any]:
    if section == "fachada":
        return s["fachada"]
    if section == "opcionales":
        return s["opcionales"]
    if section in ("cableado", "cuadrilla"):
        if not bucket:
            raise ValueError("bucket requerido")
        if bucket not in s[section]:
            s[section][bucket] = {"media": [], "obs": ""}
        return s[section][bucket]
    raise ValueError("section inválida")

def cleanup_wm_dir_if_empty() -> None:
    try:
        if os.path.isdir(WM_DIR) and not os.listdir(WM_DIR):
            os.rmdir(WM_DIR)
    except Exception:
        pass

def cleanup_session_temp_files(s_: Dict[str, Any]) -> None:
    """Borra archivos watermark temporales registrados en los items."""
    try:
        for section in ("fachada",):
            for item in s_.get(section, {}).get("media", []):
                p = item.get("wm_file")
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

        for sec in ("cableado", "cuadrilla"):
            for _, data in s_.get(sec, {}).items():
                for item in data.get("media", []):
                    p = item.get("wm_file")
                    if p and os.path.exists(p):
                        try:
                            os.remove(p)
                        except Exception:
                            pass

        for item in s_.get("opcionales", {}).get("media", []):
            p = item.get("wm_file")
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
    finally:
        cleanup_wm_dir_if_empty()

# =========================
# Media extraction + watermark
# =========================
def extract_media_from_message(update: Update) -> Optional[Dict[str, str]]:
    msg = update.message
    if not msg:
        return None

    if msg.photo:
        return {"type": "photo", "file_id": msg.photo[-1].file_id}

    if msg.video:
        return {"type": "video", "file_id": msg.video.file_id}

    if msg.document and (msg.document.mime_type or "").startswith("video/"):
        return {"type": "video", "file_id": msg.document.file_id}

    return None

def _fmt_latlon(lat: Optional[float], lon: Optional[float]) -> str:
    if lat is None or lon is None:
        return "Lat/Lon: N/D"
    return f"Lat/Lon: {lat:.6f}, {lon:.6f}"

def _try_load_font(size: int) -> ImageFont.FreeTypeFont:
    for font_name in ("arial.ttf", "Arial.ttf"):
        try:
            return ImageFont.truetype(font_name, size)
        except Exception:
            continue
    return ImageFont.load_default()

async def apply_watermark_photo_if_needed(
    app: Application,
    file_id: str,
    lat: Optional[float],
    lon: Optional[float],
    sent_dt_local: str
) -> Tuple[str, Optional[str]]:
    """
    Devuelve (file_id_original, path_local_watermarked_or_none)
    - En fotos: descarga, coloca texto y guarda en WM_DIR para re-enviar como archivo local.
    """
    if not ENABLE_WATERMARK_PHOTOS:
        return file_id, None

    try:
        os.makedirs(WM_DIR, exist_ok=True)

        tg_file = await tg_call_with_retry(lambda: app.bot.get_file(file_id), what="get_file")
        local_in = os.path.join(WM_DIR, f"in_{int(time.time()*1000)}.jpg")
        local_out = os.path.join(WM_DIR, f"wm_{int(time.time()*1000)}.jpg")

        await tg_call_with_retry(lambda: tg_file.download_to_drive(custom_path=local_in), what="download_to_drive")

        im = Image.open(local_in).convert("RGB")
        draw = ImageDraw.Draw(im)
        font = _try_load_font(WM_FONT_SIZE)

        text = f"{sent_dt_local} | {_fmt_latlon(lat, lon)}"

        padding = 10
        try:
            bbox = draw.textbbox((0, 0), text, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        except Exception:
            tw, th = int(draw.textlength(text, font=font)), WM_FONT_SIZE + 6

        x = 10
        y = im.height - th - padding*2 - 10
        rect = [x - 5, y - 5, x + tw + padding, y + th + padding]

        draw.rectangle(rect, fill=(0, 0, 0))
        draw.text((x + 5, y + 5), text, font=font, fill=(255, 255, 255))

        im.save(local_out, "JPEG", quality=90)

        try:
            os.remove(local_in)
        except Exception:
            pass

        return file_id, local_out
    except Exception as e:
        logging.warning(f"No se pudo aplicar watermark: {e}")
        return file_id, None

# =========================
# UX anti-spam: notify aggregated media count
# =========================
async def _media_notify_after_debounce(app: Application, chat_id: int, s_: Dict[str, Any], section: str, bucket: Optional[str]):
    """
    Envía/edita un solo mensaje con el total de archivos cargados en el bucket actual,
    para evitar spam cuando se suben varias fotos al mismo tiempo.
    """
    try:
        await asyncio.sleep(MEDIA_NOTIFY_DEBOUNCE_SEC)

        b = ensure_bucket(s_, section, bucket)
        cnt = len(b.get("media", []))
        text = f"✅ Guardado ({cnt}/{MAX_MEDIA_PER_BUCKET})."

        last_id = s_.get("media_notify_last_msg_id")
        last_text = s_.get("media_notify_last_text", "")

        # Si el texto no cambió, no hacer nada
        if last_text == text and last_id:
            return

        if last_id:
            try:
                await tg_call_with_retry(
                    lambda: app.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=int(last_id),
                        text=text,
                        reply_markup=evidence_controls_keyboard(),
                    ),
                    what="edit_media_notify",
                )
                s_["media_notify_last_text"] = text
                return
            except Exception:
                # si no se puede editar (p.ej. mensaje viejo), cae a enviar nuevo
                pass

        msg = await tg_call_with_retry(
            lambda: app.bot.send_message(chat_id=chat_id, text=text, reply_markup=evidence_controls_keyboard()),
            what="send_media_notify",
        )
        s_["media_notify_last_msg_id"] = msg.message_id
        s_["media_notify_last_text"] = text
    except Exception as e:
        logging.warning(f"media notify error: {e}")

def _cancel_media_notify_task(s_: Dict[str, Any]):
    t = s_.get("media_notify_task")
    if t and isinstance(t, asyncio.Task) and not t.done():
        try:
            t.cancel()
        except Exception:
            pass
    s_["media_notify_task"] = None

# =========================
# FLOW: /inicio
# =========================
async def inicio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not in_group(update):
        await send_message(update, context, "Este bot se usa desde un grupo AUDITORIAS_... (no en privado).")
        return ConversationHandler.END

    # Reset sesión
    context.user_data.pop("s", None)
    s_ = sess(context)
    s_["origin_chat_id"] = update.effective_chat.id
    s_["created_by"] = str(update.effective_user.id if update.effective_user else "")
    s_["expecting_codigo"] = False  # ✅ FIX: al iniciar, NO esperar código

    # Pre-cargar route (si existe)
    r = get_route_for_chat(update.effective_chat.id)
    if r:
        s_["evidence_chat_id"] = _parse_int_chat_id(r.get("evidence_chat_id"))
        s_["summary_chat_id"] = _parse_int_chat_id(r.get("summary_chat_id"))

    await send_message(
        update,
        context,
        "PASO 1 - NOMBRE DEL SUPERVISOR",
        reply_markup=build_supervisor_menu(),
    )
    return S_SUPERVISOR

async def on_pick_supervisor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    data = query.data or ""
    if data == "SUP_NONE":
        await safe_edit_or_send(query, "⚠️ No hay supervisores activos en la hoja SUPERVISORES.", reply_markup=None)
        return S_SUPERVISOR

    m = re.match(r"^SUP_PICK\|(\d+)$", data)
    if not m:
        return S_SUPERVISOR

    idx = int(m.group(1))
    sup = pick_supervisor_by_index(idx)
    if not sup:
        await safe_edit_or_send(query, "⚠️ Supervisor inválido (recarga listas con /reload_sheet).", reply_markup=None)
        return S_SUPERVISOR

    s_["supervisor"] = sup

    await safe_edit_or_send(
        query,
        "PASO 2 - OPERADOR",
        reply_markup=kb_inline([(x, f"OP_{i}") for i, x in enumerate(OPERADORES)], cols=2),
    )
    return S_OPERADOR

async def on_pick_operador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    m = re.match(r"OP_(\d+)", query.data or "")
    if not m:
        return S_OPERADOR

    op = OPERADORES[int(m.group(1))]
    s_["operador"] = op

    if op == "TU FIBRA":
        # ✅ Elegir técnico desde sheet
        await safe_edit_or_send(
            query,
            "PASO 2.1 - ELIGE TÉCNICO (TU FIBRA)",
            reply_markup=build_tecnicos_tufibra_menu(),
        )
        return S_OPERADOR  # se mantiene, manejamos TF_PICK en el mismo estado
    else:
        # ✅ WIN: búsqueda en CUADRILLAS_WIN con sugerencias
        s_["win_query_last"] = ""
        s_["win_matches"] = []
        s_["expecting_codigo"] = False  # ✅ FIX: aún NO estamos esperando código (evita codigo_global)
        await safe_edit_or_send(
            query,
            "PASO 2.1 - BUSCAR CUADRILLA (WIN)\n\n"
            "✍️ Escribe parte del nombre o código.\n"
            "Ejemplos:\n"
            "• arucutipa\n"
            "• P32\n"
            "• olma sgi\n\n"
            f"🧠 El bot mostrará hasta {WIN_SUGGEST_MAX} sugerencias (máximo).",
            reply_markup=None,
        )
        return S_WIN_CUADRILLA

async def on_pick_tecnico_tufibra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Este handler corre dentro del estado S_OPERADOR cuando operador=TU FIBRA.
    """
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    data = query.data or ""
    if data == "TF_NONE":
        await safe_edit_or_send(query, "⚠️ No hay técnicos activos en TECNICOS_TUFIBRA.", reply_markup=None)
        return S_OPERADOR

    m = re.match(r"^TF_PICK\|(\d+)$", data)
    if not m:
        return S_OPERADOR

    idx = int(m.group(1))
    tec = pick_tecnico_tufibra_by_index(idx)
    if not tec:
        await safe_edit_or_send(query, "⚠️ Técnico inválido (recarga listas con /reload_sheet).", reply_markup=None)
        return S_OPERADOR

    s_["tecnico"] = tec
    s_["expecting_codigo"] = True  # ✅ FIX: recién aquí el bot espera el código

    await safe_edit_or_send(
        query,
        "PASO 3 - INGRESA CÓDIGO DE PEDIDO\n\n✅ Puede ser números o letras.",
        reply_markup=None,
    )
    return S_CODIGO

async def on_win_cuadrilla_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    WIN UX:
    - supervisor escribe texto
    - buscamos en CUADRILLAS_WIN
      * 1 match => autoselecciona y pasa a PASO 3
      * 2 a 5 => botones
      * >5 => 5 botones + "Refinar búsqueda"
    """
    s_ = sess(context)
    if not update.message or not update.message.text:
        await send_message(update, context, "❌ Escribe parte del nombre/código para buscar la cuadrilla.")
        return S_WIN_CUADRILLA

    q = update.message.text.strip()
    if len(q) < 2:
        await send_message(update, context, "❌ Texto muy corto. Escribe al menos 2 caracteres (ej: P32 / arucutipa).")
        return S_WIN_CUADRILLA

    # ✅ FIX: durante búsqueda WIN, NO esperamos código (evita codigo_global)
    s_["expecting_codigo"] = False

    # Intentar cargar CUADRILLAS_WIN (si falla, avisar claro)
    if not _gs_ready():
        await send_message(update, context, "⚠️ Sheets no está configurado (SHEET_ID/credenciales).")
        return S_WIN_CUADRILLA

    matches = win_find_matches(q)
    s_["win_query_last"] = q
    s_["win_matches"] = matches[:max(WIN_SUGGEST_MAX, 0)] if matches else []

    if not matches:
        await send_message(
            update,
            context,
            "❌ No encontré coincidencias.\n\n"
            "✅ Prueba así:\n"
            "• Solo apellido (ej: arucutipa)\n"
            "• Código (ej: P32)\n"
            "• 2 palabras (ej: olma sgi)\n\n"
            "✍️ Escribe otra búsqueda:",
        )
        return S_WIN_CUADRILLA

    if len(matches) == 1:
        # Autoselecciona
        sel = matches[0]
        s_["tecnico"] = sel.get("nombre_completo", "")
        s_["expecting_codigo"] = True  # ✅ FIX: recién aquí el bot espera el código
        await send_message(
            update,
            context,
            "✅ Cuadrilla seleccionada automáticamente:\n"
            f"{s_.get('tecnico','')}\n\n"
            "PASO 3 - INGRESA CÓDIGO DE PEDIDO\n\n✅ Puede ser números o letras.",
        )
        return S_CODIGO

    # 2 a N => botones (según regla)
    kb = win_build_buttons(matches)
    # Mensaje de contexto con conteo
    if len(matches) > WIN_BUTTONS_MAX:
        await send_message(
            update,
            context,
            f"Encontré {len(matches)} coincidencias.\n"
            f"Mostrando {WIN_BUTTONS_MAX}. Pulsa una o refina tu búsqueda:",
            reply_markup=kb,
        )
    else:
        await send_message(
            update,
            context,
            f"Encontré {len(matches)} coincidencias. Elige una:",
            reply_markup=kb,
        )
    return S_WIN_CUADRILLA

async def on_win_pick_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Selección por botón:
    - botón muestra short_label
    - guardamos nombre_completo completo en s_["tecnico"]
    """
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    m = re.match(r"^WIN_PICK\|(\d+)$", query.data or "")
    if not m:
        return S_WIN_CUADRILLA

    idx = int(m.group(1))
    matches: List[Dict[str, Any]] = s_.get("win_matches", []) or []
    if not matches or idx < 0 or idx >= min(len(matches), WIN_BUTTONS_MAX):
        await safe_edit_or_send(query, "⚠️ Selección inválida. Escribe de nuevo la búsqueda.", reply_markup=None)
        s_["win_matches"] = []
        s_["win_query_last"] = ""
        s_["expecting_codigo"] = False
        return S_WIN_CUADRILLA

    selected = matches[idx]
    full = (selected.get("nombre_completo") or "").strip()
    if not full:
        await safe_edit_or_send(query, "⚠️ Cuadrilla inválida. Escribe de nuevo la búsqueda.", reply_markup=None)
        s_["win_matches"] = []
        s_["win_query_last"] = ""
        s_["expecting_codigo"] = False
        return S_WIN_CUADRILLA

    s_["tecnico"] = full
    s_["expecting_codigo"] = True  # ✅ FIX: recién aquí el bot espera el código

    await safe_edit_or_send(
        query,
        "✅ Cuadrilla seleccionada:\n"
        f"{full}\n\n"
        "PASO 3 - INGRESA CÓDIGO DE PEDIDO\n\n✅ Puede ser números o letras.",
        reply_markup=None,
    )
    return S_CODIGO

async def on_win_refine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)
    s_["win_matches"] = []
    s_["expecting_codigo"] = False  # ✅ FIX: refinar búsqueda NO es captura de código
    await safe_edit_or_send(
        query,
        "🔎 Refinar búsqueda\n\n"
        "✍️ Escribe una búsqueda más específica.\n"
        "Ejemplos:\n"
        "• P32 arucutipa\n"
        "• OLMA SGI arucutipa\n"
        "• arucutipa apaza",
        reply_markup=None,
    )
    return S_WIN_CUADRILLA

# =========================
# RESCATE: detectar código si el ConversationHandler se desincroniza
# =========================
def looks_like_codigo(text: str) -> bool:
    t = (text or "").strip()
    return 3 <= len(t) <= 30 and re.match(r"^[A-Za-z0-9_-]+$", t) is not None

async def codigo_global(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Rescate: si el ConversationHandler no toma el código, lo capturamos aquí."""
    if not in_group(update):
        return
    if not update.message or not update.message.text:
        return

    s_ = context.user_data.get("s")
    if not s_:
        return

    # Solo si hay sesión activa del mismo chat y todavía no hay código
    if s_.get("origin_chat_id") != update.effective_chat.id:
        return
    if s_.get("codigo"):
        return

    # ✅ FIX: SOLO capturar si el bot realmente está esperando el código (PASO 3 ya mostrado)
    if not s_.get("expecting_codigo", False):
        return

    text = update.message.text.strip()
    if not looks_like_codigo(text):
        return

    logging.info("⚠️ Código capturado por handler GLOBAL (rescate).")
    await on_codigo(update, context)

async def on_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("✅ on_codigo() ENTER")
    s_ = sess(context)

    codigo = (update.message.text or "").strip()
    if not codigo:
        await send_message(update, context, "❌ Código vacío. Intenta nuevamente.")
        return S_CODIGO

    s_["codigo"] = codigo
    s_["expecting_codigo"] = False  # ✅ FIX: ya capturamos el código, apagamos bandera

    # Buscar plantilla en Sheets (si está configurado)
    if _gs_ready():
        try:
            found = gs_fetch_last_plantilla_for_codigo(codigo)
            if found:
                s_["plantilla_uuid"] = found.get("PlantillaUUID", "")
                # ✅ Solo vincular: Contrata, Distrito, Gestor
                s_["plantilla_contrata"] = found.get("Contrata", "")
                s_["plantilla_distrito"] = found.get("Distrito", "")
                s_["plantilla_gestor"] = found.get("Gestor", "")
        except Exception as e:
            logging.warning(f"No se pudo leer plantilla de Sheets: {e}")

    await send_message(
        update,
        context,
        "PASO 4 - TIPO DE SUPERVISIÓN",
        reply_markup=kb_inline(
            [("🔥SUPERVISION EN CALIENTE", "TIPO_CALIENTE"), ("🧊SUPERVISION EN FRIO", "TIPO_FRIO")],
            cols=1,
        ),
    )
    return S_TIPO

async def on_pick_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    if query.data == "TIPO_CALIENTE":
        s_["tipo"] = "CALIENTE"
    elif query.data == "TIPO_FRIO":
        s_["tipo"] = "FRIO"
    else:
        return S_TIPO

    await safe_edit_or_send(
        query,
        "PASO 5 - REPORTA TU UBICACIÓN\n\n"
        "📌 En grupos, Telegram no permite solicitar ubicación con botón.\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual",
        reply_markup=None,
    )

    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("📍 ENVIAR UBICACION (manual)")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await tg_call_with_retry(
        lambda: context.application.bot.send_message(chat_id=query.message.chat_id, text="👇", reply_markup=kb),
        what="send_location_instructions",
    )
    return S_UBICACION

async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    loc = update.message.location if update.message else None
    if not loc:
        await send_message(update, context, "❌ No recibí ubicación. Envíala con 📎 -> Ubicación -> Enviar ubicación actual.")
        return S_UBICACION

    s_["location"] = (loc.latitude, loc.longitude)
    s_["current_section"] = "fachada"
    s_["current_bucket"] = None

    # reset anti-spam notify per nueva sección
    _cancel_media_notify_task(s_)
    s_["media_notify_last_msg_id"] = None
    s_["media_notify_last_text"] = ""

    await send_message(
        update,
        context,
        f"PASO 6 - EVIDENCIA DE FACHADA\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos (fotos o videos).",
        reply_markup=ReplyKeyboardRemove(),
    )
    return S_FACHADA_MEDIA

# =========================
# Media (fotos + videos)
# =========================
async def on_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    section = s_.get("current_section")
    bucket = s_.get("current_bucket")

    if not section:
        return

    item = extract_media_from_message(update)
    if not item:
        await send_message(update, context, "❌ Solo se aceptan fotos o videos.")
        return S_CARGA_MEDIA_BUCKET if section != "fachada" else S_FACHADA_MEDIA

    b = ensure_bucket(s_, section, bucket)
    media_list = b["media"]

    if len(media_list) >= MAX_MEDIA_PER_BUCKET:
        await send_message(
            update,
            context,
            f"⚠️ Límite alcanzado ({MAX_MEDIA_PER_BUCKET}). Presiona ✅ EVIDENCIAS COMPLETAS.",
            reply_markup=evidence_controls_keyboard(),
        )
        return S_CARGA_MEDIA_BUCKET if section != "fachada" else S_FACHADA_MEDIA

    # Watermark solo para fotos (por defecto está desactivado)
    if item["type"] == "photo" and ENABLE_WATERMARK_PHOTOS:
        lat, lon = s_.get("location") if s_.get("location") else (None, None)
        sent_dt = now_peru_str()
        _, wm_path = await apply_watermark_photo_if_needed(
            context.application,
            item["file_id"],
            lat,
            lon,
            sent_dt_local=sent_dt,
        )
        if wm_path:
            item["wm_file"] = wm_path

    media_list.append(item)

    # ✅ Anti-spam: no enviar un mensaje por cada foto.
    # Programar (debounce) un mensaje único/edición con el total.
    _cancel_media_notify_task(s_)
    s_["media_notify_task"] = asyncio.create_task(
        _media_notify_after_debounce(
            context.application,
            update.effective_chat.id,
            s_,
            section,
            bucket,
        )
    )

    return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

async def on_add_more_or_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    section = s_.get("current_section")
    bucket = s_.get("current_bucket")
    b = ensure_bucket(s_, section, bucket)

    if query.data == "ADD_MORE":
        await safe_edit_or_send(query, "📸🎥 Envía el siguiente archivo (foto o video).", reply_markup=None)
        return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

    if query.data == "DONE_MEDIA":
        if len(b["media"]) < 1:
            await safe_edit_or_send(query, "⚠️ Debes cargar al menos 1 archivo antes de completar.", reply_markup=None)
            return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

        # reset anti-spam notify al cambiar de estado
        _cancel_media_notify_task(s_)
        s_["media_notify_last_msg_id"] = None
        s_["media_notify_last_text"] = ""

        if section == "fachada":
            await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
            return S_MENU_PRINCIPAL

        await safe_edit_or_send(
            query,
            "¿Deseas ingresar Observación?",
            reply_markup=kb_inline([("SI", "OBS_SI"), ("NO", "OBS_NO")], cols=2),
        )
        return S_ASK_OBS

    return S_MENU_PRINCIPAL

async def on_obs_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)
    section = s_.get("current_section")

    if query.data == "OBS_SI":
        await safe_edit_or_send(query, "📝 Escribe tu observación:", reply_markup=None)
        return S_WRITE_OBS

    if query.data == "OBS_NO":
        if section == "cableado":
            await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
            return S_MENU_CABLEADO
        if section == "cuadrilla":
            await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
            return S_MENU_CUADRILLA
        if section == "opcionales":
            await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
            return S_MENU_PRINCIPAL

    return S_MENU_PRINCIPAL

async def on_write_obs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    section = s_.get("current_section")
    bucket = s_.get("current_bucket")
    obs = (update.message.text or "").strip()

    b = ensure_bucket(s_, section, bucket)

    if b.get("obs"):
        b["obs"] = (b["obs"].rstrip() + "\n" + obs).strip()
    else:
        b["obs"] = obs

    if section == "cableado":
        await send_message(update, context, "✅ Observación guardada.\n\nQUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
        return S_MENU_CABLEADO
    if section == "cuadrilla":
        await send_message(update, context, "✅ Observación guardada.\n\nQUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
        return S_MENU_CUADRILLA
    if section == "opcionales":
        await send_message(update, context, "✅ Observación guardada.\n\nPASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    await send_message(update, context, "✅ Observación guardada.")
    return S_MENU_PRINCIPAL

# =========================
# Menú principal + submenús
# =========================
async def on_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    # reset anti-spam notify al cambiar de sección/bucket
    _cancel_media_notify_task(s_)
    s_["media_notify_last_msg_id"] = None
    s_["media_notify_last_text"] = ""

    if query.data == "MENU_CABLEADO":
        s_["current_section"] = "cableado"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
        return S_MENU_CABLEADO

    if query.data == "MENU_CUADRILLA":
        s_["current_section"] = "cuadrilla"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
        return S_MENU_CUADRILLA

    if query.data == "MENU_OPCIONALES":
        s_["current_section"] = "opcionales"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, f"🚨 EVIDENCIAS OPCIONALES\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
        return S_CARGA_MEDIA_BUCKET

    if query.data == "FINALIZAR":
        await safe_edit_or_send(query, "INGRESAR OBSERVACIONES FINALES\n(Escribe el texto final)", reply_markup=None)
        return S_FINAL_TEXT

    return S_MENU_PRINCIPAL

async def on_menu_cableado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    _cancel_media_notify_task(s_)
    s_["media_notify_last_msg_id"] = None
    s_["media_notify_last_text"] = ""

    data = query.data or ""
    if data == "FIN_CABLEADO":
        await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    s_["current_section"] = "cableado"
    s_["current_bucket"] = data
    ensure_bucket(s_, "cableado", data)

    await safe_edit_or_send(query, f"🏗️ CABLEADO - {data}\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
    return S_CARGA_MEDIA_BUCKET

async def on_menu_cuadrilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    _cancel_media_notify_task(s_)
    s_["media_notify_last_msg_id"] = None
    s_["media_notify_last_text"] = ""

    data = query.data or ""
    if data == "FIN_CUADRILLA":
        await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    s_["current_section"] = "cuadrilla"
    s_["current_bucket"] = data
    ensure_bucket(s_, "cuadrilla", data)

    await safe_edit_or_send(query, f"👷‍♂️ CUADRILLA - {data}\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
    return S_CARGA_MEDIA_BUCKET

# =========================
# Finalización: enviar a Evidencias/Resumen según ROUTING + guardar en Supervisiones_v2
# =========================
def build_summary(s_: Dict[str, Any]) -> str:
    lat, lon = s_["location"] if s_.get("location") else (None, None)
    maps_direct = f"https://maps.google.com/?q={lat},{lon}" if lat is not None else "No disponible"

    extra = ""
    if s_.get("plantilla_contrata") or s_.get("plantilla_distrito") or s_.get("plantilla_gestor") or s_.get("plantilla_uuid"):
        extra = (
            "\n🧩 Datos de Plantilla:\n"
            f"• Contrata: {s_.get('plantilla_contrata','')}\n"
            f"• Distrito: {s_.get('plantilla_distrito','')}\n"
            f"• Gestor: {s_.get('plantilla_gestor','')}\n"
            f"• PlantillaUUID: {s_.get('plantilla_uuid','')}\n"
        )

    return (
        "📋 SUPERVISIÓN FINALIZADA\n\n"
        f"👷 Supervisor: {s_.get('supervisor','')}\n"
        f"🏢 Operador: {s_.get('operador','')}\n"
        f"🧑‍🔧 Técnico/Cuadrilla: {s_.get('tecnico','')}\n"
        f"🧾 Código de pedido: {s_.get('codigo','')}\n"
        f"🔥 Tipo de supervisión: {s_.get('tipo','')}\n\n"
        f"📍 Ubicación:\n{maps_direct}\n"
        f"{extra}\n"
        "📝 Observaciones finales:\n"
        f"{s_.get('final_text','')}"
    )

def to_input_media(item: Dict[str, str]):
    if item["type"] == "photo":
        return InputMediaPhoto(item["file_id"])
    return InputMediaVideo(item["file_id"])

async def send_media_section(app: Application, chat_id: int, title: str, media_items: List[Dict[str, str]]):
    """
    Envío robusto:
    - Mensaje título
    - Envío por grupos de 10 (media_group)
    - Si hay wm_file (cuando watermark habilitado), se envía por send_photo individual
    - TODO con retry (RetryAfter)
    """
    if not media_items:
        return

    await tg_call_with_retry(lambda: app.bot.send_message(chat_id=chat_id, text=title), what="send_media_title")

    batch: List[Dict[str, str]] = []
    for it in media_items:
        if it.get("type") == "photo" and it.get("wm_file") and os.path.exists(it["wm_file"]):
            # flush batch primero
            if batch:
                for chunk in chunk_list(batch, 10):
                    media = [to_input_media(x) for x in chunk]
                    await tg_call_with_retry(lambda m=media: app.bot.send_media_group(chat_id=chat_id, media=m), what="send_media_group_flush")
                batch = []
            # enviar foto watermark individual
            with open(it["wm_file"], "rb") as f:
                await tg_call_with_retry(lambda fh=f: app.bot.send_photo(chat_id=chat_id, photo=fh), what="send_photo_wm")
        else:
            batch.append(it)

    if batch:
        for chunk in chunk_list(batch, 10):
            media = [to_input_media(x) for x in chunk]
            await tg_call_with_retry(lambda m=media: app.bot.send_media_group(chat_id=chat_id, media=m), what="send_media_group")

def map_obs_columns_v2() -> Dict[Tuple[str, str], str]:
    # ✅ Nombres compactos recomendados para Supervisiones_v2
    return {
        ("cableado", "CTO"): "Obs_CTO",
        ("cableado", "POSTE"): "Obs_POSTE",
        ("cableado", "RUTA"): "Obs_RUTA",
        ("cableado", "FALSO_TRAMO"): "Obs_FALSO_TRAMO",
        ("cableado", "ANCLAJE"): "Obs_ANCLAJE",
        ("cableado", "RESERVA"): "Obs_RESERVA_DOMICILIO",
        ("cableado", "ROSETA"): "Obs_ROSETA",
        ("cableado", "EQUIPOS"): "Obs_EQUIPOS",

        ("cuadrilla", "FOTO_TECNICOS"): "Obs_TECNICOS",
        ("cuadrilla", "SCTR"): "Obs_SCTR",
        ("cuadrilla", "ATS"): "Obs_ATS",
        ("cuadrilla", "LICENCIA"): "Obs_LICENCIA",
        ("cuadrilla", "UNIDAD"): "Obs_UNIDAD",
        ("cuadrilla", "SOAT"): "Obs_SOAT",
        ("cuadrilla", "HERRAMIENTAS"): "Obs_HERRAMIENTAS",
        ("cuadrilla", "KIT_FIBRA"): "Obs_KIT_FIBRA",
        ("cuadrilla", "ESCALERA_TEL"): "Obs_ESCALERA_TELESCOPICA",
        ("cuadrilla", "ESCALERA_INT"): "Obs_ESCALERA_INTERNOS",
        ("cuadrilla", "BOTIQUIN"): "Obs_BOTIQUIN",
    }

def maps_link_from_latlon(lat: Optional[float], lon: Optional[float]) -> str:
    if lat is None or lon is None:
        return ""
    # Link directo y estable:
    return f"https://maps.google.com/?q={lat},{lon}"

def build_supervisiones_v2_row(s_: Dict[str, Any], estado: str, motivo_cancelacion: str = "") -> Dict[str, Any]:
    """
    Construye payload para Supervisiones_v2.
    Se escribe por headers: lo que no exista en la hoja se ignora.
    """
    lat, lon = s_["location"] if s_.get("location") else (None, None)
    origin_chat_id = s_.get("origin_chat_id")
    ev_chat_id = s_.get("evidence_chat_id")
    su_chat_id = s_.get("summary_chat_id")

    row: Dict[str, Any] = {}
    row["ID_Supervision"] = s_.get("id_supervision", "")
    row["ESTADO"] = estado
    row["Fecha_Creacion"] = s_.get("fecha_creacion", "")
    row["Fecha_Cierre"] = s_.get("fecha_cierre", now_peru_str())

    row["Supervisor"] = s_.get("supervisor", "")
    row["Operador"] = s_.get("operador", "")
    row["Técnico"] = s_.get("tecnico", "")
    row["Contrata"] = s_.get("plantilla_contrata", "")
    row["Gestor"] = s_.get("plantilla_gestor", "")
    row["Código_Pedido"] = s_.get("codigo", "")
    row["Tipo_Supervision"] = s_.get("tipo", "")
    row["Distrito"] = s_.get("plantilla_distrito", "")

    # Ubicación estructurada
    row["Latitud"] = f"{lat:.15f}" if isinstance(lat, (int, float)) else ""
    row["Longitud"] = f"{lon:.15f}" if isinstance(lon, (int, float)) else ""
    row["Link_Ubicacion"] = maps_link_from_latlon(lat, lon)

    # Observaciones por bucket
    m = map_obs_columns_v2()
    for bucket, data in s_.get("cableado", {}).items():
        col = m.get(("cableado", bucket))
        if col:
            row[col] = data.get("obs", "")

    for bucket, data in s_.get("cuadrilla", {}).items():
        col = m.get(("cuadrilla", bucket))
        if col:
            row[col] = data.get("obs", "")

    # Opcionales / final
    row["Obs_ADICIONALES"] = s_.get("opcionales", {}).get("obs", "")
    row["Obs_FINALES"] = s_.get("final_text", "")

    # Trazabilidad
    row["PlantillaUUID"] = s_.get("plantilla_uuid", "")
    row["Origin_Chat_ID"] = str(origin_chat_id) if origin_chat_id is not None else ""
    row["Evidence_Chat_ID"] = str(ev_chat_id) if ev_chat_id is not None else ""
    row["Summary_Chat_ID"] = str(su_chat_id) if su_chat_id is not None else ""

    # Auditoría
    row["Creado_Por"] = s_.get("created_by", "")
    row["Cancelado_Por"] = s_.get("cancelado_por", "")
    row["Motivo_Cancelacion"] = motivo_cancelacion
    row["Updated_At"] = now_peru_str()

    return row

async def on_final_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    s_["final_text"] = (update.message.text or "").strip()

    origin_chat_id = s_.get("origin_chat_id")
    if origin_chat_id is None:
        await send_message(update, context, "❌ No se detectó el grupo de origen. Inicia con /inicio en el grupo AUDITORIAS.")
        cleanup_session_temp_files(s_)
        context.user_data.pop("s", None)
        return ConversationHandler.END

    # Resolución de rutas desde cache (routing)
    r = get_route_for_chat(origin_chat_id)
    if r:
        s_["evidence_chat_id"] = _parse_int_chat_id(r.get("evidence_chat_id"))
        s_["summary_chat_id"] = _parse_int_chat_id(r.get("summary_chat_id"))

    dest_evidencias_id = s_.get("evidence_chat_id")
    dest_summary_id = s_.get("summary_chat_id")

    if not dest_evidencias_id and not dest_summary_id:
        await send_message(
            update,
            context,
            "⚠️ Este grupo aún no tiene rutas activas (ROUTING).\n"
            "Usa /config (admin) para vincular Evidencias/Resumen por código.\n"
            "También puedes ver rutas con /config → 📌 Ver rutas de este grupo.",
        )
        cleanup_session_temp_files(s_)
        context.user_data.pop("s", None)
        return ConversationHandler.END

    # Marcar cierre
    s_["fecha_cierre"] = now_peru_str()
    s_["estado"] = "Completado"

    # =========================================================
    # ✅ CAMBIO CLAVE: GUARDAR EN SHEETS PRIMERO (ANTI-PÉRDIDA DE DATA)
    # - Si Telegram se frena con 429/RetryAfter, igual queda registrado.
    # =========================================================
    sheets_ok = False
    if _gs_ready():
        try:
            payload = build_supervisiones_v2_row(s_, estado="Completado", motivo_cancelacion="")
            logging.info(f"🟦 Guardando en '{SHEET_TAB_SUPERVISIONES_V2}' id={s_.get('id_supervision')}")
            gs_append_dict(SHEET_TAB_SUPERVISIONES_V2, payload)
            logging.info("✅ Guardado en Sheets OK (Supervisiones_v2).")
            sheets_ok = True
        except Exception as e:
            logging.exception("❌ Error guardando supervisión v2 en Sheets")
            # OJO: no abortamos; seguimos con Telegram.
            try:
                await send_message(update, context, f"⚠️ No pude guardar en Sheets.\nDetalle: {e}")
            except Exception:
                pass

    summary = build_summary(s_)

    # =========================================================
    # ✅ CAMBIO CLAVE: ENVÍO TELEGRAM ROBUSTO
    # - Todo dentro de try/except para que NUNCA reviente el handler.
    # - Con retry ante RetryAfter.
    # =========================================================
    telegram_send_errors: List[str] = []

    # Enviar resumen
    try:
        if dest_summary_id:
            await tg_call_with_retry(lambda: context.application.bot.send_message(chat_id=dest_summary_id, text=summary), what="send_summary")
        elif dest_evidencias_id:
            # Si no hay summary, enviamos resumen a evidencias por compat
            await tg_call_with_retry(lambda: context.application.bot.send_message(chat_id=dest_evidencias_id, text=summary), what="send_summary_to_evidence")
    except Exception as e:
        logging.exception("⚠️ Falló envío de RESUMEN a Telegram (se continúa).")
        telegram_send_errors.append(f"Resumen: {e}")

    # Enviar media a evidencias si existe
    if dest_evidencias_id:
        try:
            await send_media_section(context.application, dest_evidencias_id, "🧱 FACHADA", s_["fachada"]["media"])
        except Exception as e:
            logging.exception("⚠️ Falló envío sección FACHADA (se continúa).")
            telegram_send_errors.append(f"Fachada: {e}")

        for bucket, data in s_["cableado"].items():
            title = f"🏗️ CABLEADO - {bucket}"
            if data.get("obs"):
                title += f"\n📝 Obs: {data['obs']}"
            try:
                await send_media_section(context.application, dest_evidencias_id, title, data.get("media", []))
            except Exception as e:
                logging.exception("⚠️ Falló envío sección CABLEADO %s (se continúa).", bucket)
                telegram_send_errors.append(f"Cableado {bucket}: {e}")

        for bucket, data in s_["cuadrilla"].items():
            title = f"👷‍♂️ CUADRILLA - {bucket}"
            if data.get("obs"):
                title += f"\n📝 Obs: {data['obs']}"
            try:
                await send_media_section(context.application, dest_evidencias_id, title, data.get("media", []))
            except Exception as e:
                logging.exception("⚠️ Falló envío sección CUADRILLA %s (se continúa).", bucket)
                telegram_send_errors.append(f"Cuadrilla {bucket}: {e}")

        opc = s_["opcionales"]
        if opc.get("media"):
            title = "🚨 OPCIONALES"
            if opc.get("obs"):
                title += f"\n📝 Obs: {opc['obs']}"
            try:
                await send_media_section(context.application, dest_evidencias_id, title, opc["media"])
            except Exception as e:
                logging.exception("⚠️ Falló envío sección OPCIONALES (se continúa).")
                telegram_send_errors.append(f"Opcionales: {e}")

    # Mensaje final al grupo origen
    try:
        msg = (
            f"✅ SE FINALIZÓ SUPERVISIÓN\n"
            f"🧾 Código: {s_.get('codigo','')}\n"
            f"📌 Estado: Completado\n"
            f"📊 Sheets: {'OK' if sheets_ok else 'PENDIENTE/ERROR'}"
        )
        if telegram_send_errors:
            msg += "\n\n⚠️ Nota: Hubo demoras/errores al enviar evidencias a Telegram (flood control). Revisa logs."
        await send_message(update, context, msg)
    except Exception:
        pass

    cleanup_session_temp_files(s_)
    context.user_data.pop("s", None)
    return ConversationHandler.END

# =========================
# Cancelar supervisión (guardar fila parcial con ESTADO=No Completado)
# =========================
async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = context.user_data.get("s")
    if not s_:
        await send_message(update, context, "❌ No hay una supervisión activa para cancelar.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    # Marcar cancelación
    s_["fecha_cierre"] = now_peru_str()
    s_["estado"] = "No Completado"
    s_["cancelado_por"] = str(update.effective_user.id if update.effective_user else "")
    s_["expecting_codigo"] = False  # ✅ FIX: al cancelar, no esperar código
    # Motivo opcional (si luego quieres pedirlo). Aquí lo dejamos vacío por ahora.
    s_["motivo_cancelacion"] = s_.get("motivo_cancelacion", "")

    # Guardar en Sheets al cancelar (fila parcial)
    if _gs_ready():
        try:
            payload = build_supervisiones_v2_row(s_, estado="No Completado", motivo_cancelacion=s_.get("motivo_cancelacion", ""))
            logging.info(f"🟦 Guardando CANCELADO en '{SHEET_TAB_SUPERVISIONES_V2}' id={s_.get('id_supervision')}")
            gs_append_dict(SHEET_TAB_SUPERVISIONES_V2, payload)
            logging.info("✅ Guardado cancelado en Sheets OK (Supervisiones_v2).")
        except Exception as e:
            logging.exception("❌ Error guardando cancelación v2 en Sheets")
            try:
                await send_message(update, context, f"⚠️ Cancelado, pero NO pude guardar en Sheets.\nDetalle: {e}")
            except Exception:
                pass

    cleanup_session_temp_files(s_)
    context.user_data.pop("s", None)
    await send_message(update, context, "❌ Proceso cancelado.\n📌 Estado: No Completado", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# =========================
# /config (admin-only) + menú
# =========================
CFG_MENU_KB = kb_inline(
    [
        ("🔗 Vincular Evidencias", "CFG_LINK_EVID"),
        ("🧾 Vincular Resumen", "CFG_LINK_SUMM"),
        ("📌 Ver rutas de este grupo", "CFG_VIEW"),
        ("❌ Cerrar", "CFG_CLOSE"),
    ],
    cols=1,
)

def CFG_ORIGIN_KB() -> InlineKeyboardMarkup:
    return kb_inline(
        [
            ("🧾 Generar código Evidencias", "CFG_GEN_EVID"),
            ("🧾 Generar código Resumen", "CFG_GEN_SUMM"),
            ("⬅️ Volver", "CFG_BACK"),
        ],
        cols=1,
    )

async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not in_group(update):
        await send_message(update, context, "Usa /config dentro de un grupo.")
        return ConversationHandler.END

    if not await is_admin(update, context):
        await send_message(update, context, "⛔ Solo administradores pueden usar /config.")
        return ConversationHandler.END

    await send_message(update, context, "⚙️ CONFIGURACIÓN", reply_markup=CFG_MENU_KB)
    return S_CFG_MENU

async def on_cfg_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "CFG_CLOSE":
        await safe_edit_or_send(query, "✅ Config cerrado.", reply_markup=None)
        return ConversationHandler.END

    if query.data == "CFG_VIEW":
        chat_id = query.message.chat_id
        r = get_route_for_chat(chat_id)
        if r and r.get("activo"):
            ev = r.get("evidence_chat_id", "")
            su = r.get("summary_chat_id", "")
            alias = r.get("alias", "")
            txt = (
                "📌 RUTAS DE ESTE GRUPO (ORIGEN)\n\n"
                f"• origin_chat_id: {chat_id}\n"
                f"• alias: {alias}\n"
                f"• evidence_chat_id: {ev or '(no vinculado)'}\n"
                f"• summary_chat_id: {su or '(no vinculado)'}\n"
                f"• activo: ✅\n"
            )
        elif r and not r.get("activo"):
            txt = (
                "📌 RUTAS DE ESTE GRUPO\n\n"
                f"• origin_chat_id: {chat_id}\n"
                "• activo: ❌ (ruta inactiva)\n"
            )
        else:
            txt = "📌 Este grupo no es ORIGEN (no tiene fila activa en ROUTING)."

        await safe_edit_or_send(query, txt, reply_markup=CFG_MENU_KB)
        return S_CFG_MENU

    # Vincular evidencias / resumen: el destino debe pegar un código
    if query.data in ("CFG_LINK_EVID", "CFG_LINK_SUMM"):
        purpose = "EVIDENCE" if query.data == "CFG_LINK_EVID" else "SUMMARY"
        context.chat_data["cfg_purpose"] = purpose
        await safe_edit_or_send(
            query,
            f"🧾 Pega el CÓDIGO para vincular {'EVIDENCIAS' if purpose=='EVIDENCE' else 'RESUMEN'}.\n\n"
            "✅ Envía el código en un solo mensaje.",
            reply_markup=None,
        )
        return S_CFG_WAIT_CODE

    return S_CFG_MENU

async def on_cfg_wait_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    El DESTINO pega un código. Se valida/consume y se actualiza ROUTING.
    """
    if not update.message or not update.message.text:
        await send_message(update, context, "❌ Pega el código en texto.")
        return S_CFG_WAIT_CODE

    if not await is_admin(update, context):
        await send_message(update, context, "⛔ Solo administradores pueden vincular por código.")
        return ConversationHandler.END

    code = update.message.text.strip().upper()
    purpose = context.chat_data.get("cfg_purpose", "EVIDENCE")

    if not _gs_ready():
        await send_message(update, context, "⚠️ Sheets no está configurado.")
        return ConversationHandler.END

    try:
        # Buscar code en PAIRING
        ws = gs_ws(SHEET_TAB_PAIRING)
        headers = gs_headers(SHEET_TAB_PAIRING)
        h2i = {h: i for i, h in enumerate(headers)}
        values = ws.get_all_values()
        if not values or len(values) < 2:
            await send_message(update, context, "❌ No hay registros en PAIRING.")
            return ConversationHandler.END

        idx_code = h2i.get("code")
        idx_origin = h2i.get("origin_chat_id")
        idx_purpose = h2i.get("purpose")
        idx_expires = h2i.get("expires_at")
        idx_used = h2i.get("used")

        if idx_code is None or idx_origin is None or idx_purpose is None or idx_expires is None or idx_used is None:
            await send_message(update, context, "⚠️ Headers de PAIRING incompletos (code/origin_chat_id/purpose/expires_at/used).")
            return ConversationHandler.END

        found_row_idx = None
        found_row = None
        for r in range(2, len(values) + 1):
            row = values[r - 1]
            c = row[idx_code].strip().upper() if idx_code < len(row) else ""
            if c == code:
                found_row_idx = r
                found_row = row
                break

        if not found_row_idx or not found_row:
            await send_message(update, context, "❌ Código no encontrado.")
            return ConversationHandler.END

        row_purpose = found_row[idx_purpose].strip().upper() if idx_purpose < len(found_row) else ""
        if row_purpose != purpose:
            await send_message(update, context, f"❌ El código es para {row_purpose}, no para {purpose}.")
            return ConversationHandler.END

        used_val = found_row[idx_used].strip() if idx_used < len(found_row) else "0"
        if _is_truthy(used_val) or used_val == "1":
            await send_message(update, context, "❌ Código ya fue usado.")
            return ConversationHandler.END

        exp_val = found_row[idx_expires].strip() if idx_expires < len(found_row) else ""
        if not exp_val or is_expired(exp_val):
            await send_message(update, context, "❌ Código expirado. Genera uno nuevo.")
            return ConversationHandler.END

        origin_chat_id = found_row[idx_origin].strip() if idx_origin < len(found_row) else ""
        if not origin_chat_id:
            await send_message(update, context, "❌ Código inválido (sin origin_chat_id).")
            return ConversationHandler.END

        # Consumir código: used=1 + used_by/used_at
        patch_pair = {
            "used": "1",
            "used_by": str(update.effective_user.id if update.effective_user else ""),
            "used_at": now_peru_str(),
        }
        gs_update_row_by_headers(SHEET_TAB_PAIRING, found_row_idx, patch_pair)

        # Actualizar ROUTING (crear si no existe)
        dest_chat_id = update.effective_chat.id

        row_idx_route = gs_find_row_index_first(SHEET_TAB_ROUTING, {"origin_chat_id": origin_chat_id})
        patch_route = {
            "origin_chat_id": origin_chat_id,
            "activo": "1",
            "updated_by": str(update.effective_user.id if update.effective_user else ""),
            "updated_at": now_peru_str(),
        }
        if purpose == "EVIDENCE":
            patch_route["evidence_chat_id"] = str(dest_chat_id)
        else:
            patch_route["summary_chat_id"] = str(dest_chat_id)

        if row_idx_route:
            gs_update_row_by_headers(SHEET_TAB_ROUTING, row_idx_route, patch_route)
        else:
            # append nueva fila con headers (lo que no exista se ignora)
            gs_append_dict(SHEET_TAB_ROUTING, patch_route)

        # refrescar cache routing
        load_routing_cache(force=True)

        await send_message(
            update,
            context,
            "✅ Vinculación realizada.\n\n"
            f"• ORIGEN: {origin_chat_id}\n"
            f"• DESTINO ({'EVIDENCIAS' if purpose=='EVIDENCE' else 'RESUMEN'}): {dest_chat_id}\n"
            "📌 Puedes verificar con /config → Ver rutas.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    except Exception as e:
        logging.exception("Error en vinculación por código")
        await send_message(update, context, f"❌ Error vinculando.\nDetalle: {e}")
        return ConversationHandler.END

async def on_cfg_back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_or_send(query, "⚙️ CONFIGURACIÓN", reply_markup=CFG_MENU_KB)
    return S_CFG_MENU

async def cmd_config_origin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Opción alternativa: /config_origin para ORIGEN (admin) y generar códigos.
    (No es obligatoria, pero útil si quieres separar menús).
    """
    if not in_group(update):
        await send_message(update, context, "Usa /config_origin dentro de un grupo.")
        return ConversationHandler.END
    if not await is_admin(update, context):
        await send_message(update, context, "⛔ Solo administradores pueden usar /config_origin.")
        return ConversationHandler.END

    await send_message(update, context, "⚙️ CONFIG ORIGEN (generar códigos)", reply_markup=CFG_ORIGIN_KB())
    return S_CFG_MENU

async def on_cfg_origin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Generación de códigos (en ORIGEN) para Evidencias/Resumen.
    """
    query = update.callback_query
    await query.answer()

    if query.data == "CFG_BACK":
        await safe_edit_or_send(query, "⚙️ CONFIG ORIGEN (generar códigos)", reply_markup=CFG_ORIGIN_KB())
        return S_CFG_MENU

    if query.data not in ("CFG_GEN_EVID", "CFG_GEN_SUMM"):
        return S_CFG_MENU

    if not _gs_ready():
        await safe_edit_or_send(query, "⚠️ Sheets no está configurado.", reply_markup=CFG_ORIGIN_KB())
        return S_CFG_MENU

    purpose = "EVIDENCE" if query.data == "CFG_GEN_EVID" else "SUMMARY"
    origin_chat_id = query.message.chat_id
    code = gen_pairing_code(8)

    row = {
        "code": code,
        "origin_chat_id": str(origin_chat_id),
        "purpose": purpose,
        "expires_at": pairing_expires_at_str(PAIRING_TTL_MINUTES),
        "used": "0",
        "created_by": str(query.from_user.id if query.from_user else ""),
        "created_at": now_peru_str(),
        "used_by": "",
        "used_at": "",
    }

    try:
        gs_append_dict(SHEET_TAB_PAIRING, row)
        await safe_edit_or_send(
            query,
            f"✅ Código generado ({'EVIDENCIAS' if purpose=='EVIDENCE' else 'RESUMEN'})\n\n"
            f"🧾 CÓDIGO: `{code}`\n"
            f"⏳ Expira en: {PAIRING_TTL_MINUTES} minutos\n\n"
            "Pégalo en el grupo DESTINO usando /config → Vincular ...",
            reply_markup=CFG_ORIGIN_KB(),
        )
        return S_CFG_MENU
    except Exception as e:
        logging.exception("Error generando código")
        await safe_edit_or_send(query, f"❌ No pude generar código.\nDetalle: {e}", reply_markup=CFG_ORIGIN_KB())
        return S_CFG_MENU

# =========================
# Error handler
# =========================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled error:", exc_info=context.error)

# =========================
# main()
# =========================
def main():
    if not BOT_TOKEN:
        raise SystemExit("Configura BOT_TOKEN como variable de entorno en Railway o en tu entorno local.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    # ---- comandos sheets/plantillas
    app.add_handler(CommandHandler("plantilla", cmd_plantilla))
    app.add_handler(CommandHandler("cancelar_plantilla", cmd_cancelar_plantilla))
    app.add_handler(CommandHandler("reload_sheet", cmd_reload_sheet))

    # ---- /config routing/pairing
    # Nota: /config gestiona DESTINO (pega código) y ver rutas.
    # /config_origin (opcional) gestiona ORIGEN (genera códigos).
    cfg_conv = ConversationHandler(
        entry_points=[CommandHandler("config", cmd_config), CommandHandler("config_origin", cmd_config_origin)],
        per_chat=True,
        per_user=True,
        states={
            S_CFG_MENU: [
                CallbackQueryHandler(on_cfg_menu, pattern=r"^CFG_(LINK_EVID|LINK_SUMM|VIEW|CLOSE)$"),
                CallbackQueryHandler(on_cfg_origin_menu, pattern=r"^CFG_(GEN_EVID|GEN_SUMM|BACK)$"),
            ],
            S_CFG_WAIT_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_cfg_wait_code),
            ],
        },
        fallbacks=[],
        allow_reentry=True,
    )
    app.add_handler(cfg_conv, group=0)

    media_filter = (
        filters.PHOTO
        | filters.VIDEO
        | filters.Document.MimeType("video/mp4")
        | filters.Document.MimeType("video/quicktime")
        | filters.Document.MimeType("video/x-matroska")
        | filters.Document.MimeType("video/webm")
        | filters.Document.MimeType("video/*")
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("inicio", inicio)],
        per_chat=True,
        per_user=True,
        states={
            S_SUPERVISOR: [CallbackQueryHandler(on_pick_supervisor, pattern=r"^SUP_(PICK\|\d+|NONE)$")],

            # Paso 2: operador (y técnico TU FIBRA en el mismo estado)
            S_OPERADOR: [
                CallbackQueryHandler(on_pick_operador, pattern=r"^OP_\d+$"),
                CallbackQueryHandler(on_pick_tecnico_tufibra, pattern=r"^(TF_PICK\|\d+|TF_NONE)$"),
            ],

            # WIN: búsqueda + botones
            S_WIN_CUADRILLA: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_win_cuadrilla_text),
                CallbackQueryHandler(on_win_pick_match, pattern=r"^WIN_PICK\|\d+$"),
                CallbackQueryHandler(on_win_refine, pattern=r"^WIN_REFINE$"),
            ],

            S_CODIGO: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_codigo)],
            S_TIPO: [CallbackQueryHandler(on_pick_tipo, pattern=r"^TIPO_")],
            S_UBICACION: [MessageHandler(filters.LOCATION, on_location)],

            S_FACHADA_MEDIA: [
                MessageHandler(media_filter, on_media),
                CallbackQueryHandler(on_add_more_or_done, pattern=r"^(ADD_MORE|DONE_MEDIA)$"),
            ],

            S_MENU_PRINCIPAL: [
                CallbackQueryHandler(on_main_menu, pattern=r"^(MENU_.*|FINALIZAR)$")
            ],

            S_MENU_CABLEADO: [CallbackQueryHandler(on_menu_cableado, pattern=CABLEADO_PATTERN)],
            S_MENU_CUADRILLA: [CallbackQueryHandler(on_menu_cuadrilla, pattern=CUADRILLA_PATTERN)],

            S_CARGA_MEDIA_BUCKET: [
                MessageHandler(media_filter, on_media),
                CallbackQueryHandler(on_add_more_or_done, pattern=r"^(ADD_MORE|DONE_MEDIA)$"),
            ],

            S_ASK_OBS: [CallbackQueryHandler(on_obs_choice, pattern=r"^OBS_")],
            S_WRITE_OBS: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_write_obs)],

            S_FINAL_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_final_text)],
        },
        fallbacks=[CommandHandler("cancelar", cancelar)],
        allow_reentry=True,
    )

    # 0) Primero el ConversationHandler principal
    app.add_handler(conv, group=1)

    # 1) Rescate de código (si el ConversationHandler no lo toma)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, codigo_global), group=2)

    # 2) Captura de plantilla (va después del rescate)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, auto_capture_plantilla), group=3)

    logging.info("✅ Bot iniciado. Polling...")

    try:
        app.run_polling(close_loop=False, drop_pending_updates=True)
    except Exception as e:
        logging.critical("FATAL ERROR: el bot se detuvo por una excepción no manejada.", exc_info=e)
        raise

if __name__ == "__main__":
    main()
