# -*- coding: utf-8 -*-
"""QQ三国个人信息 - Web版后端"""

from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, desc, or_
from datetime import datetime, timedelta
from typing import Optional, List
import glob, sys
from pydantic import BaseModel, field_validator
from pathlib import Path
import uvicorn
import shutil, logging, json, sqlite3 as _sqlite3


def _safe_backup(src_path, dst_path):
    """使用 sqlite3 backup API 安全备份（WAL模式兼容）"""
    src_conn = _sqlite3.connect(str(src_path))
    dst_conn = _sqlite3.connect(str(dst_path))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

import os
APP_API_SIGNATURE = "qqsg-web-20260426-consume-daily-active"
if getattr(sys, 'frozen', False) and sys.stdout is None:
    _log_dir = Path(os.environ.get('LOCALAPPDATA', Path.home())) / "QQ三国资产管理"
    _log_dir.mkdir(parents=True, exist_ok=True)
    _log_file = str(_log_dir / "server.log")
    sys.stdout = open(_log_file, 'a', encoding='utf-8', errors='replace')
    sys.stderr = sys.stdout
    logging.basicConfig(
        filename=_log_file, level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8',
    )

from database import init_db, get_db, Transaction, CategoryItem, ItemPriceAnchor, AnchorImage, PriceHistory, DungeonRevenue, DungeonImage, CategoryItemImage, TransactionImage, PriceImage, EstimatedValueHistory, AuditLog, Setting, AssetSnapshot, Note, NoteImage, DB_PATH, DB_SOURCE

# ── 初始化 ──────────────────────────────────────────────────────────
init_db()
app = FastAPI(title="QQ三国个人信息")

# 全局异常处理 - 防止未捕获异常导致服务不可用
from starlette.requests import Request
from starlette.responses import JSONResponse
@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception):
    logging.error(f"未捕获异常 [{request.method} {request.url.path}]: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": f"服务器内部错误: {type(exc).__name__}"})
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

if getattr(sys, 'frozen', False):
    _BASE_DIR = Path(sys._MEIPASS)
else:
    _BASE_DIR = Path(__file__).resolve().parent


def _resolve_static_dir():
    candidates = []
    source_web_dir = (os.environ.get('QQSG_SOURCE_WEB_DIR') or '').strip()
    if source_web_dir:
        candidates.append(Path(source_web_dir) / "static")
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.extend([
            exe_dir.parent.parent / "static",
            exe_dir.parent.parent / "web" / "static",
            exe_dir.parent / "web" / "static",
            exe_dir / "web" / "static",
        ])
    candidates.append(_BASE_DIR / "static")
    seen = set()
    for candidate in candidates:
        candidate_text = os.path.abspath(str(candidate))
        if candidate_text in seen:
            continue
        seen.add(candidate_text)
        if candidate.is_dir() and (candidate / "index.html").exists():
            return candidate
    return _BASE_DIR / "static"


STATIC_DIR = _resolve_static_dir()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── 数据库智能备份 ─────────────────────────────────────────
import threading as _threading
from database import SessionLocal as _BkSession

_last_backup_time = None
_last_mirror_backup_time = None
_last_backup_status = {"time": None, "path": None, "success": None, "error": None}
_last_daily_backup_status = {"time": None, "path": None, "success": None, "error": None}
_last_local_mirror_status = {"time": None, "path": None, "success": None, "error": None}
_LOCAL_BACKUP_DIR = DB_PATH.parent / "backups"
_LOCAL_LATEST_MIRROR = _LOCAL_BACKUP_DIR / "qqsg_latest_mirror.db"
_REMOTE_LATEST_MIRROR_NAME = "qqsg_latest_mirror.db"
_MIRROR_BACKUP_MIN_INTERVAL_SECONDS = 120
_sync_thread_lock = _threading.Lock()
_sync_thread = None
_sync_pending = False


def _write_backup_mirror(dst_path: Path):
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst_path.parent / f"{dst_path.name}.tmp"
    tmp_path.unlink(missing_ok=True)
    _safe_backup(DB_PATH, tmp_path)
    tmp_path.replace(dst_path)


def _refresh_local_latest_backup(now: datetime | None = None):
    now = now or datetime.now()
    try:
        _write_backup_mirror(_LOCAL_LATEST_MIRROR)
        _last_local_mirror_status.update(time=now.isoformat(), path=str(_LOCAL_LATEST_MIRROR), success=True, error=None)
    except Exception as e:
        _last_local_mirror_status.update(time=now.isoformat(), path=str(_LOCAL_LATEST_MIRROR), success=False, error=str(e))
        logging.warning(f"本地最新镜像备份失败: {e}")


def _sync_db():
    """智能备份：持续维护本地最新镜像，并按节流策略写入远程镜像/日快照"""
    global _last_backup_time, _last_mirror_backup_time
    now = datetime.now()
    _refresh_local_latest_backup(now)
    backup_path = ''
    try:
        db = _BkSession()
        try:
            bp_row = db.query(Setting).filter(Setting.key == 'backup_path').first()
            kc_row = db.query(Setting).filter(Setting.key == 'backup_keep_count').first()
        finally:
            db.close()
        backup_path = bp_row.value.strip() if bp_row and bp_row.value else ''
        if not backup_path:
            return
        keep_count = int(kc_row.value) if kc_row and kc_row.value else 7
        bdir = Path(backup_path)
        bdir.mkdir(parents=True, exist_ok=True)
        if (_last_mirror_backup_time is None) or ((now - _last_mirror_backup_time).total_seconds() >= _MIRROR_BACKUP_MIN_INTERVAL_SECONDS):
            _write_backup_mirror(bdir / _REMOTE_LATEST_MIRROR_NAME)
            _last_mirror_backup_time = now
        if _last_backup_time and (now - _last_backup_time).total_seconds() < 86400:
            return
        ts = now.strftime('%Y%m%d_%H%M%S')
        dest = bdir / f'qq_sanguo_{ts}.db'
        _safe_backup(DB_PATH, dest)
        _last_backup_time = now
        _last_backup_status.update(time=now.isoformat(), path=str(dest), success=True, error=None)
        existing = sorted(bdir.glob('qq_sanguo_*.db'), key=lambda p: p.name)
        while len(existing) > keep_count:
            existing.pop(0).unlink()
    except Exception as e:
        _last_backup_status.update(time=now.isoformat(), path=backup_path, success=False, error=str(e))
        logging.warning(f'数据库备份失败: {e}')


def _schedule_sync_db():
    global _sync_thread, _sync_pending

    def _runner():
        global _sync_thread, _sync_pending
        while True:
            try:
                _sync_db()
            except Exception as e:
                logging.warning(f'数据库同步任务失败: {e}')
            with _sync_thread_lock:
                if _sync_pending:
                    _sync_pending = False
                    continue
                _sync_thread = None
                break

    with _sync_thread_lock:
        if _sync_thread and _sync_thread.is_alive():
            _sync_pending = True
            return
        _sync_pending = False
        _sync_thread = _threading.Thread(target=_runner, daemon=True)
        _sync_thread.start()


class _BackupASGIMiddleware:
    """纯ASGI中间件 - 不使用BaseHTTPMiddleware，避免连接泄漏和内存问题"""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status_code = 200
        async def _send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 200)
            await send(message)
        try:
            await self.app(scope, receive, _send_wrapper)
        except Exception:
            raise
        finally:
            method = scope.get("method", "GET")
            if method in ("POST", "PUT", "DELETE") and status_code < 400:
                _schedule_sync_db()

app.add_middleware(_BackupASGIMiddleware)


def _audit(db: Session, table: str, record_id: int, action: str, changes: dict = None):
    """记录审计日志"""
    db.add(AuditLog(
        table_name=table, record_id=record_id, action=action,
        changes=json.dumps(changes, ensure_ascii=False, default=str) if changes else None,
        timestamp=datetime.now(),
    ))


def _diff_row(row, new_data: dict) -> dict:
    """比较旧行和新数据，返回有变化的字段 {field: {old, new}}"""
    changes = {}
    for k, v in new_data.items():
        old = getattr(row, k, None)
        if isinstance(old, datetime):
            old = old.isoformat()
        if isinstance(v, datetime):
            v = v.isoformat()
        if str(old) != str(v):
            changes[k] = {"old": old, "new": v}
    return changes


def _get_active_row(db: Session, model, record_id: int, not_found: str):
    filters = [model.id == record_id]
    if hasattr(model, 'deleted_at'):
        filters.append(model.deleted_at == None)
    row = db.query(model).filter(*filters).first()
    if not row:
        raise HTTPException(404, not_found)
    return row


def _coerce_float(value, field_name: str) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{field_name}必须是数字")


def _coerce_int(value, field_name: str, minimum: int | None = None) -> int:
    try:
        num = int(value)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{field_name}必须是整数")
    if minimum is not None and num < minimum:
        raise HTTPException(400, f"{field_name}不能小于 {minimum}")
    return num


# ── 动态配置加载 ─────────────────────────────────────────
def _load_json_setting(db, key: str, default=None):
    row = db.query(Setting).filter(Setting.key == key).first()
    if not row or not row.value:
        return default if default is not None else []
    try:
        return json.loads(row.value)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else []

def _get_tx_type_configs(db) -> list:
    return _load_json_setting(db, 'tx_type_options', [])

def _get_tx_type_names(db) -> list:
    return [t['name'] for t in _get_tx_type_configs(db)]

def _get_categories(db) -> list:
    return _load_json_setting(db, 'category_options', ['装备','灵兽','元神','灵魄','子女','倒货项目'])

def _get_channels(db) -> list:
    return _load_json_setting(db, 'channel_options', ['商人','玩家'])

# ── Pydantic Schemas ───────────────────────────────────────────────
class TxCreate(BaseModel):
    timestamp: datetime
    type: str
    direction: str = 'expense'  # income/expense
    quantity: float = 0
    unit_price: float = 0
    channel: Optional[str] = None
    status: str = 'normal'  # normal/void
    notes: Optional[str] = None

class TxUpdate(TxCreate):
    pass

class CatItemCreate(BaseModel):
    timestamp: datetime
    category: str
    type: str           # 买入/卖出
    item_name: str
    item_group: Optional[str] = ''
    quantity: int = 1           # 数量
    cost_mode: str = 'coin'
    coin_price: float = 0       # 单价 (亿三国币)
    cash_price: float = 0       # 单价 (元)
    purchase_rate: float = 0    # 购买当天币价 (1亿=X元)
    estimated_value: float = 0  # 预估市场价值 (亿三国币)
    estimated_rmb: float = 0
    holding_status: str = '持有中'  # 持有中/已出手/已消耗
    sell_rate: float = 0            # 卖出当天币价
    source_type: str = '手动录入'
    source_ref: Optional[str] = None
    valuation_mode: str = ''
    channel: Optional[str] = None
    notes: Optional[str] = None

class CatItemUpdate(CatItemCreate):
    pass


class CatItemConsumeCreate(BaseModel):
    quantity: int = 1
    timestamp: Optional[datetime] = None
    notes: Optional[str] = None


class CatItemGroupConsumeCreate(BaseModel):
    category: str
    item_name: str
    quantity: int = 1
    timestamp: Optional[datetime] = None
    notes: Optional[str] = None

class CatItemSellCreate(BaseModel):
    quantity: int = 1
    coin_price: float = 0
    sell_rate: Optional[float] = None
    timestamp: Optional[datetime] = None
    notes: Optional[str] = None

class CatItemGroupSellCreate(BaseModel):
    category: str
    item_name: str
    quantity: int = 1
    coin_price: float = 0
    sell_rate: Optional[float] = None
    timestamp: Optional[datetime] = None
    notes: Optional[str] = None

class PriceCreate(BaseModel):
    timestamp: datetime
    price: float
    price_10e: float = 0
    price_recycle: float = 0
    source: Optional[str] = None
    notes: Optional[str] = None

class PriceUpdate(PriceCreate):
    pass

class DropItemSchema(BaseModel):
    name: str
    qty: int = 1

class DungeonCreate(BaseModel):
    timestamp: datetime
    dungeon_name: str
    revenue_coin: float = 0
    drop_items: list[DropItemSchema] = []
    notes: Optional[str] = None

class DungeonUpdate(DungeonCreate):
    pass

class ImageUpload(BaseModel):
    image_data: str

    @field_validator('image_data')
    @classmethod
    def check_size(cls, v):
        if len(v) > 8_000_000:  # ~6MB base64
            raise ValueError('图片数据过大，请压缩后再上传（最大约6MB）')
        if v and not v.startswith('data:image'):
            raise ValueError('图片格式无效，请使用图片文件')
        return v

class OptionsUpdate(BaseModel):
    items: list[str]

class SettingUpdate(BaseModel):
    key: str
    value: str


class DailyActiveSyncCreate(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class DailyActiveAdjustCreate(BaseModel):
    date: str
    amount_wan: float
    notes: Optional[str] = None


class ItemPriceAnchorCreate(BaseModel):
    timestamp: datetime
    item_name: str
    category: Optional[str] = None
    market_price: float = 0
    price_unit: str = '亿'
    source: Optional[str] = None
    notes: Optional[str] = None


# ── 首页 ────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"), headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0"
    })

@app.get("/design")
async def design_preview():
    preview = _BASE_DIR / "design_preview.html"
    if preview.exists():
        return FileResponse(str(preview))
    raise HTTPException(404, "design_preview.html not found")

@app.get("/api/health")
async def health_check():
    """健康检查端点 - 前端心跳检测用"""
    return {"status": "ok", "time": datetime.now().isoformat(), "db_path": str(DB_PATH), "db_source": DB_SOURCE, "api_signature": APP_API_SIGNATURE}


@app.get("/favicon.ico")
async def favicon():
    ico = STATIC_DIR.parent / "app.ico"
    if ico.exists():
        return FileResponse(str(ico), media_type="image/x-icon")
    return Response(status_code=204)


# ── 辅助：计算交易金额 ────────────────────────────────────────────
def _tx_rmb(t: Transaction) -> float:
    """买币/卖币: qty*price; 三国点及自定义: unit_price 即为折合人民币总额"""
    if t.type in ("买币", "卖币"):
        return t.quantity * t.unit_price
    return t.unit_price


def _anchor_price_yi(v: float, unit: str) -> float:
    if (unit or '亿') == '万':
        return float(v or 0) / 10000
    return float(v or 0)


def _anchor_dict(r: ItemPriceAnchor, latest_rate: float = 0, db: Session = None) -> dict:
    market_price_yi = _anchor_price_yi(r.market_price, r.price_unit)
    images = []
    if db:
        imgs = db.query(AnchorImage).filter(AnchorImage.anchor_id == r.id).order_by(AnchorImage.id).all()
        images = [{"id": img.id, "data": img.image_data} for img in imgs]
    return {
        "id": r.id,
        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        "item_name": r.item_name,
        "category": r.category,
        "market_price": round(r.market_price or 0, 4),
        "price_unit": r.price_unit or '亿',
        "market_price_yi": round(market_price_yi, 4),
        "market_price_rmb": round(market_price_yi * latest_rate, 2) if latest_rate else 0,
        "source": r.source,
        "notes": r.notes,
        "images": images,
    }


def _tx_dict(r: Transaction, db: Session = None) -> dict:
    rmb = _tx_rmb(r)
    direction = getattr(r, 'direction', None) or 'expense'
    cur_change = 0.0
    if r.type == "买币":
        cur_change = r.quantity
    elif r.type == "卖币":
        cur_change = -r.quantity
    cashflow = -rmb if direction == 'expense' else rmb
    images = []
    if db:
        imgs = db.query(TransactionImage).filter(TransactionImage.transaction_id == r.id).all()
        images = [{"id": img.id, "data": img.image_data} for img in imgs]
    return {
        "id": r.id,
        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        "type": r.type,
        "direction": direction,
        "quantity": r.quantity,
        "unit_price": r.unit_price,
        "channel": r.channel,
        "status": getattr(r, 'status', 'normal') or 'normal',
        "notes": r.notes,
        "rmb_amount": round(rmb, 2),
        "currency_change": round(cur_change, 2),
        "cashflow": round(cashflow, 2),
        "images": images,
    }


# ── 交易流水 CRUD ──────────────────────────────────────────────────
@app.get("/api/transactions")
def list_transactions(
    type: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=500, le=10000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(Transaction).filter(Transaction.deleted_at == None).order_by(desc(Transaction.timestamp))
    if type:
        q = q.filter(Transaction.type == type)
    if status:
        q = q.filter(Transaction.status == status)
    if start_date:
        q = q.filter(Transaction.timestamp >= start_date)
    if end_date:
        q = q.filter(Transaction.timestamp <= end_date + " 23:59:59")
    total = q.count()
    rows = q.offset(offset).limit(limit).all()
    return {"total": total, "items": [_tx_dict(r, db) for r in rows]}


@app.post("/api/transactions")
def create_transaction(tx: TxCreate, db: Session = Depends(get_db)):
    valid_types = _get_tx_type_names(db)
    if valid_types and tx.type not in valid_types:
        raise HTTPException(400, f"无效交易类型: {tx.type}")
    if tx.quantity <= 0:
        raise HTTPException(400, "数量必须大于 0")
    row = Transaction(**tx.model_dump())
    db.add(row); db.flush()
    _audit(db, 'transactions', row.id, 'create', tx.model_dump())
    db.commit(); db.refresh(row)
    return _tx_dict(row)


@app.put("/api/transactions/{tid}")
def update_transaction(tid: int, tx: TxUpdate, db: Session = Depends(get_db)):
    valid_types = _get_tx_type_names(db)
    if valid_types and tx.type not in valid_types:
        raise HTTPException(400, f"无效交易类型: {tx.type}")
    if tx.quantity <= 0:
        raise HTTPException(400, "数量必须大于 0")
    row = _get_active_row(db, Transaction, tid, "交易不存在")
    changes = _diff_row(row, tx.model_dump())
    if changes:
        _audit(db, 'transactions', tid, 'edit', changes)
    for k, v in tx.model_dump().items():
        setattr(row, k, v)
    db.commit(); db.refresh(row)
    return _tx_dict(row)


@app.delete("/api/transactions/{tid}")
def delete_transaction(tid: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, Transaction, tid, "交易不存在")
    _audit(db, 'transactions', tid, 'delete', {"type": row.type, "quantity": row.quantity, "unit_price": row.unit_price})
    row.deleted_at = datetime.now()
    db.commit()
    return {"ok": True}


@app.put("/api/transactions/{tid}/void")
def void_transaction(tid: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, Transaction, tid, "交易不存在")
    old_status = row.status or 'normal'
    row.status = 'void' if old_status == 'normal' else 'normal'
    _audit(db, 'transactions', tid, 'void', {"status": {"old": old_status, "new": row.status}})
    db.commit(); db.refresh(row)
    return _tx_dict(row)


# ── 交易流水截图 ──────────────────────────────────────────────────
@app.get("/api/transactions/{tid}/images")
def list_tx_images(tid: int, db: Session = Depends(get_db)):
    imgs = db.query(TransactionImage).filter(TransactionImage.transaction_id == tid).all()
    return [{"id": img.id, "data": img.image_data} for img in imgs]


@app.post("/api/transactions/{tid}/images")
def upload_tx_image(tid: int, payload: ImageUpload, db: Session = Depends(get_db)):
    row = _get_active_row(db, Transaction, tid, "交易不存在")
    img = TransactionImage(transaction_id=row.id, image_data=payload.image_data)
    db.add(img); db.commit(); db.refresh(img)
    return {"id": img.id}


@app.delete("/api/transaction-images/{img_id}")
def delete_tx_image(img_id: int, db: Session = Depends(get_db)):
    img = db.query(TransactionImage).filter(TransactionImage.id == img_id).first()
    if not img:
        raise HTTPException(404, "图片不存在")
    db.delete(img); db.commit()
    return {"ok": True}


# ── 分类物品 CRUD ──────────────────────────────────────────────────
def _effective_vm(cost_mode: str, vm: str) -> str:
    """估值方式：coin/rmb 独立于计价模式，'' 按计价模式自然默认"""
    if vm in ('coin', 'rmb'):
        return vm
    return 'coin' if cost_mode == 'coin' else 'rmb'


def _cat_dict(r: CategoryItem, latest_rate: float = 0, db: Session = None) -> dict:
    qty = getattr(r, 'quantity', None) or 1
    cost_mode = getattr(r, 'cost_mode', 'coin') or 'coin'
    vm = getattr(r, 'valuation_mode', '') or ''
    total_coin = (r.coin_price or 0) * qty
    total_cash = (getattr(r, 'cash_price', 0) or 0) * qty
    purchase_rmb = round(total_cash if cost_mode == 'rmb_direct' else total_coin * (r.purchase_rate or 0), 2)
    ev = r.estimated_value or 0
    estimated_rmb = getattr(r, 'estimated_rmb', 0) or 0
    # 统一估值逻辑：由 valuation_mode 决定，与 cost_mode 无关
    eff_vm = _effective_vm(cost_mode, vm)
    if eff_vm == 'coin':
        coin_val = (ev if ev > 0 else (r.coin_price or 0)) * qty
        current_rmb = round(coin_val * latest_rate, 2) if latest_rate else 0
    else:
        if estimated_rmb > 0:
            current_rmb = round(estimated_rmb * qty, 2)
        elif cost_mode == 'rmb_direct':
            current_rmb = round((getattr(r, 'cash_price', 0) or 0) * qty, 2)
        else:
            # coin item + rmb valuation 但未填 estimated_rmb：回退到币值计算
            coin_val = (ev if ev > 0 else (r.coin_price or 0)) * qty
            current_rmb = round(coin_val * latest_rate, 2) if latest_rate else 0
    images = []
    if db:
        imgs = db.query(CategoryItemImage).filter(CategoryItemImage.item_id == r.id).all()
        images = [{"id": img.id, "data": img.image_data} for img in imgs]
    return {
        "id": r.id,
        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        "category": r.category,
        "type": r.type,
        "item_name": r.item_name,
        "item_group": getattr(r, 'item_group', '') or '',
        "quantity": qty,
        "cost_mode": cost_mode,
        "coin_price": r.coin_price,
        "cash_price": getattr(r, 'cash_price', 0) or 0,
        "purchase_rate": r.purchase_rate,
        "estimated_value": ev,
        "estimated_rmb": estimated_rmb,
        "holding_status": getattr(r, 'holding_status', '持有中') or '持有中',
        "sell_rate": getattr(r, 'sell_rate', 0) or 0,
        "status_changed_at": r.status_changed_at.isoformat() if getattr(r, 'status_changed_at', None) else None,
        "source_type": getattr(r, 'source_type', '手动录入') or '手动录入',
        "source_ref": getattr(r, 'source_ref', None),
        "valuation_mode": vm,
        "coin_total": round(total_coin, 4),
        "cash_total": round(total_cash, 2),
        "purchase_rmb": purchase_rmb,
        "current_rmb": current_rmb,
        "channel": r.channel,
        "notes": r.notes,
        "images": images,
        "image_count": len(images),
    }


def _item_purchase_rmb(item: CategoryItem) -> float:
    qty = getattr(item, 'quantity', 1) or 1
    cost_mode = getattr(item, 'cost_mode', 'coin') or 'coin'
    if cost_mode == 'rmb_direct':
        return round((getattr(item, 'cash_price', 0) or 0) * qty, 2)
    item_rate = getattr(item, 'purchase_rate', 0) or 0
    if getattr(item, 'type', '') == '卖出':
        sell_rate = getattr(item, 'sell_rate', 0) or 0
        if sell_rate > 0:
            item_rate = sell_rate
    return round((item.coin_price or 0) * qty * item_rate, 2)


def _item_current_rmb(item: CategoryItem, rate: float) -> float:
    qty = getattr(item, 'quantity', 1) or 1
    cost_mode = getattr(item, 'cost_mode', 'coin') or 'coin'
    vm = getattr(item, 'valuation_mode', '') or ''
    ev = item.estimated_value or 0
    estimated_rmb = getattr(item, 'estimated_rmb', 0) or 0
    eff_vm = _effective_vm(cost_mode, vm)
    if eff_vm == 'coin':
        coin_val = (ev if ev > 0 else (item.coin_price or 0)) * qty
        return round(coin_val * rate, 2) if rate else 0
    else:
        if estimated_rmb > 0:
            return round(estimated_rmb * qty, 2)
        elif cost_mode == 'rmb_direct':
            return round((getattr(item, 'cash_price', 0) or 0) * qty, 2)
        else:
            coin_val = (ev if ev > 0 else (item.coin_price or 0)) * qty
            return round(coin_val * rate, 2) if rate else 0


def _item_coin_cost(item: CategoryItem) -> float:
    if (getattr(item, 'cost_mode', 'coin') or 'coin') != 'coin':
        return 0.0
    qty = getattr(item, 'quantity', 1) or 1
    return (item.coin_price or 0) * qty


def _category_item_payload(row: CategoryItem) -> dict:
    return {
        "timestamp": row.timestamp,
        "category": row.category,
        "type": row.type,
        "item_name": row.item_name,
        "item_group": getattr(row, 'item_group', '') or '',
        "quantity": getattr(row, 'quantity', 1) or 1,
        "cost_mode": getattr(row, 'cost_mode', 'coin') or 'coin',
        "coin_price": row.coin_price or 0,
        "cash_price": getattr(row, 'cash_price', 0) or 0,
        "purchase_rate": row.purchase_rate or 0,
        "estimated_value": row.estimated_value or 0,
        "estimated_rmb": getattr(row, 'estimated_rmb', 0) or 0,
        "holding_status": getattr(row, 'holding_status', '持有中') or '持有中',
        "sell_rate": getattr(row, 'sell_rate', 0) or 0,
        "status_changed_at": getattr(row, 'status_changed_at', None),
        "source_type": getattr(row, 'source_type', '手动录入') or '手动录入',
        "source_ref": getattr(row, 'source_ref', None),
        "valuation_mode": getattr(row, 'valuation_mode', '') or '',
        "channel": row.channel,
        "notes": row.notes,
    }


def _merge_item_notes(base_notes: Optional[str], extra_notes: Optional[str]) -> Optional[str]:
    base = (base_notes or '').strip()
    extra = (extra_notes or '').strip()
    if base and extra:
        return f"{base} {extra}"
    return base or extra or None


def _consume_item_row(db: Session, row: CategoryItem, quantity: int, changed_at: datetime, notes: Optional[str] = None) -> dict:
    if row.type != "买入":
        raise HTTPException(400, "只有买入记录可以标记已消耗")
    if (getattr(row, 'holding_status', '持有中') or '持有中') != '持有中':
        raise HTTPException(400, "只有持有中的记录可以标记已消耗")
    current_qty = getattr(row, 'quantity', 1) or 1
    if quantity < 1:
        raise HTTPException(400, "数量必须大于 0")
    if quantity > current_qty:
        raise HTTPException(400, f"可消耗数量不足，当前仅剩 {current_qty}")
    if quantity == current_qty:
        old_status = getattr(row, 'holding_status', '持有中') or '持有中'
        old_changed_at = getattr(row, 'status_changed_at', None)
        old_notes = row.notes
        row.holding_status = "已消耗"
        row.status_changed_at = changed_at
        row.notes = _merge_item_notes(row.notes, notes)
        changes = {
            "holding_status": {"old": old_status, "new": row.holding_status},
            "status_changed_at": {"old": old_changed_at.isoformat() if old_changed_at else None, "new": changed_at.isoformat()},
            "consumed_qty": quantity,
        }
        if row.notes != old_notes:
            changes["notes"] = {"old": old_notes, "new": row.notes}
        _audit(db, 'category_items', row.id, 'consume', changes)
        return {"consumed": quantity, "remaining": 0, "created_id": None}
    payload = _category_item_payload(row)
    payload["quantity"] = quantity
    payload["holding_status"] = "已消耗"
    payload["status_changed_at"] = changed_at
    payload["notes"] = _merge_item_notes(payload.get("notes"), f"[拆分自#{row.id}，已消耗{quantity}]")
    payload["notes"] = _merge_item_notes(payload.get("notes"), notes)
    consumed_row = CategoryItem(**payload)
    db.add(consumed_row)
    row.quantity = current_qty - quantity
    db.flush()
    _audit(db, 'category_items', row.id, 'consume_split', {
        "quantity": {"old": current_qty, "new": row.quantity},
        "consumed_qty": quantity,
        "created_id": consumed_row.id,
    })
    _audit(db, 'category_items', consumed_row.id, 'consume_create', {
        "source_id": row.id,
        "quantity": quantity,
        "holding_status": "已消耗",
        "status_changed_at": changed_at.isoformat(),
        "notes": consumed_row.notes,
    })
    return {"consumed": quantity, "remaining": row.quantity, "created_id": consumed_row.id}


def _consume_item_group(db: Session, category: str, item_name: str, quantity: int, changed_at: datetime, notes: Optional[str] = None) -> dict:
    rows = db.query(CategoryItem).filter(
        CategoryItem.category == category,
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).order_by(CategoryItem.timestamp, CategoryItem.id).all()
    available = sum((getattr(row, 'quantity', 1) or 1) for row in rows)
    if quantity < 1:
        raise HTTPException(400, "数量必须大于 0")
    if available < quantity:
        raise HTTPException(400, f"{item_name} 可消耗数量不足，当前仅剩 {available}")
    remaining = quantity
    affected_rows = 0
    note_used = False
    for row in rows:
        if remaining <= 0:
            break
        row_qty = getattr(row, 'quantity', 1) or 1
        take_qty = min(row_qty, remaining)
        _consume_item_row(db, row, take_qty, changed_at, notes if not note_used else None)
        remaining -= take_qty
        affected_rows += 1
        note_used = note_used or bool(notes)
    return {"consumed": quantity, "affected_rows": affected_rows, "remaining_available": available - quantity}


def _write_asset_snapshot(db: Session, snapshot_month: str, values: dict, source: str = 'system', notes: str = None):
    row = db.query(AssetSnapshot).filter(AssetSnapshot.snapshot_month == snapshot_month).order_by(desc(AssetSnapshot.snapshot_time)).first()
    if row:
        row.snapshot_time = datetime.now()
        row.rate = values['rate']
        row.coin_hold = values['coin_hold']
        row.coin_value = values['coin_value']
        row.held_value = values['held_value']
        row.total_asset = values['total_asset']
        row.total_invest = values['total_invest']
        row.total_cashout = values['total_cashout']
        row.pnl = values['pnl']
        row.source = source
        row.notes = notes
    else:
        db.add(AssetSnapshot(
            snapshot_month=snapshot_month,
            snapshot_time=datetime.now(),
            rate=values['rate'],
            coin_hold=values['coin_hold'],
            coin_value=values['coin_value'],
            held_value=values['held_value'],
            total_asset=values['total_asset'],
            total_invest=values['total_invest'],
            total_cashout=values['total_cashout'],
            pnl=values['pnl'],
            source=source,
            notes=notes,
        ))


def _latest_rate(db: Session) -> float:
    """返回最新 10E+ 币价，用于全局计算"""
    row = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(desc(PriceHistory.timestamp)).first()
    return (row.price_10e or row.price) if row else 0


@app.get("/api/category-items")
def list_category_items(
    category: Optional[str] = None,
    holding_status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    rate = _latest_rate(db)
    q = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).order_by(desc(CategoryItem.timestamp))
    if category:
        q = q.filter(CategoryItem.category == category)
    if holding_status:
        q = q.filter(CategoryItem.holding_status == holding_status)
    if start_date:
        q = q.filter(CategoryItem.timestamp >= start_date)
    if end_date:
        q = q.filter(CategoryItem.timestamp <= end_date + " 23:59:59")
    rows = q.limit(limit).all()
    return [_cat_dict(r, rate, db) for r in rows]


def _auto_mark_sold(db: Session, category: str, item_name: str, sell_qty: int):
    """卖出物品时，自动将同名买入记录标记为「已出手」(FIFO先进先出，支持拆分)"""
    buys = db.query(CategoryItem).filter(
        CategoryItem.category == category,
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).order_by(CategoryItem.timestamp).all()
    remaining = sell_qty
    now = datetime.now()
    for b in buys:
        if remaining <= 0:
            break
        bqty = getattr(b, 'quantity', 1) or 1
        if bqty <= remaining:
            # 整条记录全部卖出
            b.holding_status = "已出手"
            b.status_changed_at = now
            remaining -= bqty
        else:
            # 部分卖出：拆分记录
            # 新建一条已出手的记录（卖出数量）
            sold_part = CategoryItem(
                timestamp=b.timestamp,
                category=b.category,
                type=b.type,
                item_name=b.item_name,
                item_group=getattr(b, 'item_group', '') or '',
                quantity=remaining,
                cost_mode=getattr(b, 'cost_mode', 'coin') or 'coin',
                coin_price=b.coin_price,
                cash_price=getattr(b, 'cash_price', 0) or 0,
                estimated_value=b.estimated_value,
                estimated_rmb=getattr(b, 'estimated_rmb', 0) or 0,
                purchase_rate=b.purchase_rate,
                sell_rate=getattr(b, 'sell_rate', 0),
                holding_status="已出手",
                status_changed_at=now,
                source_type=getattr(b, 'source_type', '手动录入') or '手动录入',
                source_ref=getattr(b, 'source_ref', None),
                valuation_mode=getattr(b, 'valuation_mode', '') or '',
                channel=b.channel,
                notes=(b.notes or '') + f' [拆分自#{b.id}，卖出{remaining}]',
            )
            db.add(sold_part)
            # 原记录保留剩余数量
            b.quantity = bqty - remaining
            remaining = 0


def _sync_item_sell_status(db: Session, category: str, item_name: str):
    if not category or not item_name:
        return
    sell_rows = db.query(CategoryItem).filter(
        CategoryItem.category == category,
        CategoryItem.item_name == item_name,
        CategoryItem.type == "卖出",
        CategoryItem.deleted_at == None,
    ).all()
    target_sold_qty = sum((getattr(r, 'quantity', 1) or 1) for r in sell_rows)
    buy_rows = db.query(CategoryItem).filter(
        CategoryItem.category == category,
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.deleted_at == None,
        CategoryItem.holding_status != "已消耗",
    ).order_by(CategoryItem.timestamp, CategoryItem.id).all()
    current_sold_qty = sum((getattr(r, 'quantity', 1) or 1) for r in buy_rows if getattr(r, 'holding_status', '持有中') == '已出手')
    now = datetime.now()
    if current_sold_qty < target_sold_qty:
        remaining = target_sold_qty - current_sold_qty
        for row in buy_rows:
            if remaining <= 0:
                break
            if getattr(row, 'holding_status', '持有中') == '已出手':
                continue
            qty = getattr(row, 'quantity', 1) or 1
            if qty <= remaining:
                row.holding_status = "已出手"
                row.status_changed_at = now
                remaining -= qty
            else:
                sold_part = CategoryItem(
                    timestamp=row.timestamp,
                    category=row.category,
                    type=row.type,
                    item_name=row.item_name,
                    item_group=getattr(row, 'item_group', '') or '',
                    quantity=remaining,
                    cost_mode=getattr(row, 'cost_mode', 'coin') or 'coin',
                    coin_price=row.coin_price,
                    cash_price=getattr(row, 'cash_price', 0) or 0,
                    estimated_value=row.estimated_value,
                    estimated_rmb=getattr(row, 'estimated_rmb', 0) or 0,
                    purchase_rate=row.purchase_rate,
                    sell_rate=getattr(row, 'sell_rate', 0),
                    holding_status="已出手",
                    status_changed_at=now,
                    source_type=getattr(row, 'source_type', '手动录入') or '手动录入',
                    source_ref=getattr(row, 'source_ref', None),
                    valuation_mode=getattr(row, 'valuation_mode', '') or '',
                    channel=row.channel,
                    notes=(row.notes or '') + f' [同步售出自#{row.id}，数量{remaining}]',
                )
                db.add(sold_part)
                row.quantity = qty - remaining
                row.status_changed_at = None
                remaining = 0
    elif current_sold_qty > target_sold_qty:
        restore_qty = current_sold_qty - target_sold_qty
        sold_rows = [r for r in buy_rows if getattr(r, 'holding_status', '持有中') == '已出手']
        sold_rows.sort(key=lambda r: ((getattr(r, 'status_changed_at', None) or getattr(r, 'timestamp', None) or datetime.min), getattr(r, 'id', 0)), reverse=True)
        for row in sold_rows:
            if restore_qty <= 0:
                break
            qty = getattr(row, 'quantity', 1) or 1
            if qty <= restore_qty:
                row.holding_status = "持有中"
                row.status_changed_at = None
                restore_qty -= qty
            else:
                held_part = CategoryItem(
                    timestamp=row.timestamp,
                    category=row.category,
                    type=row.type,
                    item_name=row.item_name,
                    item_group=getattr(row, 'item_group', '') or '',
                    quantity=restore_qty,
                    cost_mode=getattr(row, 'cost_mode', 'coin') or 'coin',
                    coin_price=row.coin_price,
                    cash_price=getattr(row, 'cash_price', 0) or 0,
                    estimated_value=row.estimated_value,
                    estimated_rmb=getattr(row, 'estimated_rmb', 0) or 0,
                    purchase_rate=row.purchase_rate,
                    sell_rate=getattr(row, 'sell_rate', 0),
                    holding_status="持有中",
                    status_changed_at=None,
                    source_type=getattr(row, 'source_type', '手动录入') or '手动录入',
                    source_ref=getattr(row, 'source_ref', None),
                    valuation_mode=getattr(row, 'valuation_mode', '') or '',
                    channel=row.channel,
                    notes=(row.notes or '') + f' [恢复持有自#{row.id}，数量{restore_qty}]',
                )
                db.add(held_part)
                row.quantity = qty - restore_qty
                restore_qty = 0


@app.post("/api/category-items")
def create_category_item(item: CatItemCreate, db: Session = Depends(get_db)):
    valid_cats = _get_categories(db)
    if valid_cats and item.category not in valid_cats:
        raise HTTPException(400, f"无效分类: {item.category}")
    if item.type not in ("买入", "卖出"):
        raise HTTPException(400, f"无效类型: {item.type}，只能是买入或卖出")
    if item.cost_mode not in ("coin", "rmb_direct"):
        raise HTTPException(400, f"无效成本模式: {item.cost_mode}")
    if (item.quantity or 0) < 1:
        raise HTTPException(400, "数量必须大于 0")
    data = item.model_dump()
    data['item_group'] = str(data.get('item_group') or '').strip()[:50]
    # 卖出时自动填充当前币价作为 sell_rate
    if item.type == "卖出" and (not item.sell_rate or item.sell_rate == 0):
        data['sell_rate'] = _latest_rate(db)
    row = CategoryItem(**data)
    db.add(row)
    db.flush()
    _audit(db, 'category_items', row.id, 'create', data)
    if item.type == "卖出":
        _sync_item_sell_status(db, item.category, item.item_name)
    db.commit(); db.refresh(row)
    return _cat_dict(row, _latest_rate(db), db)


@app.put("/api/category-items/{cid}")
def update_category_item(cid: int, item: CatItemUpdate, db: Session = Depends(get_db)):
    valid_cats = _get_categories(db)
    if valid_cats and item.category not in valid_cats:
        raise HTTPException(400, f"无效分类: {item.category}")
    if item.type not in ("买入", "卖出"):
        raise HTTPException(400, f"无效类型: {item.type}，只能是买入或卖出")
    if item.cost_mode not in ("coin", "rmb_direct"):
        raise HTTPException(400, f"无效成本模式: {item.cost_mode}")
    if (item.quantity or 0) < 1:
        raise HTTPException(400, "数量必须大于 0")
    row = _get_active_row(db, CategoryItem, cid, "记录不存在")
    # 禁止修改买入/卖出类型，防止币持仓计算错乱（应新建卖出记录而非修改原买入记录）
    if row.type != item.type:
        raise HTTPException(
            400,
            f"不允许将「{row.type}」记录改为「{item.type}」。"
            f"如需记录售出，请保持原买入记录不变（状态改为已出手），"
            f"另新建一条「卖出」记录填写售出价格。"
        )
    data = item.model_dump()
    data['item_group'] = str(data.get('item_group') or '').strip()[:50]
    changes = _diff_row(row, data)
    if changes:
        _audit(db, 'category_items', cid, 'edit', changes)
    was_sell = row.type == "卖出"
    old_category = row.category
    old_item_name = row.item_name
    old_hs = getattr(row, 'holding_status', '持有中')
    old_ev = row.estimated_value or 0
    for k, v in data.items():
        setattr(row, k, v)
    # 持有状态变更时记录时间戳
    new_hs = getattr(row, 'holding_status', '持有中')
    if new_hs != old_hs:
        row.status_changed_at = datetime.now()
    # 卖出时自动填充 sell_rate
    if row.type == "卖出" and (not getattr(row, 'sell_rate', 0)):
        row.sell_rate = _latest_rate(db)
    new_ev = row.estimated_value or 0
    if abs(new_ev - old_ev) > 1e-6:
        db.add(EstimatedValueHistory(item_id=cid, old_value=old_ev, new_value=new_ev, timestamp=datetime.now()))
    touched_groups = set()
    if was_sell:
        touched_groups.add((old_category, old_item_name))
    if row.type == "卖出":
        touched_groups.add((row.category, row.item_name))
    for cat_name, item_name in touched_groups:
        _sync_item_sell_status(db, cat_name, item_name)
    db.commit(); db.refresh(row)
    return _cat_dict(row, _latest_rate(db), db)


@app.delete("/api/category-items/{cid}")
def delete_category_item(cid: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, CategoryItem, cid, "记录不存在")
    _audit(db, 'category_items', cid, 'delete', {"item_name": row.item_name, "category": row.category, "type": row.type})
    sync_group = (row.category, row.item_name) if row.type in ("卖出", "买入") else None
    row.deleted_at = datetime.now()
    if sync_group:
        _sync_item_sell_status(db, sync_group[0], sync_group[1])
    db.commit()
    return {"ok": True}


# ── 物品估值变动历史 ──────────────────────────────────────────────
@app.get("/api/category-items/{cid}/value-history")
def get_value_history(cid: int, db: Session = Depends(get_db)):
    rows = db.query(EstimatedValueHistory).filter(EstimatedValueHistory.item_id == cid).order_by(EstimatedValueHistory.timestamp).all()
    return [{"id": r.id, "old_value": r.old_value, "new_value": r.new_value, "timestamp": r.timestamp.isoformat() if r.timestamp else None} for r in rows]


# ── 分类物品截图 ──────────────────────────────────────────────────
@app.get("/api/category-items/{cid}/images")
def list_cat_item_images(cid: int, db: Session = Depends(get_db)):
    imgs = db.query(CategoryItemImage).filter(CategoryItemImage.item_id == cid).all()
    return [{"id": img.id, "data": img.image_data} for img in imgs]


@app.post("/api/category-items/{cid}/images")
def upload_cat_item_image(cid: int, payload: ImageUpload, db: Session = Depends(get_db)):
    row = _get_active_row(db, CategoryItem, cid, "物品不存在")
    img = CategoryItemImage(item_id=row.id, image_data=payload.image_data)
    db.add(img); db.commit(); db.refresh(img)
    return {"id": img.id}


@app.delete("/api/category-item-images/{img_id}")
def delete_cat_item_image(img_id: int, db: Session = Depends(get_db)):
    img = db.query(CategoryItemImage).filter(CategoryItemImage.id == img_id).first()
    if not img:
        raise HTTPException(404, "图片不存在")
    db.delete(img); db.commit()
    return {"ok": True}


@app.post("/api/category-items/group/consume")
def consume_category_item_group(payload: CatItemGroupConsumeCreate, db: Session = Depends(get_db)):
    if not (payload.category or '').strip():
        raise HTTPException(400, "分类不能为空")
    if not (payload.item_name or '').strip():
        raise HTTPException(400, "物品名称不能为空")
    if (payload.quantity or 0) < 1:
        raise HTTPException(400, "数量必须大于 0")
    changed_at = payload.timestamp or datetime.now()
    result = _consume_item_group(db, payload.category, payload.item_name, payload.quantity, changed_at, payload.notes)
    db.commit()
    return {
        "ok": True,
        "consumed": result["consumed"],
        "affected": result["affected_rows"],
        "remaining": result["remaining_available"],
    }


@app.post("/api/category-items/{cid}/consume")
def consume_category_item(cid: int, payload: CatItemConsumeCreate, db: Session = Depends(get_db)):
    row = _get_active_row(db, CategoryItem, cid, "记录不存在")
    if (payload.quantity or 0) < 1:
        raise HTTPException(400, "数量必须大于 0")
    changed_at = payload.timestamp or datetime.now()
    result = _consume_item_row(db, row, payload.quantity, changed_at, payload.notes)
    db.commit()
    return {
        "ok": True,
        "consumed": result["consumed"],
        "remaining": result["remaining"],
        "created_id": result["created_id"],
    }


# ── 登记卖出 ─────────────────────────────────────────────────────
@app.post("/api/category-items/{cid}/sell")
def sell_category_item(cid: int, payload: CatItemSellCreate, db: Session = Depends(get_db)):
    """从一条买入记录登记卖出：自动创建卖出记录并标记原记录已出手"""
    row = _get_active_row(db, CategoryItem, cid, "记录不存在")
    if row.type != "买入":
        raise HTTPException(400, "只有买入记录可以登记卖出")
    hs = getattr(row, 'holding_status', '持有中') or '持有中'
    if hs != '持有中':
        raise HTTPException(400, f"只有持有中的记录可以卖出，当前状态: {hs}")
    current_qty = getattr(row, 'quantity', 1) or 1
    sell_qty = payload.quantity or 1
    if sell_qty < 1:
        raise HTTPException(400, "数量必须大于 0")
    if sell_qty > current_qty:
        raise HTTPException(400, f"可卖出数量不足，当前仅剩 {current_qty}")
    if (payload.coin_price or 0) <= 0:
        raise HTTPException(400, "请填写卖出单价")
    now = payload.timestamp or datetime.now()
    sell_rate = payload.sell_rate or _latest_rate(db)
    sell_row = CategoryItem(
        timestamp=now,
        category=row.category,
        type="卖出",
        item_name=row.item_name,
        item_group=getattr(row, 'item_group', '') or '',
        quantity=sell_qty,
        cost_mode=getattr(row, 'cost_mode', 'coin') or 'coin',
        coin_price=payload.coin_price,
        cash_price=0,
        purchase_rate=row.purchase_rate or 0,
        estimated_value=0,
        estimated_rmb=0,
        holding_status="已出手",
        sell_rate=sell_rate,
        status_changed_at=now,
        source_type=getattr(row, 'source_type', '手动录入') or '手动录入',
        valuation_mode=getattr(row, 'valuation_mode', '') or '',
        channel=row.channel,
        notes=_merge_item_notes(None, payload.notes),
    )
    db.add(sell_row)
    db.flush()
    _audit(db, 'category_items', sell_row.id, 'sell_create', {
        "source_buy_id": cid,
        "quantity": sell_qty,
        "coin_price": payload.coin_price,
        "sell_rate": sell_rate,
    })
    _sync_item_sell_status(db, row.category, row.item_name)
    db.commit()
    db.refresh(sell_row)
    return {
        "ok": True,
        "sold": sell_qty,
        "sell_record_id": sell_row.id,
        "coin_price": payload.coin_price,
    }


@app.post("/api/category-items/group/sell")
def sell_category_item_group(payload: CatItemGroupSellCreate, db: Session = Depends(get_db)):
    """从物品组登记卖出（汇总行操作）"""
    if not (payload.category or '').strip():
        raise HTTPException(400, "分类不能为空")
    if not (payload.item_name or '').strip():
        raise HTTPException(400, "物品名称不能为空")
    sell_qty = payload.quantity or 1
    if sell_qty < 1:
        raise HTTPException(400, "数量必须大于 0")
    if (payload.coin_price or 0) <= 0:
        raise HTTPException(400, "请填写卖出单价")
    held_rows = db.query(CategoryItem).filter(
        CategoryItem.category == payload.category,
        CategoryItem.item_name == payload.item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).all()
    available = sum((getattr(r, 'quantity', 1) or 1) for r in held_rows)
    if available < sell_qty:
        raise HTTPException(400, f"{payload.item_name} 可卖出数量不足，当前仅剩 {available}")
    now = payload.timestamp or datetime.now()
    sell_rate = payload.sell_rate or _latest_rate(db)
    first_row = held_rows[0] if held_rows else None
    sell_row = CategoryItem(
        timestamp=now,
        category=payload.category,
        type="卖出",
        item_name=payload.item_name,
        item_group=getattr(first_row, 'item_group', '') or '' if first_row else '',
        quantity=sell_qty,
        cost_mode=getattr(first_row, 'cost_mode', 'coin') or 'coin' if first_row else 'coin',
        coin_price=payload.coin_price,
        cash_price=0,
        purchase_rate=(first_row.purchase_rate or 0) if first_row else 0,
        estimated_value=0,
        estimated_rmb=0,
        holding_status="已出手",
        sell_rate=sell_rate,
        status_changed_at=now,
        source_type='手动录入',
        valuation_mode=getattr(first_row, 'valuation_mode', '') or '' if first_row else '',
        channel=first_row.channel if first_row else None,
        notes=_merge_item_notes(None, payload.notes),
    )
    db.add(sell_row)
    db.flush()
    _audit(db, 'category_items', sell_row.id, 'sell_create', {
        "quantity": sell_qty,
        "coin_price": payload.coin_price,
        "sell_rate": sell_rate,
    })
    _sync_item_sell_status(db, payload.category, payload.item_name)
    db.commit()
    return {
        "ok": True,
        "sold": sell_qty,
        "sell_record_id": sell_row.id,
        "coin_price": payload.coin_price,
        "remaining": available - sell_qty,
    }


# ── 分类汇总 ──────────────────────────────────────────────────────
@app.get("/api/category-overview")
def category_overview(db: Session = Depends(get_db)):
    """所有分类的总览（只读）"""
    rate = _latest_rate(db)
    items = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).order_by(desc(CategoryItem.timestamp)).all()
    buy_rmb = sum(_item_purchase_rmb(i) for i in items if i.type == "买入")
    sell_rmb = sum(_item_purchase_rmb(i) for i in items if i.type == "卖出")
    # 持有中物品的三国币总量 和 RMB市值
    held_coins = sum(
        ((i.estimated_value if (i.estimated_value or 0) > 0 else i.coin_price) * (getattr(i, 'quantity', 1) or 1))
        for i in items
        if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中' and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'coin'
    )
    held_value = round(sum(_item_current_rmb(i, rate) for i in items if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中'), 2)
    return {
        "category": "总览",
        "total_items": len(items),
        "buy_rmb": round(buy_rmb, 2),
        "sell_rmb": round(sell_rmb, 2),
        "net_cost": round(buy_rmb - sell_rmb, 2),
        "held_coins": round(held_coins, 2),
        "held_value": round(held_value, 2),
        "items": [_cat_dict(i, rate, db) for i in items],
    }


@app.get("/api/categories/{name}")
def category_detail(name: str, db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    items = db.query(CategoryItem).filter(CategoryItem.category == name, CategoryItem.deleted_at == None).order_by(desc(CategoryItem.timestamp)).all()
    buy_rmb = sum(_item_purchase_rmb(i) for i in items if i.type == "买入")
    sell_rmb = sum(_item_purchase_rmb(i) for i in items if i.type == "卖出")
    # 仅统计「持有中」的买入物品作为当前持有价值
    held_coins = sum(
        ((i.estimated_value if (i.estimated_value or 0) > 0 else i.coin_price) * (getattr(i, 'quantity', 1) or 1))
        for i in items
        if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中' and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'coin'
    )
    held_value = sum(_item_current_rmb(i, rate) for i in items if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中')
    return {
        "category": name,
        "total_items": len(items),
        "buy_rmb": round(buy_rmb, 2),
        "sell_rmb": round(sell_rmb, 2),
        "net_cost": round(buy_rmb - sell_rmb, 2),
        "held_coins": round(held_coins, 2),
        "held_value": round(held_value, 2),
        "items": [_cat_dict(i, rate, db) for i in items],
    }


# ── 币价历史 CRUD ──────────────────────────────────────────────────
def _price_dict(r, db: Session = None):
    image = None
    if db:
        img = db.query(PriceImage).filter(PriceImage.price_id == r.id).order_by(desc(PriceImage.id)).first()
        if img:
            image = {"id": img.id, "data": img.image_data}
    return {"id": r.id, "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "price": r.price, "price_10e": r.price_10e or 0, "price_recycle": r.price_recycle or 0,
            "source": r.source, "notes": r.notes, "image": image}

@app.get("/api/prices")
def list_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(desc(PriceHistory.timestamp))
    if start_date:
        q = q.filter(PriceHistory.timestamp >= start_date)
    if end_date:
        q = q.filter(PriceHistory.timestamp <= end_date + " 23:59:59")
    rows = q.limit(limit).all()
    return [_price_dict(r, db) for r in rows]


@app.get("/api/prices/latest")
def latest_price(db: Session = Depends(get_db)):
    row = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(desc(PriceHistory.timestamp)).first()
    if not row:
        return {"price": 0, "price_10e": 0, "price_recycle": 0}
    return {"price": row.price, "price_10e": row.price_10e or 0, "price_recycle": row.price_recycle or 0}


@app.get("/api/prices/stats")
def price_stats(db: Session = Depends(get_db)):
    """返回7日/30日均价、日涨跌幅、周涨跌幅"""
    now = datetime.now()
    all_rows = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(desc(PriceHistory.timestamp)).limit(500).all()
    if not all_rows:
        return {"avg_7d": 0, "avg_30d": 0, "daily_change": 0, "daily_change_pct": 0,
                "weekly_change": 0, "weekly_change_pct": 0, "latest": 0}
    latest_val = all_rows[0].price_10e or all_rows[0].price
    d7 = now - timedelta(days=7)
    d30 = now - timedelta(days=30)
    d1 = now - timedelta(days=1)
    vals_7d = [(r.price_10e or r.price) for r in all_rows if r.timestamp and r.timestamp >= d7]
    vals_30d = [(r.price_10e or r.price) for r in all_rows if r.timestamp and r.timestamp >= d30]
    avg_7d = round(sum(vals_7d) / len(vals_7d), 1) if vals_7d else 0
    avg_30d = round(sum(vals_30d) / len(vals_30d), 1) if vals_30d else 0
    prev_day = [(r.price_10e or r.price) for r in all_rows if r.timestamp and r.timestamp < d1]
    prev_week = [(r.price_10e or r.price) for r in all_rows if r.timestamp and r.timestamp < d7]
    prev_day_val = prev_day[0] if prev_day else latest_val
    prev_week_val = prev_week[0] if prev_week else latest_val
    daily_change = round(latest_val - prev_day_val, 1)
    daily_pct = round(daily_change / prev_day_val * 100, 2) if prev_day_val else 0
    weekly_change = round(latest_val - prev_week_val, 1)
    weekly_pct = round(weekly_change / prev_week_val * 100, 2) if prev_week_val else 0
    return {
        "latest": latest_val, "avg_7d": avg_7d, "avg_30d": avg_30d,
        "daily_change": daily_change, "daily_change_pct": daily_pct,
        "weekly_change": weekly_change, "weekly_change_pct": weekly_pct,
    }


@app.post("/api/prices")
def create_price(p: PriceCreate, db: Session = Depends(get_db)):
    row = PriceHistory(**p.model_dump())
    db.add(row); db.flush()
    _audit(db, 'price_history', row.id, 'create', p.model_dump())
    db.commit(); db.refresh(row)
    return _price_dict(row, db)


@app.put("/api/prices/{pid}")
def update_price(pid: int, p: PriceUpdate, db: Session = Depends(get_db)):
    row = _get_active_row(db, PriceHistory, pid, "记录不存在")
    changes = _diff_row(row, p.model_dump())
    if changes:
        _audit(db, 'price_history', pid, 'edit', changes)
    for k, v in p.model_dump().items():
        setattr(row, k, v)
    db.commit(); db.refresh(row)
    return _price_dict(row, db)


@app.post("/api/prices/{pid}/image")
def upsert_price_image(pid: int, payload: ImageUpload, db: Session = Depends(get_db)):
    row = _get_active_row(db, PriceHistory, pid, "币价记录不存在")
    img = db.query(PriceImage).filter(PriceImage.price_id == pid).first()
    if img:
        img.image_data = payload.image_data
    else:
        img = PriceImage(price_id=row.id, image_data=payload.image_data)
        db.add(img)
    db.commit(); db.refresh(img)
    return {"id": img.id}


@app.delete("/api/prices/{pid}/image")
def delete_price_image(pid: int, db: Session = Depends(get_db)):
    img = db.query(PriceImage).filter(PriceImage.price_id == pid).first()
    if not img:
        return {"ok": True}
    db.delete(img); db.commit()
    return {"ok": True}


@app.delete("/api/prices/{pid}")
def delete_price(pid: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, PriceHistory, pid, "记录不存在")
    _audit(db, 'price_history', pid, 'delete', {"price": row.price, "price_10e": row.price_10e})
    row.deleted_at = datetime.now()
    db.commit()
    return {"ok": True}


@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db)):
    latest_price = _latest_rate(db)
    _sync_daily_active_entries(db)
    daily_active_entries = _get_daily_active_entries(db)

    # ── 读取初始设置 ──
    def _setting_float(key: str, default: float = 0) -> float:
        row = db.query(Setting).filter(Setting.key == key).first()
        return float(row.value) if row and row.value else default

    initial_coin = _setting_float("initial_coin_balance")
    initial_invest = _setting_float("initial_investment")
    annual_target = _setting_float("annual_target", 10000)

    # ── 交易流水汇总（排除作废） ──
    all_tx = db.query(Transaction).filter(Transaction.deleted_at == None).order_by(Transaction.timestamp).all()
    tx_coin_in = 0.0
    tx_coin_out = 0.0
    total_buy_coin_rmb = 0.0
    total_sell_coin_rmb = 0.0
    total_buy_point_rmb = 0.0
    total_sell_point_rmb = 0.0
    point_in = 0.0              # 三国点充入量
    point_out = 0.0             # 三国点卖出量
    total_custom_expense = 0.0
    total_custom_income = 0.0
    year = datetime.now().year
    month = datetime.now().month
    year_profit = 0.0       # 年度盈亏 = 倒货/其他净收益（与月度汇总一致）
    month_profit = 0.0      # 月度盈亏
    year_recharge = 0.0     # 年度总投入(买币+充点)
    year_cashout = 0.0      # 年度总回收(卖币+卖点)
    year_custom_expense = 0.0   # 年度其他支出
    year_custom_income = 0.0    # 年度其他收入
    year_trade_income = 0.0     # 年度倒货收入
    year_trade_expense = 0.0    # 年度倒货支出
    year_dungeon_rmb = 0.0      # 年度副本收益(折RMB)

    for t in all_tx:
        if getattr(t, 'status', 'normal') == 'void':
            continue
        rmb = _tx_rmb(t)
        direction = getattr(t, 'direction', None) or 'expense'
        is_this_year = t.timestamp and t.timestamp.year == year
        is_this_month = is_this_year and t.timestamp.month == month
        if t.type == "买币":
            tx_coin_in += t.quantity
            total_buy_coin_rmb += rmb
            if is_this_year: year_recharge += rmb
        elif t.type == "卖币":
            tx_coin_out += t.quantity
            total_sell_coin_rmb += rmb
            if is_this_year: year_cashout += rmb
        elif t.type == "三国点充值":
            total_buy_point_rmb += rmb
            point_in += t.quantity
            if is_this_year: year_recharge += rmb
        elif t.type == "三国点售卖":
            total_sell_point_rmb += rmb
            point_out += t.quantity
            if is_this_year: year_cashout += rmb
        else:
            if direction == 'income':
                total_custom_income += rmb
                if is_this_year: year_custom_income += rmb
            else:
                total_custom_expense += rmb
                if is_this_year: year_custom_expense += rmb
            # 倒货/其他交易计入年度盈亏（与月度汇总一致）
            trade_flow = rmb if direction == 'income' else -rmb
            if is_this_year:
                year_profit += trade_flow
                if direction == 'income':
                    year_trade_income += rmb
                else:
                    year_trade_expense += rmb
            if is_this_month: month_profit += trade_flow

    # ── 分类物品汇总（动态分类） ──
    categories = _get_categories(db)
    all_items = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).all()
    items_coin_spent = 0.0    # 买入物品花掉的币(亿) — 用于币持仓计算
    items_coin_received = 0.0 # 卖出物品收到的币(亿) — 用于币持仓计算
    direct_item_buy_rmb = 0.0
    direct_item_sell_rmb = 0.0
    cat_stats = []
    asset_composition = []
    held_items_value = 0.0    # 仅「持有中」物品的当前市值(元)
    for cat in categories:
        cat_items = [i for i in all_items if i.category == cat]
        if not cat_items:
            continue
        qty_fn = lambda i: getattr(i, 'quantity', 1) or 1
        buy_coins = sum(_item_coin_cost(i) for i in cat_items if i.type == "买入")
        sell_coins = sum(_item_coin_cost(i) for i in cat_items if i.type == "卖出")
        buy_rmb = sum(_item_purchase_rmb(i) for i in cat_items if i.type == "买入")
        sell_rmb = sum(_item_purchase_rmb(i) for i in cat_items if i.type == "卖出")
        # 只统计「持有中」的买入物品作为当前持有价值
        held_cur = sum(
            _item_current_rmb(i, latest_price)
            for i in cat_items
            if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中'
        )
        items_coin_spent += buy_coins
        items_coin_received += sell_coins
        direct_item_buy_rmb += sum(_item_purchase_rmb(i) for i in cat_items if i.type == "买入" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct')
        direct_item_sell_rmb += sum(_item_purchase_rmb(i) for i in cat_items if i.type == "卖出" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct')
        held_items_value += held_cur
        cat_stats.append({
            "category": cat,
            "buy_rmb": round(buy_rmb, 2),
            "sell_rmb": round(sell_rmb, 2),
            "net_cost": round(buy_rmb - sell_rmb, 2),
            "held_value": round(held_cur, 2),
        })
        if held_cur > 0:
            asset_composition.append({"name": cat, "value": round(held_cur, 2)})
    # 未分类的物品
    uncategorized = [i for i in all_items if i.category not in categories]
    if uncategorized:
        qty_fn = lambda i: getattr(i, 'quantity', 1) or 1
        uc_held = sum(
            _item_current_rmb(i, latest_price)
            for i in uncategorized
            if i.type == "买入" and getattr(i, 'holding_status', '持有中') == '持有中'
        )
        items_coin_spent += sum(_item_coin_cost(i) for i in uncategorized if i.type == "买入")
        items_coin_received += sum(_item_coin_cost(i) for i in uncategorized if i.type == "卖出")
        direct_item_buy_rmb += sum(_item_purchase_rmb(i) for i in uncategorized if i.type == "买入" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct')
        direct_item_sell_rmb += sum(_item_purchase_rmb(i) for i in uncategorized if i.type == "卖出" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct')
        held_items_value += uc_held

    # 年度直接现金买卖物品
    year_item_buy_rmb = sum(
        _item_purchase_rmb(i)
        for i in all_items
        if i.type == "买入" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct'
        and i.timestamp and i.timestamp.year == year
    )
    year_item_sell_rmb = sum(
        _item_purchase_rmb(i)
        for i in all_items
        if i.type == "卖出" and (getattr(i, 'cost_mode', 'coin') or 'coin') == 'rmb_direct'
        and i.timestamp and i.timestamp.year == year
    )

    # 已消耗物品成本（买入成本，用于展示消耗了多少价值）
    consumed_cost_rmb = sum(
        _item_purchase_rmb(i)
        for i in all_items
        if i.type == "买入" and getattr(i, 'holding_status', '') == '已消耗'
    )
    consumed_year_cost = 0.0
    consumed_month_cost = 0.0
    for i in all_items:
        if i.type != "买入" or getattr(i, 'holding_status', '') != '已消耗':
            continue
        ref_time = getattr(i, 'status_changed_at', None) or i.timestamp
        if not ref_time:
            continue
        item_cost = _item_purchase_rmb(i)
        if ref_time.year == year:
            consumed_year_cost += item_cost
            if ref_time.month == month:
                consumed_month_cost += item_cost

    # ── 核心计算 ──
    # 副本收益统计
    all_dungeons = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).all()
    dungeon_total_coin = sum(d.revenue_coin or 0 for d in all_dungeons)
    dungeon_runs = len(all_dungeons)
    coin_from_daily_active = round(sum((row.get("amount", 0) or 0) for row in daily_active_entries), 4)
    daily_active_days = len({row.get("date") for row in daily_active_entries if row.get("date")})
    # 币持仓 = 初始背包币 + 交易净变化 + 物品买卖净变化 + 副本收益 + 校准调整 - 零散支出
    coin_from_initial = initial_coin
    coin_from_tx = tx_coin_in - tx_coin_out
    coin_from_items = -items_coin_spent + items_coin_received
    coin_from_dungeons = dungeon_total_coin
    # 校准调整总额（每次校准记录的差值之和）
    calibrations = _get_json_setting(db, "coin_calibrations")
    coin_calibration = sum(r.get("diff", 0) for r in calibrations)
    # 零散支出总额
    misc_exps = _get_json_setting(db, "misc_coin_expenses")
    coin_misc_expense = sum(r.get("amount", 0) for r in misc_exps)
    coin_balance = coin_from_initial + coin_from_tx + coin_from_items + coin_from_dungeons + coin_from_daily_active + coin_calibration - coin_misc_expense
    coin_market_value = coin_balance * latest_price
    # 总资产 = 币持仓市值 + 持有中物品市值
    total_assets = coin_market_value + held_items_value
    tracked_investment = total_buy_coin_rmb + total_buy_point_rmb + total_custom_expense + direct_item_buy_rmb
    baseline_investment = initial_invest
    total_investment = baseline_investment + tracked_investment
    total_realized = total_sell_coin_rmb + total_sell_point_rmb + total_custom_income + direct_item_sell_rmb
    net_cash_occupied = total_investment - total_realized
    cash_recovery_rate = round(total_realized / total_investment * 100, 1) if total_investment > 0 else 0
    # 综合盈亏 = 总资产 + 已变现 - 总投入
    comprehensive_pnl = total_assets + total_realized - total_investment
    profit_rate = round(comprehensive_pnl / total_investment * 100, 1) if total_investment > 0 else 0
    tracked_pnl = total_assets + total_realized - tracked_investment
    tracked_profit_rate = round(tracked_pnl / tracked_investment * 100, 1) if tracked_investment > 0 else 0
    investment_duplicate_risk = baseline_investment > 0 and tracked_investment > 0

    # 资产构成加入币持仓
    if coin_market_value > 0:
        asset_composition.insert(0, {"name": "币持仓", "value": round(coin_market_value, 2)})

    # 副本收益年度/月度拆分（使用当月币价折算，与月度汇总一致）
    _monthly_rate = _build_monthly_rate_map(db)
    _daily_active_roll = _daily_active_rollup(daily_active_entries, _monthly_rate, latest_price)
    year_activity_rmb = sum(v for k, v in _daily_active_roll["by_month"].items() if k.startswith(f"{year}-"))
    month_activity_rmb = _daily_active_roll["by_month"].get(f"{year}-{month:02d}", 0.0)
    dungeon_total_rmb_acc = 0.0   # 按逐月币价折算的副本总RMB
    for dg in all_dungeons:
        if not dg.timestamp:
            dungeon_total_rmb_acc += (dg.revenue_coin or 0) * latest_price
            continue
        dg_month_key = dg.timestamp.strftime("%Y-%m")
        dg_rate = _resolve_month_rate(dg_month_key, _monthly_rate, latest_price)
        dg_rmb = (dg.revenue_coin or 0) * dg_rate
        dungeon_total_rmb_acc += dg_rmb
        if dg.timestamp.year == year:
            year_profit += dg_rmb
            year_dungeon_rmb += dg_rmb
        if dg.timestamp.year == year and dg.timestamp.month == month:
            month_profit += dg_rmb
    year_profit += year_activity_rmb
    month_profit += month_activity_rmb
    year_profit -= consumed_year_cost
    month_profit -= consumed_month_cost

    # 首次使用引导
    show_guide = initial_coin == 0 and initial_invest == 0 and db.query(Transaction).count() == 0

    recent = db.query(Transaction).filter(Transaction.deleted_at == None, Transaction.status != 'void').order_by(desc(Transaction.timestamp)).limit(10).all()

    _write_asset_snapshot(db, datetime.now().strftime("%Y-%m"), {
        "rate": round(latest_price, 2),
        "coin_hold": round(coin_balance, 4),
        "coin_value": round(coin_market_value, 2),
        "held_value": round(held_items_value, 2),
        "total_asset": round(total_assets, 2),
        "total_invest": round(total_investment, 2),
        "total_cashout": round(total_realized, 2),
        "pnl": round(comprehensive_pnl, 2),
    }, source='system', notes='仪表盘自动刷新当前月快照')
    db.commit()

    return {
        "latest_price": latest_price,
        "coin_balance": round(coin_balance, 4),
        "coin_from_initial": round(coin_from_initial, 4),
        "coin_from_tx": round(coin_from_tx, 4),
        "coin_from_items": round(coin_from_items, 4),
        "coin_from_dungeons": round(coin_from_dungeons, 4),
        "coin_from_daily_active": round(coin_from_daily_active, 4),
        "coin_calibration": round(coin_calibration, 4),
        "coin_misc_expense": round(coin_misc_expense, 4),
        "items_coin_spent": round(items_coin_spent, 4),
        "items_coin_received": round(items_coin_received, 4),
        "coin_market_value": round(coin_market_value, 2),
        "item_net_value": round(held_items_value, 2),
        "total_assets": round(total_assets, 2),
        "baseline_investment": round(baseline_investment, 2),
        "tracked_investment": round(tracked_investment, 2),
        "total_investment": round(total_investment, 2),
        "total_realized": round(total_realized, 2),
        "cash_recovered": round(total_realized, 2),
        "net_cash_occupied": round(net_cash_occupied, 2),
        "cash_recovery_rate": cash_recovery_rate,
        "tracked_pnl": round(tracked_pnl, 2),
        "comprehensive_pnl": round(comprehensive_pnl, 2),
        "profit_rate": profit_rate,
        "tracked_profit_rate": tracked_profit_rate,
        "investment_duplicate_risk": investment_duplicate_risk,
        "buy_coin_rmb": round(total_buy_coin_rmb, 2),
        "sell_coin_rmb": round(total_sell_coin_rmb, 2),
        "coin_net": round(total_sell_coin_rmb - total_buy_coin_rmb, 2),
        "buy_point_rmb": round(total_buy_point_rmb, 2),
        "sell_point_rmb": round(total_sell_point_rmb, 2),
        "point_net": round(total_sell_point_rmb - total_buy_point_rmb, 2),
        "initial_coin": round(initial_coin, 4),
        "initial_invest": initial_invest,
        "year_cashflow": round(year_profit, 2),
        "month_cashflow": round(month_profit, 2),
        "year_recharge": round(year_recharge, 2),
        "year_cashout": round(year_cashout, 2),
        "year_item_buy_rmb": round(year_item_buy_rmb, 2),
        "year_item_sell_rmb": round(year_item_sell_rmb, 2),
        "year_custom_expense": round(year_custom_expense, 2),
        "year_custom_income": round(year_custom_income, 2),
        "year_total_spend": round(year_recharge + year_item_buy_rmb + year_custom_expense, 2),
        "year_total_income": round(year_cashout + year_item_sell_rmb + year_custom_income, 2),
        "year_trade_income": round(year_trade_income, 2),
        "year_trade_expense": round(year_trade_expense, 2),
        "year_trade_pnl": round(year_trade_income - year_trade_expense, 2),
        "year_dungeon_rmb": round(year_dungeon_rmb, 2),
        "year_daily_active_rmb": round(year_activity_rmb, 2),
        "month_daily_active_rmb": round(month_activity_rmb, 2),
        "consumed_year_cost": round(consumed_year_cost, 2),
        "annual_target": annual_target,
        "target_progress": round(year_profit / annual_target * 100, 1) if annual_target else 0,
        "category_stats": cat_stats,
        "asset_composition": asset_composition,
        "custom_expense": round(total_custom_expense, 2),
        "custom_income": round(total_custom_income, 2),
        "direct_item_buy_rmb": round(direct_item_buy_rmb, 2),
        "direct_item_sell_rmb": round(direct_item_sell_rmb, 2),
        "point_balance": round(point_in - point_out, 2),
        "point_in": round(point_in, 2),
        "point_out": round(point_out, 2),
        "consumed_cost_rmb": round(consumed_cost_rmb, 2),
        "dungeon_total_coin": round(dungeon_total_coin, 4),
        "dungeon_total_rmb": round(dungeon_total_rmb_acc, 2),
        "daily_active_total_coin": round(_daily_active_roll["total_coin"], 4),
        "daily_active_total_rmb": round(_daily_active_roll["total_rmb"], 2),
        "daily_active_total_wan": round(_daily_active_roll["total_coin"] * 10000, 2),
        "daily_active_days": daily_active_days,
        "dungeon_runs": dungeon_runs,
        "show_guide": show_guide,
        "recent_transactions": [_tx_dict(r) for r in recent],
        "total_transactions": db.query(Transaction).filter(Transaction.deleted_at == None, Transaction.status != 'void').count(),
        "total_category_items": db.query(CategoryItem).filter(CategoryItem.deleted_at == None).count(),
    }


# ── 物品搜索 & 统计 (搜索分类物品) ────────────────────────────────
@app.get("/api/items/search")
def search_items(q: str = "", db: Session = Depends(get_db)):
    if not q:
        return []
    item_rows = (
        db.query(distinct(CategoryItem.item_name))
        .filter(CategoryItem.item_name.isnot(None), CategoryItem.deleted_at == None)
        .filter(CategoryItem.item_name.like(f"%{q}%"))
        .limit(20)
        .all()
    )
    anchor_rows = (
        db.query(distinct(ItemPriceAnchor.item_name))
        .filter(ItemPriceAnchor.item_name.isnot(None), ItemPriceAnchor.deleted_at == None)
        .filter(ItemPriceAnchor.item_name.like(f"%{q}%"))
        .limit(20)
        .all()
    )
    names = []
    seen = set()
    for row in item_rows + anchor_rows:
        val = row[0]
        if val and val not in seen:
            names.append(val)
            seen.add(val)
    return names[:20]


@app.get("/api/items/{name}/stats")
def item_stats(name: str, db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    items = db.query(CategoryItem).filter(CategoryItem.item_name.like(f"%{name}%"), CategoryItem.deleted_at == None).order_by(desc(CategoryItem.timestamp)).all()
    anchors = db.query(ItemPriceAnchor).filter(ItemPriceAnchor.item_name.like(f"%{name}%"), ItemPriceAnchor.deleted_at == None).order_by(desc(ItemPriceAnchor.timestamp)).all()
    buy_items = [i for i in items if i.type == "买入"]
    sell_items = [i for i in items if i.type == "卖出"]
    buy_cost = sum(_item_purchase_rmb(i) for i in buy_items)
    sell_income = sum(_item_purchase_rmb(i) for i in sell_items)
    # 仅统计「持有中」的买入物品作为当前持有价值
    held_value = sum(
        _item_current_rmb(i, rate)
        for i in buy_items
        if getattr(i, 'holding_status', '持有中') == '持有中'
    )
    anchor_values = [_anchor_price_yi(a.market_price, a.price_unit) for a in anchors]
    latest_anchor = anchors[0] if anchors else None
    return {
        "name": name,
        "buy_count": len(buy_items),
        "sell_count": len(sell_items),
        "buy_cost_rmb": round(buy_cost, 2),
        "sell_income_rmb": round(sell_income, 2),
        "net_cost_rmb": round(buy_cost - sell_income, 2),
        "held_value_rmb": round(held_value, 2),
        "anchor_count": len(anchors),
        "latest_anchor": _anchor_dict(latest_anchor, rate, db) if latest_anchor else None,
        "anchor_summary": {
            "latest_yi": round(anchor_values[0], 4) if anchor_values else 0,
            "min_yi": round(min(anchor_values), 4) if anchor_values else 0,
            "max_yi": round(max(anchor_values), 4) if anchor_values else 0,
            "avg_yi": round(sum(anchor_values) / len(anchor_values), 4) if anchor_values else 0,
        },
        "anchors": [_anchor_dict(a, rate, db) for a in anchors[:30]],
        "items": [_cat_dict(i, rate, db) for i in items],
    }


@app.get("/api/items/{name}/anchors")
def list_item_price_anchors(name: str, db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    rows = db.query(ItemPriceAnchor).filter(ItemPriceAnchor.item_name.like(f"%{name}%"), ItemPriceAnchor.deleted_at == None).order_by(desc(ItemPriceAnchor.timestamp)).all()
    return [_anchor_dict(r, rate, db) for r in rows]


@app.get("/api/item-price-anchors")
def list_all_anchors(db: Session = Depends(get_db)):
    """返回所有价格锚点，按物品名分组"""
    rate = _latest_rate(db)
    rows = db.query(ItemPriceAnchor).filter(ItemPriceAnchor.deleted_at == None).order_by(desc(ItemPriceAnchor.timestamp)).all()
    groups = {}
    for r in rows:
        name = r.item_name
        if name not in groups:
            vals = []
            groups[name] = {"item_name": name, "category": r.category, "anchors": [], "values": vals}
        groups[name]["anchors"].append(_anchor_dict(r, rate, db))
        groups[name]["values"].append(_anchor_price_yi(r.market_price, r.price_unit))
    result = []
    for name, g in groups.items():
        vals = g.pop("values")
        g["count"] = len(vals)
        g["latest_yi"] = round(vals[0], 4) if vals else 0
        g["min_yi"] = round(min(vals), 4) if vals else 0
        g["max_yi"] = round(max(vals), 4) if vals else 0
        g["avg_yi"] = round(sum(vals) / len(vals), 4) if vals else 0
        g["latest_time"] = g["anchors"][0]["timestamp"] if g["anchors"] else None
        result.append(g)
    return result


@app.post("/api/item-price-anchors")
def create_item_price_anchor(anchor: ItemPriceAnchorCreate, db: Session = Depends(get_db)):
    item_name = (anchor.item_name or '').strip()
    if not item_name:
        raise HTTPException(400, "物品名称不能为空")
    if anchor.price_unit not in ('亿', '万'):
        raise HTTPException(400, "价格单位只能是 亿 或 万")
    row = ItemPriceAnchor(**anchor.model_dump())
    db.add(row)
    db.flush()
    _audit(db, 'item_price_anchors', row.id, 'create', anchor.model_dump())
    db.commit()
    db.refresh(row)
    return _anchor_dict(row, _latest_rate(db), db)


@app.delete("/api/item-price-anchors/{aid}")
def delete_item_price_anchor(aid: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, ItemPriceAnchor, aid, "价格锚点不存在")
    _audit(db, 'item_price_anchors', aid, 'delete', {"item_name": row.item_name, "category": row.category, "market_price": row.market_price, "price_unit": row.price_unit})
    row.deleted_at = datetime.now()
    db.commit()
    return {"ok": True}


@app.post("/api/item-price-anchors/{aid}/images")
def upload_anchor_image(aid: int, body: ImageUpload, db: Session = Depends(get_db)):
    row = db.query(ItemPriceAnchor).filter(ItemPriceAnchor.id == aid, ItemPriceAnchor.deleted_at == None).first()
    if not row:
        raise HTTPException(404, "锚点不存在")
    img = AnchorImage(anchor_id=aid, image_data=body.image_data)
    db.add(img)
    db.commit()
    db.refresh(img)
    return {"id": img.id, "data": img.image_data}


@app.delete("/api/anchor-images/{iid}")
def delete_anchor_image(iid: int, db: Session = Depends(get_db)):
    img = db.query(AnchorImage).filter(AnchorImage.id == iid).first()
    if not img:
        raise HTTPException(404, "图片不存在")
    db.delete(img)
    db.commit()
    return {"ok": True}


# ── 预估价值批量更新 & 锚点联动 ─────────────────────────────────────
@app.post("/api/category-items/batch-update-estimated")
def batch_update_estimated(payload: dict, db: Session = Depends(get_db)):
    """按物品名批量更新所有「持有中」买入记录的预估价值"""
    item_name = payload.get("item_name", "").strip()
    new_value = _coerce_float(payload.get("estimated_value", 0), "预估价值")
    if not item_name:
        raise HTTPException(400, "物品名不能为空")
    rows = db.query(CategoryItem).filter(
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).all()
    count = 0
    for r in rows:
        old_ev = r.estimated_value or 0
        if abs(new_value - old_ev) > 1e-6:
            db.add(EstimatedValueHistory(item_id=r.id, old_value=old_ev, new_value=new_value, timestamp=datetime.now()))
            r.estimated_value = new_value
            # rmb_direct 物品设置了三国币估值就自动切换估值方式
            if (getattr(r, 'cost_mode', 'coin') or 'coin') == 'rmb_direct' and new_value > 0:
                r.valuation_mode = 'coin'
            count += 1
    db.commit()
    return {"ok": True, "updated": count}


@app.post("/api/category-items/sync-from-anchor")
def sync_estimated_from_anchor(payload: dict, db: Session = Depends(get_db)):
    """从最新价格锚点同步预估价值到所有同名「持有中」物品"""
    item_name = payload.get("item_name", "").strip()
    if not item_name:
        raise HTTPException(400, "物品名不能为空")
    anchor = db.query(ItemPriceAnchor).filter(
        ItemPriceAnchor.item_name == item_name,
        ItemPriceAnchor.deleted_at == None,
    ).order_by(desc(ItemPriceAnchor.timestamp)).first()
    if not anchor:
        raise HTTPException(404, f"未找到「{item_name}」的价格锚点")
    anchor_price_yi = _anchor_price_yi(anchor.market_price, anchor.price_unit)
    rows = db.query(CategoryItem).filter(
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).all()
    count = 0
    for r in rows:
        old_ev = r.estimated_value or 0
        if abs(anchor_price_yi - old_ev) > 1e-6:
            db.add(EstimatedValueHistory(item_id=r.id, old_value=old_ev, new_value=anchor_price_yi, timestamp=datetime.now()))
            r.estimated_value = anchor_price_yi
            # rmb_direct 物品设置了三国币估值就自动切换估值方式
            if (getattr(r, 'cost_mode', 'coin') or 'coin') == 'rmb_direct' and anchor_price_yi > 0:
                r.valuation_mode = 'coin'
            count += 1
    db.commit()
    return {"ok": True, "updated": count, "anchor_price": anchor_price_yi}


@app.post("/api/category-items/batch-switch-coin-valuation")
def batch_switch_coin_valuation(payload: dict, db: Session = Depends(get_db)):
    """批量将指定物品的 rmb_direct 记录切换为三国币估值，
    如果 estimated_value 为 0，用 cash_price / rate 自动折算"""
    item_name = payload.get("item_name", "").strip()
    if not item_name:
        raise HTTPException(400, "物品名不能为空")
    rate = _latest_rate(db)
    if not rate:
        raise HTTPException(400, "暂无币价记录，无法折算")
    rows = db.query(CategoryItem).filter(
        CategoryItem.item_name == item_name,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中",
        CategoryItem.deleted_at == None,
    ).all()
    count = 0
    for r in rows:
        cm = getattr(r, 'cost_mode', 'coin') or 'coin'
        if cm != 'rmb_direct':
            continue
        vm = getattr(r, 'valuation_mode', '') or ''
        if vm == 'coin' and (r.estimated_value or 0) > 0:
            continue  # 已经是币估且有值，跳过
        ev = r.estimated_value or 0
        if ev <= 0:
            # 用单价/币价折算为三国币
            cp = getattr(r, 'cash_price', 0) or 0
            use_rate = getattr(r, 'purchase_rate', 0) or rate
            ev = round(cp / use_rate, 4) if use_rate else 0
        if ev > 0:
            old_ev = r.estimated_value or 0
            if abs(ev - old_ev) > 1e-6:
                db.add(EstimatedValueHistory(item_id=r.id, old_value=old_ev, new_value=ev, timestamp=datetime.now()))
            r.estimated_value = ev
            r.valuation_mode = 'coin'
            count += 1
    db.commit()
    return {"ok": True, "updated": count, "rate_used": rate}


# ── 币持仓校准 & 零散支出 ──────────────────────────────────────────
def _get_json_setting(db, key):
    row = db.query(Setting).filter(Setting.key == key).first()
    if row and row.value:
        try:
            return json.loads(row.value)
        except (json.JSONDecodeError, TypeError):
            return []
    return []

def _set_json_setting(db, key, val):
    row = db.query(Setting).filter(Setting.key == key).first()
    if row:
        row.value = json.dumps(val, ensure_ascii=False)
    else:
        db.add(Setting(key=key, value=json.dumps(val, ensure_ascii=False)))
    db.commit()


def _setting_text(db, key: str, default: str = "") -> str:
    row = db.query(Setting).filter(Setting.key == key).first()
    if not row or row.value is None:
        return default
    return str(row.value)


def _setting_float(db, key: str, default: float = 0) -> float:
    raw = _setting_text(db, key, "")
    if raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _parse_day(value, field_name: str = "日期"):
    raw = str(value or '').strip()[:10]
    if not raw:
        raise HTTPException(400, f"{field_name}不能为空")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"{field_name}格式必须是 YYYY-MM-DD")


def _iter_days(start_day, end_day):
    cur = start_day
    while cur <= end_day:
        yield cur
        cur += timedelta(days=1)


def _build_monthly_rate_map(db: Session) -> dict:
    prices = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(PriceHistory.timestamp).all()
    monthly_rate = {}
    for p in prices:
        if p.timestamp:
            monthly_rate[p.timestamp.strftime("%Y-%m")] = p.price_10e or p.price or 0
    return monthly_rate


def _resolve_month_rate(month_key: str, monthly_rate: dict, fallback_rate: float) -> float:
    rate = monthly_rate.get(month_key, 0)
    if rate == 0:
        for prev_m in sorted(monthly_rate.keys(), reverse=True):
            if prev_m <= month_key:
                rate = monthly_rate[prev_m]
                break
    return rate or fallback_rate


def _save_daily_active_entries(db: Session, entries: list[dict]):
    normalized = []
    for i, row in enumerate(entries, start=1):
        normalized.append({
            "id": i,
            "date": str(row.get("date") or "")[:10],
            "amount": round(float(row.get("amount", 0) or 0), 4),
            "source": "manual" if row.get("source") == "manual" else "auto",
            "notes": str(row.get("notes") or ""),
            "created_at": str(row.get("created_at") or datetime.now().isoformat()),
        })
    _set_json_setting(db, "daily_active_coin_entries", normalized)
    return normalized


def _get_daily_active_entries(db: Session):
    rows = _get_json_setting(db, "daily_active_coin_entries")
    cleaned = []
    dirty = False
    for idx, row in enumerate(rows, start=1):
        date_str = str(row.get("date") or "")[:10]
        if not date_str:
            dirty = True
            continue
        try:
            amount = round(float(row.get("amount", 0) or 0), 4)
        except (TypeError, ValueError):
            amount = 0.0
            dirty = True
        source = "manual" if row.get("source") == "manual" else "auto"
        created_at = str(row.get("created_at") or datetime.now().isoformat())
        cleaned.append({
            "id": int(row.get("id") or idx),
            "date": date_str,
            "amount": amount,
            "source": source,
            "notes": str(row.get("notes") or ""),
            "created_at": created_at,
        })
    expected_ids = list(range(1, len(cleaned) + 1))
    if dirty or [r["id"] for r in cleaned] != expected_ids:
        cleaned = _save_daily_active_entries(db, cleaned)
    return cleaned


def _sync_daily_active_entries(db: Session, start_day=None, end_day=None):
    daily_wan = _setting_float(db, "daily_active_coin_wan", 0)
    start_text = _setting_text(db, "daily_active_start_date", "").strip()
    if start_day is None:
        if not start_text:
            return {"created": 0, "added_coin": 0.0, "entries": _get_daily_active_entries(db)}
        start_day = _parse_day(start_text, "每日活跃起始日期")
    if end_day is None:
        end_day = datetime.now().date()
    if start_day > end_day or daily_wan <= 0:
        return {"created": 0, "added_coin": 0.0, "entries": _get_daily_active_entries(db)}
    amount_coin = round(daily_wan / 10000.0, 4)
    if amount_coin <= 0:
        return {"created": 0, "added_coin": 0.0, "entries": _get_daily_active_entries(db)}
    entries = _get_daily_active_entries(db)
    auto_days = {row.get("date") for row in entries if row.get("source") != "manual"}
    created = 0
    added_coin = 0.0
    now_iso = datetime.now().isoformat()
    for day in _iter_days(start_day, end_day):
        day_str = day.strftime("%Y-%m-%d")
        if day_str in auto_days:
            continue
        entries.append({
            "id": len(entries) + 1,
            "date": day_str,
            "amount": amount_coin,
            "source": "auto",
            "notes": "",
            "created_at": now_iso,
        })
        auto_days.add(day_str)
        created += 1
        added_coin += amount_coin
    if created:
        entries = _save_daily_active_entries(db, entries)
    return {"created": created, "added_coin": round(added_coin, 4), "entries": entries}


def _daily_active_rollup(entries: list[dict], monthly_rate: dict, fallback_rate: float):
    by_month = {}
    total_coin = 0.0
    total_rmb = 0.0
    for row in entries:
        amount = round(float(row.get("amount", 0) or 0), 4)
        date_str = str(row.get("date") or "")[:10]
        if not date_str:
            continue
        total_coin += amount
        month_key = date_str[:7]
        rate = _resolve_month_rate(month_key, monthly_rate, fallback_rate)
        rmb = amount * rate
        total_rmb += rmb
        by_month[month_key] = by_month.get(month_key, 0.0) + rmb
    return {
        "total_coin": round(total_coin, 4),
        "total_rmb": round(total_rmb, 2),
        "by_month": {k: round(v, 2) for k, v in by_month.items()},
    }


def _daily_active_snapshot(db: Session):
    entries = sorted(
        _get_daily_active_entries(db),
        key=lambda r: (r.get("date") or "", r.get("created_at") or "", r.get("id") or 0),
        reverse=True,
    )
    days_map = {}
    manual_entries = []
    total_coin = 0.0
    for row in entries:
        amount = round(float(row.get("amount", 0) or 0), 4)
        total_coin += amount
        day_key = row.get("date")
        if not day_key:
            continue
        bucket = days_map.setdefault(day_key, {"date": day_key, "auto_coin": 0.0, "manual_coin": 0.0, "total_coin": 0.0})
        if row.get("source") == "manual":
            bucket["manual_coin"] += amount
            manual_entries.append({
                "id": row.get("id"),
                "date": day_key,
                "amount_coin": amount,
                "amount_wan": round(amount * 10000, 2),
                "notes": row.get("notes") or "",
                "created_at": row.get("created_at"),
            })
        else:
            bucket["auto_coin"] += amount
        bucket["total_coin"] += amount
    days = []
    for day_key in sorted(days_map.keys(), reverse=True):
        bucket = days_map[day_key]
        days.append({
            "date": day_key,
            "auto_coin": round(bucket["auto_coin"], 4),
            "manual_coin": round(bucket["manual_coin"], 4),
            "total_coin": round(bucket["total_coin"], 4),
            "auto_wan": round(bucket["auto_coin"] * 10000, 2),
            "manual_wan": round(bucket["manual_coin"] * 10000, 2),
            "total_wan": round(bucket["total_coin"] * 10000, 2),
        })
    manual_entries.sort(key=lambda r: (r.get("date") or "", r.get("created_at") or "", r.get("id") or 0), reverse=True)
    return {
        "config": {
            "daily_wan": round(_setting_float(db, "daily_active_coin_wan", 0), 2),
            "start_date": _setting_text(db, "daily_active_start_date", "").strip(),
        },
        "days": days,
        "manual_entries": manual_entries,
        "total_coin": round(total_coin, 4),
        "total_wan": round(total_coin * 10000, 2),
        "total_days": len(days),
        "manual_count": len(manual_entries),
        "entry_count": len(entries),
    }


@app.get("/api/daily-active-coins")
def get_daily_active_coins(db: Session = Depends(get_db)):
    _sync_daily_active_entries(db)
    return _daily_active_snapshot(db)


@app.post("/api/daily-active-coins/sync")
def sync_daily_active_coins(body: DailyActiveSyncCreate, db: Session = Depends(get_db)):
    start_day = _parse_day(body.start_date, "每日活跃起始日期") if body.start_date else None
    end_day = _parse_day(body.end_date, "结束日期") if body.end_date else None
    result = _sync_daily_active_entries(db, start_day=start_day, end_day=end_day)
    snapshot = _daily_active_snapshot(db)
    return {**result, **snapshot}


@app.post("/api/daily-active-coins/adjust")
def add_daily_active_adjustment(body: DailyActiveAdjustCreate, db: Session = Depends(get_db)):
    day = _parse_day(body.date, "调整日期")
    amount_wan = _coerce_float(body.amount_wan, "调整数量")
    if abs(amount_wan) < 0.0001:
        raise HTTPException(400, "调整数量不能为 0")
    amount_coin = round(amount_wan / 10000.0, 4)
    entries = _get_daily_active_entries(db)
    entries.append({
        "id": len(entries) + 1,
        "date": day.strftime("%Y-%m-%d"),
        "amount": amount_coin,
        "source": "manual",
        "notes": body.notes or "",
        "created_at": datetime.now().isoformat(),
    })
    _save_daily_active_entries(db, entries)
    return {"ok": True, "amount_coin": amount_coin, "amount_wan": round(amount_coin * 10000, 2)}


@app.delete("/api/daily-active-coins/{eid}")
def delete_daily_active_adjustment(eid: int, db: Session = Depends(get_db)):
    entries = _get_daily_active_entries(db)
    target = next((row for row in entries if row.get("id") == eid), None)
    if not target:
        raise HTTPException(404, "记录不存在")
    if target.get("source") != "manual":
        raise HTTPException(400, "自动补录记录不能直接删除，请用按天修正减少数量")
    entries = [row for row in entries if row.get("id") != eid]
    _save_daily_active_entries(db, entries)
    return {"ok": True}


@app.get("/api/coin-calibrations")
def get_calibrations(db: Session = Depends(get_db)):
    return _get_json_setting(db, "coin_calibrations")


@app.post("/api/coin-calibrations")
def add_calibration(body: dict, db: Session = Depends(get_db)):
    """添加一次校准记录: {actual_coin, system_coin, notes}"""
    records = _get_json_setting(db, "coin_calibrations")
    actual = _coerce_float(body.get("actual_coin", 0), "实际币量")
    system = _coerce_float(body.get("system_coin", 0), "系统币量")
    diff = round(actual - system, 4)
    records.append({
        "id": len(records) + 1,
        "timestamp": datetime.now().isoformat(),
        "actual_coin": actual,
        "system_coin": round(system, 4),
        "diff": diff,
        "notes": body.get("notes", ""),
    })
    _set_json_setting(db, "coin_calibrations", records)
    return {"ok": True, "diff": diff}


@app.delete("/api/coin-calibrations/{cid}")
def del_calibration(cid: int, db: Session = Depends(get_db)):
    records = _get_json_setting(db, "coin_calibrations")
    records = [r for r in records if r.get("id") != cid]
    # re-index
    for i, r in enumerate(records):
        r["id"] = i + 1
    _set_json_setting(db, "coin_calibrations", records)
    return {"ok": True}


@app.get("/api/misc-expenses")
def get_misc_expenses(db: Session = Depends(get_db)):
    return _get_json_setting(db, "misc_coin_expenses")


@app.post("/api/misc-expenses")
def add_misc_expense(body: dict, db: Session = Depends(get_db)):
    """添加零散支出: {amount, notes}  amount单位为亿"""
    records = _get_json_setting(db, "misc_coin_expenses")
    records.append({
        "id": len(records) + 1,
        "timestamp": datetime.now().isoformat(),
        "amount": round(_coerce_float(body.get("amount", 0), "支出数量"), 4),
        "notes": body.get("notes", ""),
    })
    _set_json_setting(db, "misc_coin_expenses", records)
    return {"ok": True}


@app.delete("/api/misc-expenses/{eid}")
def del_misc_expense(eid: int, db: Session = Depends(get_db)):
    records = _get_json_setting(db, "misc_coin_expenses")
    records = [r for r in records if r.get("id") != eid]
    for i, r in enumerate(records):
        r["id"] = i + 1
    _set_json_setting(db, "misc_coin_expenses", records)
    return {"ok": True}


# ── 月度汇总 ───────────────────────────────────────────────────────
@app.get("/api/monthly-summary")
def monthly_summary(db: Session = Depends(get_db)):
    _sync_daily_active_entries(db)
    daily_active_entries = _get_daily_active_entries(db)
    all_tx = db.query(Transaction).filter(Transaction.deleted_at == None).order_by(Transaction.timestamp).all()
    months = {}

    def _ensure_month(key: str):
        if key not in months:
            months[key] = {"month": key, "recharge": 0, "cashout": 0,
                           "trade_in": 0, "trade_out": 0, "dungeon": 0, "activity": 0, "consumed": 0,
                           "item_buy_rmb": 0, "item_sell_rmb": 0}

    for t in all_tx:
        if not t.timestamp or getattr(t, 'status', 'normal') == 'void':
            continue
        key = t.timestamp.strftime("%Y-%m")
        _ensure_month(key)
        rmb = _tx_rmb(t)
        direction = getattr(t, 'direction', None) or 'expense'
        if t.type == "买币":
            months[key]["recharge"] += rmb
        elif t.type == "卖币":
            months[key]["cashout"] += rmb
        elif t.type == "三国点充值":
            months[key]["recharge"] += rmb
        elif t.type == "三国点售卖":
            months[key]["cashout"] += rmb
        else:
            if direction == 'income':
                months[key]["trade_in"] += rmb
            else:
                months[key]["trade_out"] += rmb

    # 副本收益按月汇总（使用当月币价折算，而非最新币价）
    monthly_rate = _build_monthly_rate_map(db)
    fallback_rate = _latest_rate(db)
    all_dg = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).all()
    for d in all_dg:
        if not d.timestamp:
            continue
        key = d.timestamp.strftime("%Y-%m")
        _ensure_month(key)
        rate = _resolve_month_rate(key, monthly_rate, fallback_rate)
        months[key]["dungeon"] += (d.revenue_coin or 0) * rate
    daily_active_roll = _daily_active_rollup(daily_active_entries, monthly_rate, fallback_rate)
    for key, val in daily_active_roll["by_month"].items():
        _ensure_month(key)
        months[key]["activity"] += val

    all_items = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).all()
    for item in all_items:
        # 统计直接现金买卖物品的月度金额
        if (getattr(item, 'cost_mode', 'coin') or 'coin') == 'rmb_direct' and item.timestamp:
            ikey = item.timestamp.strftime("%Y-%m")
            _ensure_month(ikey)
            if item.type == '买入':
                months[ikey]["item_buy_rmb"] += _item_purchase_rmb(item)
            elif item.type == '卖出':
                months[ikey]["item_sell_rmb"] += _item_purchase_rmb(item)
        # 统计已消耗物品成本
        if item.type != '买入':
            continue
        if getattr(item, 'holding_status', '') != '已消耗':
            continue
        ref_time = getattr(item, 'status_changed_at', None) or item.timestamp
        if not ref_time:
            continue
        key = ref_time.strftime("%Y-%m")
        _ensure_month(key)
        months[key]["consumed"] += _item_purchase_rmb(item)

    result = []
    cum_pnl = 0
    for key in sorted(months.keys()):
        m = months[key]
        trade_pnl = m["trade_in"] - m["trade_out"]
        monthly_pnl = trade_pnl + m.get("dungeon", 0) + m.get("activity", 0) - m.get("consumed", 0)
        cum_pnl += monthly_pnl
        result.append({
            "month": m["month"],
            "recharge": round(m["recharge"], 2),
            "cashout": round(m["cashout"], 2),
            "item_buy_rmb": round(m.get("item_buy_rmb", 0), 2),
            "item_sell_rmb": round(m.get("item_sell_rmb", 0), 2),
            "trade_in": round(m["trade_in"], 2),
            "trade_out": round(m["trade_out"], 2),
            "trade_pnl": round(trade_pnl, 2),
            "dungeon": round(m["dungeon"], 2),
            "activity": round(m.get("activity", 0), 2),
            "consumed": round(m.get("consumed", 0), 2),
            "monthly_pnl": round(monthly_pnl, 2),
            "cumulative_pnl": round(cum_pnl, 2),
        })
    return result


# ── 交易统计看板 ──────────────────────────────────────────────
@app.get("/api/transaction-stats")
def transaction_stats(db: Session = Depends(get_db)):
    now = datetime.now()
    week_ago = now - timedelta(days=7)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    all_tx = db.query(Transaction).filter(
        Transaction.deleted_at == None,
        Transaction.status != 'void'
    ).order_by(desc(Transaction.timestamp)).all()

    week_count = 0
    month_count = 0
    best_trade = None
    worst_trade = None
    best_val = -float('inf')
    worst_val = float('inf')
    total_profit = 0.0
    profit_trades = 0

    for t in all_tx:
        rmb = _tx_rmb(t)
        direction = getattr(t, 'direction', None) or 'expense'
        cashflow = rmb if direction == 'income' else -rmb
        if t.timestamp and t.timestamp >= week_ago:
            week_count += 1
        if t.timestamp and t.timestamp >= month_start:
            month_count += 1
        # 只统计非买币/卖币的交易作为"盈亏"交易
        if t.type not in ('买币', '卖币', '三国点充值', '三国点售卖'):
            total_profit += cashflow
            profit_trades += 1
            if cashflow > best_val:
                best_val = cashflow
                best_trade = {"type": t.type, "rmb": round(cashflow, 2), "notes": t.notes or '', "time": t.timestamp.strftime('%m-%d') if t.timestamp else ''}
            if cashflow < worst_val:
                worst_val = cashflow
                worst_trade = {"type": t.type, "rmb": round(cashflow, 2), "notes": t.notes or '', "time": t.timestamp.strftime('%m-%d') if t.timestamp else ''}

    avg_profit = round(total_profit / profit_trades, 2) if profit_trades else 0
    return {
        "total_count": len(all_tx),
        "week_count": week_count,
        "month_count": month_count,
        "profit_trades": profit_trades,
        "avg_profit": avg_profit,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
    }


# ── 月度对比 ──────────────────────────────────────────────────
@app.get("/api/month-compare")
def month_compare(m1: str, m2: str, db: Session = Depends(get_db)):
    """对比两个月份的数据，m1/m2 格式: YYYY-MM"""
    _sync_daily_active_entries(db)
    daily_active_entries = _get_daily_active_entries(db)
    monthly_rate = _build_monthly_rate_map(db)
    fallback_rate = _latest_rate(db)
    daily_active_roll = _daily_active_rollup(daily_active_entries, monthly_rate, fallback_rate)

    all_tx = db.query(Transaction).filter(
        Transaction.deleted_at == None,
        Transaction.status != 'void'
    ).all()
    all_dg = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).all()
    all_items = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).all()

    def _month_data(month_key: str) -> dict:
        recharge = cashout = trade_in = trade_out = dg_rmb = activity_rmb = consumed = 0.0
        tx_count = dg_count = 0
        for t in all_tx:
            if not t.timestamp or t.timestamp.strftime("%Y-%m") != month_key:
                continue
            tx_count += 1
            rmb = _tx_rmb(t)
            direction = getattr(t, 'direction', None) or 'expense'
            if t.type == "买币":
                recharge += rmb
            elif t.type == "卖币":
                cashout += rmb
            elif t.type == "三国点充值":
                recharge += rmb
            elif t.type == "三国点售卖":
                cashout += rmb
            else:
                if direction == 'income':
                    trade_in += rmb
                else:
                    trade_out += rmb
        for d in all_dg:
            if not d.timestamp or d.timestamp.strftime("%Y-%m") != month_key:
                continue
            dg_count += 1
            dg_rmb += (d.revenue_coin or 0) * _resolve_month_rate(month_key, monthly_rate, fallback_rate)

        activity_rmb = daily_active_roll["by_month"].get(month_key, 0)

        for item in all_items:
            if item.type != '买入':
                continue
            if getattr(item, 'holding_status', '') != '已消耗':
                continue
            ref_time = getattr(item, 'status_changed_at', None) or item.timestamp
            if not ref_time or ref_time.strftime("%Y-%m") != month_key:
                continue
            consumed += _item_purchase_rmb(item)

        trade_pnl = trade_in - trade_out
        total_pnl = trade_pnl + dg_rmb + activity_rmb - consumed
        return {
            "month": month_key,
            "tx_count": tx_count,
            "recharge": round(recharge, 2),
            "cashout": round(cashout, 2),
            "trade_in": round(trade_in, 2),
            "trade_out": round(trade_out, 2),
            "trade_pnl": round(trade_pnl, 2),
            "dg_count": dg_count,
            "dg_rmb": round(dg_rmb, 2),
            "activity_rmb": round(activity_rmb, 2),
            "consumed": round(consumed, 2),
            "total_pnl": round(total_pnl, 2),
        }

    d1 = _month_data(m1)
    d2 = _month_data(m2)

    def _diff(a, b):
        return round(b - a, 2) if a is not None and b is not None else 0

    diff = {}
    for k in ['tx_count', 'recharge', 'cashout', 'trade_in', 'trade_out', 'trade_pnl', 'dg_count', 'dg_rmb', 'activity_rmb', 'consumed', 'total_pnl']:
        diff[k] = _diff(d1[k], d2[k])

    return {"m1": d1, "m2": d2, "diff": diff}


# ── 副本收益 CRUD ─────────────────────────────────────────────
def _parse_drops(raw: str) -> list:
    """解析掉落物品JSON，兼容旧纯文本格式"""
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except (json.JSONDecodeError, TypeError):
        pass
    return [{"name": raw, "qty": 1}] if raw.strip() else []


def _dg_dict(r: DungeonRevenue, rate: float = 0, db: Session = None) -> dict:
    drops = _parse_drops(r.revenue_items)
    images = []
    if db:
        imgs = db.query(DungeonImage).filter(DungeonImage.dungeon_id == r.id).all()
        images = [{"id": img.id, "data": img.image_data} for img in imgs]
    return {
        "id": r.id,
        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        "dungeon_name": r.dungeon_name,
        "revenue_coin": r.revenue_coin,
        "revenue_rmb": round(r.revenue_coin * rate, 2) if rate else 0,
        "drop_items": drops,
        "image_count": len(images),
        "images": images,
        "notes": r.notes,
    }


@app.get("/api/dungeons")
def list_dungeons(limit: int = 500, db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    rows = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).order_by(desc(DungeonRevenue.timestamp)).limit(limit).all()
    total_coin = sum(r.revenue_coin for r in rows)
    return {
        "total_coin": round(total_coin, 2),
        "total_rmb": round(total_coin * rate, 2),
        "total_runs": len(rows),
        "items": [_dg_dict(r, rate, db) for r in rows],
    }


@app.post("/api/dungeons")
def create_dungeon(d: DungeonCreate, db: Session = Depends(get_db)):
    drop_items = [item.model_dump() for item in d.drop_items]
    row = DungeonRevenue(
        timestamp=d.timestamp,
        dungeon_name=d.dungeon_name,
        revenue_coin=d.revenue_coin,
        revenue_items=json.dumps(drop_items, ensure_ascii=False),
        notes=d.notes,
    )
    db.add(row); db.flush()
    _audit(db, 'dungeon_revenues', row.id, 'create', {"timestamp": d.timestamp, "dungeon_name": d.dungeon_name, "revenue_coin": d.revenue_coin, "drop_items": drop_items, "notes": d.notes})
    db.commit(); db.refresh(row)
    return _dg_dict(row, _latest_rate(db), db)


@app.put("/api/dungeons/{did}")
def update_dungeon(did: int, d: DungeonUpdate, db: Session = Depends(get_db)):
    row = db.query(DungeonRevenue).filter(DungeonRevenue.id == did, DungeonRevenue.deleted_at == None).first()
    if not row:
        raise HTTPException(404, "记录不存在或已删除")
    old_data = {"dungeon_name": row.dungeon_name, "revenue_coin": row.revenue_coin, "notes": row.notes}
    new_data = {"dungeon_name": d.dungeon_name, "revenue_coin": d.revenue_coin, "notes": d.notes}
    changes = {k: {"old": old_data[k], "new": new_data[k]} for k in old_data if str(old_data[k]) != str(new_data[k])}
    if changes:
        _audit(db, 'dungeon_revenues', did, 'edit', changes)
    row.timestamp = d.timestamp
    row.dungeon_name = d.dungeon_name
    row.revenue_coin = d.revenue_coin
    row.revenue_items = json.dumps([item.model_dump() for item in d.drop_items], ensure_ascii=False)
    row.notes = d.notes
    db.commit(); db.refresh(row)
    return _dg_dict(row, _latest_rate(db), db)


@app.post("/api/dungeons/{did}/transfer-drop")
def transfer_dungeon_drop(did: int, payload: dict, db: Session = Depends(get_db)):
    """将副本掉落物品一键转入分类物品管理（买入价0，来源为副本）"""
    dg = _get_active_row(db, DungeonRevenue, did, "副本记录不存在")
    item_name = str(payload.get("item_name", "") or "").strip()
    qty = _coerce_int(payload.get("qty", 1), "数量", 1)
    category = str(payload.get("category", "装备") or "装备").strip() or "装备"
    if not item_name:
        raise HTTPException(400, "物品名不能为空")
    valid_cats = _get_categories(db)
    if valid_cats and category not in valid_cats:
        raise HTTPException(400, f"无效分类: {category}")
    rate = _latest_rate(db)
    row = CategoryItem(
        timestamp=dg.timestamp,
        category=category,
        type="买入",
        item_name=item_name,
        item_group=str(payload.get("item_group", "") or "").strip(),
        quantity=qty,
        cost_mode="coin",
        coin_price=0,
        cash_price=0,
        purchase_rate=rate,
        estimated_value=0,
        estimated_rmb=0,
        holding_status="持有中",
        source_type="副本掉落",
        source_ref=f"dungeon:{dg.id}:{item_name}:{qty}",
        channel="副本掉落",
        notes=f"来自副本「{dg.dungeon_name}」掉落",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _cat_dict(row, rate, db)


@app.delete("/api/dungeons/{did}")
def delete_dungeon(did: int, db: Session = Depends(get_db)):
    row = _get_active_row(db, DungeonRevenue, did, "记录不存在")
    _audit(db, 'dungeon_revenues', did, 'delete', {"dungeon_name": row.dungeon_name, "revenue_coin": row.revenue_coin})
    row.deleted_at = datetime.now()
    db.commit()
    return {"ok": True}


# ── 副本收益图片 ──────────────────────────────────────────────
@app.get("/api/dungeons/{did}/images")
def list_dungeon_images(did: int, db: Session = Depends(get_db)):
    rows = db.query(DungeonImage).filter(DungeonImage.dungeon_id == did).order_by(DungeonImage.id).all()
    return [{"id": r.id, "image_data": r.image_data} for r in rows]


@app.post("/api/dungeons/{did}/images")
def upload_dungeon_image(did: int, img: ImageUpload, db: Session = Depends(get_db)):
    row = _get_active_row(db, DungeonRevenue, did, "副本记录不存在")
    di = DungeonImage(dungeon_id=row.id, image_data=img.image_data)
    db.add(di); db.commit(); db.refresh(di)
    return {"id": di.id}


@app.delete("/api/dungeon-images/{iid}")
def delete_dungeon_image(iid: int, db: Session = Depends(get_db)):
    row = db.query(DungeonImage).filter(DungeonImage.id == iid).first()
    if not row:
        raise HTTPException(404, "图片不存在")
    db.delete(row); db.commit()
    return {"ok": True}


# ── 副本/掉落物品下拉选项管理 ────────────────────────────────
def _get_options(db: Session, key: str) -> list:
    row = db.query(Setting).filter(Setting.key == key).first()
    if not row or not row.value:
        return []
    try:
        return json.loads(row.value)
    except (json.JSONDecodeError, TypeError):
        return []


def _set_options(db: Session, key: str, items: list):
    row = db.query(Setting).filter(Setting.key == key).first()
    val = json.dumps(items, ensure_ascii=False)
    if row:
        row.value = val
    else:
        db.add(Setting(key=key, value=val))
    db.commit()


@app.get("/api/dungeon-name-options")
def get_dungeon_name_options(db: Session = Depends(get_db)):
    return _get_options(db, "dungeon_name_options")


@app.put("/api/dungeon-name-options")
def set_dungeon_name_options(opt: OptionsUpdate, db: Session = Depends(get_db)):
    _set_options(db, "dungeon_name_options", opt.items)
    return {"ok": True}


@app.get("/api/drop-item-options")
def get_drop_item_options(db: Session = Depends(get_db)):
    return _get_options(db, "drop_item_options")


@app.put("/api/drop-item-options")
def set_drop_item_options(opt: OptionsUpdate, db: Session = Depends(get_db)):
    _set_options(db, "drop_item_options", opt.items)
    return {"ok": True}


# ── 副本月度汇总 ─────────────────────────────────────────────
@app.get("/api/dungeon-monthly")
def dungeon_monthly(db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    rows = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).order_by(DungeonRevenue.timestamp).all()
    monthly = {}
    for r in rows:
        if not r.timestamp:
            continue
        key = r.timestamp.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"month": key, "runs": 0, "total_coin": 0.0, "drops": {}}
        monthly[key]["runs"] += 1
        monthly[key]["total_coin"] += r.revenue_coin
        for d in _parse_drops(r.revenue_items):
            name = d.get("name", "")
            qty = d.get("qty", 0)
            if name:
                monthly[key]["drops"][name] = monthly[key]["drops"].get(name, 0) + qty
    result = []
    for m in sorted(monthly.values(), key=lambda x: x["month"]):
        m["total_rmb"] = round(m["total_coin"] * rate, 2)
        m["total_coin"] = round(m["total_coin"], 2)
        m["drop_summary"] = [{"name": k, "qty": v} for k, v in m["drops"].items()]
        del m["drops"]
        result.append(m)
    return result


# ── 设置 ───────────────────────────────────────────────────────────
@app.get("/api/settings")
def get_settings(db: Session = Depends(get_db)):
    rows = db.query(Setting).all()
    return {r.key: r.value for r in rows}


@app.put("/api/settings")
def update_settings(s: SettingUpdate, db: Session = Depends(get_db)):
    row = db.query(Setting).filter(Setting.key == s.key).first()
    if row:
        row.value = s.value
    else:
        db.add(Setting(key=s.key, value=s.value))
    db.commit()
    return {"ok": True}


# ── 元信息 ─────────────────────────────────────────────────────────
@app.get("/api/meta")
def meta(db: Session = Depends(get_db)):
    return {
        "categories": _get_categories(db),
        "tx_types": _get_tx_type_configs(db),
        "channels": _get_channels(db),
    }


# ── 交易类型管理 ──────────────────────────────────────────────────
class TxTypeConfig(BaseModel):
    name: str
    direction: str = 'expense'
    is_coin: bool = False

class TxTypeListUpdate(BaseModel):
    items: list[TxTypeConfig]

@app.get("/api/tx-type-options")
def get_tx_type_options(db: Session = Depends(get_db)):
    return _get_tx_type_configs(db)

@app.put("/api/tx-type-options")
def set_tx_type_options(opt: TxTypeListUpdate, db: Session = Depends(get_db)):
    data = [t.model_dump() for t in opt.items]
    row = db.query(Setting).filter(Setting.key == 'tx_type_options').first()
    val = json.dumps(data, ensure_ascii=False)
    if row:
        row.value = val
    else:
        db.add(Setting(key='tx_type_options', value=val))
    db.commit()
    return {"ok": True}


# ── 分类管理 ──────────────────────────────────────────────────────
@app.get("/api/category-options")
def get_category_options(db: Session = Depends(get_db)):
    return _get_categories(db)

@app.put("/api/category-options")
def set_category_options(opt: OptionsUpdate, db: Session = Depends(get_db)):
    _set_options(db, 'category_options', opt.items)
    return {"ok": True}


# ── 渠道管理 ──────────────────────────────────────────────────────
@app.get("/api/channel-options")
def get_channel_options(db: Session = Depends(get_db)):
    return _get_channels(db)

@app.put("/api/channel-options")
def set_channel_options(opt: OptionsUpdate, db: Session = Depends(get_db)):
    _set_options(db, 'channel_options', opt.items)
    return {"ok": True}


# ── 回收站 ───────────────────────────────────────────────────────────
@app.get("/api/recycle-bin")
def list_recycle_bin(db: Session = Depends(get_db)):
    """列出所有被软删除的记录"""
    items = []
    for row in db.query(Transaction).filter(Transaction.deleted_at != None).order_by(desc(Transaction.deleted_at)).all():
        items.append({"id": row.id, "type": "transaction", "label": f"交易: {row.type} {row.quantity}×{row.unit_price}", "deleted_at": row.deleted_at.isoformat() if row.deleted_at else None, "timestamp": row.timestamp.isoformat() if row.timestamp else None})
    for row in db.query(CategoryItem).filter(CategoryItem.deleted_at != None).order_by(desc(CategoryItem.deleted_at)).all():
        items.append({"id": row.id, "type": "category_item", "label": f"物品: [{row.category}] {row.type} {row.item_name}", "deleted_at": row.deleted_at.isoformat() if row.deleted_at else None, "timestamp": row.timestamp.isoformat() if row.timestamp else None})
    for row in db.query(PriceHistory).filter(PriceHistory.deleted_at != None).order_by(desc(PriceHistory.deleted_at)).all():
        items.append({"id": row.id, "type": "price", "label": f"币价: {row.price_10e or row.price} 元/亿", "deleted_at": row.deleted_at.isoformat() if row.deleted_at else None, "timestamp": row.timestamp.isoformat() if row.timestamp else None})
    for row in db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at != None).order_by(desc(DungeonRevenue.deleted_at)).all():
        items.append({"id": row.id, "type": "dungeon", "label": f"副本: {row.dungeon_name} {row.revenue_coin}亿", "deleted_at": row.deleted_at.isoformat() if row.deleted_at else None, "timestamp": row.timestamp.isoformat() if row.timestamp else None})
    items.sort(key=lambda x: x["deleted_at"] or "", reverse=True)
    return items


@app.post("/api/recycle-bin/restore/{record_type}/{record_id}")
def restore_from_recycle_bin(record_type: str, record_id: int, db: Session = Depends(get_db)):
    """从回收站恢复记录"""
    table_map = {"transaction": "transactions", "category_item": "category_items", "price": "price_history", "dungeon": "dungeon_revenues"}
    model_map = {"transaction": Transaction, "category_item": CategoryItem, "price": PriceHistory, "dungeon": DungeonRevenue}
    model = model_map.get(record_type)
    if not model:
        raise HTTPException(400, f"无效类型: {record_type}")
    row = db.query(model).filter(model.id == record_id).first()
    if not row:
        raise HTTPException(404, "记录不存在")
    if not row.deleted_at:
        return {"ok": True, "msg": "记录未被删除"}
    _audit(db, table_map.get(record_type, record_type), record_id, 'restore', {})
    row.deleted_at = None
    if record_type == 'category_item' and getattr(row, 'type', None) in ('卖出', '买入'):
        _sync_item_sell_status(db, row.category, row.item_name)
    db.commit()
    return {"ok": True}


@app.get("/api/audit-logs")
def list_audit_logs(limit: int = 200, db: Session = Depends(get_db)):
    """查看审计日志"""
    rows = db.query(AuditLog).order_by(desc(AuditLog.timestamp)).limit(limit).all()
    result = []
    for r in rows:
        changes = None
        if r.changes:
            try:
                changes = json.loads(r.changes)
            except (json.JSONDecodeError, TypeError):
                changes = r.changes
        result.append({
            "id": r.id, "table_name": r.table_name, "record_id": r.record_id,
            "action": r.action, "changes": changes,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        })
    return result


# ── 物品利润分析 ─────────────────────────────────────────────────
@app.get("/api/item-profit-analysis")
def item_profit_analysis(db: Session = Depends(get_db)):
    """按物品名匹配买入/卖出，计算每个物品的利润"""
    rate = _latest_rate(db)
    items = db.query(CategoryItem).filter(
        CategoryItem.deleted_at == None,
        CategoryItem.type.in_(["买入", "卖出"])
    ).order_by(CategoryItem.item_name, CategoryItem.timestamp).all()

    # 按物品名分组
    from collections import defaultdict
    groups = defaultdict(lambda: {"buys": [], "sells": []})
    for item in items:
        qty = getattr(item, 'quantity', 1) or 1
        coin = _item_coin_cost(item)
        rmb = _item_purchase_rmb(item)
        entry = {"id": item.id, "category": item.category, "coin": coin,
                 "rate": (item.purchase_rate or 0), "rmb": rmb, "qty": qty,
                 "timestamp": item.timestamp.isoformat() if item.timestamp else None,
                 "status": getattr(item, 'holding_status', '持有中'),
                 "current_rmb": _item_current_rmb(item, rate)}
        if item.type == "买入":
            groups[item.item_name]["buys"].append(entry)
        else:
            groups[item.item_name]["sells"].append(entry)

    result = []
    for name, g in groups.items():
        buy_total_rmb = sum(b["rmb"] for b in g["buys"])
        sell_total_rmb = sum(s["rmb"] for s in g["sells"])
        cat = g["buys"][0]["category"] if g["buys"] else (g["sells"][0]["category"] if g["sells"] else "")
        is_consumable = (cat == "消耗品")
        # ── 已出手：已实现利润 ──
        sold_buys = [b for b in g["buys"] if b["status"] == "已出手"]
        sold_cost = sum(b["rmb"] for b in sold_buys)
        realized = sell_total_rmb - sold_cost
        # ── 已消耗：仅 status=="已消耗"，与 Dashboard 口径一致 ──
        sold_ids = {b["id"] for b in sold_buys}
        consumed_buys = [b for b in g["buys"] if b["id"] not in sold_ids and
                         b["status"] == "已消耗"]
        consumed_cost = sum(b["rmb"] for b in consumed_buys)
        realized -= consumed_cost
        # ── 浮动盈亏：排除已出手和已消耗，仅真正持有中 ──
        consumed_ids = {b["id"] for b in consumed_buys}
        held_buys = [b for b in g["buys"] if b["id"] not in sold_ids and b["id"] not in consumed_ids]
        held_rmb = sum(b["current_rmb"] for b in held_buys)
        held_cost = sum(b["rmb"] for b in held_buys)
        unrealized = held_rmb - held_cost
        total_profit = round(realized + unrealized, 2)
        profit_rate = round(total_profit / buy_total_rmb * 100, 1) if buy_total_rmb > 0 else 0

        result.append({
            "item_name": name, "category": cat,
            "is_consumable": is_consumable,
            "buy_count": len(g["buys"]),
            "sell_count": len(g["sells"]),
            "consumed_count": len(consumed_buys),
            "consumed_cost": round(consumed_cost, 2),
            "buy_total_rmb": round(buy_total_rmb, 2),
            "sell_total_rmb": round(sell_total_rmb, 2),
            "held_rmb": round(held_rmb, 2),
            "held_cost": round(held_cost, 2),
            "realized": round(realized, 2),
            "unrealized": round(unrealized, 2),
            "total_profit": total_profit,
            "profit_rate": profit_rate,
        })
    result.sort(key=lambda x: abs(x["total_profit"]), reverse=True)
    return result


# ── 资产趋势 ─────────────────────────────────────────────────────
@app.get("/api/asset-trend")
def asset_trend(db: Session = Depends(get_db)):
    """按月计算资产快照：币持仓市值、物品持仓市值、总投入、综合盈亏"""
    snapshot_rows = db.query(AssetSnapshot).order_by(AssetSnapshot.snapshot_month).all()
    if snapshot_rows:
        latest_by_month = {}
        for row in snapshot_rows:
            latest_by_month[row.snapshot_month] = row
        return [{
            "month": row.snapshot_month,
            "rate": round(row.rate, 2),
            "coin_hold": round(row.coin_hold, 2),
            "coin_value": round(row.coin_value, 2),
            "held_value": round(row.held_value, 2),
            "total_asset": round(row.total_asset, 2),
            "total_invest": round(row.total_invest, 2),
            "total_cashout": round(row.total_cashout, 2),
            "pnl": round(row.pnl, 2),
        } for _, row in sorted(latest_by_month.items())]

    # 获取初始设置（与 dashboard 使用相同的 key）
    def _sf(key, default=0):
        row = db.query(Setting).filter(Setting.key == key).first()
        return float(row.value) if row and row.value else default

    init_coin = _sf("initial_coin_balance")
    init_invest = _sf("initial_investment")
    # 校准调整 & 零散支出（与 dashboard 一致）
    calibrations = _get_json_setting(db, "coin_calibrations")
    coin_calibration = sum(r.get("diff", 0) for r in calibrations)
    misc_exps = _get_json_setting(db, "misc_coin_expenses")
    coin_misc_expense = sum(r.get("amount", 0) for r in misc_exps)

    # 获取每月最后一条币价
    prices = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(PriceHistory.timestamp).all()
    monthly_rate = {}
    for p in prices:
        if not p.timestamp:
            continue
        key = p.timestamp.strftime("%Y-%m")
        monthly_rate[key] = p.price_10e or p.price or 0

    # 获取所有交易（按时间排序）
    all_tx = db.query(Transaction).filter(Transaction.deleted_at == None).order_by(Transaction.timestamp).all()
    # 获取所有物品
    all_items = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).order_by(CategoryItem.timestamp).all()
    # 获取所有副本收益
    all_dungeons = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).order_by(DungeonRevenue.timestamp).all()

    # 收集所有出现过的月份（必须包含所有数据源，否则会遗漏计算）
    all_months = set()
    for p in prices:
        if p.timestamp:
            all_months.add(p.timestamp.strftime("%Y-%m"))
    for t in all_tx:
        if t.timestamp:
            all_months.add(t.timestamp.strftime("%Y-%m"))
    for dg in all_dungeons:
        if dg.timestamp:
            all_months.add(dg.timestamp.strftime("%Y-%m"))
    for item in all_items:
        if item.timestamp:
            all_months.add(item.timestamp.strftime("%Y-%m"))

    if not all_months:
        return []

    sorted_months = sorted(all_months)

    # 逐月计算（coin_hold 初始值与 dashboard 一致：初始币 + 校准 - 零散支出）
    result = []
    coin_hold = init_coin + coin_calibration - coin_misc_expense
    total_invest = init_invest
    total_cashout = 0

    for month in sorted_months:
        # 本月交易影响
        for t in all_tx:
            if not t.timestamp or getattr(t, 'status', 'normal') == 'void':
                continue
            t_month = t.timestamp.strftime("%Y-%m")
            if t_month != month:
                continue
            rmb = _tx_rmb(t)
            direction = getattr(t, 'direction', None) or 'expense'
            if t.type == "买币":
                coin_hold += t.quantity
                total_invest += rmb
            elif t.type == "卖币":
                coin_hold -= t.quantity
                total_cashout += rmb
            elif t.type == "三国点充值":
                total_invest += rmb
            elif t.type == "三国点售卖":
                total_cashout += rmb
            else:
                if direction == 'income':
                    total_cashout += rmb
                else:
                    total_invest += rmb

        # 物品买卖对币持仓的影响
        for item in all_items:
            if not item.timestamp:
                continue
            i_month = item.timestamp.strftime("%Y-%m")
            if i_month != month:
                continue
            coin = _item_coin_cost(item)
            if item.type == "卖出":
                coin_hold += coin
            elif item.type == "买入":
                coin_hold -= coin
            if (getattr(item, 'cost_mode', 'coin') or 'coin') == 'rmb_direct':
                if item.type == '买入':
                    total_invest += _item_purchase_rmb(item)
                elif item.type == '卖出':
                    total_cashout += _item_purchase_rmb(item)

        # 副本收益的币自动计入coin_hold（与dashboard一致）
        for dg in all_dungeons:
            if not dg.timestamp:
                continue
            if dg.timestamp.strftime("%Y-%m") == month:
                coin_hold += dg.revenue_coin or 0

        # 当月币价（取当月最后的，或用最近已知的）
        rate = monthly_rate.get(month, 0)
        if rate == 0:
            for prev_m in sorted(monthly_rate.keys(), reverse=True):
                if prev_m <= month:
                    rate = monthly_rate[prev_m]
                    break

        # 物品持有市值（截至本月）
        # 使用 status_changed_at 判断历史状态：如果物品在本月之后才变更状态，
        # 则在本月快照中仍视为"持有中"
        month_end = month + "-31"
        held_value = 0
        for item in all_items:
            if not item.timestamp or item.type != "买入":
                continue
            if item.timestamp.strftime("%Y-%m") > month:
                continue
            hs = getattr(item, 'holding_status', '持有中')
            sca = getattr(item, 'status_changed_at', None)
            # 当前仍持有中 → 计入
            # 已出手/已消耗 但状态变更时间在本月之后 → 在本月时仍持有，计入
            # 已出手/已消耗 但无变更时间记录(历史数据) → 保守计入
            if hs == '持有中' or (sca and sca.strftime("%Y-%m") > month) or (hs != '持有中' and not sca):
                held_value += _item_current_rmb(item, rate)

        coin_value = coin_hold * rate
        total_asset = coin_value + held_value
        pnl = total_asset + total_cashout - total_invest

        # 仅输出有币价数据的月份，无币价的月份仅用于累积计算
        if rate > 0:
            snapshot = {
                "month": month,
                "rate": round(rate, 2),
                "coin_hold": round(coin_hold, 2),
                "coin_value": round(coin_value, 2),
                "held_value": round(held_value, 2),
                "total_asset": round(total_asset, 2),
                "total_invest": round(total_invest, 2),
                "total_cashout": round(total_cashout, 2),
                "pnl": round(pnl, 2),
            }
            _write_asset_snapshot(db, month, snapshot, source='system', notes='系统按历史数据回算生成')
            result.append(snapshot)
    db.commit()
    return result


@app.post("/api/asset-snapshots/rebuild")
def rebuild_asset_snapshots(db: Session = Depends(get_db)):
    db.query(AssetSnapshot).delete()
    db.commit()
    rows = asset_trend(db)
    return {"ok": True, "count": len(rows)}


# ── 副本效率对比 ─────────────────────────────────────────────────
@app.get("/api/dungeon-efficiency")
def dungeon_efficiency(db: Session = Depends(get_db)):
    """按副本名统计平均收益、总次数、总收益"""
    from collections import defaultdict
    rate = _latest_rate(db)
    rows = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).all()
    groups = defaultdict(lambda: {"runs": 0, "total_coin": 0, "total_drops": defaultdict(int)})
    for r in rows:
        g = groups[r.dungeon_name]
        g["runs"] += 1
        g["total_coin"] += r.revenue_coin or 0
        drops = _parse_drops(r.revenue_items)
        for d in drops:
            if d.get("name"):
                g["total_drops"][d["name"]] += d.get("qty", 1)

    result = []
    for name, g in groups.items():
        avg_coin = g["total_coin"] / g["runs"] if g["runs"] else 0
        result.append({
            "dungeon_name": name,
            "runs": g["runs"],
            "total_coin": round(g["total_coin"], 2),
            "avg_coin": round(avg_coin, 2),
            "total_rmb": round(g["total_coin"] * rate, 2),
            "avg_rmb": round(avg_coin * rate, 2),
            "top_drops": sorted([{"name": k, "qty": v} for k, v in g["total_drops"].items()],
                                key=lambda x: x["qty"], reverse=True)[:5],
        })
    result.sort(key=lambda x: x["avg_coin"], reverse=True)
    return result


# ── 数据导出 CSV ─────────────────────────────────────────────────
import csv, io

@app.get("/api/export/transactions")
def export_transactions(db: Session = Depends(get_db)):
    rows = db.query(Transaction).filter(Transaction.deleted_at == None).order_by(desc(Transaction.timestamp)).all()
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["ID", "时间", "类型", "方向", "数量(亿)", "单价(元/亿)", "总金额(元)", "渠道", "状态", "备注"])
    for r in rows:
        w.writerow([r.id, r.timestamp.strftime("%Y-%m-%d %H:%M") if r.timestamp else "",
                    r.type, getattr(r, 'direction', ''), r.quantity, r.unit_price,
                    round(_tx_rmb(r), 2), getattr(r, 'channel', ''),
                    r.status or 'normal', r.notes or ''])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=transactions.csv"})


@app.get("/api/export/category-items")
def export_category_items(db: Session = Depends(get_db)):
    rows = db.query(CategoryItem).filter(CategoryItem.deleted_at == None).order_by(desc(CategoryItem.timestamp)).all()
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["ID", "时间", "分类", "项目组", "类型", "物品名", "币价(亿)", "购买时币价(元/亿)", "数量", "预估价值(亿)", "持有状态", "备注"])
    for r in rows:
        w.writerow([r.id, r.timestamp.strftime("%Y-%m-%d %H:%M") if r.timestamp else "",
                    r.category, getattr(r, 'item_group', '') or '', r.type, r.item_name, r.coin_price, r.purchase_rate,
                    getattr(r, 'quantity', 1) or 1, r.estimated_value or 0,
                    getattr(r, 'holding_status', '持有中'), r.notes or ''])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=category_items.csv"})


@app.get("/api/export/prices")
def export_prices(db: Session = Depends(get_db)):
    rows = db.query(PriceHistory).filter(PriceHistory.deleted_at == None).order_by(desc(PriceHistory.timestamp)).all()
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["ID", "时间", "币价(元/亿)", "10E+币价", "回收币价", "来源", "备注"])
    for r in rows:
        w.writerow([r.id, r.timestamp.strftime("%Y-%m-%d %H:%M") if r.timestamp else "",
                    r.price, getattr(r, 'price_10e', 0) or 0, getattr(r, 'price_recycle', 0) or 0,
                    r.source or '', r.notes or ''])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=prices.csv"})


@app.get("/api/export/dungeons")
def export_dungeons(db: Session = Depends(get_db)):
    rows = db.query(DungeonRevenue).filter(DungeonRevenue.deleted_at == None).order_by(desc(DungeonRevenue.timestamp)).all()
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["ID", "时间", "副本名", "收益(亿)", "掉落物品", "备注"])
    for r in rows:
        drops = ""
        try:
            dl = json.loads(r.revenue_items) if r.revenue_items else []
            drops = ", ".join(f"{d.get('name','')}x{d.get('qty',0)}" for d in dl if d.get('name'))
        except Exception:
            drops = r.revenue_items or ''
        w.writerow([r.id, r.timestamp.strftime("%Y-%m-%d %H:%M") if r.timestamp else "",
                    r.dungeon_name, r.revenue_coin, drops, r.notes or ''])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=dungeons.csv"})


# ── 启动 ───────────────────────────────────────────────────────────
# ── 每日备份 ───────────────────────────────────────────────────────
def _daily_backup():
    import time as _time
    BACKUP_DIR = Path("F:\\三国数据库")
    while True:
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        _refresh_local_latest_backup(now)
        # 本地备份兜底（始终执行）
        try:
            _LOCAL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            local_dst = _LOCAL_BACKUP_DIR / f"qq_sanguo_{today}.db"
            _safe_backup(DB_PATH, local_dst)
            local_backups = sorted(_LOCAL_BACKUP_DIR.glob("qq_sanguo_*.db"))
            if len(local_backups) > 10:
                for old in local_backups[:-10]:
                    old.unlink(missing_ok=True)
        except Exception as e:
            logging.warning(f"本地备份失败: {e}")
        # 远程备份（F盘）
        try:
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            dst = BACKUP_DIR / f"qq_sanguo_{today}.db"
            _safe_backup(DB_PATH, dst)
            all_backups = sorted(BACKUP_DIR.glob("qq_sanguo_*.db"))
            if len(all_backups) > 30:
                for old in all_backups[:-30]:
                    old.unlink(missing_ok=True)
            _last_daily_backup_status.update(time=now.isoformat(), path=str(dst), success=True, error=None)
            logging.info(f"数据库已备份到 {dst}")
        except Exception as e:
            _last_daily_backup_status.update(time=now.isoformat(), path=str(BACKUP_DIR), success=False, error=str(e))
            logging.warning(f"远程备份失败: {e}")
        _time.sleep(86400)  # 24小时


@app.get("/api/backup-status")
def get_backup_status():
    db_size = 0
    try:
        db_size = DB_PATH.stat().st_size
    except Exception:
        pass
    local_dir = _LOCAL_BACKUP_DIR
    local_count = 0
    local_latest = None
    local_mirror_exists = False
    local_mirror_mtime = None
    try:
        local_files = sorted(local_dir.glob("qq_sanguo_*.db"))
        local_count = len(local_files)
        if local_files:
            local_latest = local_files[-1].name
    except Exception:
        pass
    try:
        if _LOCAL_LATEST_MIRROR.exists():
            local_mirror_exists = True
            local_mirror_mtime = datetime.fromtimestamp(_LOCAL_LATEST_MIRROR.stat().st_mtime).isoformat()
    except Exception:
        pass
    return {
        "daily_backup": _last_daily_backup_status,
        "sync_backup": _last_backup_status,
        "local_latest_mirror": _last_local_mirror_status,
        "db_size_mb": round(db_size / 1024 / 1024, 2),
        "active_db_path": str(DB_PATH),
        "active_db_source": DB_SOURCE,
        "local_backup_dir": str(local_dir),
        "local_backup_count": local_count,
        "local_backup_latest": local_latest,
        "local_mirror_exists": local_mirror_exists,
        "local_mirror_mtime": local_mirror_mtime,
    }


# ══ 笔记本 API ══════════════════════════════════════════════════════
class NoteCreate(BaseModel):
    title: str = '未命名笔记'
    content: Optional[str] = ''
    folder: Optional[str] = '默认'
    pinned: Optional[int] = 0
    tags: Optional[str] = ''
    color: Optional[str] = ''

class NoteUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    folder: Optional[str] = None
    pinned: Optional[int] = None
    tags: Optional[str] = None
    color: Optional[str] = None

@app.get("/api/notes")
def list_notes(folder: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(Note).filter(Note.deleted_at == None)
    if folder:
        q = q.filter(Note.folder == folder)
    notes = q.order_by(Note.pinned.desc(), Note.updated_at.desc()).all()
    result = []
    for n in notes:
        raw = (n.content or '')
        import re
        preview = re.sub(r'<[^>]+>', '', raw)[:120]
        result.append({
            "id": n.id, "title": n.title,
            "folder": n.folder or '默认', "pinned": n.pinned or 0,
            "tags": n.tags or '', "color": n.color or '',
            "created_at": n.created_at.isoformat() if n.created_at else None,
            "updated_at": n.updated_at.isoformat() if n.updated_at else None,
            "preview": preview,
        })
    return result

@app.get("/api/notes/folders")
def list_note_folders(db: Session = Depends(get_db)):
    rows = db.query(Note.folder, func.count(Note.id)).filter(Note.deleted_at == None).group_by(Note.folder).all()
    return [{"name": r[0] or '默认', "count": r[1]} for r in rows]

@app.get("/api/notes/{note_id}")
def get_note(note_id: int, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at == None).first()
    if not n:
        raise HTTPException(404, "笔记不存在")
    return {
        "id": n.id, "title": n.title, "content": n.content or '',
        "folder": n.folder or '默认', "pinned": n.pinned or 0,
        "tags": n.tags or '', "color": n.color or '',
        "created_at": n.created_at.isoformat() if n.created_at else None,
        "updated_at": n.updated_at.isoformat() if n.updated_at else None,
    }

@app.post("/api/notes")
def create_note(body: NoteCreate, db: Session = Depends(get_db)):
    n = Note(title=body.title, content=body.content, folder=body.folder or '默认',
             pinned=body.pinned or 0, tags=body.tags or '', color=body.color or '')
    db.add(n)
    db.commit()
    db.refresh(n)
    return {"id": n.id, "title": n.title}

@app.put("/api/notes/{note_id}")
def update_note(note_id: int, body: NoteUpdate, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at == None).first()
    if not n:
        raise HTTPException(404, "笔记不存在")
    if body.title is not None: n.title = body.title
    if body.content is not None: n.content = body.content
    if body.folder is not None: n.folder = body.folder
    if body.pinned is not None: n.pinned = body.pinned
    if body.tags is not None: n.tags = body.tags
    if body.color is not None: n.color = body.color
    n.updated_at = datetime.now()
    db.commit()
    return {"id": n.id, "title": n.title}

@app.delete("/api/notes/{note_id}")
def delete_note(note_id: int, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at == None).first()
    if not n:
        raise HTTPException(404, "笔记不存在或已删除")
    n.deleted_at = datetime.now()
    db.commit()
    return {"ok": True}

@app.post("/api/notes/{note_id}/images")
def upload_note_image(note_id: int, body: ImageUpload, db: Session = Depends(get_db)):
    note = _get_active_row(db, Note, note_id, "笔记不存在")
    img = NoteImage(note_id=note.id, image_data=body.image_data)
    db.add(img)
    db.commit()
    db.refresh(img)
    return {"id": img.id}

@app.delete("/api/note-images/{image_id}")
def delete_note_image(image_id: int, db: Session = Depends(get_db)):
    img = db.query(NoteImage).filter(NoteImage.id == image_id).first()
    if img:
        db.delete(img)
        db.commit()
    return {"ok": True}


# ══ 全局搜索 ══════════════════════════════════════════════════════
@app.get("/api/search")
def global_search(q: str = "", db: Session = Depends(get_db)):
    if not q.strip():
        return {"transactions": [], "items": [], "notes": []}
    like = f"%{q}%"
    txs = db.query(Transaction).filter(
        Transaction.deleted_at == None,
        or_(Transaction.type.ilike(like), Transaction.notes.ilike(like), Transaction.channel.ilike(like))
    ).order_by(Transaction.timestamp.desc()).limit(20).all()
    items = db.query(CategoryItem).filter(
        CategoryItem.deleted_at == None,
        CategoryItem.item_name.ilike(like)
    ).order_by(CategoryItem.timestamp.desc()).limit(20).all()
    notes = db.query(Note).filter(
        Note.deleted_at == None,
        or_(Note.title.ilike(like), Note.content.ilike(like), Note.tags.ilike(like))
    ).order_by(Note.updated_at.desc()).limit(20).all()
    import re
    return {
        "transactions": [{"id": t.id, "description": (t.type or '')+((' · '+t.notes) if t.notes else ''),
                          "amount": float(t.quantity or 0) * float(t.unit_price or 0),
                          "type": t.type, "date": t.timestamp.isoformat() if t.timestamp else None} for t in txs],
        "items": [{"id": i.id, "name": i.item_name, "category": i.category, "type": i.type,
                   "status": getattr(i, 'holding_status', '持有中')} for i in items],
        "notes": [{"id": n.id, "title": n.title, "folder": n.folder,
                   "preview": re.sub(r'<[^>]+>', '', n.content or '')[:80]} for n in notes],
    }

# ══ 批量操作 ══════════════════════════════════════════════════════
class BatchUpdateBody(BaseModel):
    ids: List[int]
    holding_status: Optional[str] = None
    category: Optional[str] = None
    item_group: Optional[str] = None

@app.post("/api/items/batch-update")
def batch_update_items(body: BatchUpdateBody, db: Session = Depends(get_db)):
    if body.holding_status is None and body.category is None and body.item_group is None:
        raise HTTPException(400, "至少提供一个更新字段")
    if body.holding_status is not None and body.holding_status not in ("持有中", "已出手", "已消耗"):
        raise HTTPException(400, f"无效持有状态: {body.holding_status}")
    if body.category is not None:
        valid_cats = _get_categories(db)
        if valid_cats and body.category not in valid_cats:
            raise HTTPException(400, f"无效分类: {body.category}")
    items = db.query(CategoryItem).filter(CategoryItem.id.in_(body.ids), CategoryItem.deleted_at == None).all()
    touched_sell_groups = set()
    for item in items:
        if body.holding_status is not None and item.type == '买入':
            item.holding_status = body.holding_status
            if body.holding_status in ("已出手", "已消耗"):
                item.status_changed_at = datetime.now()
            else:
                item.status_changed_at = None
        if body.category is not None and body.category != item.category:
            if item.type == '卖出':
                touched_sell_groups.add((item.category, item.item_name))
            item.category = body.category
            if item.type == '卖出':
                touched_sell_groups.add((item.category, item.item_name))
        if body.item_group is not None:
            item.item_group = str(body.item_group or '').strip()[:50]
    for cat_name, item_name in touched_sell_groups:
        _sync_item_sell_status(db, cat_name, item_name)
    db.commit()
    return {"updated": len(items)}

class BatchDeleteBody(BaseModel):
    ids: List[int]

@app.post("/api/items/batch-delete")
def batch_delete_items(body: BatchDeleteBody, db: Session = Depends(get_db)):
    items = db.query(CategoryItem).filter(CategoryItem.id.in_(body.ids), CategoryItem.deleted_at == None).all()
    touched_sell_groups = set()
    for item in items:
        _audit(db, 'category_items', item.id, 'delete', {"item_name": item.item_name, "category": item.category, "type": item.type})
        if item.type == '卖出':
            touched_sell_groups.add((item.category, item.item_name))
        item.deleted_at = datetime.now()
    for cat_name, item_name in touched_sell_groups:
        _sync_item_sell_status(db, cat_name, item_name)
    db.commit()
    return {"deleted": len(items)}

# ══ 资产分布 ══════════════════════════════════════════════════════
@app.get("/api/asset-distribution")
def asset_distribution(db: Session = Depends(get_db)):
    rate = _latest_rate(db)
    items = db.query(CategoryItem).filter(
        CategoryItem.deleted_at == None,
        CategoryItem.type == "买入",
        CategoryItem.holding_status == "持有中"
    ).all()
    by_cat = {}
    by_item = {}
    for item in items:
        cat = item.category or "未分类"
        rmb = _item_current_rmb(item, rate)
        cost = _item_purchase_rmb(item)
        by_cat[cat] = by_cat.get(cat, {"value": 0, "cost": 0, "count": 0})
        by_cat[cat]["value"] += rmb
        by_cat[cat]["cost"] += cost
        by_cat[cat]["count"] += 1
        key = f"{cat}|{item.item_name}"
        by_item[key] = by_item.get(key, {"value": 0, "cost": 0, "count": 0, "name": item.item_name, "cat": cat})
        by_item[key]["value"] += rmb
        by_item[key]["cost"] += cost
        by_item[key]["count"] += 1
    cat_dist = [{"category": k, "value": round(v["value"], 2), "cost": round(v["cost"], 2),
                 "count": v["count"], "pnl": round(v["value"] - v["cost"], 2)} for k, v in by_cat.items()]
    cat_dist.sort(key=lambda x: -x["value"])
    item_dist = [{"name": v["name"], "category": v["cat"], "value": round(v["value"], 2),
                  "cost": round(v["cost"], 2), "count": v["count"]} for v in by_item.values()]
    item_dist.sort(key=lambda x: -x["value"])
    return {"by_category": cat_dist, "by_item": item_dist[:30]}

# ══ 交易模板 ══════════════════════════════════════════════════════
@app.get("/api/tx-templates")
def get_tx_templates(db: Session = Depends(get_db)):
    import json
    s = db.query(Setting).filter(Setting.key == "tx_templates").first()
    if s:
        try:
            return json.loads(s.value)
        except Exception:
            return []
    return [
        {"name": "日常副本收益", "type": "副本", "description": "每日副本固定收益", "amount": 0, "direction": "income"},
        {"name": "购买消耗品", "type": "消耗", "description": "", "amount": 0, "direction": "expense"},
        {"name": "倒货买入", "type": "买入", "description": "", "amount": 0, "direction": "expense"},
        {"name": "倒货卖出", "type": "卖出", "description": "", "amount": 0, "direction": "income"},
    ]

@app.put("/api/tx-templates")
def save_tx_templates(templates: list, db: Session = Depends(get_db)):
    import json
    s = db.query(Setting).filter(Setting.key == "tx_templates").first()
    val = json.dumps(templates, ensure_ascii=False)
    if s:
        s.value = val
    else:
        db.add(Setting(key="tx_templates", value=val))
    db.commit()
    return {"ok": True}

# ══ 笔记本扩展API ══════════════════════════════════════════════════
@app.post("/api/notes/{note_id}/duplicate")
def duplicate_note(note_id: int, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at == None).first()
    if not n:
        raise HTTPException(404, "笔记不存在")
    dup = Note(title=n.title + " (副本)", content=n.content, folder=n.folder,
               pinned=0, tags=n.tags, color=n.color)
    db.add(dup)
    db.commit()
    db.refresh(dup)
    return {"id": dup.id, "title": dup.title}

@app.get("/api/notes-trash")
def list_trash_notes(db: Session = Depends(get_db)):
    notes = db.query(Note).filter(Note.deleted_at != None).order_by(Note.deleted_at.desc()).all()
    import re
    return [{"id": n.id, "title": n.title, "folder": n.folder,
             "deleted_at": n.deleted_at.isoformat() if n.deleted_at else None,
             "preview": re.sub(r'<[^>]+>', '', n.content or '')[:80]} for n in notes]

@app.post("/api/notes/{note_id}/restore")
def restore_note(note_id: int, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at != None).first()
    if not n:
        raise HTTPException(404, "笔记不存在或未删除")
    n.deleted_at = None
    n.updated_at = datetime.now()
    db.commit()
    return {"id": n.id, "title": n.title}

@app.delete("/api/notes/{note_id}/permanent")
def permanent_delete_note(note_id: int, db: Session = Depends(get_db)):
    n = db.query(Note).filter(Note.id == note_id, Note.deleted_at != None).first()
    if not n:
        raise HTTPException(404, "笔记不存在或未移入回收站")
    db.query(NoteImage).filter(NoteImage.note_id == n.id).delete()
    db.delete(n)
    db.commit()
    return {"ok": True}


import os as _os, subprocess as _subprocess

@app.post("/api/restart")
def restart_server(background_tasks: BackgroundTasks):
    """重启服务 - 当前进程退出后重新拉起服务"""

    def _resolve_restart_target():
        if getattr(sys, 'frozen', False):
            exe_path = _os.path.abspath(sys.executable)
            return [exe_path], _os.path.dirname(exe_path)
        base_dir = _os.path.dirname(_os.path.abspath(__file__))
        main_py = _os.path.join(base_dir, "main.py")
        return [_os.path.abspath(sys.executable), main_py], base_dir

    def _do_restart():
        import time
        cmd_args, workdir = _resolve_restart_target()
        if _os.name == 'nt':
            create_no_window = getattr(_subprocess, 'CREATE_NO_WINDOW', 0x08000000)
            detached_process = getattr(_subprocess, 'DETACHED_PROCESS', 0x00000008)
            create_breakaway = getattr(_subprocess, 'CREATE_BREAKAWAY_FROM_JOB', 0x01000000)
            create_new_group = getattr(_subprocess, 'CREATE_NEW_PROCESS_GROUP', 0x00000200)
            creation_flags = create_no_window | detached_process | create_breakaway | create_new_group
            logging.info("restart spawn target=%s cwd=%s", cmd_args, workdir)
            _subprocess.Popen(
                cmd_args,
                cwd=workdir,
                stdin=_subprocess.DEVNULL,
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
                creationflags=creation_flags,
                close_fds=True,
            )
        else:
            _subprocess.Popen(
                cmd_args,
                cwd=workdir,
                stdin=_subprocess.DEVNULL,
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        time.sleep(0.5)
        _os._exit(0)

    background_tasks.add_task(_do_restart)
    return {"ok": True, "message": "服务正在重启，请稍候..."}


@app.post("/api/shutdown")
def shutdown_server(background_tasks: BackgroundTasks):
    """关闭服务 - 在响应返回后执行一次本地镜像备份并退出当前进程"""

    def _do_shutdown():
        import time
        try:
            _refresh_local_latest_backup(datetime.now())
        except Exception as e:
            logging.warning(f"关闭前写入本地最新镜像失败: {e}")
        time.sleep(0.35)
        _os._exit(0)

    background_tasks.add_task(_do_shutdown)
    return {"ok": True, "message": "程序正在关闭..."}


if __name__ == "__main__":
    import socket, webbrowser, threading, time as _t
    from urllib import request as _url_request

    preferred_port = int(os.environ.get("QQSG_PORT", "8000") or 8000)
    bind_host = os.environ.get("QQSG_HOST", "0.0.0.0") or "0.0.0.0"

    def _lan_urls(p):
        urls = []
        try:
            hostname = socket.gethostname()
            for ip in socket.gethostbyname_ex(hostname)[2]:
                if ip and not ip.startswith("127.") and ":" not in ip:
                    urls.append(f"http://{ip}:{p}")
        except Exception:
            pass
        return sorted(set(urls))

    def _port_in_use(p):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('127.0.0.1', p)) == 0

    def _health_ok(p):
        try:
            with _url_request.urlopen(f'http://127.0.0.1:{p}/api/health', timeout=1.5) as resp:
                if getattr(resp, 'status', 200) != 200:
                    return False
                data = json.loads(resp.read().decode('utf-8', errors='replace') or '{}')
                return data.get('status') == 'ok' and data.get('api_signature') == APP_API_SIGNATURE
        except Exception:
            return False

    def _find_existing_service(start_port=8000, max_tries=20):
        for candidate in range(start_port, start_port + max_tries):
            if _health_ok(candidate):
                return candidate
        return None

    def _find_available_port(start_port: int, max_tries: int = 20):
        for candidate in range(start_port, start_port + max_tries):
            if not _port_in_use(candidate):
                return candidate
        raise RuntimeError(f"{start_port}-{start_port + max_tries - 1} 范围内没有可用端口")

    # 启动备份线程
    _backup_thread = threading.Thread(target=_daily_backup, daemon=True)
    _backup_thread.start()

    existing_port = _find_existing_service(8000, 20)
    if existing_port is not None:
        print("=" * 50)
        print("  QQ三国个人信息 Web版")
        print(f"  http://127.0.0.1:{existing_port}")
        for url in _lan_urls(existing_port):
            print(f"  局域网访问: {url}")
        print("=" * 50)
        if not os.environ.get('QQSG_NO_BROWSER'):
            threading.Timer(0.3, lambda: webbrowser.open(f'http://127.0.0.1:{existing_port}')).start()
        raise SystemExit(0)

    port = preferred_port
    if _port_in_use(port):
        deadline = _t.time() + 6
        while _t.time() < deadline:
            if _health_ok(port):
                print("=" * 50)
                print("  QQ三国个人信息 Web版")
                print(f"  http://127.0.0.1:{port}")
                for url in _lan_urls(port):
                    print(f"  局域网访问: {url}")
                print("=" * 50)
                if not os.environ.get('QQSG_NO_BROWSER'):
                    threading.Timer(0.3, lambda: webbrowser.open(f'http://127.0.0.1:{port}')).start()
                raise SystemExit(0)
            if not _port_in_use(port):
                break
            _t.sleep(0.5)
        if _port_in_use(port):
            port = _find_available_port(max(8000, preferred_port + 1), 20)
            logging.warning(f"默认端口 {preferred_port} 已被占用，自动切换到 {port}")
    os.environ["QQSG_PORT"] = str(port)

    print("=" * 50)
    print("  QQ三国个人信息 Web版")
    print(f"  http://127.0.0.1:{port}")
    for url in _lan_urls(port):
        print(f"  局域网访问: {url}")
    print("=" * 50)
    if not os.environ.get('QQSG_NO_BROWSER'):
        threading.Timer(1.5, lambda: webbrowser.open(f'http://127.0.0.1:{port}')).start()

    # 自动重启保护：如果服务异常退出，自动重新启动
    _max_restarts = 10
    _restart_count = 0
    while _restart_count < _max_restarts:
        try:
            uvicorn.run(
                app,
                host=bind_host,
                port=port,
                timeout_keep_alive=30,     # 空闲连接30秒后关闭，防止连接堆积
                limit_concurrency=100,     # 最大并发连接数
                limit_max_requests=5000,   # 每5000个请求后自动重启worker，防止内存泄漏
            )
            break  # 正常退出（Ctrl+C）时不重启
        except KeyboardInterrupt:
            break
        except SystemExit:
            break
        except Exception as e:
            _restart_count += 1
            logging.error(f"服务异常退出(第{_restart_count}次): {e}")
            if _restart_count < _max_restarts:
                logging.info(f"3秒后自动重启...")
                _t.sleep(3)
                if port != preferred_port and not _port_in_use(preferred_port):
                    port = preferred_port
                    os.environ["QQSG_PORT"] = str(port)
            else:
                logging.error(f"已达最大重启次数({_max_restarts})，服务停止")
                break
