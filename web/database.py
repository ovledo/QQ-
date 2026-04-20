# -*- coding: utf-8 -*-
"""SQLite 数据库模型与会话管理"""

from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Text, inspect, text as sa_text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from pathlib import Path
import sys

import os
_DB_NAME = "qq_sanguo.db"
_DB_POINTER_NAME = "qqsg_db_path.txt"
_db_dir_override = (os.environ.get('QQSG_DB_DIR') or '').strip()
_db_file_override = (os.environ.get('QQSG_DB_FILE') or '').strip()


def _normalize_db_candidate(raw: str):
    raw = (raw or '').strip().strip('"')
    if not raw:
        return None
    candidate = Path(raw).expanduser()
    if candidate.suffix.lower() != '.db':
        candidate = candidate / _DB_NAME
    return candidate


def _pick_existing_db(candidates):
    existing = []
    seen = set()
    for candidate in candidates:
        if not candidate or not candidate.exists():
            continue
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        existing.append(candidate)
    if not existing:
        return None
    return max(existing, key=lambda p: (p.stat().st_size, p.stat().st_mtime))


def _write_db_pointer(pointer_file: Path, db_file: Path):
    try:
        pointer_file.write_text(str(db_file), encoding='utf-8')
    except Exception:
        pass


def _resolve_db_path():
    dir_override = _normalize_db_candidate(_db_dir_override)
    file_override = _normalize_db_candidate(_db_file_override)
    if dir_override:
        dir_override.parent.mkdir(parents=True, exist_ok=True)
        return dir_override, 'env_dir'
    if file_override:
        file_override.parent.mkdir(parents=True, exist_ok=True)
        return file_override, 'env_file'
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).resolve().parent
        pointer_file = exe_dir / _DB_POINTER_NAME
        pinned = None
        try:
            if pointer_file.exists():
                pinned = _normalize_db_candidate(pointer_file.read_text(encoding='utf-8'))
        except Exception:
            pinned = None
        if pinned and pinned.exists():
            return pinned, 'pinned'
        portable = exe_dir / _DB_NAME
        if portable.exists():
            _write_db_pointer(pointer_file, portable)
            return portable, 'portable'
        appdata_db = Path(os.environ.get('LOCALAPPDATA', Path.home())) / "QQ三国资产管理" / _DB_NAME
        sibling_candidates = [
            exe_dir.parent / _DB_NAME,
            exe_dir.parent.parent / _DB_NAME,
            exe_dir.parent.parent / 'data' / _DB_NAME,
        ]
        chosen = _pick_existing_db([*sibling_candidates, appdata_db])
        if chosen:
            _write_db_pointer(pointer_file, chosen)
            return chosen, 'auto_existing'
        appdata_db.parent.mkdir(parents=True, exist_ok=True)
        _write_db_pointer(pointer_file, appdata_db)
        return appdata_db, 'appdata_new'
    source_db = Path(__file__).resolve().parent / _DB_NAME
    source_db.parent.mkdir(parents=True, exist_ok=True)
    return source_db, 'source'


DB_PATH, DB_SOURCE = _resolve_db_path()
_APP_DIR = DB_PATH.parent
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    pool_recycle=1800,    # 每30分钟回收连接，防止连接失效
    pool_pre_ping=True,   # 使用前检测连接是否存活
)

from sqlalchemy import event as _sa_event
@_sa_event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Transaction(Base):
    """交易流水 — 仅跟踪三国币与三国点的买卖"""
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)
    type = Column(String(20), nullable=False)          # 买币/卖币/三国点充值/三国点售卖/自定义
    direction = Column(String(10), nullable=False, default='expense')  # income/expense
    quantity = Column(Float, nullable=False, default=0) # 亿三国币 or 三国点数量
    unit_price = Column(Float, nullable=False, default=0) # 元/亿 or 折合人民币(总额)
    channel = Column(String(50), nullable=True)        # 商人/玩家 (买卖币); 自由输入 (三国点)
    status = Column(String(10), nullable=False, default='normal')  # normal/void
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, nullable=True, default=None)     # 软删除时间戳


class CategoryItem(Base):
    """分类物品 — 装备/灵兽/元神/灵魄/子女/倒货项目 的交易记录"""
    __tablename__ = "category_items"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)
    category = Column(String(20), nullable=False)      # 装备/灵兽/元神/灵魄/子女/倒货项目
    type = Column(String(10), nullable=False)           # 买入/卖出
    item_name = Column(String(100), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)      # 数量
    cost_mode = Column(String(20), nullable=False, default='coin')      # coin/rmb_direct
    coin_price = Column(Float, nullable=False, default=0)     # 单价 (亿三国币)
    cash_price = Column(Float, nullable=False, default=0)     # 单价 (元)
    purchase_rate = Column(Float, nullable=False, default=0)  # 购买当天币价 (1亿三国币=X元)
    estimated_value = Column(Float, nullable=True, default=0)  # 预估市场价值 (亿三国币)
    estimated_rmb = Column(Float, nullable=True, default=0)    # 预估市场价值 (元)
    holding_status = Column(String(10), nullable=False, default='持有中')  # 持有中/已出手/已消耗
    sell_rate = Column(Float, nullable=True, default=0)           # 卖出当天币价 (1亿三国币=X元)
    status_changed_at = Column(DateTime, nullable=True, default=None)  # 持有状态变更时间
    source_type = Column(String(20), nullable=False, default='手动录入')  # 手动录入/副本掉落/任务奖励/合成产出/拆解产出
    source_ref = Column(String(120), nullable=True, default=None)       # 来源关联标识，如 dungeon:12:天赋丹:1
    valuation_mode = Column(String(20), nullable=True, default='')   # '': 默认, 'coin': 按三国币估值
    channel = Column(String(50), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, nullable=True, default=None)     # 软删除时间戳


class ItemPriceAnchor(Base):
    __tablename__ = "item_price_anchors"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)
    item_name = Column(String(100), nullable=False, index=True)
    category = Column(String(20), nullable=True)
    market_price = Column(Float, nullable=False, default=0)
    price_unit = Column(String(10), nullable=False, default='亿')
    source = Column(String(100), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, nullable=True, default=None)


class AnchorImage(Base):
    """价格锚点截图"""
    __tablename__ = "anchor_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    anchor_id = Column(Integer, nullable=False)              # 关联 item_price_anchors.id
    image_data = Column(Text, nullable=False)                # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class PriceHistory(Base):
    """币价历史 — 精确到时间，支持每天多次记录"""
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)  # 精确到时间
    price = Column(Float, nullable=False)              # 1亿三国币 = X 元
    price_10e = Column(Float, nullable=True, default=0)  # 10E+币价
    price_recycle = Column(Float, nullable=True, default=0)  # 回收币价
    source = Column(String(100), nullable=True)
    notes = Column(Text, nullable=True)
    deleted_at = Column(DateTime, nullable=True, default=None)     # 软删除时间戳


class DungeonRevenue(Base):
    """副本收益记录"""
    __tablename__ = "dungeon_revenues"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)
    dungeon_name = Column(String(100), nullable=False)     # 副本名称
    revenue_coin = Column(Float, nullable=False, default=0) # 收益(亿三国币)
    revenue_items = Column(Text, nullable=True)             # 掉落物品描述
    image_text = Column(Text, nullable=True)                # OCR识别的原始文本
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, nullable=True, default=None)     # 软删除时间戳


class DungeonImage(Base):
    """副本收益截图"""
    __tablename__ = "dungeon_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dungeon_id = Column(Integer, nullable=False)          # 关联 dungeon_revenues.id
    image_data = Column(Text, nullable=False)              # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class TransactionImage(Base):
    """交易流水截图"""
    __tablename__ = "transaction_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    transaction_id = Column(Integer, nullable=False)       # 关联 transactions.id
    image_data = Column(Text, nullable=False)              # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class CategoryItemImage(Base):
    """分类物品截图"""
    __tablename__ = "category_item_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    item_id = Column(Integer, nullable=False)              # 关联 category_items.id
    image_data = Column(Text, nullable=False)              # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class PriceImage(Base):
    """币价记录截图（每条币价仅保留一张）"""
    __tablename__ = "price_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    price_id = Column(Integer, nullable=False)             # 关联 price_history.id
    image_data = Column(Text, nullable=False)              # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class EstimatedValueHistory(Base):
    """物品预估价值变动历史"""
    __tablename__ = "estimated_value_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    item_id = Column(Integer, nullable=False)              # 关联 category_items.id
    old_value = Column(Float, default=0)                   # 旧预估值(亿)
    new_value = Column(Float, default=0)                   # 新预估值(亿)
    timestamp = Column(DateTime, default=datetime.now)


class AuditLog(Base):
    """编辑审计日志 — 记录每次修改的旧值和新值"""
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    table_name = Column(String(50), nullable=False)      # 表名: transactions/category_items/...
    record_id = Column(Integer, nullable=False)           # 被修改记录的ID
    action = Column(String(20), nullable=False)           # edit/delete/restore/void
    changes = Column(Text, nullable=True)                 # JSON: {"field": {"old": x, "new": y}}
    timestamp = Column(DateTime, default=datetime.now)


class AssetSnapshot(Base):
    __tablename__ = "asset_snapshots"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    snapshot_month = Column(String(7), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, default=datetime.now)
    rate = Column(Float, nullable=False, default=0)
    coin_hold = Column(Float, nullable=False, default=0)
    coin_value = Column(Float, nullable=False, default=0)
    held_value = Column(Float, nullable=False, default=0)
    total_asset = Column(Float, nullable=False, default=0)
    total_invest = Column(Float, nullable=False, default=0)
    total_cashout = Column(Float, nullable=False, default=0)
    pnl = Column(Float, nullable=False, default=0)
    source = Column(String(20), nullable=False, default='system')
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)


class Note(Base):
    """笔记本 — 支持富文本和图片"""
    __tablename__ = "notes"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    title = Column(String(200), nullable=False, default='未命名笔记')
    content = Column(Text, nullable=True)              # HTML富文本内容
    folder = Column(String(50), nullable=True, default='默认')  # 文件夹/分类
    pinned = Column(Integer, nullable=False, default=0)          # 0/1 置顶
    tags = Column(String(500), nullable=True, default='')          # 逗号分隔的标签列表
    color = Column(String(20), nullable=True, default='')           # 颜色标签: blue/green/gold/red/purple
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    deleted_at = Column(DateTime, nullable=True, default=None)


class NoteImage(Base):
    """笔记内嵌图片"""
    __tablename__ = "note_images"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    note_id = Column(Integer, nullable=True)             # 关联 notes.id，可为null表示临时上传
    image_data = Column(Text, nullable=False)             # base64编码图片
    created_at = Column(DateTime, default=datetime.now)


class Setting(Base):
    """系统配置"""
    __tablename__ = "settings"

    key = Column(String(50), primary_key=True)
    value = Column(String(500), nullable=True)


def init_db():
    """建表 + 写入默认配置 + 自动迁移新列"""
    Base.metadata.create_all(bind=engine)
    # 自动迁移：为已有表添加新列
    insp = inspect(engine)
    _migrate_cols = [
        ('category_items', 'estimated_value', 'FLOAT DEFAULT 0'),
        ('category_items', 'quantity', 'INTEGER DEFAULT 1'),
        ('category_items', 'cost_mode', "VARCHAR(20) DEFAULT 'coin'"),
        ('category_items', 'cash_price', 'FLOAT DEFAULT 0'),
        ('category_items', 'estimated_rmb', 'FLOAT DEFAULT 0'),
        ('category_items', 'holding_status', "VARCHAR(10) DEFAULT '持有中'"),
        ('category_items', 'source_type', "VARCHAR(20) DEFAULT '手动录入'"),
        ('category_items', 'source_ref', 'VARCHAR(120) DEFAULT NULL'),
        ('price_history', 'price_10e', 'FLOAT DEFAULT 0'),
        ('price_history', 'price_recycle', 'FLOAT DEFAULT 0'),
        ('transactions', 'status', "VARCHAR(10) DEFAULT 'normal'"),
        ('transactions', 'direction', "VARCHAR(10) DEFAULT 'expense'"),
        ('transactions', 'deleted_at', 'DATETIME DEFAULT NULL'),
        ('category_items', 'deleted_at', 'DATETIME DEFAULT NULL'),
        ('item_price_anchors', 'deleted_at', 'DATETIME DEFAULT NULL'),
        ('price_history', 'deleted_at', 'DATETIME DEFAULT NULL'),
        ('dungeon_revenues', 'deleted_at', 'DATETIME DEFAULT NULL'),
        ('category_items', 'sell_rate', 'FLOAT DEFAULT 0'),
        ('category_items', 'status_changed_at', 'DATETIME DEFAULT NULL'),
        ('notes', 'tags', "VARCHAR(500) DEFAULT ''"),
        ('notes', 'color', "VARCHAR(20) DEFAULT ''"),
        ('category_items', 'valuation_mode', "VARCHAR(20) DEFAULT ''"),
    ]
    for tbl, col, typedef in _migrate_cols:
        if tbl in insp.get_table_names():
            cols = [c['name'] for c in insp.get_columns(tbl)]
            if col not in cols:
                with engine.connect() as conn:
                    conn.execute(sa_text(f'ALTER TABLE {tbl} ADD COLUMN {col} {typedef}'))
                    conn.commit()
    db = SessionLocal()
    try:
        defaults = {
            "annual_target": "10000",
            "currency_name": "三国币",
            "initial_coin_balance": "0",
            "initial_investment": "0",
            "dungeon_name_options": "[]",
            "drop_item_options": "[]",
            "backup_path": "",
            "backup_keep_count": "7",
            "tx_type_options": '[{"name":"买币","direction":"expense","is_coin":true},{"name":"卖币","direction":"income","is_coin":true},{"name":"三国点充值","direction":"expense","is_coin":false},{"name":"三国点售卖","direction":"income","is_coin":false},{"name":"任务奖励","direction":"income","is_coin":false},{"name":"副本掉落","direction":"income","is_coin":false}]',
            "category_options": '["装备","灵兽","元神","灵魄","子女","倒货项目"]',
            "channel_options": '["商人","玩家"]',
        }
        for k, v in defaults.items():
            if not db.query(Setting).filter(Setting.key == k).first():
                db.add(Setting(key=k, value=v))
        db.commit()
        # 迁移：为已有 tx_type_options 追加缺失的默认类型
        import json as _json
        _new_types = [
            {"name": "任务奖励", "direction": "income", "is_coin": False},
            {"name": "副本掉落", "direction": "income", "is_coin": False},
        ]
        tx_row = db.query(Setting).filter(Setting.key == 'tx_type_options').first()
        if tx_row and tx_row.value:
            try:
                existing = _json.loads(tx_row.value)
                existing_names = {t['name'] for t in existing}
                added = [t for t in _new_types if t['name'] not in existing_names]
                if added:
                    existing.extend(added)
                    tx_row.value = _json.dumps(existing, ensure_ascii=False)
                    db.commit()
            except (ValueError, TypeError):
                pass
    finally:
        db.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
