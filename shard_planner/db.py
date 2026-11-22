"""SQLite 连接与 schema 初始化。

schema 只做加法式迁移（CREATE TABLE IF NOT EXISTS / ALTER TABLE 补列），
因此原 SQLite 卷可以直接挂载启动：旧计划保留原编号，并在启动时回填
跨参数复用所需的清单身份指纹与分片形态摘要。
"""
from __future__ import annotations

import os

import sqlite3
from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_hash TEXT NOT NULL UNIQUE,
    worker_count INTEGER NOT NULL,
    worker_byte_limit INTEGER NOT NULL,
    entity_count INTEGER NOT NULL,
    partition_count INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    total_records INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'published',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS shards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_index INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    total_records INTEGER NOT NULL,
    UNIQUE (plan_id, shard_index)
);

CREATE TABLE IF NOT EXISTS shard_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    partition_key TEXT NOT NULL,
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);

-- 已证实“无可行组合”的依据（求解超时不入表，不能作为无解依据）
CREATE TABLE IF NOT EXISTS infeasible_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_key TEXT NOT NULL,
    worker_count INTEGER NOT NULL,
    worker_byte_limit INTEGER NOT NULL,
    source_plan_id INTEGER REFERENCES plans(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (manifest_key, worker_count, worker_byte_limit)
);

CREATE INDEX IF NOT EXISTS idx_shards_plan ON shards(plan_id);
CREATE INDEX IF NOT EXISTS idx_shard_entities_shard ON shard_entities(shard_id);
CREATE INDEX IF NOT EXISTS idx_partitions_plan ON partitions(plan_id);
CREATE INDEX IF NOT EXISTS idx_partitions_shard ON partitions(shard_id);
"""

# 依赖后期补列的索引：在 ALTER TABLE 补列之后再创建
_POST_MIGRATION_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_plans_manifest_key ON plans(manifest_key)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_manifest ON infeasible_evidence(manifest_key)",
)

# 后期补列（加法式迁移，旧卷启动时自动补齐）
_ADDED_PLAN_COLUMNS = {
    "manifest_key": "TEXT",          # 只与清单内容有关的身份指纹（跨参数复用）
    "non_empty_shards": "INTEGER",   # 非空分片数
    "max_shard_bytes": "INTEGER",    # 最重分片的字节数
    "basis_plan_id": "INTEGER",      # 复用发布时的依据计划编号
}


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute("PRAGMA busy_timeout = 5000")
    return g.db


def close_db(_exc=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _existing_columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _backfill_legacy_plans(db: sqlite3.Connection) -> None:
    """为旧卷里缺少复用摘要的计划回填三个新列。

    只在启动迁移时扫描旧分区明细（一次性）；请求路径上的复用判定
    只读 plans 表的摘要列，不扫旧分区明细。
    """
    # 延迟导入，避免 db ↔ store/planner 的导入环
    from .planner import manifest_identity

    rows = db.execute(
        "SELECT id FROM plans "
        "WHERE manifest_key IS NULL OR non_empty_shards IS NULL OR max_shard_bytes IS NULL"
    ).fetchall()
    for row in rows:
        plan_id = row["id"]
        parts = db.execute(
            "SELECT partition_key, entity, bytes, records FROM partitions WHERE plan_id = ?",
            (plan_id,),
        ).fetchall()
        if parts:
            from .planner import Partition

            partitions = [
                Partition(p["partition_key"], p["entity"], p["bytes"], p["records"])
                for p in parts
            ]
            manifest_key = manifest_identity(partitions)
        else:
            # 理论上不会出现（已发布计划必有分区），兜底避免 NULL 唯一键问题
            manifest_key = ""
        summary = db.execute(
            """
            SELECT COUNT(*) AS n,
                   COALESCE(MAX(total_bytes), 0) AS max_bytes
            FROM shards
            WHERE plan_id = ?
              AND id IN (SELECT DISTINCT shard_id FROM shard_entities)
            """,
            (plan_id,),
        ).fetchone()
        db.execute(
            "UPDATE plans SET manifest_key = ?, non_empty_shards = ?, max_shard_bytes = ? WHERE id = ?",
            (manifest_key, summary["n"], summary["max_bytes"], plan_id),
        )


def init_db(app) -> None:
    path = app.config["DATABASE"]
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with app.app_context():
        db = get_db()
        db.executescript(SCHEMA)
        existing = _existing_columns(db, "plans")
        for column, decl in _ADDED_PLAN_COLUMNS.items():
            if column not in existing:
                db.execute(f"ALTER TABLE plans ADD COLUMN {column} {decl}")
        for statement in _POST_MIGRATION_INDEXES:
            db.execute(statement)
        _backfill_legacy_plans(db)
        db.execute("PRAGMA journal_mode = WAL")
        db.commit()
