"""计划的持久化与查询。计划只插入、不更新：新清单产生新计划，已发布计划不被改写。"""
from __future__ import annotations

import sqlite3

from .planner import Partition, aggregate_entities


def find_plan_by_hash(db: sqlite3.Connection, manifest_hash: str):
    return db.execute("SELECT * FROM plans WHERE manifest_hash = ?", (manifest_hash,)).fetchone()


def list_plans(db: sqlite3.Connection):
    return db.execute("SELECT * FROM plans ORDER BY id DESC").fetchall()


def find_reusable_plan(db: sqlite3.Connection, manifest_key: str,
                       worker_count: int, worker_byte_limit: int):
    """在同一清单的已发布计划中找可复用分配。

    复用条件（只看 plans 表的摘要列，不扫旧分区明细）：
    - 非空分片数不超过本次 worker 数（分片形态放得下）；
    - 最重分片字节数不超过本次单 worker 字节上限（每个分片仍合规）。
    取编号最小的一条，保证同一请求序列下结果确定。
    """
    return db.execute(
        """
        SELECT * FROM plans
        WHERE manifest_key = ?
          AND non_empty_shards <= ?
          AND max_shard_bytes <= ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (manifest_key, worker_count, worker_byte_limit),
    ).fetchone()


def get_plan_entity_assignment(db: sqlite3.Connection, plan_id: int) -> list[list[str]]:
    """读取依据计划的实体分组（非空分片按原 shard_index 升序）。

    只读 shard_entities，不读 partitions 明细。
    """
    shard_rows = db.execute(
        "SELECT id, shard_index FROM shards WHERE plan_id = ? ORDER BY shard_index",
        (plan_id,),
    ).fetchall()
    assignment: list[list[str]] = []
    for shard in shard_rows:
        entities = [
            row["entity"]
            for row in db.execute(
                "SELECT entity FROM shard_entities WHERE shard_id = ? ORDER BY entity",
                (shard["id"],),
            ).fetchall()
        ]
        if entities:
            assignment.append(entities)
    return assignment


def find_dominating_infeasible_evidence(db: sqlite3.Connection, manifest_key: str,
                                        worker_count: int, worker_byte_limit: int):
    """找一条可直接推出“本次也无解”的历史依据。

    依据规格 (W', C') 的 worker 数与字节上限都不低于本次参数时，
    在 (W', C') 下无可行组合 ⇒ 在更紧的 (W, C) 下同样无可行组合。
    取最宽松（最贴近本次参数）的一条，保证提示有确定性：
    先按 worker_count、再按字节上限升序，取第一条。
    """
    return db.execute(
        """
        SELECT * FROM infeasible_evidence
        WHERE manifest_key = ?
          AND worker_count >= ?
          AND worker_byte_limit >= ?
        ORDER BY worker_count ASC, worker_byte_limit ASC, id ASC
        LIMIT 1
        """,
        (manifest_key, worker_count, worker_byte_limit),
    ).fetchone()


def insert_infeasible_evidence(db: sqlite3.Connection, manifest_key: str,
                               worker_count: int, worker_byte_limit: int) -> None:
    """登记一条已证实的无可行组合依据。求解超时不得调用本函数。

    同清单同规格重复登记时静默忽略（UNIQUE 约束兜底）。
    """
    db.execute(
        """
        INSERT OR IGNORE INTO infeasible_evidence
            (manifest_key, worker_count, worker_byte_limit)
        VALUES (?, ?, ?)
        """,
        (manifest_key, worker_count, worker_byte_limit),
    )
    db.commit()


def create_plan(
    db: sqlite3.Connection,
    manifest_hash: str,
    manifest_key: str,
    worker_count: int,
    worker_byte_limit: int,
    partitions: list[Partition],
    shards: list[list[str]],
    basis_plan_id: int | None = None,
) -> int:
    """把可行方案整体写入一个事务。manifest_hash 冲突时抛 sqlite3.IntegrityError。"""
    entity_agg = aggregate_entities(partitions)
    total_bytes = sum(p.bytes for p in partitions)
    total_records = sum(p.records for p in partitions)

    non_empty_shards = 0
    max_shard_bytes = 0
    try:
        db.execute("BEGIN")
        cur = db.execute(
            """
            INSERT INTO plans (manifest_hash, manifest_key, worker_count, worker_byte_limit,
                               entity_count, partition_count, total_bytes, total_records,
                               non_empty_shards, max_shard_bytes, basis_plan_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (manifest_hash, manifest_key, worker_count, worker_byte_limit,
             len(entity_agg), len(partitions), total_bytes, total_records,
             0, 0, basis_plan_id),
        )
        plan_id = cur.lastrowid

        entity_to_shard: dict[str, int] = {}
        for shard_index, entities in enumerate(shards):
            shard_bytes = sum(entity_agg[e][0] for e in entities)
            shard_records = sum(entity_agg[e][1] for e in entities)
            if entities:
                non_empty_shards += 1
                max_shard_bytes = max(max_shard_bytes, shard_bytes)
            cur = db.execute(
                "INSERT INTO shards (plan_id, shard_index, total_bytes, total_records) VALUES (?, ?, ?, ?)",
                (plan_id, shard_index, shard_bytes, shard_records),
            )
            shard_id = cur.lastrowid
            for entity in entities:
                entity_to_shard[entity] = shard_id
                db.execute(
                    "INSERT INTO shard_entities (shard_id, entity, bytes, records) VALUES (?, ?, ?, ?)",
                    (shard_id, entity, entity_agg[entity][0], entity_agg[entity][1]),
                )

        for p in sorted(partitions, key=lambda p: (p.entity, p.partition_key)):
            db.execute(
                """
                INSERT INTO partitions (plan_id, shard_id, partition_key, entity, bytes, records)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (plan_id, entity_to_shard[p.entity], p.partition_key, p.entity, p.bytes, p.records),
            )

        db.execute(
            "UPDATE plans SET non_empty_shards = ?, max_shard_bytes = ? WHERE id = ?",
            (non_empty_shards, max_shard_bytes, plan_id),
        )
        db.commit()
        return plan_id
    except Exception:
        db.rollback()
        raise


def get_plan_detail(db: sqlite3.Connection, plan_id: int):
    """返回计划详情：plan 行 + 每个分片的实体与分区列表。不存在时返回 None。"""
    plan = db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    if plan is None:
        return None
    shard_rows = db.execute(
        "SELECT * FROM shards WHERE plan_id = ? ORDER BY shard_index", (plan_id,)
    ).fetchall()
    shards = []
    for shard in shard_rows:
        entities = db.execute(
            "SELECT entity, bytes, records FROM shard_entities WHERE shard_id = ? ORDER BY entity",
            (shard["id"],),
        ).fetchall()
        parts = db.execute(
            """
            SELECT partition_key, entity, bytes, records
            FROM partitions WHERE shard_id = ? ORDER BY entity, partition_key
            """,
            (shard["id"],),
        ).fetchall()
        shards.append({"row": shard, "entities": entities, "partitions": parts})
    return {"plan": plan, "shards": shards}
