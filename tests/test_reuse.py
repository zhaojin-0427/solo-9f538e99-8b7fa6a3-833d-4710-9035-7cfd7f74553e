"""跨参数复用测试：已发布分配复用、无解依据复用、超时不登记、旧卷迁移。"""
import json

import pytest

from shard_planner import create_app
from shard_planner.db import get_db
from shard_planner.planner import (
    SOLVER_LIMIT,
    PlanResult,
    manifest_identity,
    normalize_manifest,
    reuse_assignment,
)

# 与 test_app.py 同款可行清单：orders=500、users=400、events=700，总计 1600
FEASIBLE_MANIFEST = [
    {"partition_key": "dt=2026-01-01/hour=00", "entity": "orders", "bytes": 300, "records": 30},
    {"partition_key": "dt=2026-01-01/hour=01", "entity": "orders", "bytes": 200, "records": 20},
    {"partition_key": "dt=2026-01-01", "entity": "users", "bytes": 400, "records": 40},
    {"partition_key": "dt=2026-01-02", "entity": "events", "bytes": 700, "records": 70},
]

# 5 个各 600 的实体：总量 3000。3×1000 下装箱无解（每箱至多 1 个）。
INFEASIBLE_MANIFEST = [
    {"partition_key": f"p{i}", "entity": f"e{i}", "bytes": 600, "records": 1}
    for i in range(5)
]


def post_plan(client, manifest, workers, limit):
    return client.post(
        "/plans",
        data={
            "manifest_text": json.dumps(manifest),
            "worker_count": str(workers),
            "worker_byte_limit": str(limit),
        },
    )


def plan_count(db):
    return db.execute("SELECT COUNT(*) AS c FROM plans").fetchone()["c"]


def evidence_count(db):
    return db.execute("SELECT COUNT(*) AS c FROM infeasible_evidence").fetchone()["c"]


# ---------------------------------------------------------------------------
# planner 单元：清单身份指纹与分配套用
# ---------------------------------------------------------------------------

def test_manifest_identity_order_and_param_insensitive():
    p1 = normalize_manifest(FEASIBLE_MANIFEST)
    p2 = normalize_manifest(list(reversed(FEASIBLE_MANIFEST)))
    assert manifest_identity(p1) == manifest_identity(p2)
    # 身份指纹不含参数
    assert manifest_identity(p1) == manifest_identity(p2)


def test_manifest_identity_distinguishes_content():
    p1 = normalize_manifest(FEASIBLE_MANIFEST)
    changed = [dict(r) for r in FEASIBLE_MANIFEST]
    changed[0]["bytes"] = 301
    p2 = normalize_manifest(changed)
    assert manifest_identity(p1) != manifest_identity(p2)


def test_reuse_assignment_pads_empty_shards():
    basis = [["a", "b"], ["c"]]
    assert reuse_assignment(basis, 4) == [["a", "b"], ["c"], [], []]
    assert reuse_assignment(basis, 2) == [["a", "b"], ["c"]]
    assert reuse_assignment(basis, 1) is None


# ---------------------------------------------------------------------------
# 已发布分配复用
# ---------------------------------------------------------------------------

def test_reuse_published_assignment_with_looser_limit(client, db):
    # 初始发布：2 worker、上限 900
    first = post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    assert first.headers["Location"].endswith("/plans/1")

    # 更宽松的字节上限：最重分片（events=700）与非空分片数（2）都符合
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/2")
    assert plan_count(db) == 2

    new_plan = db.execute("SELECT * FROM plans WHERE id = 2").fetchone()
    assert new_plan["basis_plan_id"] == 1
    assert new_plan["worker_count"] == 3
    assert new_plan["worker_byte_limit"] == 1000
    assert new_plan["non_empty_shards"] == 2
    assert new_plan["max_shard_bytes"] <= 1000
    assert new_plan["manifest_key"] == db.execute(
        "SELECT manifest_key FROM plans WHERE id = 1"
    ).fetchone()["manifest_key"]

    # 新计划的分片合规、实体不拆分
    shards = db.execute("SELECT * FROM shards WHERE plan_id = 2 ORDER BY shard_index").fetchall()
    assert len(shards) == 3  # 尾部补了一个空分片
    for shard in shards:
        assert shard["total_bytes"] <= 1000
    rows = db.execute(
        "SELECT entity, COUNT(DISTINCT shard_id) AS n FROM partitions "
        "WHERE plan_id = 2 GROUP BY entity"
    ).fetchall()
    assert all(r["n"] == 1 for r in rows)

    # 页面列出依据计划号
    page = client.get("/plans/2").get_data(as_text=True)
    assert "#1" in page and "复用" in page


def test_reuse_hit_does_not_rerun_solver(client, db, monkeypatch):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)

    def boom(*_a, **_kw):
        raise AssertionError("命中复用时不应重跑装箱")

    monkeypatch.setattr("shard_planner.routes.build_plan", boom)
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=4, limit=2000)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/2")
    assert plan_count(db) == 2


def test_reuse_hit_does_not_scan_old_partition_details(client, db, monkeypatch):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    statements: list[str] = []
    db.set_trace_callback(lambda sql: statements.append(" ".join(sql.split())))
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp.status_code == 302
    db.set_trace_callback(None)
    # 复用判定与读取依据都不扫旧 partitions：没有任何针对 partitions 的 SELECT；
    # 只有写新计划自身明细的 INSERT（其 plan_id 是新计划 2）。
    selects_from_partitions = [
        s for s in statements
        if s.upper().startswith("SELECT") and "FROM PARTITIONS" in s.upper()
    ]
    assert selects_from_partitions == []
    old_partition_reads = [
        s for s in statements
        if s.upper().startswith("SELECT") and "PARTITIONS" in s.upper() and "PLAN_ID = 1" in s.upper()
    ]
    assert old_partition_reads == []


def test_reuse_with_reordered_rows_same_identity(client, db):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    resp = post_plan(client, list(reversed(FEASIBLE_MANIFEST)), workers=3, limit=1000)
    assert resp.headers["Location"].endswith("/plans/2")
    assert db.execute("SELECT basis_plan_id FROM plans WHERE id = 2").fetchone()["basis_plan_id"] == 1


def test_same_params_return_original_id_even_after_reuse(client, db):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)   # #1
    post_plan(client, FEASIBLE_MANIFEST, workers=3, limit=1000)  # #2（复用）
    # 参数完全相同（换序）仍返回原编号
    resp = post_plan(client, list(reversed(FEASIBLE_MANIFEST)), workers=3, limit=1000)
    assert resp.headers["Location"].endswith("/plans/2")
    assert plan_count(db) == 2


def test_no_reuse_when_limit_too_tight(client, db, monkeypatch):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    # 上限 600：events 实体 700 超限，先被单实体预检拦下，不查复用
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=4, limit=600)
    assert resp.status_code == 422
    assert "单实体超限" in resp.get_data(as_text=True)
    assert plan_count(db) == 1
    assert evidence_count(db) == 0


def test_no_reuse_when_worker_count_smaller_than_non_empty_shards(client, db):
    post_plan(client, FEASIBLE_MANIFEST, workers=4, limit=900)  # 2 个非空分片
    # 只要 1 个 worker：总量 1600 > 1×900，总容量预检先拦下
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=1, limit=900)
    assert resp.status_code == 422
    assert "分片超限" in resp.get_data(as_text=True)
    assert plan_count(db) == 1


def test_reusable_candidate_summary_columns(client, db):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    plan = db.execute("SELECT * FROM plans WHERE id = 1").fetchone()
    assert plan["non_empty_shards"] == 2
    assert plan["max_shard_bytes"] == max(500 + 400, 700)  # 900
    assert plan["manifest_key"]
    assert plan["basis_plan_id"] is None


# ---------------------------------------------------------------------------
# 无解依据复用
# ---------------------------------------------------------------------------

def test_infeasible_result_persists_evidence(client, db):
    resp = post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp.status_code == 422
    assert "无可行组合" in resp.get_data(as_text=True)
    assert plan_count(db) == 0  # 无解不发布计划
    rows = db.execute("SELECT * FROM infeasible_evidence").fetchall()
    assert len(rows) == 1
    assert rows[0]["worker_count"] == 3
    assert rows[0]["worker_byte_limit"] == 1000


def test_dominating_evidence_short_circuits(client, db, monkeypatch):
    # 在 (4, 1000) 下证实无解
    r1 = post_plan(client, INFEASIBLE_MANIFEST, workers=4, limit=1000)
    assert r1.status_code == 422
    assert evidence_count(db) == 1

    def boom(*_a, **_kw):
        raise AssertionError("命中无解依据时不应重跑装箱")

    monkeypatch.setattr("shard_planner.routes.build_plan", boom)

    # 更紧的 (3, 1000)：依据规格 W=4≥3、C=1000≥1000，直接报无解
    r2 = post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1000)
    assert r2.status_code == 422
    html = r2.get_data(as_text=True)
    assert "无可行组合" in html
    assert "依据编号 #1" in html
    assert "4" in html and "1000" in html
    assert evidence_count(db) == 1  # 未新增依据
    assert plan_count(db) == 0


def test_non_dominating_evidence_is_ignored(client, db):
    # 在 (3, 1000) 下证实无解（总量 3000 恰好等于总容量，属于装箱无解而非容量不足）
    post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1000)
    assert evidence_count(db) == 1
    # 字节上限放宽到 1500：依据的 C=1000 < 1500，不构成支配，照常求解 → 有解
    # （3000 ≤ 3×1500，且 600+600 ≤ 1500，可两两装箱）
    resp = post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1500)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/1")
    plan = db.execute("SELECT * FROM plans WHERE id = 1").fetchone()
    assert plan["worker_byte_limit"] == 1500
    assert evidence_count(db) == 1  # 未新增依据


def test_smaller_byte_limit_evidence_does_not_dominate(client, db):
    # (4, 1000) 无解：总量 3000 < 4000，属装箱无解
    post_plan(client, INFEASIBLE_MANIFEST, workers=4, limit=1000)
    # (4, 1200)：依据 C=1000 < 1200，不支配；新规格可行（600+600 ≤ 1200），照常求解发布
    resp = post_plan(client, INFEASIBLE_MANIFEST, workers=4, limit=1200)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/1")
    assert evidence_count(db) == 1  # 未再登记无解依据


def test_solver_timeout_is_not_evidence(client, db, monkeypatch):
    monkeypatch.setattr(
        "shard_planner.routes.build_plan",
        lambda *_a, **_kw: PlanResult(False, SOLVER_LIMIT, {"node_budget": 1}),
    )
    resp = post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp.status_code == 422
    assert "求解超时" in resp.get_data(as_text=True)
    assert evidence_count(db) == 0

    # 下次相同请求仍会正常尝试求解（去掉 monkeypatch 后真的无解）
    monkeypatch.undo()
    resp2 = post_plan(client, INFEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp2.status_code == 422
    assert "无可行组合" in resp2.get_data(as_text=True)
    assert evidence_count(db) == 1


def test_evidence_survives_restart(tmp_path):
    db_path = tmp_path / "restart.db"
    app = create_app({"TESTING": True, "DATABASE": str(db_path)})
    with app.app_context():
        db = get_db()
        from shard_planner import store
        store.insert_infeasible_evidence(db, "key-x", 4, 1000)

    # 模拟重启：用同一文件新建应用
    app2 = create_app({"TESTING": True, "DATABASE": str(db_path)})
    with app2.app_context():
        db2 = get_db()
        from shard_planner import store
        hit = store.find_dominating_infeasible_evidence(db2, "key-x", 3, 900)
        assert hit is not None and hit["worker_count"] == 4
        miss = store.find_dominating_infeasible_evidence(db2, "key-x", 5, 1000)
        assert miss is None


def test_duplicate_evidence_insert_ignored(tmp_path):
    app = create_app({"TESTING": True, "DATABASE": str(tmp_path / "dup.db")})
    with app.app_context():
        db = get_db()
        from shard_planner import store
        store.insert_infeasible_evidence(db, "k", 3, 1000)
        store.insert_infeasible_evidence(db, "k", 3, 1000)  # 不报错
        assert evidence_count(db) == 1


# ---------------------------------------------------------------------------
# 旧 SQLite 卷迁移
# ---------------------------------------------------------------------------

LEGACY_SCHEMA = """
CREATE TABLE plans (
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
CREATE TABLE shards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_index INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    total_records INTEGER NOT NULL,
    UNIQUE (plan_id, shard_index)
);
CREATE TABLE shard_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);
CREATE TABLE partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    partition_key TEXT NOT NULL,
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);
"""


def test_legacy_volume_migrates_and_keeps_ids(tmp_path):
    import sqlite3

    db_path = tmp_path / "legacy.db"
    raw = sqlite3.connect(db_path)
    raw.executescript(LEGACY_SCHEMA)
    raw.execute(
        "INSERT INTO plans (manifest_hash, worker_count, worker_byte_limit, "
        "entity_count, partition_count, total_bytes, total_records) "
        "VALUES ('oldhash', 2, 900, 3, 4, 1600, 160)"
    )
    raw.execute("INSERT INTO shards (plan_id, shard_index, total_bytes, total_records) "
                "VALUES (1, 0, 900, 90)")
    raw.execute("INSERT INTO shards (plan_id, shard_index, total_bytes, total_records) "
                "VALUES (1, 1, 700, 70)")
    raw.executemany(
        "INSERT INTO shard_entities (shard_id, entity, bytes, records) VALUES (?, ?, ?, ?)",
        [(1, "orders", 500, 50), (1, "users", 400, 40), (2, "events", 700, 70)],
    )
    raw.executemany(
        "INSERT INTO partitions (plan_id, shard_id, partition_key, entity, bytes, records) "
        "VALUES (1, ?, ?, ?, ?, ?)",
        [
            (1, "dt=2026-01-01/hour=00", "orders", 300, 30),
            (1, "dt=2026-01-01/hour=01", "orders", 200, 20),
            (1, "dt=2026-01-01", "users", 400, 40),
            (2, "dt=2026-01-02", "events", 700, 70),
        ],
    )
    raw.commit()
    raw.close()

    # 用新代码打开旧卷：迁移应自动完成
    app = create_app({"TESTING": True, "DATABASE": str(db_path)})
    with app.app_context():
        db = get_db()
        plan = db.execute("SELECT * FROM plans WHERE id = 1").fetchone()
        assert plan["id"] == 1  # 旧编号不变
        assert plan["non_empty_shards"] == 2
        assert plan["max_shard_bytes"] == 900
        expected_key = manifest_identity(normalize_manifest(FEASIBLE_MANIFEST))
        assert plan["manifest_key"] == expected_key

    # 旧计划可按原编号查看，且能作为复用依据
    client = app.test_client()
    page = client.get("/plans/1")
    assert page.status_code == 200
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=3, limit=1000)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/2")
    with app.app_context():
        db = get_db()
        new_plan = db.execute("SELECT * FROM plans WHERE id = 2").fetchone()
        assert new_plan["basis_plan_id"] == 1
