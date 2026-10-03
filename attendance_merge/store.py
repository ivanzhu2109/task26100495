"""基于 SQLite 的考勤补传归并存储。

只提供两类操作：
- ingest_batch：批量补传打卡事件（幂等，乱序、重复、跨批次均可）。
- query_valid：查询指定人员在左闭右开 UTC 毫秒区间内的有效打卡。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

# 同一人员相邻有效打卡的最小间隔（10 分钟，单位毫秒）。
MIN_INTERVAL_MS = 600_000


@dataclass(frozen=True)
class PunchEvent:
    """一条补传的打卡事件。event_id 全局唯一，ts_ms 为校准后的 UTC 毫秒。"""

    event_id: str
    person_id: str
    ts_ms: int


@dataclass(frozen=True)
class ValidPunch:
    """一条有效打卡，按 (ts_ms, event_id) 升序返回。"""

    event_id: str
    ts_ms: int


class AttendanceStore:
    """考勤归并库入口。数据保存在调用方指定的 SQLite 文件中。"""

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS events (
        person_id TEXT NOT NULL,
        ts_ms     INTEGER NOT NULL,
        event_id  TEXT NOT NULL,
        valid     INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (person_id, ts_ms, event_id)
    ) WITHOUT ROWID;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);
    CREATE INDEX IF NOT EXISTS idx_events_valid
        ON events(person_id, valid, ts_ms, event_id);
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> "AttendanceStore":
        if self._conn is not None:
            return self
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(self._SCHEMA)
        conn.commit()
        self._conn = conn
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "AttendanceStore":
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ingest_batch(self, events: Iterable[PunchEvent]) -> int:
        """补传一批事件，返回实际新增的唯一事件数。"""
        conn = self._require_conn()
        batch = list(events)
        if not batch:
            return 0

        inserted_by_person: dict[str, list[tuple[int, str]]] = {}
        with conn:  # 单事务：批量入库 + 增量重算
            conn.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _batch("
                "event_id TEXT PRIMARY KEY, person_id TEXT, ts_ms INTEGER)"
            )
            conn.execute("DELETE FROM _batch")
            conn.executemany(
                "INSERT OR IGNORE INTO _batch(event_id, person_id, ts_ms)"
                " VALUES(?, ?, ?)",
                [(e.event_id, e.person_id, e.ts_ms) for e in batch],
            )
            # 库内已存在的事件（本批之前已收到过）。
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT b.event_id FROM _batch b"
                    " CROSS JOIN events e ON e.event_id = b.event_id"
                )
            }
            conn.execute(
                "INSERT OR IGNORE INTO events(person_id, ts_ms, event_id, valid)"
                " SELECT person_id, ts_ms, event_id, 0 FROM _batch"
            )
            for event_id, person_id, ts_ms in conn.execute(
                "SELECT event_id, person_id, ts_ms FROM _batch"
            ):
                if event_id not in existing:
                    inserted_by_person.setdefault(person_id, []).append(
                        (ts_ms, event_id)
                    )
            conn.execute("DELETE FROM _batch")

            for person_id, points in inserted_by_person.items():
                self._recompute_person(conn, person_id, min(points), max(points))

        return sum(len(v) for v in inserted_by_person.values())

    def query_valid(
        self, person_id: str, start_ms: int, end_ms: int
    ) -> list[ValidPunch]:
        """返回 [start_ms, end_ms) 内的有效打卡，按 (ts_ms, event_id) 升序。"""
        conn = self._require_conn()
        rows = conn.execute(
            "SELECT event_id, ts_ms FROM events"
            " WHERE person_id = ? AND valid = 1 AND ts_ms >= ? AND ts_ms < ?"
            " ORDER BY ts_ms, event_id",
            (person_id, start_ms, end_ms),
        )
        return [ValidPunch(event_id=r[0], ts_ms=r[1]) for r in rows]

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("AttendanceStore 未打开，请先调用 open()")
        return self._conn

    @staticmethod
    def _recompute_person(
        conn: sqlite3.Connection,
        person_id: str,
        start: tuple[int, str],
        last_insert: tuple[int, str],
    ) -> None:
        """从该人员最早的新增点开始，增量重算有效性。

        规则：按 (ts_ms, event_id) 升序，距上一条有效记录 >= MIN_INTERVAL_MS
        才有效；被抑制记录不推进窗口。一旦重算到一条“本应有效且已标记有效”
        的记录、且本批全部新增点都已越过，后续链条与已存状态一致，可提前停止。
        """
        start_ts, start_eid = start
        row = conn.execute(
            "SELECT ts_ms FROM events"
            " WHERE person_id = ? AND valid = 1"
            " AND (ts_ms, event_id) < (?, ?)"
            " ORDER BY ts_ms DESC, event_id DESC LIMIT 1",
            (person_id, start_ts, start_eid),
        ).fetchone()
        last_valid_ts: Optional[int] = row[0] if row else None

        cursor = conn.execute(
            "SELECT ts_ms, event_id, valid FROM events"
            " WHERE person_id = ?"
            " AND (ts_ms, event_id) >= (?, ?)"
            " ORDER BY ts_ms, event_id",
            (person_id, start_ts, start_eid),
        )
        for ts_ms, event_id, stored_valid in cursor:
            is_valid = last_valid_ts is None or (
                ts_ms - last_valid_ts >= MIN_INTERVAL_MS
            )
            if is_valid:
                last_valid_ts = ts_ms
                if stored_valid and (ts_ms, event_id) >= last_insert:
                    break  # 链条已与已存状态汇合，后续无需改动
            if is_valid != bool(stored_valid):
                conn.execute(
                    "UPDATE events SET valid = ?"
                    " WHERE person_id = ? AND ts_ms = ? AND event_id = ?",
                    (1 if is_valid else 0, person_id, ts_ms, event_id),
                )
