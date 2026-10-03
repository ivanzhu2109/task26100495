"""考勤补传归并核心实现。

存储设计：
- 单表 events，主键 (person_id, ts_ms, event_id)，WITHOUT ROWID 聚簇存储，
  使单人按 (ts_ms, event_id) 有序扫描与区间查询都走主键索引。
- event_id 全局唯一，另加 UNIQUE 索引用于重带去重。
- valid 列持久化每条记录当前的有效性，补传时增量重算并落库。

增量重算：
- 新事件只影响其插入位置之后的记录；插入点之前的记录有效性不变。
- 对每个有补传的人员，从该人本批最小键的前一条有效记录（锚点）开始向后扫描，
  逐条重算有效性；一旦遇到新旧均为有效的记录，说明抑制状态已收敛，
  其后记录不受影响，可提前停止。因此只影响末尾少量记录的补传是 O(受影响行数)。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

#: 抑制窗口：600000 毫秒（10 分钟）
SUPPRESSION_WINDOW_MS = 600_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    person_id TEXT NOT NULL,
    ts_ms     INTEGER NOT NULL,
    event_id  TEXT NOT NULL,
    valid     INTEGER NOT NULL,
    PRIMARY KEY (person_id, ts_ms, event_id)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);
"""


@dataclass(frozen=True)
class Event:
    """一条打卡事件。event_id 全局唯一（ASCII），ts_ms 为校准后的 UTC 毫秒。"""

    event_id: str
    person_id: str
    ts_ms: int


class AttendanceStore:
    """基于 SQLite 文件的考勤归并存储。

    - ingest(batch): 批量补传事件，幂等，结果与上传顺序/批次/重传次数无关。
    - query_valid(person_id, start_ms, end_ms): 返回左闭右开区间内的有效记录。
    """

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @staticmethod
    def _validate(ev: Event) -> None:
        if not ev.event_id.isascii() or not ev.event_id:
            raise ValueError(f"event_id 必须为非空 ASCII: {ev.event_id!r}")
        if not isinstance(ev.ts_ms, int):
            raise TypeError(f"ts_ms 必须为 int: {ev.ts_ms!r}")

    def ingest(self, batch: Iterable[Event]) -> int:
        """补传一批事件，返回本次新入库的唯一事件数。"""
        uniq: Dict[str, Event] = {}
        for ev in batch:
            self._validate(ev)
            uniq.setdefault(ev.event_id, ev)
        if not uniq:
            return 0

        # 每个人员在本批中的最小/最大 (ts_ms, event_id)：
        # 从最小键开始重算；在越过最大键之前不允许因收敛提前停止，
        # 否则同批靠后的新事件造成的影响会被漏掉。
        key_range: Dict[str, List[Tuple[int, str]]] = {}
        for ev in uniq.values():
            key = (ev.ts_ms, ev.event_id)
            rng = key_range.setdefault(ev.person_id, [key, key])
            if key < rng[0]:
                rng[0] = key
            if key > rng[1]:
                rng[1] = key

        with self._conn:  # 事务：全部插入 + 增量重算要么都生效要么都回滚
            before = self._conn.total_changes
            self._conn.executemany(
                "INSERT OR IGNORE INTO events(person_id, ts_ms, event_id, valid)"
                " VALUES (?, ?, ?, 0)",
                [(ev.person_id, ev.ts_ms, ev.event_id) for ev in uniq.values()],
            )
            inserted = self._conn.total_changes - before
            if inserted:
                for person_id, (lo, hi) in key_range.items():
                    self._recompute_from(person_id, lo, hi)
        return inserted

    def _recompute_from(
        self,
        person_id: str,
        key: Tuple[int, str],
        no_stop_before: Tuple[int, str],
    ) -> None:
        """从 key 之前的最后一条有效记录开始，向后重算该人员的有效性。

        遇到新旧均有效的记录即可判定收敛并提前停止，但前提是当前位置
        已越过 no_stop_before（本批最后一个新事件的键）。
        """
        anchor = self._conn.execute(
            "SELECT ts_ms, event_id FROM events"
            " WHERE person_id = ? AND (ts_ms, event_id) < (?, ?) AND valid = 1"
            " ORDER BY ts_ms DESC, event_id DESC LIMIT 1",
            (person_id, key[0], key[1]),
        ).fetchone()

        if anchor is None:
            last_valid_ts: Optional[int] = None
            rows = self._conn.execute(
                "SELECT ts_ms, event_id, valid FROM events"
                " WHERE person_id = ? ORDER BY ts_ms, event_id",
                (person_id,),
            )
        else:
            last_valid_ts = anchor[0]
            rows = self._conn.execute(
                "SELECT ts_ms, event_id, valid FROM events"
                " WHERE person_id = ? AND (ts_ms, event_id) > (?, ?)"
                " ORDER BY ts_ms, event_id",
                (person_id, anchor[0], anchor[1]),
            )

        updates: List[Tuple[int, int, str]] = []
        for ts_ms, event_id, old_valid in rows:
            new_valid = int(
                last_valid_ts is None
                or ts_ms - last_valid_ts >= SUPPRESSION_WINDOW_MS
            )
            if new_valid:
                if old_valid and (ts_ms, event_id) > no_stop_before:
                    # 新旧均有效且已越过本批全部新事件：
                    # 抑制状态已收敛，其后记录不变，提前停止。
                    break
                last_valid_ts = ts_ms
            if new_valid != old_valid:
                updates.append((new_valid, ts_ms, event_id))
        self._conn.executemany(
            "UPDATE events SET valid = ?"
            " WHERE person_id = ? AND ts_ms = ? AND event_id = ?",
            [(v, person_id, ts, eid) for v, ts, eid in updates],
        )

    def query_valid(
        self, person_id: str, start_ms: int, end_ms: int
    ) -> List[Event]:
        """查询 [start_ms, end_ms) 内的有效记录，按 (ts_ms, event_id) 升序。"""
        rows = self._conn.execute(
            "SELECT event_id, ts_ms FROM events"
            " WHERE person_id = ? AND valid = 1 AND ts_ms >= ? AND ts_ms < ?"
            " ORDER BY ts_ms, event_id",
            (person_id, start_ms, end_ms),
        )
        return [Event(event_id=eid, person_id=person_id, ts_ms=ts) for eid, ts in rows]

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "AttendanceStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
