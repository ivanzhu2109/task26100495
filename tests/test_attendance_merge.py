"""attendance_merge 的正确性测试：与独立参考实现逐条比对。"""

from __future__ import annotations

import random
import sqlite3
from pathlib import Path

import pytest

from attendance_merge import MIN_INTERVAL_MS, AttendanceStore, PunchEvent

MINUTE = 60_000


def reference_valid(events: list[PunchEvent], person_id: str) -> list[tuple[str, int]]:
    """独立参考实现：对全部唯一事件直接应用业务规则。"""
    unique: dict[str, PunchEvent] = {}
    for e in events:
        unique.setdefault(e.event_id, e)
    ordered = sorted(
        (e for e in unique.values() if e.person_id == person_id),
        key=lambda e: (e.ts_ms, e.event_id),
    )
    result: list[tuple[str, int]] = []
    last_ts: int | None = None
    for e in ordered:
        if last_ts is None or e.ts_ms - last_ts >= MIN_INTERVAL_MS:
            result.append((e.event_id, e.ts_ms))
            last_ts = e.ts_ms
    return result


def query_all(store: AttendanceStore, person_id: str) -> list[tuple[str, int]]:
    return [(v.event_id, v.ts_ms) for v in store.query_valid(person_id, 0, 2**62)]


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "attendance.db"


def test_chain_reaction_on_late_backfill(db_path: Path) -> None:
    """验收例：2,11,20,29,38 -> 2,20,38；补传 0 后 -> 0,11,29。"""
    with AttendanceStore(str(db_path)).open() as store:
        first = [PunchEvent(f"e{m:02d}", "p1", m * MINUTE) for m in (2, 11, 20, 29, 38)]
        assert store.ingest_batch(first) == 5
        assert query_all(store, "p1") == [
            ("e02", 2 * MINUTE),
            ("e20", 20 * MINUTE),
            ("e38", 38 * MINUTE),
        ]
        assert store.ingest_batch([PunchEvent("e00", "p1", 0)]) == 1
        assert query_all(store, "p1") == [
            ("e00", 0),
            ("e11", 11 * MINUTE),
            ("e29", 29 * MINUTE),
        ]


def test_long_chain_has_no_fixed_lookback(db_path: Path) -> None:
    """长序列的连锁修正必须一直传到历史末尾，不能在固定回看范围处停止。"""
    n = 5000
    minutes = [9 * i + 2 for i in range(n)]  # 间隔 9 分钟，全部互相抑制
    events = [PunchEvent(f"e{i:06d}", "p1", m * MINUTE) for i, m in enumerate(minutes)]
    with AttendanceStore(str(db_path)).open() as store:
        store.ingest_batch(events)
        assert query_all(store, "p1") == reference_valid(events, "p1")
        # 在最前面补一条 0 分钟，整条链的有效性全部翻转。
        store.ingest_batch([PunchEvent("head", "p1", 0)])
        events.append(PunchEvent("head", "p1", 0))
        got = query_all(store, "p1")
        assert got == reference_valid(events, "p1")
        # 链尾也必须被修正：末尾一条由无效变有效。
        assert got[-1] == (f"e{n - 1:06d}", minutes[-1] * MINUTE)


def test_same_millisecond_and_boundary_gaps(db_path: Path) -> None:
    """同毫秒按 event_id 字典序；599999 被抑制，600000 恰好有效。"""
    base = 10**12
    events = [
        PunchEvent("b", "p1", base),
        PunchEvent("a", "p1", base),  # 同毫秒，a 排在 b 前
        PunchEvent("c", "p1", base + MIN_INTERVAL_MS - 1),  # 599999，抑制
        PunchEvent("d", "p1", base + MIN_INTERVAL_MS),      # 恰好 600000，有效
        PunchEvent("e", "p1", base + 2 * MIN_INTERVAL_MS - 1),  # 相对 d 抑制
        PunchEvent("f", "p1", base + 2 * MIN_INTERVAL_MS),      # 相对 d 有效
    ]
    with AttendanceStore(str(db_path)).open() as store:
        store.ingest_batch(events)
        assert query_all(store, "p1") == [
            ("a", base),
            ("d", base + MIN_INTERVAL_MS),
            ("f", base + 2 * MIN_INTERVAL_MS),
        ]


def test_random_order_batches_and_replay(db_path: Path) -> None:
    """不同顺序、不同批次、重复重放，结果与参考实现一致。"""
    rng = random.Random(20261004)
    persons = ["p1", "p2", "p3"]
    base = 1_700_000_000_000
    events: list[PunchEvent] = []
    for i in range(3000):
        person = rng.choice(persons)
        gap_kind = rng.random()
        if gap_kind < 0.3:
            offset = MIN_INTERVAL_MS  # 恰好边界
        elif gap_kind < 0.5:
            offset = MIN_INTERVAL_MS - 1
        elif gap_kind < 0.6:
            offset = 0  # 同毫秒
        else:
            offset = rng.randrange(0, 3 * MIN_INTERVAL_MS)
        ts = base + (i // 3) * MIN_INTERVAL_MS + offset - MIN_INTERVAL_MS
        events.append(PunchEvent(f"ev{i:05d}", person, max(ts, 0)))

    shuffled = events[:]
    rng.shuffle(shuffled)
    # 随机切批，并随机重复重放一些批次。
    batches: list[list[PunchEvent]] = []
    pos = 0
    while pos < len(shuffled):
        size = rng.randrange(1, 200)
        batches.append(shuffled[pos : pos + size])
        pos += size
    replay = [rng.choice(batches) for _ in range(5)]

    with AttendanceStore(str(db_path)).open() as store:
        for batch in batches + replay:
            store.ingest_batch(batch)
        for person in persons:
            assert query_all(store, person) == reference_valid(events, person)


def test_query_window_is_pure_crop(db_path: Path) -> None:
    """查询起点落在抑制窗口内部时，结果仍是完整有效集的区间裁剪。"""
    events = [PunchEvent(f"e{i:03d}", "p1", i * 7 * MINUTE) for i in range(200)]
    with AttendanceStore(str(db_path)).open() as store:
        store.ingest_batch(events)
        full = query_all(store, "p1")
        # 起点落在某个抑制窗口内部（非有效记录时刻）。
        start = 5 * 7 * MINUTE + 1
        end = 150 * 7 * MINUTE - 1
        got = [(v.event_id, v.ts_ms) for v in store.query_valid("p1", start, end)]
        assert got == [item for item in full if start <= item[1] < end]


def test_persistence_across_reopen(db_path: Path) -> None:
    """正常关闭后重新打开，数据与查询结果保留。"""
    events = [PunchEvent(f"e{i:03d}", "p1", i * MINUTE) for i in range(100)]
    store = AttendanceStore(str(db_path)).open()
    store.ingest_batch(events)
    before = query_all(store, "p1")
    store.close()

    with AttendanceStore(str(db_path)).open() as reopened:
        assert query_all(reopened, "p1") == before
        # 重新打开后补传仍能触发正确的连锁修正。
        reopened.ingest_batch([PunchEvent("head", "p1", 0)])
        events.append(PunchEvent("head", "p1", 0))
        assert query_all(reopened, "p1") == reference_valid(events, "p1")


def test_edge_cases(db_path: Path) -> None:
    with AttendanceStore(str(db_path)).open() as store:
        assert store.ingest_batch([]) == 0
        assert store.query_valid("nobody", 0, 2**62) == []
        # 重复事件内容相同，幂等。
        e = PunchEvent("x", "p1", 123)
        assert store.ingest_batch([e, e, e]) == 1
        assert store.ingest_batch([e]) == 0
        assert query_all(store, "p1") == [("x", 123)]


def test_unopened_store_raises(db_path: Path) -> None:
    store = AttendanceStore(str(db_path))
    with pytest.raises(RuntimeError):
        store.query_valid("p1", 0, 1)
    with pytest.raises(RuntimeError):
        store.ingest_batch([PunchEvent("a", "p1", 0)])


def test_events_isolated_per_person(db_path: Path) -> None:
    """不同人员独立计算，互不影响。"""
    events = [
        PunchEvent("a1", "p1", 0),
        PunchEvent("b1", "p2", 1),  # p2 的记录不影响 p1 的窗口
        PunchEvent("a2", "p1", 5 * MINUTE),  # 距 a1 不足 10 分钟，抑制
    ]
    with AttendanceStore(str(db_path)).open() as store:
        store.ingest_batch(events)
        assert query_all(store, "p1") == [("a1", 0)]
        assert query_all(store, "p2") == [("b1", 1)]


@pytest.mark.parametrize("seed", [1, 7, 42, 2026])
def test_incremental_ingest_matches_reference_each_batch(
    db_path: Path, seed: int
) -> None:
    """逐批补传（含乱序、同毫秒、边界间隔、重复），每批后与参考实现比对。"""
    rng = random.Random(seed)
    persons = ["p1", "p2"]
    base = 1_600_000_000_000
    all_events: list[PunchEvent] = []
    with AttendanceStore(str(db_path)).open() as store:
        for step in range(40):
            batch: list[PunchEvent] = []
            for _ in range(rng.randrange(1, 30)):
                kind = rng.random()
                if kind < 0.25:
                    offset = MIN_INTERVAL_MS
                elif kind < 0.45:
                    offset = MIN_INTERVAL_MS - 1
                elif kind < 0.55:
                    offset = 0
                else:
                    offset = rng.randrange(0, 5 * MIN_INTERVAL_MS)
                ts = base + rng.randrange(0, 400) * MIN_INTERVAL_MS + offset
                batch.append(
                    PunchEvent(f"s{seed}e{step:03d}n{_}", rng.choice(persons), ts)
                )
            # 随机重放历史批次中的事件（内容相同）。
            if all_events and rng.random() < 0.5:
                batch.extend(rng.choice(all_events) for _ in range(3))
            store.ingest_batch(batch)
            all_events.extend(batch)
            for person in persons:
                assert query_all(store, person) == reference_valid(
                    all_events, person
                )
