"""规模场景耗时基准。

场景（对应非功能要求）：
- 单人 100 万条历史记录初始导入（不计入考核，只报告耗时）。
- 连续 1000 次单事件末尾补传，每次紧接一次末尾 1 小时查询，合计计时，
  要求 <= 15 秒。补传交替模拟两类常见情形（每次只影响末尾 0~1 条，
  满足"不超过 20 条"的前提）：
  1) 断联恢复后向末尾追加新事件；
  2) 同一毫秒补到字典序更小的事件标识，按 ASCII 规则夺走有效性。
- 附加场景：一次插入到历史最前端、引发全量长链变化的补传，
  耗时允许随受影响记录数增长，单独报告。
- 最后对全量数据独立重算，校验增量维护结果的正确性。
"""

import os
import tempfile
import time

from attendance_merge import SUPPRESSION_WINDOW_MS, AttendanceStore, Event

PERSON = "employee-0001"
CHAIN_PERSON = "employee-chain"
INITIAL_EVENTS = 1_000_000
CHAIN_EVENTS = 200_000
CHAIN_STEP_MS = 540_000  # 9 分钟一条：相位偏移会一路传到历史末尾
BACKFILL_ROUNDS = 1_000
STEP_MS = 60_000  # 初始记录每分钟一条
TAIL_STEP_MS = 30_000  # 末尾区域每 30 秒一条
HOUR_MS = 3_600_000


def build_store(path: str) -> AttendanceStore:
    store = AttendanceStore(path)
    t0 = time.perf_counter()
    batch = (
        Event(event_id=f"init-{i:07d}", person_id=PERSON, ts_ms=i * STEP_MS)
        for i in range(INITIAL_EVENTS)
    )
    n = store.ingest(batch)
    chain = (
        Event(
            event_id=f"chain-{i:07d}",
            person_id=CHAIN_PERSON,
            ts_ms=i * CHAIN_STEP_MS,
        )
        for i in range(CHAIN_EVENTS)
    )
    n += store.ingest(chain)
    dt = time.perf_counter() - t0
    assert n == INITIAL_EVENTS + CHAIN_EVENTS
    print(f"[初始导入] {n} 条: {dt:.2f} 秒（不计入考核）")
    return store


def run_tail_benchmark(store: AttendanceStore) -> None:
    tail_ts = (INITIAL_EVENTS - 1) * STEP_MS
    max_flips = 0
    queried = 0
    t0 = time.perf_counter()
    for i in range(BACKFILL_ROUNDS):
        if i % 2 == 0:
            # 情形 1：末尾追加一条新事件
            tail_ts += TAIL_STEP_MS
            ev = Event(event_id=f"tail-{i:05d}", person_id=PERSON, ts_ms=tail_ts)
        else:
            # 情形 2：与最近一条有效记录同毫秒、但事件标识字典序更小
            last_valid = store._conn.execute(
                "SELECT ts_ms FROM events"
                " WHERE person_id = ? AND valid = 1"
                " ORDER BY ts_ms DESC, event_id DESC LIMIT 1",
                (PERSON,),
            ).fetchone()[0]
            ev = Event(event_id=f"bkf-{i:05d}", person_id=PERSON, ts_ms=last_valid)
        before = store._conn.total_changes
        store.ingest([ev])
        max_flips = max(max_flips, store._conn.total_changes - before - 1)
        queried += len(store.query_valid(PERSON, tail_ts - HOUR_MS, tail_ts + 1))
    dt = time.perf_counter() - t0
    print(
        f"[末尾补传] {BACKFILL_ROUNDS} 次单事件补传（单次最多翻转 {max_flips} 条，"
        f"要求 <= 20）+ 每次末尾 1 小时查询，合计: {dt:.3f} 秒（要求 <= 15 秒），"
        f"查询共返回 {queried} 条"
    )
    assert max_flips <= 20, f"单次补传影响超过 20 条: {max_flips}"
    assert dt <= 15, "超出 15 秒限制"


def run_long_chain_benchmark(store: AttendanceStore) -> None:
    """在历史最前端补一条记录，有效性变化一路传播到历史末尾。"""
    before = store._conn.total_changes
    t0 = time.perf_counter()
    store.ingest(
        [Event(event_id="aaa-head", person_id=CHAIN_PERSON, ts_ms=-CHAIN_STEP_MS)]
    )
    dt = time.perf_counter() - t0
    flips = store._conn.total_changes - before - 1
    print(
        f"[长链变化] 头部补 1 条引发 {flips} 条翻转（共 {CHAIN_EVENTS} 条历史）:"
        f" {dt:.3f} 秒（允许随受影响记录数增长）"
    )
    assert flips > CHAIN_EVENTS // 2, "长链未传播到历史末尾"


def verify_full_recompute(store: AttendanceStore) -> None:
    """绕开增量状态，对全量数据独立重算并比对。"""
    t0 = time.perf_counter()
    rows = store._conn.execute(
        "SELECT person_id, ts_ms, event_id, valid FROM events"
        " ORDER BY person_id, ts_ms, event_id",
    ).fetchall()
    last = None
    prev_person = None
    mismatches = 0
    for person_id, ts_ms, event_id, valid in rows:
        if person_id != prev_person:
            last = None
            prev_person = person_id
        expect = int(last is None or ts_ms - last >= SUPPRESSION_WINDOW_MS)
        if expect:
            last = ts_ms
        if expect != valid:
            mismatches += 1
    dt = time.perf_counter() - t0
    print(f"[正确性校验] 全量独立重算 {len(rows)} 条: {dt:.2f} 秒，不一致 {mismatches} 条")
    assert mismatches == 0


def main() -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    try:
        store = build_store(path)
        run_tail_benchmark(store)
        run_long_chain_benchmark(store)
        verify_full_recompute(store)
        store.close()
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(path + suffix)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    main()
