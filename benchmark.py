"""规模场景性能基准。

场景：单人 100 万条历史记录（初始导入不计时），随后连续 1000 次
单事件末尾补传（每次只影响末尾不超过 20 条记录的有效性），每次补传
紧接一次末尾 1 小时查询。要求补传 + 查询合计不超过 15 秒。

运行：python3 benchmark.py
"""

from __future__ import annotations

import os
import tempfile
import time

from attendance_merge import MIN_INTERVAL_MS, AttendanceStore, PunchEvent

INITIAL_COUNT = 1_000_000
APPEND_COUNT = 1_000
HOUR_MS = 3_600_000
BUDGET_S = 15.0


def main() -> None:
    db_path = os.path.join(tempfile.mkdtemp(prefix="attendance_bench_"), "bench.db")
    person = "p-bench"
    base_ts = 1_700_000_000_000

    with AttendanceStore(db_path).open() as store:
        # 初始导入：每 10 分钟一条，全部有效，分 100 批。
        t0 = time.perf_counter()
        batch_size = 10_000
        for start in range(0, INITIAL_COUNT, batch_size):
            store.ingest_batch(
                PunchEvent(f"init{i:07d}", person, base_ts + i * MIN_INTERVAL_MS)
                for i in range(start, start + batch_size)
            )
        print(
            f"初始导入 {INITIAL_COUNT} 条：{time.perf_counter() - t0:.2f} 秒（不计入预算）"
        )

        # 计时段：1000 次单事件末尾补传 + 末尾 1 小时查询。
        tail_ts = base_ts + INITIAL_COUNT * MIN_INTERVAL_MS
        t1 = time.perf_counter()
        last_query_hits = 0
        for i in range(APPEND_COUNT):
            store.ingest_batch(
                [PunchEvent(f"tail{i:05d}", person, tail_ts + i * MIN_INTERVAL_MS)]
            )
            hits = store.query_valid(
                person, tail_ts + i * MIN_INTERVAL_MS - HOUR_MS, 2**62
            )
            last_query_hits = len(hits)
        elapsed = time.perf_counter() - t1

    print(
        f"{APPEND_COUNT} 次补传 + 末尾 1 小时查询合计：{elapsed:.2f} 秒"
        f"（预算 {BUDGET_S:.0f} 秒，末次查询命中 {last_query_hits} 条）"
    )
    if elapsed > BUDGET_S:
        raise SystemExit("超出时间预算")
    print("通过")


if __name__ == "__main__":
    main()
