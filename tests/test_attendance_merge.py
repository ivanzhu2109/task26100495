"""考勤补传归并库正确性测试。"""

import os
import random
import tempfile
import unittest

from attendance_merge import SUPPRESSION_WINDOW_MS, AttendanceStore, Event

MINUTE = 60_000


def reference_valid(events):
    """独立参考实现：对全部唯一事件直接应用业务规则。"""
    uniq = {e.event_id: e for e in events}
    by_person = {}
    for e in uniq.values():
        by_person.setdefault(e.person_id, []).append(e)
    result = {}
    for pid, evs in by_person.items():
        evs.sort(key=lambda e: (e.ts_ms, e.event_id))
        valid, last = [], None
        for e in evs:
            if last is None or e.ts_ms - last >= SUPPRESSION_WINDOW_MS:
                valid.append(e)
                last = e.ts_ms
        result[pid] = valid
    return result


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.store = AttendanceStore(self.path)

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def valid_minutes(self, person="p1"):
        return [
            e.ts_ms // MINUTE
            for e in self.store.query_valid(person, -(10**15), 10**15)
        ]


class TestChainReaction(StoreTestCase):
    def test_backfill_zero_rewrites_validity(self):
        # 验收场景：先收 2/11/20/29/38 -> 有效 2/20/38；补传 0 -> 0/11/29
        self.store.ingest(
            [Event(f"e{m:02d}", "p1", m * MINUTE) for m in (2, 11, 20, 29, 38)]
        )
        self.assertEqual(self.valid_minutes(), [2, 20, 38])
        self.store.ingest([Event("e00", "p1", 0)])
        self.assertEqual(self.valid_minutes(), [0, 11, 29])

    def test_long_chain_propagates_to_history_end(self):
        # 每 9 分钟一条共 200 条：有效为 0,10,20,...；
        # 在头部补一条 -1 分钟的记录后，有效应变为 -1,8,17,26,... 一直传到末尾，
        # 不能在某个固定回看范围处停止修正。
        n = 200
        self.store.ingest(
            [Event(f"e{i:04d}", "p1", i * 9 * MINUTE) for i in range(n)]
        )
        self.assertEqual(self.valid_minutes()[0], 0)
        self.store.ingest([Event("head", "p1", -MINUTE)])
        got = self.valid_minutes()
        # 与独立参考计算逐条一致，且修正一直传到历史末尾
        ref = reference_valid(
            [Event(f"e{i:04d}", "p1", i * 9 * MINUTE) for i in range(n)]
            + [Event("head", "p1", -MINUTE)]
        )
        self.assertEqual(got, [e.ts_ms // MINUTE for e in ref["p1"]])
        self.assertEqual(got[0], -1)
        self.assertEqual(got[-1], 1791)  # 修正一直传到历史末尾


class TestRandomizedEquivalence(StoreTestCase):
    def test_shuffled_batches_and_replays_match_reference(self):
        rng = random.Random(20261003)
        persons = ["alice", "bob", "carol"]
        events = []
        eid = 0
        for p in persons:
            t = rng.randint(0, 10_000)
            for _ in range(400):
                # 混合：同毫秒(0)、599999、600000 及随机间隔
                t += rng.choice([0, 0, 599_999, 600_000, rng.randint(0, 900_000)])
                events.append(Event(f"ev{eid:06d}", p, t))
                eid += 1
                if rng.random() < 0.05:  # 同毫秒重复事件（不同事件标识）
                    events.append(Event(f"ev{eid:06d}", p, t))
                    eid += 1

        # 随机顺序、随机批次、并随机重放部分批次
        shuffled = events[:]
        rng.shuffle(shuffled)
        batches, i = [], 0
        while i < len(shuffled):
            j = min(len(shuffled), i + rng.randint(1, 50))
            batches.append(shuffled[i:j])
            i = j
        replay = [b for b in batches if rng.random() < 0.3]
        rng.shuffle(replay)

        for b in batches + replay:
            self.store.ingest(b)

        ref = reference_valid(events)
        for p in persons:
            got = self.store.query_valid(p, 0, 10**15)
            want = ref[p]
            self.assertEqual(
                [(e.event_id, e.ts_ms) for e in got],
                [(e.event_id, e.ts_ms) for e in want],
                f"person {p} 的有效记录与独立计算不一致",
            )

    def test_boundary_599999_600000(self):
        base = 1_000_000
        evs = [
            Event("a", "p1", base),
            Event("b", "p1", base + 599_999),   # 差 599999 -> 抑制
            Event("c", "p1", base + 600_000),   # 与 a 差 600000 -> 有效
            Event("d", "p1", base + 1_199_999), # 与 c 差 599999 -> 抑制
            Event("e", "p1", base + 1_200_000), # 与 c 差 600000 -> 有效
        ]
        for order in (evs, evs[::-1], [evs[2], evs[0], evs[4], evs[1], evs[3]]):
            self.store.close()
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(self.path + suffix)
                except FileNotFoundError:
                    pass
            self.store = AttendanceStore(self.path)
            self.store.ingest(order)
            got = [e.event_id for e in self.store.query_valid("p1", 0, 10**15)]
            self.assertEqual(got, ["a", "c", "e"], f"顺序 {order} 下结果错误")

    def test_same_millisecond_ascii_tiebreak(self):
        # 同一毫秒按事件标识 ASCII 字典序，排前者有效并开启抑制窗口
        evs = [
            Event("zz", "p1", 5_000),
            Event("aa", "p1", 5_000),
            Event("mm", "p1", 5_000),
        ]
        self.store.ingest(evs)
        got = [e.event_id for e in self.store.query_valid("p1", 0, 10**15)]
        self.assertEqual(got, ["aa"])


class TestQueryWindow(StoreTestCase):
    def test_window_only_clips_does_not_recompute(self):
        # 0, 5, 12 分钟：有效为 0 和 12（5 被 0 抑制）。
        # 查询起点 6 分钟落在 0 的抑制窗口内，结果应为 [12]，
        # 不能把 5 当作窗口起点重新有效。
        self.store.ingest(
            [
                Event("e0", "p1", 0),
                Event("e5", "p1", 5 * MINUTE),
                Event("e12", "p1", 12 * MINUTE),
            ]
        )
        got = self.store.query_valid("p1", 6 * MINUTE, 60 * MINUTE)
        self.assertEqual([e.event_id for e in got], ["e12"])

    def test_half_open_interval(self):
        self.store.ingest(
            [Event(f"e{i}", "p1", i * 10 * MINUTE) for i in range(5)]
        )
        got = self.store.query_valid("p1", 10 * MINUTE, 30 * MINUTE)
        self.assertEqual([e.event_id for e in got], ["e1", "e2"])

    def test_results_persist_across_reopen(self):
        self.store.ingest(
            [Event(f"e{m:02d}", "p1", m * MINUTE) for m in (2, 11, 20, 29, 38)]
        )
        self.store.ingest([Event("e00", "p1", 0)])
        before = self.store.query_valid("p1", 0, 10**15)
        self.store.close()
        self.store = AttendanceStore(self.path)
        after = self.store.query_valid("p1", 0, 10**15)
        self.assertEqual(before, after)
        self.assertEqual([e.ts_ms // MINUTE for e in after], [0, 11, 29])


class TestIdempotency(StoreTestCase):
    def test_replay_same_batch_is_noop(self):
        batch = [Event(f"e{i}", "p1", i * 7 * MINUTE) for i in range(10)]
        n1 = self.store.ingest(batch)
        first = self.store.query_valid("p1", 0, 10**15)
        n2 = self.store.ingest(batch)
        n3 = self.store.ingest(batch[:5])
        second = self.store.query_valid("p1", 0, 10**15)
        self.assertEqual(n1, 10)
        self.assertEqual((n2, n3), (0, 0))
        self.assertEqual(first, second)

    def test_ingest_return_counts_new_events(self):
        self.assertEqual(self.store.ingest([Event("a", "p1", 0)]), 1)
        self.assertEqual(self.store.ingest([Event("a", "p1", 0)]), 0)
        self.assertEqual(
            self.store.ingest([Event("a", "p1", 0), Event("b", "p2", 5)]), 1
        )

    def test_duplicate_within_single_batch(self):
        ev = Event("dup", "p1", 12345)
        self.assertEqual(self.store.ingest([ev, ev, ev]), 1)
        got = self.store.query_valid("p1", 0, 10**15)
        self.assertEqual([e.event_id for e in got], ["dup"])


if __name__ == "__main__":
    unittest.main()
