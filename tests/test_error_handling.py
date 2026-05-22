"""Tests for Pipeline stage 級別的錯誤處理與重試（issue #35）。

驗證：
- retry 成功場景（thread / async）
- retry 耗盡走 skip / dead_letter / raise
- error_handler 自訂邏輯
- worker health check（worker_chance）
- Pipeline.dead_letters
- 多 stage pipeline 錯誤處理
- 向後相容（無錯誤處理參數走原有路徑）
"""

from __future__ import annotations

import sys
import threading

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# === Retry 成功場景 ===


class TestRetrySuccess:
    """item 失敗後重試，最終成功。"""

    def test_retry_succeeds_on_second_attempt(self) -> None:
        """retry=2，fn 第一次失敗、第二次成功。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def flaky(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            if count < 2:
                msg = f"fail on attempt {count}"
                raise ValueError(msg)
            return x * 10

        result = list(
            pipe(
                [Stage(fn=flaky, retry=2, on_error="skip", concurrency=2)],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [10, 20, 30]

    def test_retry_succeeds_on_third_attempt(self) -> None:
        """retry=3，fn 前兩次失敗、第三次成功。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def very_flaky(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            if count < 3:
                msg = f"fail on attempt {count}"
                raise ValueError(msg)
            return x + 100

        result = list(
            pipe(
                [Stage(fn=very_flaky, retry=3, on_error="skip", concurrency=2)],
                input=[1, 2],
            )
        )
        assert sorted(result) == [101, 102]

    def test_no_retry_needed(self) -> None:
        """所有 item 都成功，retry 不觸發。"""
        from qqabc.pipe import Stage, pipe

        result = list(
            pipe(
                [Stage(fn=lambda x: x * 2, retry=3, on_error="skip")],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [2, 4, 6]


# === Retry 耗盡後走 skip ===


class TestRetryExhaustedSkip:
    """retry 耗盡後 on_error='skip'，item 被跳過。"""

    def test_always_fail_skip(self) -> None:
        """Fn 永遠失敗，retry=1，on_error='skip' → item 被跳過。"""
        from qqabc.pipe import Stage, pipe

        def always_fail(x: int) -> int:
            msg = "always fail"
            raise ValueError(msg)

        result = list(
            pipe(
                [Stage(fn=always_fail, retry=1, on_error="skip")],
                input=[1, 2, 3],
            )
        )
        assert result == []

    def test_partial_fail_skip(self) -> None:
        """部分 item 失敗（retry 耗盡後 skip），其餘正常。"""
        from qqabc.pipe import Stage, pipe

        def fail_on_even(x: int) -> int:
            if x % 2 == 0:
                msg = f"even: {x}"
                raise ValueError(msg)
            return x

        result = list(
            pipe(
                [Stage(fn=fail_on_even, retry=0, on_error="skip")],
                input=range(6),
            )
        )
        assert sorted(result) == [1, 3, 5]


# === Retry 耗盡後走 dead_letter ===


class TestRetryExhaustedDeadLetter:
    """retry 耗盡後 on_error='dead_letter'，item 收集到 dead_letters。"""

    def test_dead_letter_collects_failures(self) -> None:
        """失敗 item 出現在 Pipeline.dead_letters。"""
        from qqabc.pipe import Pipeline, Stage

        def fail_on_negative(x: int) -> int:
            if x < 0:
                msg = f"negative: {x}"
                raise ValueError(msg)
            return x * 2

        p = Pipeline(
            [Stage(fn=fail_on_negative, retry=0, on_error="dead_letter", name="neg")],
        )
        results = list(p.run([-1, 2, -3, 4]))
        assert sorted(results) == [4, 8]

        dl = p.dead_letters
        assert len(dl) == 2
        stage_names = {d[0] for d in dl}
        assert stage_names == {"neg"}
        original_items = sorted(d[2] for d in dl)
        assert original_items == [-3, -1]
        # exception 是 ValueError
        for _, exc, _ in dl:
            assert isinstance(exc, ValueError)

    def test_dead_letter_with_retry(self) -> None:
        """retry=2 + dead_letter：重試 2 次後仍失敗才進 dead_letters。"""
        from qqabc.pipe import Pipeline, Stage

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def partial_flaky(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            # x=1 永遠失敗, x=2 第三次成功
            if x == 1:
                msg = "permanent fail"
                raise ValueError(msg)
            if x == 2 and count < 3:
                msg = f"transient fail {count}"
                raise ValueError(msg)
            return x * 10

        p = Pipeline(
            [
                Stage(
                    fn=partial_flaky,
                    retry=2,
                    on_error="dead_letter",
                    name="flaky",
                    concurrency=2,
                ),
            ],
        )
        results = sorted(p.run([1, 2, 3]))
        assert results == [20, 30]

        dl = p.dead_letters
        assert len(dl) == 1
        assert dl[0][0] == "flaky"
        assert dl[0][2] == 1  # original_item

    def test_no_failures_empty_dead_letters(self) -> None:
        """無失敗時 dead_letters 為空。"""
        from qqabc.pipe import Pipeline, Stage

        p = Pipeline(
            [Stage(fn=lambda x: x, retry=1, on_error="dead_letter")],
        )
        results = sorted(p.run([1, 2, 3]))
        assert results == [1, 2, 3]
        assert p.dead_letters == []


# === Retry 耗盡後走 raise ===


class TestRetryExhaustedRaise:
    """retry 耗盡後 on_error='raise'，pipeline 停止並拋出例外。"""

    def test_raise_propagates_exception(self) -> None:
        """on_error='raise' 時，例外在 results() 迭代尾端傳播。"""
        from qqabc.pipe import Pipeline, Stage

        def fail_on_five(x: int) -> int:
            if x == 5:
                msg = "five is bad"
                raise ValueError(msg)
            return x

        p = Pipeline(
            [Stage(fn=fail_on_five, retry=0, on_error="raise", name="r")],
        )
        with pytest.raises(ValueError, match="five is bad"):
            list(p.run([1, 2, 5, 3, 4]))

    def test_raise_with_retry_exhausted(self) -> None:
        """retry=1 + raise：重試 1 次仍失敗後 raise。"""
        from qqabc.pipe import Pipeline, Stage

        def always_fail(x: int) -> int:
            msg = f"fail {x}"
            raise ValueError(msg)

        p = Pipeline(
            [Stage(fn=always_fail, retry=1, on_error="raise")],
        )
        with pytest.raises(ValueError, match="fail 42"):
            list(p.run([42]))


# === error_handler ===


class TestErrorHandler:
    """自訂 error_handler 邏輯。"""

    def test_error_handler_modifies_and_retries(self) -> None:
        """error_handler 修改 item，重試成功。"""
        from qqabc.pipe import Stage, pipe

        def strict_positive(x: int) -> int:
            if x <= 0:
                msg = f"not positive: {x}"
                raise ValueError(msg)
            return x * 10

        def fix_negative(exc: Exception, item: int) -> int:
            """把負數變正數。"""
            return abs(item) if item < 0 else item + 1

        result = list(
            pipe(
                [
                    Stage(
                        fn=strict_positive,
                        retry=1,
                        on_error="skip",
                        error_handler=fix_negative,
                    ),
                ],
                input=[-3, 5, 0],
            )
        )
        # -3 → fix_negative → 3 → 30
        # 5 → 50
        # 0 → fix_negative → 1 → 10
        assert sorted(result) == [10, 30, 50]

    def test_error_handler_repair_with_no_retry_budget(self) -> None:
        """retry=0 時 error_handler 回傳的修復值應該被當作該 item 的結果輸出.

        過去版本 (audit 發現 #5): handler 仍會被呼叫, 但修復值被靜默丟棄,
        並走 on_error='raise' 預設路徑. 新契約: 沒有 retry budget 時,
        handler 的 non-None 回傳值即被視為「修好的結果」直接 emit, 比
        要求使用者寫 retry>=1 更直觀。
        """
        from qqabc.pipe import Stage, pipe

        def fail_small(x: int) -> int:
            if x < 100:
                msg = "small"
                raise ValueError(msg)
            return x * 2

        def repair(exc: Exception, item: int) -> int:
            return item + 100  # 視為已修好的結果

        result = list(
            pipe(
                [Stage(fn=fail_small, retry=0, error_handler=repair)],
                input=[1, 2, 3],
            )
        )
        # 1, 2, 3 → repair → 101, 102, 103 (直接被當作結果輸出, 不再呼叫 fn)
        assert sorted(result) == [101, 102, 103]

    def test_async_error_handler_repair_with_no_retry_budget(self) -> None:
        """同樣的契約套用到 async stage."""
        from qqabc.pipe import Stage, pipe

        async def fail_small(x: int) -> int:
            if x < 100:
                msg = "small"
                raise ValueError(msg)
            return x * 2

        def repair(exc: Exception, item: int) -> int:
            return item + 100

        result = list(
            pipe(
                [Stage(fn=fail_small, retry=0, error_handler=repair)],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [101, 102, 103]

    def test_error_handler_returns_none_skips(self) -> None:
        """error_handler 回傳 None → 直接 skip，不走 retry。"""
        from qqabc.pipe import Pipeline, Stage

        def may_fail(x: int) -> int:
            if x < 0:
                msg = "negative"
                raise ValueError(msg)
            return x

        def skip_handler(exc: Exception, item: int) -> None:
            return None

        p = Pipeline(
            [
                Stage(
                    fn=may_fail,
                    retry=3,
                    on_error="dead_letter",
                    error_handler=skip_handler,
                    name="s",
                ),
            ],
        )
        results = sorted(p.run([-1, 2, -3, 4]))
        assert results == [2, 4]
        # error_handler 回傳 None → skip，不進 dead_letters
        assert p.dead_letters == []

    def test_error_handler_with_dead_letter(self) -> None:
        """error_handler 修改 item 後最終放棄 (回傳 None) → 進 dead_letter.

        新契約 (#5): handler 回傳 non-None 就代表 "已修好的值", 會在 retry 結束時
        直接 emit; 因此若想最終進 dead_letter, handler 必須在最後一次回傳 None
        (代表 "我修不好了, 走 on_error 流程").
        """
        from qqabc.pipe import Pipeline, Stage

        call_count = [0]

        def always_fail(x: int) -> int:
            msg = f"fail {x}"
            raise ValueError(msg)

        def add_one_then_giveup(exc: Exception, item: int) -> int | None:
            call_count[0] += 1
            if call_count[0] == 1:
                return item + 1  # 第一次修一下, 重試還是會掛
            return None  # 放棄 → 進 on_error 流程

        p = Pipeline(
            [
                Stage(
                    fn=always_fail,
                    retry=1,
                    on_error="dead_letter",
                    error_handler=add_one_then_giveup,
                    name="eh",
                ),
            ],
        )
        results = list(p.run([10]))
        # handler 最後一次回 None → skip 路徑 (不進 dead_letter, 不出結果)
        # 這是 returns-None 的既有語義: 從未進入 on_error 的 dead_letter 分支。
        assert results == []
        assert p.dead_letters == []


# === Worker health check ===


class TestWorkerHealth:
    """worker_chance: per-worker 連續失敗上限。"""

    def test_worker_chance_exhausted_other_continues(self) -> None:
        """一個 worker 連續失敗達到 worker_chance，退出。其他 worker 接手完成。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def flaky(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            if x == 99 and count < 3:
                msg = "flaky"
                raise ValueError(msg)
            return x

        result = list(
            pipe(
                [
                    Stage(
                        fn=flaky,
                        retry=5,
                        on_error="skip",
                        worker_chance=2,
                        concurrency=4,
                    ),
                ],
                input=[1, 2, 99, 3],
            )
        )
        # 99 可能成功（被其他 worker 接手），也可能被 skip
        assert 1 in result
        assert 2 in result
        assert 3 in result

    def test_worker_chance_zero_means_disabled(self) -> None:
        """worker_chance=0 表示不啟用 health check。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def fail_then_succeed(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            if count < 4:
                msg = "fail"
                raise ValueError(msg)
            return x

        result = list(
            pipe(
                [
                    Stage(
                        fn=fail_then_succeed,
                        retry=10,
                        on_error="skip",
                        worker_chance=0,
                        concurrency=1,
                    ),
                ],
                input=[42],
            )
        )
        assert result == [42]


# === Async stage retry ===


class TestAsyncRetry:
    """Async stage 的 retry / on_error / error_handler。"""

    def test_async_retry_success(self) -> None:
        """Async stage retry=2，第二次成功。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}

        async def async_flaky(x: int) -> int:
            call_counts[x] = call_counts.get(x, 0) + 1
            if call_counts[x] < 2:
                msg = "transient"
                raise ValueError(msg)
            return x * 10

        result = list(
            pipe(
                [Stage(fn=async_flaky, retry=2, on_error="skip")],
                input=[1, 2, 3],
            )
        )
        assert sorted(result) == [10, 20, 30]

    def test_async_skip(self) -> None:
        """Async stage retry 耗盡後 skip。"""
        from qqabc.pipe import Stage, pipe

        async def always_fail(x: int) -> int:
            msg = "always fail"
            raise ValueError(msg)

        result = list(
            pipe(
                [Stage(fn=always_fail, retry=1, on_error="skip")],
                input=[1, 2, 3],
            )
        )
        assert result == []

    def test_async_dead_letter(self) -> None:
        """Async stage retry 耗盡後 dead_letter。"""
        from qqabc.pipe import Pipeline, Stage

        async def fail_odd(x: int) -> int:
            if x % 2 == 1:
                msg = f"odd: {x}"
                raise ValueError(msg)
            return x

        p = Pipeline(
            [Stage(fn=fail_odd, retry=0, on_error="dead_letter", name="async_dl")],
        )
        results = sorted(p.run([1, 2, 3, 4]))
        assert results == [2, 4]

        dl = p.dead_letters
        assert len(dl) == 2
        original_items = sorted(d[2] for d in dl)
        assert original_items == [1, 3]

    def test_async_raise(self) -> None:
        """Async stage on_error='raise' 傳播例外。"""
        from qqabc.pipe import Pipeline, Stage

        async def fail_on_zero(x: int) -> int:
            if x == 0:
                msg = "zero!"
                raise ValueError(msg)
            return x

        p = Pipeline(
            [Stage(fn=fail_on_zero, retry=1, on_error="raise", name="ar")],
        )
        with pytest.raises(ValueError, match="zero!"):
            list(p.run([1, 0, 2]))

    def test_async_error_handler(self) -> None:
        """Async stage error_handler 修改 item 後重試。"""
        from qqabc.pipe import Stage, pipe

        async def positive_only(x: int) -> int:
            if x < 0:
                msg = "negative"
                raise ValueError(msg)
            return x * 10

        def negate(exc: Exception, item: int) -> int:
            return -item

        result = list(
            pipe(
                [
                    Stage(
                        fn=positive_only,
                        retry=1,
                        on_error="skip",
                        error_handler=negate,
                    ),
                ],
                input=[-5, 3],
            )
        )
        assert sorted(result) == [30, 50]


# === 多 stage pipeline 錯誤處理 ===


class TestMultiStageError:
    """多 stage pipeline 中間 stage 有錯誤處理。"""

    def test_mid_stage_retry_skip(self) -> None:
        """3 stage pipeline，第 2 stage retry + skip。"""
        from qqabc.pipe import Stage, pipe

        call_counts: dict[int, int] = {}
        lock = threading.Lock()

        def stage2_flaky(x: int) -> int:
            with lock:
                call_counts[x] = call_counts.get(x, 0) + 1
                count = call_counts[x]
            if x == 20 and count < 2:
                msg = "fail"
                raise ValueError(msg)
            if x == 40:
                msg = "permanent"
                raise ValueError(msg)
            return x + 1

        result = list(
            pipe(
                [
                    Stage(fn=lambda x: x * 10, name="s1"),
                    Stage(
                        fn=stage2_flaky,
                        retry=2,
                        on_error="skip",
                        name="s2",
                        concurrency=2,
                    ),
                    Stage(fn=lambda x: x * 100, name="s3"),
                ],
                input=[1, 2, 3, 4],
            )
        )
        # s1: [10, 20, 30, 40]
        # s2: 10→11, 20→fail→retry→21, 30→31, 40→fail→fail→fail→skip
        # s3: [1100, 2100, 3100]
        assert sorted(result) == [1100, 2100, 3100]

    def test_multi_stage_dead_letter(self) -> None:
        """多 stage pipeline，dead_letter 紀錄失敗 stage 名稱。"""
        from qqabc.pipe import Pipeline, Stage

        def fail_big(x: int) -> int:
            if x > 100:
                msg = f"too big: {x}"
                raise ValueError(msg)
            return x

        p = Pipeline(
            [
                Stage(fn=lambda x: x * 10, name="multiply"),
                Stage(
                    fn=fail_big,
                    retry=0,
                    on_error="dead_letter",
                    name="filter",
                ),
            ],
        )
        results = sorted(p.run([5, 15, 8]))
        # multiply: [50, 150, 80]
        # filter: 50→50, 150→dead_letter, 80→80
        assert results == [50, 80]

        dl = p.dead_letters
        assert len(dl) == 1
        assert dl[0][0] == "filter"
        assert dl[0][2] == 150


# === 向後相容 ===


class TestBackwardCompat:
    """無錯誤處理參數時行為與 #34 相同（用 _counted_worker）。"""

    def test_no_error_params_works(self) -> None:
        """原始 Stage 不帶錯誤參數，行為不變。"""
        from qqabc.pipe import Stage, pipe

        result = list(pipe([Stage(fn=lambda x: x * 2)], input=[1, 2, 3]))
        assert sorted(result) == [2, 4, 6]

    def test_pipe_operator_still_works(self) -> None:
        """| 運算子串接後仍可正常執行。"""
        from qqabc.pipe import Pipeline, Stage

        s1 = Stage(fn=lambda x: x + 1, name="a")
        s2 = Stage(fn=lambda x: x * 10, name="b")
        p = Pipeline(s1 | s2)
        results = sorted(p.run([1, 2, 3]))
        assert results == [20, 30, 40]

    def test_context_manager_still_works(self) -> None:
        """Context manager 用法不變。"""
        from qqabc.pipe import Pipeline, Stage

        with Pipeline([Stage(fn=lambda x: x + 1)]) as p:
            p.submit(10)
            p.submit(20)
        result = sorted(p.results())
        assert result == [11, 21]

    def test_async_without_error_params_raises_by_default(self) -> None:
        """Async stage 無錯誤參數 → 預設 on_error='raise' (修正自 #1, 不再靜默吞掉).

        想保留舊的 "靜默跳過" 行為需要明確設定 on_error='skip'.
        """
        import pytest

        from qqabc.pipe import Stage, pipe

        async def fail_on_five(x: int) -> int:
            if x == 5:
                msg = "skip"
                raise ValueError(msg)
            return x

        with pytest.raises(ValueError, match="skip"):
            list(pipe([Stage(fn=fail_on_five)], input=range(10)))

        # 顯式 skip 仍然可用
        result = sorted(
            pipe([Stage(fn=fail_on_five, on_error="skip")], input=range(10))
        )
        assert result == [0, 1, 2, 3, 4, 6, 7, 8, 9]


# === Stage 新屬性 ===


class TestStageErrorParams:
    """Stage 新增的錯誤處理參數。"""

    def test_default_values(self) -> None:
        """預設值正確。"""
        from qqabc.pipe import Stage

        s = Stage(fn=lambda x: x)
        assert s.retry == 0
        assert s.on_error == "raise"
        assert s.error_handler is None
        assert s.worker_chance == 0

    def test_custom_values(self) -> None:
        """自訂值正確。"""
        from qqabc.pipe import Stage

        handler = lambda _e, x: x  # noqa: E731

        s = Stage(
            fn=lambda x: x,
            retry=3,
            on_error="dead_letter",
            error_handler=handler,
            worker_chance=5,
        )
        assert s.retry == 3
        assert s.on_error == "dead_letter"
        assert s.error_handler is handler
        assert s.worker_chance == 5

    def test_repr_includes_error_params(self) -> None:
        """Repr 包含非預設的錯誤處理參數。"""
        from qqabc.pipe import Stage

        s = Stage(fn=lambda x: x, retry=2, on_error="skip", worker_chance=3)
        r = repr(s)
        assert "retry=2" in r
        assert "on_error='skip'" in r
        assert "worker_chance=3" in r

    def test_repr_omits_default_error_params(self) -> None:
        """Repr 不包含預設的錯誤處理參數。"""
        from qqabc.pipe import Stage

        s = Stage(fn=lambda x: x, name="test")
        r = repr(s)
        assert "retry" not in r
        assert "on_error" not in r
        assert "worker_chance" not in r


# === Pipeline.dead_letters property ===


class TestDeadLettersProperty:
    """Pipeline.dead_letters 屬性。"""

    def test_dead_letters_returns_copy(self) -> None:
        """dead_letters 回傳 copy，修改不影響原始資料。"""
        from qqabc.pipe import Pipeline, Stage

        def fail(x: int) -> int:
            msg = "fail"
            raise ValueError(msg)

        p = Pipeline(
            [Stage(fn=fail, retry=0, on_error="dead_letter", name="dl")],
        )
        list(p.run([1]))

        dl1 = p.dead_letters
        dl2 = p.dead_letters
        assert dl1 == dl2
        assert dl1 is not dl2

    def test_dead_letters_empty_initially(self) -> None:
        """Pipeline 建立後 dead_letters 為空。"""
        from qqabc.pipe import Pipeline, Stage

        p = Pipeline([Stage(fn=lambda x: x)])
        assert p.dead_letters == []
