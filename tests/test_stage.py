"""Tests for qqabc.pipe.stage — Stage 抽象與 Executor 模型 (issue #31)。

驗證 Stage / IStage 的功能，包含：
- 基本實例化與預設值
- async 自動偵測
- __or__ 運算子串接
- IStage ABC 行為
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="qqabc.pipe requires Python 3.10+",
)


# === 基本實例化 ===


class TestStageInstantiation:
    """Stage 基本實例化與屬性存取。"""

    def test_sync_stage_with_all_params(self) -> None:
        """指定所有參數建立同步 Stage。"""
        from qqabc.pipe.stage import Stage

        def add_one(x: int) -> int:
            return x + 1

        stage = Stage(fn=add_one, executor="thread", concurrency=4, name="adder")
        assert stage.fn is add_one
        assert stage.executor == "thread"
        assert stage.concurrency == 4
        assert stage.name == "adder"

    def test_default_executor_is_thread(self) -> None:
        """未指定 executor 時，同步函式預設為 thread。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x)
        assert stage.executor == "thread"

    def test_default_concurrency_is_4(self) -> None:
        """未指定 concurrency 時預設為 4。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x)
        assert stage.concurrency == 4

    def test_custom_concurrency(self) -> None:
        """自訂 concurrency。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x, concurrency=16)
        assert stage.concurrency == 16

    def test_name_defaults_to_fn_name(self) -> None:
        """未提供 name 時使用 fn.__name__。"""
        from qqabc.pipe.stage import Stage

        def my_transform(x: int) -> int:
            return x * 2

        stage = Stage(fn=my_transform)
        assert stage.name == "my_transform"

    def test_lambda_name(self) -> None:
        """Lambda 的預設名稱為 <lambda>。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x)
        assert stage.name == "<lambda>"

    def test_custom_name_overrides_fn_name(self) -> None:
        """明確指定 name 可覆蓋 fn.__name__。"""
        from qqabc.pipe.stage import Stage

        def my_fn(x: int) -> int:
            return x

        stage = Stage(fn=my_fn, name="custom_name")
        assert stage.name == "custom_name"

    def test_callable_object_without_name(self) -> None:
        """Callable 物件沒有 __name__ 時，name 為空字串。"""
        from qqabc.pipe.stage import Stage

        class MyCallable:
            def __call__(self, x: int) -> int:
                return x + 1

        fn = MyCallable()
        # 移除 __name__ 以模擬沒有此屬性的情境
        assert not hasattr(fn, "__name__")
        stage = Stage(fn=fn)
        assert stage.name == ""

    def test_process_executor(self) -> None:
        """明確指定 executor='process'。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x, executor="process")
        assert stage.executor == "process"

    def test_async_executor_explicit(self) -> None:
        """明確指定 executor='async'。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x, executor="async")
        assert stage.executor == "async"


# === async 自動偵測 ===


class TestAsyncAutoDetection:
    """自動偵測 coroutine function 並設定 executor。"""

    def test_async_fn_auto_detects_async_executor(self) -> None:
        """Async 函式未指定 executor 時自動偵測為 async。"""
        from qqabc.pipe.stage import Stage

        async def async_fn(x: int) -> int:
            return x + 1

        stage = Stage(fn=async_fn)
        assert stage.executor == "async"

    def test_async_fn_explicit_thread_overrides(self) -> None:
        """Async 函式明確指定 executor='thread' 時覆蓋自動偵測。"""
        from qqabc.pipe.stage import Stage

        async def async_fn(x: int) -> int:
            return x + 1

        stage = Stage(fn=async_fn, executor="thread")
        assert stage.executor == "thread"

    def test_async_fn_explicit_process_overrides(self) -> None:
        """Async 函式明確指定 executor='process' 時覆蓋自動偵測。"""
        from qqabc.pipe.stage import Stage

        async def async_fn(x: int) -> int:
            return x + 1

        stage = Stage(fn=async_fn, executor="process")
        assert stage.executor == "process"

    def test_sync_fn_not_detected_as_async(self) -> None:
        """同步函式不會被偵測為 async。"""
        from qqabc.pipe.stage import Stage

        def sync_fn(x: int) -> int:
            return x + 1

        stage = Stage(fn=sync_fn)
        assert stage.executor == "thread"


# === __or__ 運算子 ===


class TestOrOperator:
    """__or__ 運算子串接 stage。"""

    def test_two_stages_pipe(self) -> None:
        """stage_a | stage_b 回傳含兩個 stage 的 list。"""
        from qqabc.pipe.stage import Stage

        a = Stage(fn=lambda x: x, name="a")
        b = Stage(fn=lambda x: x, name="b")
        chain = a | b
        assert isinstance(chain, list)
        assert len(chain) == 2
        assert chain[0] is a
        assert chain[1] is b

    def test_three_stages_pipe(self) -> None:
        """stage_a | stage_b | stage_c 回傳含三個 stage 的 list。"""
        from qqabc.pipe.stage import Stage

        a = Stage(fn=lambda x: x, name="a")
        b = Stage(fn=lambda x: x, name="b")
        c = Stage(fn=lambda x: x, name="c")
        chain = a | b | c
        assert isinstance(chain, list)
        assert len(chain) == 3
        assert chain[0] is a
        assert chain[1] is b
        assert chain[2] is c

    def test_four_stages_pipe(self) -> None:
        """四個 stage 串接。"""
        from qqabc.pipe.stage import Stage

        stages = [Stage(fn=lambda x: x, name=str(i)) for i in range(4)]
        chain = stages[0] | stages[1] | stages[2] | stages[3]
        assert len(chain) == 4
        for i, s in enumerate(chain):
            assert s is stages[i]

    def test_or_with_list_rhs(self) -> None:
        """Stage | [list_of_stages] 展開右側 list。"""
        from qqabc.pipe.stage import Stage

        a = Stage(fn=lambda x: x, name="a")
        b = Stage(fn=lambda x: x, name="b")
        c = Stage(fn=lambda x: x, name="c")
        chain = a | [b, c]
        assert len(chain) == 3
        assert chain[0] is a
        assert chain[1] is b
        assert chain[2] is c

    def test_ror_with_list_lhs(self) -> None:
        """[list_of_stages] | stage 透過 __ror__ 展開左側 list。"""
        from qqabc.pipe.stage import Stage

        a = Stage(fn=lambda x: x, name="a")
        b = Stage(fn=lambda x: x, name="b")
        c = Stage(fn=lambda x: x, name="c")
        # 明確測試 __ror__：list | stage
        chain = c.__ror__([a, b])
        assert len(chain) == 3
        assert chain[0] is a
        assert chain[1] is b
        assert chain[2] is c

    def test_ror_with_non_list(self) -> None:
        """__ror__ 處理非 list 左側運算元。"""
        from qqabc.pipe.stage import Stage

        class FakeObj:
            """模擬一個 __or__ 回傳 NotImplemented 的物件。"""

            def __or__(self, other: object) -> type:
                return NotImplemented  # type: ignore[return-value]

        stage = Stage(fn=lambda x: x, name="s")
        fake = FakeObj()
        result = fake | stage
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] is fake
        assert result[1] is stage


# === __repr__ ===


class TestRepr:
    """Stage 的 __repr__ 輸出。"""

    def test_repr_contains_key_info(self) -> None:
        """__repr__ 包含 name、executor、concurrency。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x, name="test", executor="process", concurrency=2)
        r = repr(stage)
        assert "test" in r
        assert "process" in r
        assert "2" in r

    def test_repr_format(self) -> None:
        """__repr__ 格式正確。"""
        from qqabc.pipe.stage import Stage

        stage = Stage(fn=lambda x: x, name="foo", executor="thread", concurrency=8)
        assert repr(stage) == "Stage(name='foo', executor='thread', concurrency=8)"


# === IStage ABC ===


class TestIStageABC:
    """IStage 抽象介面的行為。"""

    def test_cannot_instantiate_istage(self) -> None:
        """IStage 是抽象類別，無法直接實例化。"""
        from qqabc.pipe.stage import IStage

        with pytest.raises(TypeError):
            IStage()  # type: ignore[abstract]

    def test_custom_istage_implementation(self) -> None:
        """自訂 IStage 實作可正常運作。"""
        from qqabc.pipe.stage import ExecutorType, IStage

        class MyStage(IStage[int, int]):
            @property
            def fn(self) -> Callable[[int], int]:
                return lambda x: x * 2

            @property
            def executor(self) -> ExecutorType:
                return "thread"

            @property
            def concurrency(self) -> int:
                return 2

            @property
            def name(self) -> str:
                return "my_stage"

        stage = MyStage()
        assert stage.name == "my_stage"
        assert stage.executor == "thread"
        assert stage.concurrency == 2
        assert stage.fn(5) == 10

    def test_custom_istage_or_with_stage(self) -> None:
        """自訂 IStage 可與 Stage 用 | 串接。"""
        from qqabc.pipe.stage import ExecutorType, IStage, Stage

        class MyStage(IStage[int, int]):
            @property
            def fn(self) -> Callable[[int], int]:
                return lambda x: x * 2

            @property
            def executor(self) -> ExecutorType:
                return "process"

            @property
            def concurrency(self) -> int:
                return 1

            @property
            def name(self) -> str:
                return "custom"

        custom = MyStage()
        builtin = Stage(fn=lambda x: x, name="builtin")
        chain = custom | builtin
        assert len(chain) == 2
        assert chain[0] is custom
        assert chain[1] is builtin


# === ExecutorType ===


class TestExecutorType:
    """ExecutorType 型別别名。"""

    def test_executor_type_importable(self) -> None:
        """ExecutorType 可以正常匯入。"""
        from qqabc.pipe.stage import ExecutorType

        # 確認它是可用的 Literal type（在 runtime 它是特殊型別）
        assert ExecutorType is not None

    def test_all_executor_values(self) -> None:
        """Stage 接受三種 executor 值。"""
        from qqabc.pipe.stage import Stage

        for executor in ("thread", "process", "async"):
            stage = Stage(fn=lambda x: x, executor=executor)  # type: ignore[arg-type]
            assert stage.executor == executor


# === 透過 pipe 套件匯入 ===


class TestPipePackageExports:
    """透過 qqabc.pipe 匯入 Stage 相關符號。"""

    def test_import_stage_from_pipe(self) -> None:
        """從 qqabc.pipe 匯入 Stage。"""
        from qqabc.pipe import Stage

        stage = Stage(fn=lambda x: x + 1)
        assert stage.executor == "thread"

    def test_import_istage_from_pipe(self) -> None:
        """從 qqabc.pipe 匯入 IStage。"""
        from qqabc.pipe import IStage

        assert IStage is not None

    def test_import_executor_type_from_pipe(self) -> None:
        """從 qqabc.pipe 匯入 ExecutorType。"""
        from qqabc.pipe import ExecutorType

        assert ExecutorType is not None
