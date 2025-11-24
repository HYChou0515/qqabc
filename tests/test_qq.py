"""
Copyright 2025 Metaist LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

#!/usr/bin/env python
"""Test qqabc.msgq functions."""

# std
import operator
from typing import Callable

# pkg
import qqabc.qq


def test_q_wrapper() -> None:
    """Use underlying queue."""
    q = qqabc.qq.Q("thread")
    q.put(1)
    q.put(qqabc.qq.Msg(data=2))

    assert q.qsize() == 2, "expected function to be delegated to queue"

    want = [1, 2]
    have = [msg.data for msg in q.items(cache=True)]
    assert have == want, "expected both to be the same"

    have = [msg.data for msg in q.items(cache=True)]
    assert have == want, "expected same results after .items() twice"


# example workers


def worker_sum(q: qqabc.qq.Q, out: qqabc.qq.Q, num: int) -> None:
    """Worker that sums message data.

    Args:
        in_q (qqabc.msgq.Q): queue to read from
        out_q (qqabc.msgq.Q): queue to report count
        num (int): worker number
    """
    result = sum(msg.data if isinstance(msg.data, int) else msg.data() for msg in q)
    out.put((num, result))


# running subprocesses and threads #


def test_run_processes() -> None:
    """Run several workers with different arguments."""
    n_msg = 1000

    q, out = qqabc.qq.Q(), qqabc.qq.Q()
    workers = [
        qqabc.qq.run(worker_sum, q, out, num=i) for i in range(qqabc.qq.NUM_CPUS)
    ]

    def wrap_lambda(i: int) -> Callable[[], int]:
        """Wrap a number in a lambda so thread-context works."""
        return lambda: i

    for num in range(n_msg):
        q.put(wrap_lambda(num))

    # for num in range(n_msg):
    #     q.put(qqabc.msgq.Msg(data=num))
    q.stop(workers)

    want = sum(range(n_msg))
    have = sum(msg.data[1] for msg in out.items())
    assert have == want, f"expect sum of {want} from processes"


def test_run_threads() -> None:
    """Run threads in parallel."""
    n_msg = 1000

    q, out = qqabc.qq.Q("thread"), qqabc.qq.Q("thread")
    workers = [
        qqabc.qq.run_thread(worker_sum, q, out, num=i)
        for i in range(qqabc.qq.NUM_THREADS)
    ]

    def wrap_lambda(i: int) -> Callable[[], int]:
        """Wrap a number in a lambda so thread-context works."""
        return lambda: i

    for num in range(n_msg):
        q.put(wrap_lambda(num))
    q.stop(workers)

    want = sum(range(n_msg))
    have = sum(msg.data[1] for msg in out.items())
    assert have == want, f"expect sum of {want} from threads"


def test_map() -> None:
    """Run a function on multiple processes and threads."""
    left = range(10)
    right = range(10, 0, -1)

    want = [a + b for (a, b) in zip(left, right)]
    have = list(qqabc.qq.mapq(operator.add, left, right))
    assert have == want, "expected subprocesses to work"

    have = list(qqabc.qq.mapq(operator.add, left, right, kind="thread"))
    assert have == want, "expected threads to work"
