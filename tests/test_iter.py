"""
Copyright 2025 Metaist LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

#!/usr/bin/env python
"""Test iteration helpers."""

# native
import random

# pkg
import qqabc.qq


def ident(x: int) -> int:
    """Return the number given."""
    return x


def test_iter_q() -> None:
    """Iterate over all messages."""
    num = 1000
    q = qqabc.qq.Q()

    for _ in range(num):
        q.put(1)

    if not qqabc.qq.IS_MACOS:
        assert q.qsize() == num, "expect all messages queued"

    total = sum(msg.data for msg in q.items())
    assert num == total, "expect iterator to get all messages"


def test_sortiter_sorted_list() -> None:
    """Sort a list of sorted numbers."""
    num = 1000
    want = list(range(num))

    q = qqabc.qq.Q()
    for i in range(num):
        q.put(None, order=i)

    have = [msg.order for msg in q.end().sorted()]
    assert want == have, "expected numbers in order"


def test_sortiter_random_list() -> None:
    """Sort a list of numbers."""
    num = 1000
    want = list(range(num))

    temp = want.copy()
    random.shuffle(temp)

    q = qqabc.qq.Q()
    for num in temp:
        q.put(None, order=num)  # sending things out of order

    have = [msg.order for msg in q.items(sort=True)]
    assert want == have, "expected numbers in order"


def test_sortiter_messages() -> None:
    """Sort messages in order."""
    num = 1000
    order = list(range(num))
    want = order.copy()
    random.shuffle(order)

    q = qqabc.qq.Q()
    for o in order:
        q.put(None, order=o)

    have = [msg.order for msg in q.end().sorted()]
    assert want == have, "expected ids in order"


def test_sortiter_gap() -> None:
    """Sort messages in order even if there's a gap."""
    num = 1000
    order = list(range(num - 10)) + list(range(num - 5, num))
    want = order.copy()
    random.shuffle(order)

    q = qqabc.qq.Q()
    for o in order:
        q.put(None, order=o)

    have = [msg.order for msg in q.end().sorted()]
    assert want == have, "expected ids in order"
