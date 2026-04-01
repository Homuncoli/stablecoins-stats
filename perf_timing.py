from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
import threading
import time


@dataclass
class _Stats:
    total_ns: int = 0
    count: int = 0
    min_ns: int = 0
    max_ns: int = 0


class TimingRegistry:
    def __init__(self) -> None:
        self._enabled = False
        self._lock = threading.Lock()
        self._stats: dict[str, _Stats] = defaultdict(_Stats)

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    def clear(self) -> None:
        with self._lock:
            self._stats.clear()

    def record(self, name: str, duration_ns: int) -> None:
        if not self._enabled:
            return

        with self._lock:
            s = self._stats[name]
            s.total_ns += duration_ns
            s.count += 1
            if s.min_ns == 0 or duration_ns < s.min_ns:
                s.min_ns = duration_ns
            if duration_ns > s.max_ns:
                s.max_ns = duration_ns

    def report_lines(self) -> list[str]:
        with self._lock:
            items = list(self._stats.items())

        if not items:
            return ["No timing stats collected."]

        items.sort(key=lambda x: x[1].total_ns, reverse=True)

        lines = [
            "Timing summary across all threads (sorted by total time):",
            "section | calls | avg_ms | total_ms | min_ms | max_ms",
        ]

        for name, s in items:
            avg_ms = (s.total_ns / s.count) / 1_000_000 if s.count else 0.0
            total_ms = s.total_ns / 1_000_000
            min_ms = s.min_ns / 1_000_000
            max_ms = s.max_ns / 1_000_000
            lines.append(
                f"{name} | {s.count} | {avg_ms:.3f} | {total_ms:.3f} | {min_ms:.3f} | {max_ms:.3f}"
            )

        return lines


TIMING = TimingRegistry()


@contextmanager
def timed(name: str):
    if not TIMING.enabled:
        yield
        return

    start = time.perf_counter_ns()
    try:
        yield
    finally:
        TIMING.record(name, time.perf_counter_ns() - start)
