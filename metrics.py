from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import threading
import time

@dataclass
class TimingStats:
    total_ns: int = 0
    count: int = 0
    min_ns: int = 0
    max_ns: int = 0


class TimingRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, TimingStats] = defaultdict(TimingStats)

    def clear(self) -> None:
        with self._lock:
            self._stats.clear()

    def record(self, name: str, duration_ns: int) -> None:
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

        headers = ("section", "calls", "%", "avg_ms", "total_ms", "min_ms", "max_ms")
        rows: list[tuple[str, str, str, str, str, str, str]] = []
        grand_total_ns = sum(s.total_ns for _, s in items)
        for name, s in items:
            pct_total = (s.total_ns / grand_total_ns * 100.0) if grand_total_ns else 0.0
            avg_ms = (s.total_ns / s.count) / 1_000_000 if s.count else 0.0
            total_ms = s.total_ns / 1_000_000
            min_ms = s.min_ns / 1_000_000
            max_ms = s.max_ns / 1_000_000
            rows.append(
                (
                    name,
                    str(s.count),
                    f"{pct_total:.1f}%",
                    f"{avg_ms:.3f}",
                    f"{total_ms:.3f}",
                    f"{min_ms:.3f}",
                    f"{max_ms:.3f}",
                )
            )

        widths = [
            max(len(headers[col]), *(len(row[col]) for row in rows))
            for col in range(len(headers))
        ]

        row_format = " | ".join(
            [
                f"{{:<{widths[0]}}}",
                f"{{:>{widths[1]}}}",
                f"{{:>{widths[2]}}}",
                f"{{:>{widths[3]}}}",
                f"{{:>{widths[4]}}}",
                f"{{:>{widths[5]}}}",
                f"{{:>{widths[6]}}}",
            ]
        )

        lines = ["Timing summary across all threads (sorted by total time):"]
        lines.append(row_format.format(*headers))
        lines.append("-+-".join("-" * w for w in widths))
        for row in rows:
            lines.append(row_format.format(*row))

        return lines


TIMING_ENABLED = False
TIMING = {
    "default": TimingRegistry()
}


@contextmanager
def timed(name: str, space: str = "default", log: bool = False):
    if not TIMING_ENABLED:
        yield
        return

    start = time.perf_counter_ns()
    try:
        yield
    finally:
        if space not in TIMING:
            TIMING[space] = TimingRegistry()
        end = time.perf_counter_ns()
        TIMING[space].record(name, end - start)
        if log:
            logging.info("Timing: %s [%s] took %.3f s", name, space, (end - start) / 1_000_000_000)

def log_timings():
    if TIMING_ENABLED:
        for space, registry in TIMING.items():
            for line in registry.report_lines():
                logging.info("%s [%s]", line, space)