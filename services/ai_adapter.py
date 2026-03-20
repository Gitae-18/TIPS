# ai_adapter.py
from __future__ import annotations

import json
import os
import random
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class AiDecision:
    label: str
    score: Optional[float]
    ts: float
    raw: dict[str, Any]


class BaseAiAdapter(ABC):
    @abstractmethod
    def read(self, stop_evt: threading.Event) -> Optional[AiDecision]:
        raise NotImplementedError

    def close(self) -> None:
        return


class DummyAiAdapter(BaseAiAdapter):
    def __init__(self, period_sec: float = 5.0, label: str = "ALARM,NORMAL", score: float = 0.9):
        self.period_sec = period_sec
        self.score = score
        sequence_raw = os.getenv("DUMMY_AI_SEQUENCE", label)
        labels = [item.strip().upper() for item in sequence_raw.split(",") if item.strip()]
        self.labels = labels or [label.upper()]
        self._index = 0

    def read(self, stop_evt: threading.Event) -> Optional[AiDecision]:
        steps = max(1, int(self.period_sec / 0.1))
        for _ in range(steps):
            if stop_evt.is_set():
                return None
            time.sleep(0.1)

        label = self.labels[self._index % len(self.labels)]
        self._index += 1
        now = time.time()
        raw = {"label": label, "score": self.score, "ts": now}
        return AiDecision(label=label, score=self.score, ts=now, raw=raw)


class RandomAiAdapter(BaseAiAdapter):
    def __init__(self, period_sec: float = 1.0, alarm_probability: float = 0.15, seed: int = 7):
        self.period_sec = period_sec
        self.alarm_probability = max(0.0, min(1.0, alarm_probability))
        self._random = random.Random(seed)

    def read(self, stop_evt: threading.Event) -> Optional[AiDecision]:
        steps = max(1, int(self.period_sec / 0.1))
        for _ in range(steps):
            if stop_evt.is_set():
                return None
            time.sleep(0.1)

        label = "ALARM" if self._random.random() < self.alarm_probability else "NORMAL"
        score = round(self._random.uniform(0.7, 0.99), 3)
        now = time.time()
        raw = {
            "label": label,
            "score": score,
            "ts": now,
            "source": "random-replay",
            "alarm_probability": self.alarm_probability,
        }
        return AiDecision(label=label, score=score, ts=now, raw=raw)


class StdoutJsonAiAdapter(BaseAiAdapter):
    def __init__(self, cmd: list[str], cwd: Optional[str] = None, env: Optional[dict[str, str]] = None):
        self._proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def read(self, stop_evt: threading.Event) -> Optional[AiDecision]:
        if self._proc.stdout is None:
            return None

        line = self._proc.stdout.readline()
        if stop_evt.is_set():
            return None
        if not line:
            time.sleep(0.05)
            return None

        line = line.strip()
        try:
            obj = json.loads(line)
        except Exception:
            return AiDecision(label="UNKNOWN", score=None, ts=time.time(), raw={"line": line})

        label = str(obj.get("label", "UNKNOWN")).upper()
        score = obj.get("score", None)
        try:
            score_f = float(score) if score is not None else None
        except Exception:
            score_f = None

        ts = obj.get("ts", None)
        try:
            ts_f = float(ts) if ts is not None else time.time()
        except Exception:
            ts_f = time.time()

        return AiDecision(label=label, score=score_f, ts=ts_f, raw=obj)

    def close(self) -> None:
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
            except Exception:
                pass
