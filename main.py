from __future__ import annotations

import os
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto

from config.video_config import BridgeConfig
from services.ai_adapter import AiDecision, BaseAiAdapter, DummyAiAdapter, RandomAiAdapter
from services.ai_bridge import AiBridge, build_default_bridge
from services.dmx_service import DmxService, build_dmx_service
from services.video.frame_publisher import BaseFramePublisher, build_frame_publisher
from services.video.video_kernel import SubMonitor, VideoService


class EventType(Enum):
    AI_DECISION = auto()
    MCU_RESULT = auto()
    SHUTDOWN = auto()


@dataclass
class Event:
    type: EventType
    payload: AiDecision | dict


class AiService(threading.Thread):
    def __init__(self, event_q: queue.Queue, adapter: BaseAiAdapter):
        super().__init__(daemon=True)
        self.event_q = event_q
        self.adapter = adapter
        self.stop_evt = threading.Event()

    def run(self) -> None:
        try:
            while not self.stop_evt.is_set():
                msg = self.adapter.read(self.stop_evt)
                if not msg:
                    continue

                self.event_q.put(Event(EventType.AI_DECISION, msg))
        finally:
            self.adapter.close()

    def stop(self) -> None:
        self.stop_evt.set()


class McuControlService:
    def __init__(self, event_q: queue.Queue):
        self.event_q = event_q
        self.lock = threading.Lock()

    def set_video_mode(self, mode: str) -> None:
        with self.lock:
            ok = True
            err = ""
            try:
                pass
            except Exception as exc:
                ok = False
                err = str(exc)

            self.event_q.put(
                Event(
                    EventType.MCU_RESULT,
                    {"mode": mode, "ok": ok, "err": err, "ts": time.time()},
                )
            )


class MainClass:
    def __init__(self):
        self.config = BridgeConfig()
        self.event_q: queue.Queue[Event] = queue.Queue()
        self.replay_mode = self.config.video.frame_source.strip().lower() == "replay"

        self.ai = AiService(self.event_q, self._build_ai_adapter())
        self.mcu = McuControlService(self.event_q)
        self.video = VideoService(self.config.video)
        self.sub_monitor = SubMonitor(self.config.video)
        self.bridge: AiBridge = build_default_bridge(self.config)
        self.dmx: DmxService = build_dmx_service(self.config.dmx)
        self.frame_publisher: BaseFramePublisher = build_frame_publisher(
            self.config.video,
            self.bridge.record_jpeg_frame,
            on_complete=self._request_shutdown if self.replay_mode else None,
        )

        self.mode = "NORMAL"
        self.last_switch_ts = 0.0
        self.cooldown_sec = float(os.getenv("MODE_SWITCH_COOLDOWN_SEC", "3.0"))
        self.min_alarm_hold_sec = float(os.getenv("MIN_ALARM_HOLD_SEC", "10.0"))
        self.alarm_started_ts: float | None = None

    def start(self) -> None:
        self.dmx.apply_scene("NORMAL")
        self.dmx.start()
        if not self.replay_mode:
            self.sub_monitor.start()
        self.frame_publisher.start()
        self.ai.start()
        self.run_loop()

    def shutdown(self) -> None:
        self.ai.stop()
        self.sub_monitor.stop()
        self.frame_publisher.stop()
        self.dmx.stop()
        if not self.replay_mode:
            self.video.stop_record()
        self.bridge.close()

    def can_switch(self) -> bool:
        return (time.time() - self.last_switch_ts) >= self.cooldown_sec

    def handle_ai_decision(self, payload: AiDecision) -> None:
        self.bridge.record_ai_metadata(payload)
        label = payload.label

        if label == "ALARM":
            if self.mode != "ALARM" and self.can_switch():
                self.mode = "ALARM"
                self.last_switch_ts = time.time()
                self.alarm_started_ts = payload.ts
                recording = None if self.replay_mode else self.video.start_record(tag="event")
                self.bridge.start_event(payload, recording)
                self.dmx.apply_scene("ALARM")
                self.mcu.set_video_mode("ALARM_VIEW")

        elif label == "NORMAL" and self.mode == "ALARM":
            if self.alarm_started_ts and (time.time() - self.alarm_started_ts) < self.min_alarm_hold_sec:
                return

            if self.can_switch():
                self.mode = "NORMAL"
                self.last_switch_ts = time.time()
                recording = None if self.replay_mode else self.video.stop_record()
                self.bridge.finish_event(payload, recording)
                self.dmx.apply_scene("NORMAL")
                self.mcu.set_video_mode("NORMAL_VIEW")

    def handle_mcu_result(self, payload: dict) -> None:
        return

    def run_loop(self) -> None:
        try:
            while True:
                ev: Event = self.event_q.get()
                if ev.type == EventType.AI_DECISION:
                    self.handle_ai_decision(ev.payload)
                elif ev.type == EventType.MCU_RESULT:
                    self.handle_mcu_result(ev.payload)
                elif ev.type == EventType.SHUTDOWN:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def _build_ai_adapter(self) -> BaseAiAdapter:
        if self.replay_mode:
            return RandomAiAdapter(
                period_sec=self.config.video.replay_event_interval_sec,
                alarm_probability=self.config.video.replay_alarm_probability,
                seed=self.config.video.replay_random_seed,
            )
        return DummyAiAdapter()

    def _request_shutdown(self) -> None:
        self.event_q.put(Event(EventType.SHUTDOWN, {"source": "frame_replay_complete", "ts": time.time()}))


if __name__ == "__main__":
    MainClass().start()
