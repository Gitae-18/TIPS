from __future__ import annotations

import base64
import threading
import uuid
from abc import ABC, abstractmethod
from collections import deque
from copy import deepcopy
from dataclasses import asdict

from config.video_config import BridgeConfig
from models.models import EventWindow, JpegFrame, MetadataEnvelope, RecordingSession
from services.ai_adapter import AiDecision
from utils.mqtt_client import BaseMqttClient, MqttConnectionConfig, StdoutMqttClient, create_mqtt_client


class BaseMetadataPublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, payload: dict) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


class StdoutMetadataPublisher(BaseMetadataPublisher):
    def __init__(self, client: BaseMqttClient | None = None):
        self._client = client or StdoutMqttClient()

    def publish(self, topic: str, payload: dict) -> None:
        self._client.publish_json(topic, payload)

    def close(self) -> None:
        self._client.close()


class MetadataRingBuffer:
    def __init__(self, maxlen: int):
        self._items: deque[MetadataEnvelope] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, item: MetadataEnvelope) -> None:
        with self._lock:
            self._items.append(item)

    def snapshot_since(self, ts: float) -> list[MetadataEnvelope]:
        with self._lock:
            return [item for item in self._items if item.ts >= ts]


class AiBridge:
    def __init__(
        self,
        config: BridgeConfig,
        metadata_publisher: BaseMetadataPublisher,
    ):
        self.config = config
        self.metadata_publisher = metadata_publisher
        self.metadata_ring = MetadataRingBuffer(config.metadata.metadata_ring_size)
        self._frame_lock = threading.Lock()
        self._pending_frames: list[dict] = []
        self._frame_batch_index = 0
        self._latest_ai_payload: dict | None = None
        self._event_lock = threading.Lock()
        self._active_event: EventWindow | None = None

    def record_ai_metadata(self, decision: AiDecision) -> None:
        topic = f"{self.config.metadata.mqtt_topic_prefix}/ai/decision"
        payload = {
            "label": decision.label,
            "score": decision.score,
            "ts": decision.ts,
            "raw": decision.raw,
        }
        self.metadata_ring.append(MetadataEnvelope(topic=topic, payload=payload, ts=decision.ts))
        with self._frame_lock:
            self._latest_ai_payload = deepcopy(payload)

    def record_jpeg_frame(self, frame: JpegFrame) -> None:
        batch_payload: dict | None = None
        topic: str | None = None
        with self._frame_lock:
            frame_payload = {
                "frame_index": frame.frame_index,
                "frame_name": f"frame_{frame.frame_index:06d}.jpg",
                "timestamp_epoch": frame.ts,
                "width": frame.width,
                "height": frame.height,
                "frame_metadata": deepcopy(frame.frame_metadata),
                "ai_metadata": deepcopy(self._latest_ai_payload),
            }
            if frame.jpeg_bytes is not None:
                frame_payload["image_jpeg_b64"] = base64.b64encode(frame.jpeg_bytes).decode("ascii")
            self._pending_frames.append(frame_payload)
            if len(self._pending_frames) < self.config.video.frame_batch_size:
                return

            self._frame_batch_index += 1
            frames = self._pending_frames
            self._pending_frames = []
            has_images = any("image_jpeg_b64" in item for item in frames)
            batch_payload = {
                "batch_id": f"{self.config.video.camera_id}-{self._frame_batch_index:06d}",
                "batch_index": self._frame_batch_index,
                "camera_id": self.config.video.camera_id,
                "fps": self.config.video.effective_frame_fps,
                "count": len(frames),
                "start_ts": frames[0]["timestamp_epoch"],
                "end_ts": frames[-1]["timestamp_epoch"],
                "video_transport": "rtsp" if not has_images and self.config.video.frame_source.strip().lower() == "replay" else "mqtt",
                "rtsp_url": self.config.video.rtsp_publish_url if not has_images else None,
                "frames": frames,
            }
            topic = self._frame_batch_topic(has_images)

        if batch_payload is not None and topic is not None:
            self.metadata_publisher.publish(topic, batch_payload)

    def start_event(self, decision: AiDecision, recording: RecordingSession | None) -> str:
        with self._event_lock:
            event_id = f"event-{uuid.uuid4().hex[:12]}"
            self._active_event = EventWindow(
                event_id=event_id,
                label=decision.label,
                started_ts=decision.ts,
                recording=recording,
                trigger_payload=decision.raw,
                metadata_items=self.metadata_ring.snapshot_since(decision.ts),
            )

        self._publish_event_state("started", self._active_event)
        return event_id

    def finish_event(self, decision: AiDecision, recording: RecordingSession | None) -> None:
        with self._event_lock:
            event = self._active_event
            self._active_event = None

        if event is None:
            return

        event.ended_ts = decision.ts
        event.recording = recording or event.recording
        event.metadata_items = self.metadata_ring.snapshot_since(event.started_ts)

        self._publish_event_state("ended", event)
        self._publish_metadata_bundle(event)

    def close(self) -> None:
        self._flush_pending_frames()
        self.metadata_publisher.close()

    def _publish_event_state(self, state: str, event: EventWindow) -> None:
        payload = {
            "event_id": event.event_id,
            "state": state,
            "label": event.label,
            "started_ts": event.started_ts,
            "ended_ts": event.ended_ts,
            "recording_prefix": event.recording.prefix if event.recording else None,
            "trigger_payload": event.trigger_payload,
        }
        topic = f"{self.config.metadata.mqtt_topic_prefix}/events/{state}"
        self.metadata_publisher.publish(topic, payload)

    def _publish_metadata_bundle(self, event: EventWindow) -> None:
        topic = f"{self.config.metadata.mqtt_topic_prefix}/events/{event.event_id}/metadata"
        payload = {
            "event_id": event.event_id,
            "started_ts": event.started_ts,
            "ended_ts": event.ended_ts,
            "items": [asdict(item) for item in event.metadata_items],
        }
        self.metadata_publisher.publish(topic, payload)

    def _flush_pending_frames(self) -> None:
        with self._frame_lock:
            if not self._pending_frames:
                return

            self._frame_batch_index += 1
            frames = self._pending_frames
            self._pending_frames = []
            has_images = any("image_jpeg_b64" in item for item in frames)

        payload = {
            "batch_id": f"{self.config.video.camera_id}-{self._frame_batch_index:06d}",
            "batch_index": self._frame_batch_index,
            "camera_id": self.config.video.camera_id,
            "fps": self.config.video.effective_frame_fps,
            "count": len(frames),
            "start_ts": frames[0]["timestamp_epoch"],
            "end_ts": frames[-1]["timestamp_epoch"],
            "video_transport": "rtsp" if not has_images and self.config.video.frame_source.strip().lower() == "replay" else "mqtt",
            "rtsp_url": self.config.video.rtsp_publish_url if not has_images else None,
            "frames": frames,
        }
        topic = self._frame_batch_topic(has_images)
        self.metadata_publisher.publish(topic, payload)

    def _frame_batch_topic(self, has_images: bool) -> str:
        if has_images:
            return f"{self.config.metadata.mqtt_topic_prefix}/frames/jpeg/batch"
        suffix = self.config.video.replay_metadata_topic.strip("/") or "frames/metadata/batch"
        return f"{self.config.metadata.mqtt_topic_prefix}/{suffix}"


def build_default_bridge(config: BridgeConfig) -> AiBridge:
    mqtt_client = create_mqtt_client(
        MqttConnectionConfig(
            host=config.metadata.mqtt_host,
            port=config.metadata.mqtt_port,
        )
    )
    metadata_publisher: BaseMetadataPublisher = StdoutMetadataPublisher(mqtt_client)

    return AiBridge(
        config=config,
        metadata_publisher=metadata_publisher,
    )
