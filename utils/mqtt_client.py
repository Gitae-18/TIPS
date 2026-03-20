from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class MqttConnectionConfig:
    host: str
    port: int
    client_id: str = ""
    keepalive: int = 30
    qos: int = 1


class BaseMqttClient:
    def publish_json(self, topic: str, payload: dict[str, Any], qos: int | None = None) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


class StdoutMqttClient(BaseMqttClient):
    def publish_json(self, topic: str, payload: dict[str, Any], qos: int | None = None) -> None:
        print(f"[MQTT:{topic}] {json.dumps(_summarize_payload(payload), ensure_ascii=True)}")


class PahoMqttClient(BaseMqttClient):
    def __init__(self, config: MqttConnectionConfig):
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("paho-mqtt is required for MQTT publishing") from exc

        self._config = config
        self._client = mqtt.Client(client_id=config.client_id or f"tips-{uuid4().hex[:12]}")
        self._client.connect(config.host, config.port, keepalive=config.keepalive)
        self._client.loop_start()

    def publish_json(self, topic: str, payload: dict[str, Any], qos: int | None = None) -> None:
        msg = json.dumps(payload, ensure_ascii=True)
        result = self._client.publish(topic, msg, qos=self._config.qos if qos is None else qos)
        result.wait_for_publish()

    def close(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


def create_mqtt_client(config: MqttConnectionConfig) -> BaseMqttClient:
    try:
        return PahoMqttClient(config)
    except Exception:
        return StdoutMqttClient()


def _summarize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "frames" not in payload:
        return payload

    summarized = dict(payload)
    frames = payload.get("frames", [])
    summarized_frames: list[dict[str, Any]] = []
    for frame in frames:
        if not isinstance(frame, dict):
            summarized_frames.append(frame)
            continue

        frame_summary = dict(frame)
        image_b64 = frame_summary.get("image_jpeg_b64")
        if isinstance(image_b64, str):
            frame_summary["image_jpeg_b64"] = f"<base64:{len(image_b64)} chars>"
        frame_metadata = frame_summary.get("frame_metadata")
        if isinstance(frame_metadata, dict):
            frame_summary["frame_metadata"] = {
                "frame_name": frame_metadata.get("frame_name"),
                "frame_index": frame_metadata.get("frame_index"),
                "timestamp_sec": frame_metadata.get("timestamp_sec"),
                "detections_count": frame_metadata.get("detections_count"),
            }
        summarized_frames.append(frame_summary)

    summarized["frames"] = summarized_frames
    return summarized
