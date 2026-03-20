from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class MetadataEnvelope:
    topic: str
    payload: dict[str, Any]
    ts: float


@dataclass(slots=True)
class JpegFrame:
    frame_index: int
    ts: float
    jpeg_bytes: bytes | None = None
    width: int | None = None
    height: int | None = None
    frame_metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class RecordingSession:
    prefix: str
    started_ts: float
    stopped_ts: float | None = None

    @property
    def segment_glob(self) -> str:
        return f"{self.prefix}_*.mkv"


@dataclass(slots=True)
class EventWindow:
    event_id: str
    label: str
    started_ts: float
    metadata_items: list[MetadataEnvelope] = field(default_factory=list)
    recording: RecordingSession | None = None
    trigger_payload: dict[str, Any] = field(default_factory=dict)
    ended_ts: float | None = None


@dataclass(slots=True)
class ClipResult:
    event_id: str
    clip_path: Path
    started_ts: float
    ended_ts: float
    segment_paths: list[Path]
