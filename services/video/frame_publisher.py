from __future__ import annotations

import json
import os
import platform
import signal
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path

from config.video_config import VideoConfig
from models.models import JpegFrame
from services.video.video_kernel import log


class BaseFramePublisher(ABC):
    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class RtspJpegFramePublisher(threading.Thread, BaseFramePublisher):
    def __init__(self, config: VideoConfig, on_frame: Callable[[JpegFrame], None]):
        super().__init__(daemon=True)
        self.config = config
        self.on_frame = on_frame
        self.stop_evt = threading.Event()
        self._proc: subprocess.Popen | None = None
        self._is_windows = platform.system().lower().startswith("win")
        self._frame_index = 0

    def stop(self) -> None:
        self.stop_evt.set()
        self._stop_process()

    def run(self) -> None:
        cmd = [
            self.config.ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-i",
            self.config.sub_url,
            "-vf",
            f"fps={self.config.frame_sample_fps}",
            "-q:v",
            str(self.config.jpeg_quality),
            "-f",
            "image2pipe",
            "-vcodec",
            "mjpeg",
            "-",
        ]

        popen_kwargs: dict[str, object] = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.DEVNULL,
        }
        if self._is_windows:
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["preexec_fn"] = os.setsid

        try:
            self._proc = subprocess.Popen(cmd, **popen_kwargs)
        except Exception as exc:
            log("FRAME", f"START FAIL {exc}")
            return

        log("FRAME", f"JPEG PIPE START fps={self.config.frame_sample_fps} batch={self.config.frame_batch_size}")
        try:
            self._stream_frames()
        finally:
            self._stop_process()

    def _stream_frames(self) -> None:
        if self._proc is None or self._proc.stdout is None:
            return

        buffer = bytearray()
        soi = b"\xff\xd8"
        eoi = b"\xff\xd9"

        while not self.stop_evt.is_set():
            chunk = self._proc.stdout.read(65536)
            if not chunk:
                if self._proc.poll() is not None:
                    log("FRAME", "JPEG PIPE STOPPED")
                    return
                time.sleep(0.01)
                continue

            buffer.extend(chunk)

            while True:
                start = buffer.find(soi)
                if start < 0:
                    if len(buffer) > 2:
                        del buffer[:-2]
                    break

                end = buffer.find(eoi, start + 2)
                if end < 0:
                    if start > 0:
                        del buffer[:start]
                    break

                jpeg_bytes = bytes(buffer[start : end + 2])
                del buffer[: end + 2]
                self._frame_index += 1
                self.on_frame(
                    JpegFrame(
                        frame_index=self._frame_index,
                        ts=time.time(),
                        jpeg_bytes=jpeg_bytes,
                    )
                )

    def _stop_process(self) -> None:
        if self._proc is None or self._proc.poll() is not None:
            self._proc = None
            return

        try:
            if self._is_windows:
                self._proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGINT)
            self._proc.wait(timeout=3.0)
        except Exception:
            try:
                if self._is_windows:
                    self._proc.kill()
                else:
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except Exception:
                pass
        finally:
            self._proc = None


class LocalReplayFramePublisher(threading.Thread, BaseFramePublisher):
    def __init__(
        self,
        config: VideoConfig,
        on_frame: Callable[[JpegFrame], None],
        on_complete: Callable[[], None] | None = None,
    ):
        super().__init__(daemon=True)
        self.config = config
        self.on_frame = on_frame
        self.on_complete = on_complete
        self.stop_evt = threading.Event()

    def stop(self) -> None:
        self.stop_evt.set()

    def run(self) -> None:
        frames_dir = Path(self.config.replay_frames_dir)
        metadata_dir = Path(self.config.replay_metadata_dir)
        frame_paths = sorted(frames_dir.glob("*.jpg"))[: self.config.replay_max_frames]
        if not frame_paths:
            log("FRAME", f"REPLAY START FAIL no jpg files in {frames_dir}")
            if self.on_complete is not None:
                self.on_complete()
            return

        period = 1.0 / max(0.1, self.config.replay_fps)
        base_ts = time.time()
        log(
            "FRAME",
            f"REPLAY START fps={self.config.replay_fps} max_frames={len(frame_paths)} dir={frames_dir}",
        )

        try:
            for sequence_index, frame_path in enumerate(frame_paths, start=1):
                if self.stop_evt.is_set():
                    break

                metadata = self._load_frame_metadata(metadata_dir, frame_path.stem)
                frame_index = self._frame_index_from_name(frame_path.stem, sequence_index)
                frame_ts = base_ts + self._metadata_offset_sec(metadata, sequence_index, period)
                with frame_path.open("rb") as fp:
                    jpeg_bytes = fp.read()

                self.on_frame(
                    JpegFrame(
                        frame_index=frame_index,
                        ts=frame_ts,
                        jpeg_bytes=jpeg_bytes,
                        frame_metadata=metadata,
                    )
                )

                if self.stop_evt.wait(period):
                    break
        finally:
            log("FRAME", "REPLAY STOPPED")
            if self.on_complete is not None:
                self.on_complete()

    def _load_frame_metadata(self, metadata_dir: Path, frame_stem: str) -> dict | None:
        metadata_path = metadata_dir / f"{frame_stem}.json"
        if not metadata_path.exists():
            return None

        try:
            with metadata_path.open("r", encoding="utf-8") as fp:
                return json.load(fp)
        except Exception as exc:
            log("FRAME", f"METADATA LOAD FAIL {metadata_path.name} {exc}")
            return None

    def _metadata_offset_sec(self, metadata: dict | None, frame_index: int, period: float) -> float:
        if metadata is None:
            return (frame_index - 1) * period

        try:
            return float(metadata.get("timestamp_sec", (frame_index - 1) * period))
        except Exception:
            return (frame_index - 1) * period

    def _frame_index_from_name(self, frame_stem: str, fallback_index: int) -> int:
        if "_" not in frame_stem:
            return fallback_index
        try:
            return int(frame_stem.rsplit("_", 1)[-1])
        except Exception:
            return fallback_index


def build_frame_publisher(
    config: VideoConfig,
    on_frame: Callable[[JpegFrame], None],
    on_complete: Callable[[], None] | None = None,
) -> BaseFramePublisher:
    source = config.frame_source.strip().lower()
    if source == "replay":
        return LocalReplayFramePublisher(config, on_frame, on_complete=on_complete)
    return RtspJpegFramePublisher(config, on_frame)
