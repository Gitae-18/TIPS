from __future__ import annotations

import os
import platform
import signal
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.video_config import VideoConfig
from models.models import RecordingSession


def log(tag: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}][{tag}] {msg}")


class MainRecorder:
    def __init__(self, config: VideoConfig):
        self.config = config
        self._proc: subprocess.Popen | None = None
        self._active_session: RecordingSession | None = None
        self._is_windows = platform.system().lower().startswith("win")

        os.makedirs(self.config.save_dir, exist_ok=True)

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self, tag: str = "event") -> RecordingSession:
        if self.is_running():
            raise RuntimeError("Recorder already running")

        started_ts = time.time()
        ts = datetime.fromtimestamp(started_ts).strftime("%Y%m%d_%H%M%S")
        prefix = str(Path(self.config.save_dir) / f"{tag}_{ts}")
        out_pattern = f"{prefix}_%03d.mkv"
        cmd = [
            self.config.ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-i",
            self.config.main_url,
            "-c",
            "copy",
            "-f",
            "segment",
            "-segment_time",
            str(self.config.segment_sec),
            "-reset_timestamps",
            "1",
            out_pattern,
        ]

        popen_kwargs: dict[str, object] = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.PIPE,
            "text": True,
        }

        if self._is_windows:
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["preexec_fn"] = os.setsid

        self._proc = subprocess.Popen(cmd, **popen_kwargs)
        self._active_session = RecordingSession(prefix=prefix, started_ts=started_ts)
        log("MAIN", f"RECORD START -> {out_pattern}")
        return self._active_session

    def stop(self, timeout_sec: float = 3.0) -> RecordingSession | None:
        session = self._active_session
        if not self._proc:
            if session and session.stopped_ts is None:
                session.stopped_ts = time.time()
            self._active_session = None
            return session
        if self._proc.poll() is not None:
            if session and session.stopped_ts is None:
                session.stopped_ts = time.time()
            self._proc = None
            self._active_session = None
            return session

        try:
            if self._is_windows:
                self._proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGINT)

            self._proc.wait(timeout=timeout_sec)
        except Exception:
            try:
                if self._is_windows:
                    self._proc.kill()
                else:
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
                log("MAIN", "RECORD STOP (Killed)")
            except Exception:
                pass
        finally:
            self._proc = None
            if session and session.stopped_ts is None:
                session.stopped_ts = time.time()
            self._active_session = None

        return session


class VideoService:
    def __init__(self, config: VideoConfig):
        self._recorder = MainRecorder(config)
        self._session: RecordingSession | None = None

    def start_record(self, tag: str = "event") -> RecordingSession | None:
        if self._recorder.is_running():
            return self._session

        self._session = self._recorder.start(tag=tag)
        return self._session

    def stop_record(self) -> RecordingSession | None:
        if not self._session and not self._recorder.is_running():
            return None

        session = self._recorder.stop()
        self._session = None
        return session

    def current_session(self) -> RecordingSession | None:
        return self._session


class SubMonitor(threading.Thread):
    def __init__(self, config: VideoConfig, interval_sec: float = 5.0, timeout_sec: float = 5.0):
        super().__init__(daemon=True)
        self.config = config
        self.interval_sec = interval_sec
        self.timeout_sec = timeout_sec

        self._stop_evt = threading.Event()
        self.last_ok_ts: Optional[float] = None

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        while not self._stop_evt.is_set():
            ok, info = self._health_check()
            if ok:
                self.last_ok_ts = time.time()
                log("SUB", f"OK {info}")
            else:
                log("SUB", "FAIL")

            self._stop_evt.wait(self.interval_sec)

    def _health_check(self) -> tuple[bool, str]:
        cmd = [
            self.config.ffprobe_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height,r_frame_rate",
            "-of",
            "default=nw=1",
            self.config.sub_url,
        ]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.timeout_sec,
            )
            if result.returncode == 0:
                info = " ".join(line.strip() for line in result.stdout.splitlines() if line.strip())
                return True, info
            return False, ""
        except Exception:
            return False, ""
