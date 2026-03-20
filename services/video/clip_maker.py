from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from config.video_config import VideoConfig
from models.models import ClipResult, RecordingSession


class SegmentClipMaker:
    def __init__(self, config: VideoConfig):
        self.config = config
        self.clip_dir = Path(self.config.clip_dir)
        self.clip_dir.mkdir(parents=True, exist_ok=True)

    def create_clip(self, event_id: str, session: RecordingSession) -> ClipResult:
        prefix_path = Path(session.prefix)
        segment_paths = sorted(prefix_path.parent.glob(f"{prefix_path.name}_*.mkv"))
        if not segment_paths:
            raise FileNotFoundError(f"No recording segments found for {session.segment_glob}")

        clip_path = self.clip_dir / f"{event_id}.mkv"
        if len(segment_paths) == 1:
            shutil.copyfile(segment_paths[0], clip_path)
        else:
            concat_file = self.clip_dir / f"{event_id}.txt"
            concat_lines = [f"file '{path.resolve().as_posix()}'" for path in segment_paths]
            concat_file.write_text("\n".join(concat_lines), encoding="utf-8")
            try:
                cmd = [
                    self.config.ffmpeg_bin,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(concat_file),
                    "-c",
                    "copy",
                    str(clip_path),
                ]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            finally:
                concat_file.unlink(missing_ok=True)

        return ClipResult(
            event_id=event_id,
            clip_path=clip_path.resolve(),
            started_ts=session.started_ts,
            ended_ts=session.stopped_ts or session.started_ts,
            segment_paths=[path.resolve() for path in segment_paths],
        )
