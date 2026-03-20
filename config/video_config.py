from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = os.getenv(name)
    if not raw:
        return default
    values: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values or default


@dataclass(slots=True)
class VideoConfig:
    camera_id: str = os.getenv("CAMERA_ID", "cam1")
    frame_source: str = os.getenv("FRAME_SOURCE", "rtsp")
    sub_url: str = os.getenv("VIDEO_SUB_URL", "rtsp://CM5_IP:8554/cam1_sub")
    main_url: str = os.getenv("VIDEO_MAIN_URL", "rtsp://CM5_IP:8554/cam1_main")
    save_dir: str = os.getenv("VIDEO_SAVE_DIR", "./recordings")
    clip_dir: str = os.getenv("VIDEO_CLIP_DIR", "./clips")
    segment_sec: int = int(os.getenv("VIDEO_SEGMENT_SEC", "60"))
    frame_batch_size: int = int(os.getenv("FRAME_BATCH_SIZE", "10"))
    frame_sample_fps: int = int(os.getenv("FRAME_SAMPLE_FPS", "30"))
    replay_frames_dir: str = os.getenv("REPLAY_FRAMES_DIR", "/home/pi/tips/frames")
    replay_metadata_dir: str = os.getenv("REPLAY_METADATA_DIR", "/home/pi/tips/metadatas")
    replay_max_frames: int = int(os.getenv("REPLAY_MAX_FRAMES", "0"))
    replay_fps: float = float(os.getenv("REPLAY_FPS", "15"))
    replay_alarm_probability: float = float(os.getenv("REPLAY_ALARM_PROBABILITY", "0.15"))
    replay_event_interval_sec: float = float(os.getenv("REPLAY_EVENT_INTERVAL_SEC", "1.0"))
    replay_random_seed: int = int(os.getenv("REPLAY_RANDOM_SEED", "7"))
    replay_rtsp_codec: str = os.getenv("REPLAY_RTSP_CODEC", "h264")
    replay_rtsp_preset: str = os.getenv("REPLAY_RTSP_PRESET", "veryfast")
    replay_rtsp_pixel_format: str = os.getenv("REPLAY_RTSP_PIXEL_FORMAT", "yuv420p")
    replay_metadata_topic: str = os.getenv("REPLAY_METADATA_TOPIC", "frames/metadata/batch")
    jpeg_quality: int = int(os.getenv("JPEG_QUALITY", "5"))
    ffmpeg_bin: str = os.getenv("FFMPEG_BIN", "ffmpeg")
    ffprobe_bin: str = os.getenv("FFPROBE_BIN", "ffprobe")
    rtsp_publish_url: str = os.getenv("RTSP_PUBLISH_URL", "rtsp://127.0.0.1:8554/events")

    @property
    def effective_frame_fps(self) -> float:
        if self.frame_source.strip().lower() == "replay":
            return self.replay_fps
        return float(self.frame_sample_fps)


@dataclass(slots=True)
class MetadataConfig:
    mqtt_host: str = os.getenv("MQTT_HOST", "127.0.0.1")
    mqtt_port: int = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_topic_prefix: str = os.getenv("MQTT_TOPIC_PREFIX", "tips")
    metadata_ring_size: int = int(os.getenv("METADATA_RING_SIZE", "256"))


@dataclass(slots=True)
class DmxConfig:
    backend: str = os.getenv("DMX_BACKEND", "uart")
    fps: float = float(os.getenv("DMX_FPS", "30"))
    universe_size: int = int(os.getenv("DMX_UNIVERSE_SIZE", "512"))
    fixture_count: int = int(os.getenv("DMX_FIXTURE_COUNT", "2"))
    channels_per_fixture: int = int(os.getenv("DMX_CHANNELS_PER_FIXTURE", "2"))
    normal_dim_value: int = int(os.getenv("DMX_NORMAL_DIM_VALUE", "0"))
    normal_img_value: int = int(os.getenv("DMX_NORMAL_IMG_VALUE", "0"))
    alarm_dim_value: int = int(os.getenv("DMX_ALARM_DIM_VALUE", "255"))
    alarm_img_value: int = int(os.getenv("DMX_ALARM_IMG_VALUE", "16"))
    uart_port: str = os.getenv("DMX_UART_PORT", "COM1")
    uart_baudrate: int = int(os.getenv("DMX_UART_BAUDRATE", "115200"))
    uart_timeout_sec: float = float(os.getenv("DMX_UART_TIMEOUT_SEC", "0.2"))
    uart_start_address: int = int(os.getenv("DMX_UART_START_ADDRESS", "1"))
    uart_input_length: int = int(os.getenv("DMX_UART_INPUT_LENGTH", "4"))
    uart_output_length: int = int(os.getenv("DMX_UART_OUTPUT_LENGTH", "4"))
    uart_configure_on_open: bool = _env_bool("DMX_UART_CONFIGURE_ON_OPEN", False)
    gpio_pins: list[int] = field(default_factory=lambda: _env_int_list("DMX_GPIO_PINS", [17, 27, 22]))
    gpio_active_high: bool = _env_bool("DMX_GPIO_ACTIVE_HIGH", True)


@dataclass(slots=True)
class BridgeConfig:
    video: VideoConfig = field(default_factory=VideoConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    dmx: DmxConfig = field(default_factory=DmxConfig)
