from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod

from config.video_config import DmxConfig


class BaseDmxDriver(ABC):
    @abstractmethod
    def open(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def send_frame(self, frame_512: bytes) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


class DummyDmxDriver(BaseDmxDriver):
    def open(self) -> None:
        print("[DMX] Dummy driver open")

    def send_frame(self, frame_512: bytes) -> None:
        ch_preview = list(frame_512[:16])
        print(f"[DMX] send frame preview(1..16)={ch_preview}")

    def close(self) -> None:
        print("[DMX] Dummy driver close")

class UartDmxDriver(BaseDmxDriver):
    def __init__(
        self,
        port: str,
        baudrate: int,
        timeout_sec: float = 0.2,
        start_address: int = 1,
        input_length: int = 4,
        output_length: int = 4,
        configure_on_open: bool = False,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout_sec = timeout_sec
        self.start_address = start_address
        self.input_length = input_length
        self.output_length = output_length
        self.configure_on_open = configure_on_open
        self._serial = None

    def open(self) -> None:
        try:
            import serial
        except ImportError as exc:
            raise RuntimeError("pyserial is required for UART DMX output") from exc

        self._serial = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=self.timeout_sec,
            write_timeout=self.timeout_sec,
        )
        print(f"[DMX] UART open port={self.port} baudrate={self.baudrate}")
        if self.configure_on_open:
            self._configure_mapping()

    def send_frame(self, frame_512: bytes) -> None:
        if self._serial is None:
            raise RuntimeError("UART driver is not open")

        payload = frame_512[: self.input_length]
        self._serial.write(payload)
        self._serial.flush()

    def close(self) -> None:
        if self._serial is not None:
            self._serial.close()
            self._serial = None
        print("[DMX] UART close")

    def _configure_mapping(self) -> None:
        if self._serial is None:
            raise RuntimeError("UART driver is not open")

        # Assumption: the controller is already in CFG mode or accepts these commands directly.
        self._send_cfg9(f"@SADR,{self.start_address:03d}")
        self._send_cfg9(f"@BLEN,{self.input_length:03d}")
        self._send_cfg9(f"@FLEN,{self.output_length:03d}")
        self._send_cfg9("!STORECFG")

    def _send_cfg9(self, command: str) -> None:
        if self._serial is None:
            raise RuntimeError("UART driver is not open")
        if len(command) != 9:
            raise ValueError(f"DMX config command must be 9 bytes: {command}")

        self._serial.write(command.encode("ascii"))
        self._serial.flush()
        time.sleep(0.05)

class GpioDmxDriver(BaseDmxDriver):
    def __init__(self, pins: list[int], active_high: bool = True):
        self.pins = pins
        self.active_high = active_high
        self._gpio = None

    def open(self) -> None:
        try:
            import RPi.GPIO as gpio
        except ImportError as exc:
            raise RuntimeError("RPi.GPIO is required for GPIO DMX output") from exc

        self._gpio = gpio
        gpio.setmode(gpio.BCM)
        for pin in self.pins:
            gpio.setup(pin, gpio.OUT)
            gpio.output(pin, gpio.LOW if self.active_high else gpio.HIGH)
        print(f"[DMX] GPIO open pins={self.pins}")

    def send_frame(self, frame_512: bytes) -> None:
        if self._gpio is None:
            raise RuntimeError("GPIO driver is not open")

        for offset, pin in enumerate(self.pins):
            channel_value = frame_512[offset] if offset < len(frame_512) else 0
            is_on = channel_value > 0
            level = self._gpio.HIGH if is_on == self.active_high else self._gpio.LOW
            self._gpio.output(pin, level)

    def close(self) -> None:
        if self._gpio is not None:
            self._gpio.cleanup(self.pins)
            self._gpio = None
        print("[DMX] GPIO close")

class DmxService(threading.Thread):
    def __init__(
        self,
        driver: BaseDmxDriver,
        fps: float = 30.0,
        universe_size: int = 512,
        fixture_count: int = 2,
        channels_per_fixture: int = 2,
        normal_dim_value: int = 0,
        normal_img_value: int = 0,
        alarm_dim_value: int = 255,
        alarm_img_value: int = 16,
    ):
        super().__init__(daemon=True)
        self.driver = driver
        self.fps = max(1.0, float(fps))
        self.universe_size = universe_size
        self.fixture_count = max(1, int(fixture_count))
        self.channels_per_fixture = max(1, int(channels_per_fixture))
        self.normal_dim_value = normal_dim_value
        self.normal_img_value = normal_img_value
        self.alarm_dim_value = alarm_dim_value
        self.alarm_img_value = alarm_img_value

        self._lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._frame = bytearray([0] * self.universe_size)
        self._last_send_ok = True
        self._last_err: str | None = None

    def stop(self) -> None:
        self._stop_evt.set()

    def set_channel(self, ch: int, value: int) -> None:
        if ch < 1 or ch > self.universe_size:
            return
        v = 0 if value < 0 else 255 if value > 255 else int(value)
        with self._lock:
            self._frame[ch - 1] = v

    def set_channels(self, mapping: dict[int, int]) -> None:
        with self._lock:
            for ch, value in mapping.items():
                if 1 <= ch <= self.universe_size:
                    v = 0 if value < 0 else 255 if value > 255 else int(value)
                    self._frame[ch - 1] = v

    def apply_scene(self, scene: str) -> None:
        if scene == "NORMAL":
            self._apply_fixture_payload(self.normal_dim_value, self.normal_img_value)
        elif scene == "ALARM":
            self._apply_fixture_payload(self.alarm_dim_value, self.alarm_img_value)
        else:
            self._apply_fixture_payload(self.normal_dim_value, self.normal_img_value)

    def _apply_fixture_payload(self, dim_value: int, img_value: int) -> None:
        mapping: dict[int, int] = {}
        for fixture_index in range(self.fixture_count):
            base_channel = fixture_index * self.channels_per_fixture
            mapping[base_channel + 1] = dim_value
            if self.channels_per_fixture >= 2:
                mapping[base_channel + 2] = img_value
        self.set_channels(mapping)

    def run(self) -> None:
        period = 1.0 / self.fps
        try:
            self.driver.open()
        except Exception as exc:                                                
            print(f"[DMX] driver open failed, fallback to dummy: {exc}")
            self.driver = DummyDmxDriver()
            self.driver.open()

        next_t = time.time()
        while not self._stop_evt.is_set():
            now = time.time()
            if now < next_t:
                time.sleep(min(0.05, next_t - now))
                continue
            next_t += period

            with self._lock:
                frame = bytes(self._frame)

            try:
                self.driver.send_frame(frame)
                self._last_send_ok = True
                self._last_err = None
            except Exception as exc:
                self._last_send_ok = False
                self._last_err = str(exc)
                time.sleep(0.2)

        try:
            self.driver.close()
        finally:
            return


def build_dmx_service(config: DmxConfig) -> DmxService:
    backend = config.backend.strip().lower()
    driver: BaseDmxDriver

    try:
        if backend == "uart":
            driver = UartDmxDriver(
                port=config.uart_port,
                baudrate=config.uart_baudrate,
                timeout_sec=config.uart_timeout_sec,
                start_address=config.uart_start_address,
                input_length=config.uart_input_length,
                output_length=config.uart_output_length,
                configure_on_open=config.uart_configure_on_open,
            )        
        elif backend == "gpio":
            driver = GpioDmxDriver(
                pins=config.gpio_pins,
                active_high=config.gpio_active_high,
            )
        else:
            driver = DummyDmxDriver()
    except Exception as exc:
        print(f"[DMX] driver build failed, fallback to dummy: {exc}")
        driver = DummyDmxDriver()

    return DmxService(
        driver=driver,
        fps=config.fps,
        universe_size=config.universe_size,
        fixture_count=config.fixture_count,
        channels_per_fixture=config.channels_per_fixture,
        normal_dim_value=config.normal_dim_value,
        normal_img_value=config.normal_img_value,
        alarm_dim_value=config.alarm_dim_value,
        alarm_img_value=config.alarm_img_value,
    )
