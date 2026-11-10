# TIPS Project
/*TIPS VLP(Virtual Line Projector) 기반 엣지 AI 산업안전 관리 시스템 개발 (TIPS R&D)*/

본 저장소는 **TIPS 과제 관련 소프트웨어 및 기술 개발 결과물**을 관리하기 위한 프로젝트입니다.

## 1. 프로젝트 개요

본 프로젝트는 임베디드 시스템, 센서 데이터 수집, 통신, 게이트웨이 및 서버 연동 기술을 기반으로  
현장 장비의 상태를 실시간으로 수집하고 모니터링할 수 있는 시스템 개발을 목표로 합니다.

주요 개발 범위는 다음과 같습니다.

- 임베디드 보드 펌웨어 개발
- 센서 데이터 수집 및 전처리
- 유·무선 통신 기능 구현
- Gateway 연동
- MQTT 기반 서버 데이터 송수신
- 장비 상태 모니터링 및 제어
- 이상 상태 및 통신 오류 감지
- 시험 및 성능 검증

---

## 2. 시스템 구성

```text
[Sensor / Device]
        │
        │ UART / RS-485 / Wi-Fi / 기타 통신
        ▼
[Embedded Controller]
        │
        │ Data Packet
        ▼
[Gateway]
        │
        │ MQTT / TCP-IP
        ▼
[Server / Monitoring System]
```

장비에서 측정된 데이터를 임베디드 보드가 수집하고,  
Gateway를 통해 서버로 전달하여 상태 확인 및 원격 제어가 가능하도록 구성합니다.

---

## 3. 주요 기능

### Embedded

- 센서 데이터 측정
- ADC / SPI / I2C / UART 인터페이스
- 상태 데이터 패킷 생성
- 장비 제어
- 통신 오류 및 예외 처리
- 주기적 상태 보고

### Gateway

- 장비 데이터 수신
- 패킷 파싱 및 검증
- 장비별 상태 관리
- MQTT 서버 연동
- 통신 상태 감시
- 서버 명령 전달

### Server Communication

- 상태 데이터 전송
- 센서 데이터 업로드
- 제어 명령 수신
- ACK / Response 처리
- 장비 연결 상태 관리

---

## 4. 개발 환경

| 구분 | 내용 |
|---|---|
| Embedded | STM32 / ESP32 등 |
| Language | C / C++ / Python |
| OS | Linux / Raspberry Pi OS / Windows |
| Communication | UART / RS-485 / MQTT / TCP-IP |
| IDE | STM32CubeIDE / VS Code |
| Version Control | Git / GitHub |
| Debug | ST-LINK / Serial Console |

※ 실제 적용 환경에 따라 항목은 변경될 수 있습니다.

---

## 5. 프로젝트 구조

```text
TIPS/
├─ firmware/           # 임베디드 펌웨어
│  ├─ Core/
│  ├─ Drivers/
│  └─ Application/
│
├─ gateway/            # Gateway 프로그램
│  ├─ main.py
│  ├─ communication/
│  ├─ mqtt/
│  └─ config/
│
├─ docs/               # 설계 및 개발 문서
│  ├─ protocol/
│  ├─ test/
│  └─ images/
│
├─ tools/              # 테스트 및 개발 도구
│
├─ README.md
└─ .gitignore
```

프로젝트 실제 구조에 맞게 디렉터리 구성은 변경할 수 있습니다.

---

## 6. 실행 방법

### Gateway

```bash
git clone <repository-url>
cd TIPS/gateway
```

가상환경을 사용하는 경우:

```bash
python -m venv venv
```

Windows:

```bash
venv\Scripts\activate
```

Linux:

```bash
source venv/bin/activate
```

필요 패키지 설치:

```bash
pip install -r requirements.txt
```

실행:

```bash
python main.py
```

### Embedded Firmware

1. STM32CubeIDE에서 프로젝트를 엽니다.
2. 대상 MCU 및 보드 설정을 확인합니다.
3. 프로젝트를 Build 합니다.
4. ST-LINK를 이용하여 Firmware를 다운로드합니다.
5. UART Debug Console을 통해 동작 상태를 확인합니다.

---

## 7. 데이터 흐름

```text
센서 측정
   ↓
MCU 데이터 처리
   ↓
통신 패킷 생성
   ↓
Gateway 수신
   ↓
패킷 분석 및 장비 상태 저장
   ↓
MQTT Publish
   ↓
서버 / 관제 시스템
```

서버의 제어 명령은 반대 방향으로 장비에 전달됩니다.

```text
Server
   ↓
MQTT Command
   ↓
Gateway
   ↓
Device Command
   ↓
Embedded Controller
   ↓
장비 제어
```

---

## 8. 개발 및 시험 항목

- 센서 측정 정확도 확인
- 장시간 연속 동작 시험
- 통신 안정성 시험
- 패킷 오류 처리 시험
- Gateway 재접속 시험
- 네트워크 단절 및 복구 시험
- 서버 명령 송수신 시험
- 장비 상태 및 오류 코드 확인
- 환경 조건 변화에 따른 동작 시험

---

## 9. 개발 상태

현재 개발 단계에 따라 아래 항목을 갱신합니다.

- [ ] Hardware Bring-up
- [ ] Sensor Interface
- [ ] Device Firmware
- [ ] Communication Protocol
- [ ] Gateway Software
- [ ] MQTT Communication
- [ ] Server Integration
- [ ] System Test
- [ ] Field Test

---

## 10. 문서 관리

관련 설계 문서, 시험 결과 및 프로토콜 문서는 `docs/` 디렉터리에서 관리합니다.

```text
docs/
├─ system_architecture.md
├─ communication_protocol.md
├─ test_scenario.md
└─ test_result.md
```

---

## 11. Version Management

개발 이력은 Git을 이용하여 관리합니다.

예시:

```text
main        : 안정화 버전
develop     : 통합 개발 버전
feature/*   : 기능 개발
fix/*       : 오류 수정
release/*   : 배포 준비
```

Commit 예시:

```text
feat: add sensor data acquisition
fix: resolve RS-485 packet parsing issue
refactor: improve MQTT reconnect logic
docs: update communication protocol
```

---

## 12. License

본 프로젝트의 소스코드 및 문서는 TIPS 과제 및 내부 개발 목적으로 관리됩니다.  
외부 공개 및 사용은 프로젝트 정책에 따릅니다.
