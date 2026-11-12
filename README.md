# 📊 kafka-study

> **Kafbat UI를 중심으로 Apache Kafka 클러스터의 상태를 모니터링하고 제어·운영하는 방법**을 기록하는 학습 저장소입니다.  
> 복잡한 CLI 명령어 대신 직관적인 웹 UI 도구를 활용해 토픽, 컨슈머 랙(Lag), 메시지 흐름을 관측하고 실무 트러블슈팅 역량을 기르는 것을 목표로 합니다.

---

## 🎯 학습 방향성 및 목표 (Focus & Goals)

* **모니터링 도구 구축 및 운영:** Docker 없이 Standalone JAR 환경에서 Kafbat UI를 직접 구축하고 백그라운드 프로세스(스크립트)로 안정적으로 운영하기
* **메시지 흐름 관측 및 제어:** UI를 통한 토픽 생성·설정 관리, 실제 브로커로의 테스트 메시지 발행(`Produce`), 메시지 데이터 조회 및 디버깅
* **컨슈머 및 클러스터 상태 점검:** 컨슈머 그룹별 오프셋(Offset) 추적, 컨슈머 랙(Consumer Lag) 모니터링, 내부 시스템 토픽 동작 이해
* **트러블슈팅 실습:** 메시지 적체(Lag) 발생 시 오프셋 리셋(Reset) 및 파티션 재할당 상태 관측 등 운영 대응 시나리오 정리

---

## 📚 학습 목차 (Contents)

Kafbat UI를 활용하며 배운 개념과 운영 노하우를 주제별로 기록합니다.

### 1. Kafbat UI 구축 및 기본 설정 (Setup & Configuration)
* [01. Docker 없이 Kafbat UI 구축하기 & 내부 시스템 토픽 이해](./docs/01-kafbat-ui-setup-and-internal-topics.md) *(작성 완료)*
  * Linux 환경에서 JAR 파일 기반 Kafbat UI 단독 실행 및 `start.sh`, `stop.sh` 관리 스크립트 작성
  * Read-Only 모드 해제 및 UI를 통한 브로커 직접 메시지 발행(`Produce`) 주의사항
  * UI 상단에 표시되는 내부 시스템 토픽(`__consumer_offsets`, `__transaction_state`)의 역할과 숨김 필터 활용

### 2. 토픽 및 메시지 모니터링 (Topic & Message Inspection)
* Kafbat UI에서 파티션별 메시지 분포 및 직렬화 포맷(JSON, String 등) 조회하기
* 토픽 설정(Retention, Cleanup Policy 등) 확인 및 수정 방법

### 3. 컨슈머 그룹 및 장애 대응 (Consumer Monitoring & Operations)
* Consumer Group 상태 및 **Consumer Lag(지연율)** 실시간 모니터링 방법
* 장애 복구를 위한 컨슈머 오프셋 수동 초기화(Offset Reset) 실습

---

## 🛠️ 실습 환경 (Environment)

* **Monitoring & Admin UI:** Kafbat UI (Standalone JAR / Non-Docker)
* **Message Broker:** Apache Kafka
* **OS & Runtime:** Linux, Java (OpenJDK 17+)

---

## 📂 디렉토리 구조 (Repository Structure)

```text
kafka-study/
├── README.md                                       # 저장소 소개 및 모니터링 중심 학습 로드맵 (현재 문서)
├── docs/                                           # Kafbat UI 활용 및 Kafka 개념 정리 문서
│   └── 01-kafbat-ui-setup-and-internal-topics.md   # Kafbat UI 구축 및 내부 토픽 정리
└── scripts/                                        # Kafbat UI 실행/종료 관리 쉘 스크립트
    ├── start.sh
    └── stop.sh
```
