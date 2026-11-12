## 1. Docker 없이 Kafbat UI 실행하기

Kafbat UI(구 Kafka UI)는 Spring Boot 기반의 단일 JAR 파일로 배포되므로, **Java(OpenJDK 17 이상 권장)** 환경만 있다면 Docker 없이 단독으로 실행할 수 있습니다.

### ① 설정 파일 준비 (`application.yml`)
클러스터 연결 정보를 담은 설정 파일을 생성합니다. YAML 파일에서는 `#` 기호를 사용하여 주석 처리를 합니다.

```yaml
kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
      # readOnly: true  # 주석 처리 시 읽기/쓰기 모드(false)로 동작
```

### ② JAR 실행 명령어
```bash
java -Dspring.config.additional-location=./application.yml \
     --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED \
     -jar kafbat-ui-api.jar
```

---

## 2. 리눅스 실행/종료 스크립트 (`start.sh`, `stop.sh`)

공식 저장소에서는 별도의 쉘 스크립트를 제공하지 않으므로, 백그라운드 실행과 프로세스 관리를 위해 직접 스크립트를 작성하여 사용합니다.

### `start.sh` (백그라운드 실행 및 PID 저장)
```bash
#!/bin/bash

JAR_NAME="kafbat-ui-api.jar"
CONFIG_PATH="./application.yml"
LOG_FILE="./kafbat-ui.log"
PID_FILE="./kafbat-ui.pid"

if [ -f "\$PID_FILE" ] && kill -0 \((cat "\)PID_FILE") 2>/dev/null; then
    echo "Kafbat UI가 이미 실행 중입니다. (PID: \((cat\)PID_FILE))"
    exit 1
fi

nohup java -Dspring.config.additional-location=\$CONFIG_PATH \
     --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED \
     -jar \(JAR_NAME >\)LOG_FILE 2>&1 &

echo \(! >\)PID_FILE
echo "Kafbat UI 시작됨 (PID: \$!)"
```

### `stop.sh` (PID 기반 프로세스 종료)
```bash
#!/bin/bash

PID_FILE="./kafbat-ui.pid"

if [ ! -f "\$PID_FILE" ]; then
    echo "실행 중인 PID 파일을 찾을 수 없습니다."
    exit 1
fi

PID=\((cat "\)PID_FILE")
if kill -0 \$PID 2>/dev/null; then
    kill \$PID
    rm -f \$PID_FILE
    echo "Kafbat UI 종료됨 (PID: \$PID)"
else
    echo "해당 프로세스(\$PID)가 이미 종료된 상태입니다."
    rm -f \$PID_FILE
fi
```

### 💡 트러블슈팅: `Permission denied` 발생 시
* **원인 1 (파일 실행 권한 부족):** 새로 생성한 `.sh` 파일에 실행(`x`) 권한이 없는 경우
  ```bash
  chmod +x start.sh stop.sh
  ```
* **원인 2 (프로세스 종료 권한 부족):** 실행한 계정과 종료하려는 계정이 다른 경우 `sudo ./stop.sh`로 실행하거나 실행 계정으로 전환합니다.

---

## 3. 읽기 전용 모드 해제 및 메시지 직접 발행(Produce)

* `application.yml`에서 `# readOnly: true`로 주석 처리 후 재시작하면 토픽 생성, 오프셋 리셋, 메시지 발행 등 모든 관리 기능이 활성화됩니다.
* **직접 메시지 발행(`Produce Message`):**
  * Kafbat UI 내장 Producer 클라이언트를 통해 **실제 운영/스테이지 Kafka 브로커의 파티션에 데이터를 직접 기록**합니다.
  * 해당 토픽(`topic-stage`)을 구독 중인 실제 컨슈머 애플리케이션이 즉시 반응하므로, 데이터 직렬화 포맷(JSON, String 등)을 맞추어 테스트해야 합니다.

---

## 4. Kafka 내부 시스템 토픽(Internal Topics) 이해하기

Kafbat UI의 Topics 목록에서 이름 앞에 `IN`(Internal) 태그와 언더스코어 두 개(`__`)가 붙은 토픽들은 카프카 클러스터 자체 관리를 위해 자동 생성되는 특수 토픽입니다.

### ① `__consumer_offsets` (컨슈머 오프셋 관리)
* **역할:** 컨슈머 그룹(Consumer Group)이 각 토픽 파티션에서 **어디까지 메시지를 읽었는지(Offset)**를 영구 기록합니다.
* **동작:** `(컨슈머 그룹명, 토픽명, 파티션 번호)`를 Key로, `마지막 커밋 오프셋`을 Value로 저장합니다.
* **필요성:** 컨슈머 재시작 또는 리밸런싱(Rebalancing) 발생 시 중복 처리나 데이터 유실 없이 이어서 읽기 위해 사용됩니다.

### ② `__transaction_state` (트랜잭션 상태 관리)
* **역할:** **Exactly-Once Semantics(정확히 한 번 전송)**를 사용하는 프로듀서의 트랜잭션 상태(`Ongoing`, `PrepareCommit`, `CompleteCommit` 등)를 추적합니다.
* **동작:** 트랜잭션 코디네이터가 `transactional.id`별 진행 상태를 기록하여, 완료되지 않은 트랜잭션 메시지가 컨슈머에게 노출되지 않도록 격리합니다.

### 한눈에 보는 비교

| 구분 | `__consumer_offsets` | `__transaction_state` |
| :--- | :--- | :--- |
| **주요 역할** | 컨슈머 그룹별 메시지 읽은 위치(Offset) 저장 | 프로듀서의 트랜잭션 상태(진행/커밋/중단) 저장 |
| **핵심 주체** | 메시지를 구독하는 **Consumer** | 트랜잭션 API를 사용하는 **Producer** |
| **기본 파티션 수** | 50개 (`offsets.topic.num.partitions`) | 50개 (`transaction.state.log.num.partitions`) |
| **데이터 정리 정책** | **Log Compaction** (최신 Key 상태만 유지) | **Log Compaction** (최신 트랜잭션 상태만 유지) |

> ⚠️ **운영 주의사항**  
> 내부 시스템 토픽을 삭제하거나 임의로 메시지를 발행(`Produce`)하면 클러스터 장애가 발생합니다. 화면에서 일반 토픽만 보려면 Kafbat UI 상단 필터에서 **`Hide internal topics`** 옵션을 활성화하세요.
