# actorbase Long-Run 테스트 설계

수 분 이상 실제 클러스터를 운영하면서 데이터 정합성을 검증하는 자동화 테스트.
split / migrate / PS 강제 장애 / graceful shutdown 이벤트를 혼합한 뒤
정답지와 실제 데이터를 비교한다.

---

## 목표

| 검증 항목 | 내용 |
|---|---|
| 데이터 정합성 | 모든 연산 후 정답지와 실제 KV 데이터가 일치 |
| 장애 내성 | SIGKILL 이후 failover가 완료되고 데이터 손실 없음 |
| Graceful shutdown | SIGTERM 이후 drainPartitions가 완료되고 데이터 손실 없음 |
| Split 정합성 | split 전후로 키가 올바른 파티션에서 조회됨 |
| Migrate 정합성 | migrate 전후로 키가 올바른 PS에서 조회됨 |
| SDK 재시도 | ErrPartitionBusy / ErrPartitionMoved 시 재시도 후 성공 |

### WAL replay 검증이 자연스럽게 이루어지는 이유

단편적인 통합 테스트에서는 "WAL에만 있는 데이터"를 의도적으로 만들어야 한다.
long-run 테스트에서는 이 케이스가 **자연스럽게** 발생한다.

**kv_server 기본 옵션값 (모두 `ps.Config` 기본값 사용):**

| 설정 | 기본값 | 의미 |
|---|---|---|
| `CheckpointWALThreshold` | 100건 | WAL 100건마다 자동 checkpoint |
| `CheckpointInterval` | 1분 | 1분마다 periodic checkpoint |
| `EtcdLeaseTTL` | 10초 | SIGKILL 후 10~15초 뒤 failover 시작 |
| `IdleTimeout` | 5분 | 5분 비활동 시 actor evict (checkpoint 포함) |
| `FlushInterval` | 10ms | WAL 배치 flush 최대 대기 (응답 수신 = flush 완료 보장) |
| `DrainTimeout` | 60초 | SIGTERM 시 drain 최대 대기 |

**longrun에서 checkpoint와 WAL의 흐름:**

```
부하 생성기 (100ms 간격, 20 worker):
  초당 약 200건 쓰기 → WAL threshold(100건) → 약 0.5초마다 자동 checkpoint
  + 1분마다 periodic checkpoint
  + split/migrate 이벤트 시 명시 checkpoint

chaos SIGKILL 시점은 랜덤:
  → 마지막 checkpoint 이후 최대 0.5초치 WAL이 "WAL에만 있는 데이터"
  → 정답지에는 이 데이터가 기록되어 있음 (응답 수신 = WAL flush 완료)
  → failover 후 WAL replay로 복원되어야 검증 통과
```

부하가 계속 걸리는 상황에서 chaos SIGKILL이 발생하므로,
"checkpoint 이후 WAL에만 있는 데이터"가 필연적으로 존재한다.
이를 정답지와 대조함으로써 WAL replay 정합성이 자동으로 검증된다.

---

## 테스트 구성

```
etcd (2379)
  │
  ├── PM (8000)
  │
  ├── PS-1 (8001) ─┐
  ├── PS-2 (8002)  ├── wal: /tmp/ab/wal (공유, 파티션별 서브디렉토리로 격리)
  └── PS-3 (8003) ─┘   checkpoint: /tmp/ab/checkpoint (공유)

kv_longrun (부하 생성 + 정답지 기록)
chaos.sh   (split / migrate / kill / graceful 이벤트 주입)
```

---

## 정답지 메커니즘

### 핵심 원칙

- **정답지는 서버 응답을 받은 후 기록한다.** 클라이언트가 요청만 보내고 기록하면, 재시도로 실제 값이 달라질 수 있다.
- **동일 키는 하나의 goroutine만 담당한다.** 키 공간을 goroutine 수로 sharding하여 같은 키에 대한 동시 쓰기를 방지한다.
- **정답지 파일은 JSON으로 주기적으로 flush한다.** 비정상 종료에도 마지막 flush까지의 데이터는 보존된다.

### 키 공간 설계

```
전체 키 수: 10,000개 (key:0000000 ~ key:0009999)
goroutine 수: W (기본 20)
goroutine i 담당 키: key:i*500 ~ key:i*500+499
```

동일 goroutine이 담당 범위의 키만 set/del/get하므로 정답지 기록에 mutex 불필요.

### 정답지 파일 형식

```json
{
  "key:0000001": "aGVsbG8=",
  "key:0000002": null,
  "key:0000003": "d29ybGQ="
}
```

- 값이 있으면 Base64 인코딩된 바이트
- `null`은 del로 삭제된 키 (get 시 not found 기대)
- 파일은 10초마다 flush, 종료 시 최종 flush

### 연산 비율

```
set: 60%   → 성공 시 정답지에 key → value 기록
del: 20%   → 성공 시 정답지에 key → null 기록
get: 20%   → 응답 값을 정답지와 실시간 비교 (인라인 검증)
```

get의 인라인 검증: 정답지에 없는 키이면 not found 기대, 있으면 값 일치 확인.
단, ErrPartitionBusy / chaos 이벤트 중에는 일시적 불일치가 허용된다 (재시도 후 성공 기대).

---

## Chaos 이벤트 스케줄

`chaos.sh`가 별도 프로세스로 실행되며 아래 이벤트를 순차 주입한다.

| 시간 (경과) | 이벤트 | 설명 |
|---|---|---|
| T+30s | split | 가장 큰 파티션을 중간 키로 split |
| T+60s | migrate | 임의 파티션을 PS-2로 migrate |
| T+90s | split | 두 번째 split |
| T+120s | SIGKILL PS-3 | PS-3 강제 종료. PM이 자동 failover 실행 |
| T+150s | PS-3 재기동 | PS-3 재시작 (빈 WAL, 공유 checkpoint 접근 가능) |
| T+180s | migrate | 파티션 일부를 PS-3으로 migrate (재기동 검증) |
| T+210s | split | 세 번째 split |
| T+240s | SIGTERM PS-1 | PS-1 graceful shutdown. drainPartitions 실행 |
| T+270s | PS-1 재기동 | PS-1 재시작 |
| T+300s | migrate | 파티션 일부를 PS-1으로 migrate |
| T+330s | split | 네 번째 split |
| T+360s | SIGKILL PS-2 | PS-2 강제 종료. 자동 failover |
| T+390s | PS-2 재기동 | PS-2 재시작 |
| T+420s 이후 | 부하만 유지 | 30초간 안정화 대기 |
| T+450s | 종료 | kv_longrun 부하 생성 종료, 검증 시작 |

### chaos 이벤트 구현

`abctl routing --json`은 구현하지 않았다. 대신 `abctl routing` 텍스트 출력을 awk로 파싱한다.

```bash
# chaos.sh 핵심 함수들

# 라우팅 테이블 파싱: id\tstart\tend\tnode (탭 구분)
get_routing_entries() {
  "$ABCTL" -pm "$PM_ADDR" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$2"\t"$3"\t"$4}'
}

# "key:NNNNNNNN" → 정수, "(start)" → -1, "(end)" → 10000
key_to_num() {
  local k="$1"
  case "$k" in
    "(start)") echo "-1" ;;
    "(end)")   echo "10000" ;;
    key:*)     local num="${k#key:}"; echo "$((10#$num))" ;;
    *)         echo "0" ;;
  esac
}

# 라우팅에서 분할 가능한 파티션을 찾아 범위 중간점으로 split
do_split() {
  # 각 파티션의 start/end를 파싱하여 중간점 계산. abctl에 하드코딩된 키 사용 안 함.
  while IFS=$'\t' read -r pid pstart pend pnode; do
    local s e mid
    s=$(key_to_num "$pstart"); e=$(key_to_num "$pend")
    mid=$(( (s + e) / 2 ))
    if [[ $mid -gt $s && $mid -lt $e ]]; then
      local split_key; split_key=$(printf "key:%08d" "$mid")
      "$ABCTL" -pm "$PM_ADDR" split "$pid" "$split_key"
      return
    fi
  done <<< "$(get_routing_entries)"
}

# target_node에 없는 파티션을 찾아 migrate
do_migrate() {
  local target_node="$1"
  while IFS=$'\t' read -r pid pstart pend pnode; do
    if [[ "$pnode" != "$target_node" ]]; then
      "$ABCTL" -pm "$PM_ADDR" migrate "$pid" "$target_node"
      return
    fi
  done <<< "$(get_routing_entries)"
}

# PID 파일로 특정 노드에 시그널 전송
kill_ps() {
  local node_id="$1" signal="$2"
  kill "-$signal" "$(cat "/tmp/ab_${node_id//-/_}.pid")"
}

# kv_server를 재기동하고 PID 파일 갱신
start_ps() {
  local node_id="$1" addr="$2"
  "$KV_SERVER" -node-id "$node_id" -addr "$addr" \
    -etcd "$ETCD_ADDR" -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" \
    >> "/tmp/ab_${node_id//-/_}.log" 2>&1 &
  echo $! > "/tmp/ab_${node_id//-/_}.pid"
}
```

> `do_split`은 하드코딩된 split key 대신 라우팅 테이블에서 파티션 범위를 동적으로 파싱하여 중간점을 계산한다. 여러 번 split 후 범위가 좁아져도 항상 유효한 키를 사용한다.
>
> `do_migrate`는 항상 첫 번째 파티션을 대상으로 하지 않고, target 노드에 없는 파티션을 선택한다. "partition already on node" 오류를 방지한다.

---

## 검증 절차

### 1단계: 부하 생성 종료

kv_longrun이 --duration 경과 후 정답지를 최종 flush하고 종료한다.

### 2단계: 안정화 대기

chaos 이벤트로 발생한 failover / migrate가 완료될 때까지 대기한다.
(`abctl routing`으로 모든 파티션이 Active 상태인지 확인)

```bash
./bin/abctl -pm $PM_ADDR routing
# 모든 파티션 STATUS = active 확인
```

### 3단계: 정답지 vs 실제 데이터 비교

`kv_longrun --verify` 모드로 정답지 파일의 모든 키를 get하여 비교한다.

```
정답지 키 수: N
일치:         M  (M == N 이면 PASS)
불일치:       N-M
  - 값 불일치: key:0000001 expected=hello actual=world
  - 존재해야 하는데 없음: key:0000002 expected=hello actual=<not found>
  - 없어야 하는데 있음: key:0000003 expected=<deleted> actual=hello
```

### 4단계: 결과 판정

```
PASS: 모든 키 일치 (불일치 0건)
FAIL: 1건 이상 불일치 → 불일치 항목 출력 후 exit 1
```

---

## 구현 현황

모든 항목 구현 완료.

| 구현 파일 | 상태 | 설명 |
|---|---|---|
| `examples/kv_longrun/main.go` | ✅ | 진입점. stopCh + WaitGroup으로 in-flight 요청 완료 후 flush |
| `examples/kv_longrun/ledger.go` | ✅ | 정답지 관리 (메모리 map + JSON 파일) |
| `examples/kv_longrun/worker.go` | ✅ | 부하 생성 goroutine. ctx 대신 stopCh로 종료 판단 |
| `examples/kv_longrun/verifier.go` | ✅ | 정답지 vs 실제 데이터 비교 |
| `test/longrun/chaos.sh` | ✅ | 동적 파티션 파싱으로 split/migrate 실행 |
| `test/longrun/run.sh` | ✅ | 전체 자동화 스크립트. 실행별 로그 디렉토리 생성 |

### kv_longrun 플래그

```
-pm              PM 주소 (기본: localhost:8000)
-duration        실행 시간 (기본: 5m)
-workers         goroutine 수 (기본: 20)
-keys            전체 키 수 (기본: 10000)
-ledger          정답지 파일 경로 (기본: /tmp/ab_ledger.json)
-flush-interval  정답지 flush 주기 (기본: 10s)
-set-ratio       set 비율 (기본: 0.6)
-del-ratio       del 비율 (기본: 0.2)
-verify          검증 모드: 정답지 파일을 읽어 실제 데이터와 비교
```

---

## 실행 절차

```bash
# 1. 빌드 (프로젝트 루트에서)
go build -o bin/pm          ./cmd/pm
go build -o bin/abctl       ./cmd/abctl
go build -o bin/kv_server   ./examples/kv_server
go build -o bin/kv_longrun  ./examples/kv_longrun

# 2. 실행 (etcd가 localhost:2379에서 실행 중이어야 함)
bash test/longrun/run.sh

# 3. 결과 확인: PASS or FAIL + 불일치 항목 출력
# 로그: /tmp/actorbase_runs/<YYYYMMDD_HHMMSS>/
# 심볼릭 링크: /tmp/ab_pm.log, /tmp/ab_ps1.log 등
```

---

## 알려진 한계 및 주의사항

### fs WAL의 failover 정합성

WAL 디렉토리를 모든 PS가 공유하므로(`-wal-dir /tmp/ab/wal`), SIGKILL로 PS를 종료해도
다른 PS가 동일 경로에서 해당 파티션의 WAL을 읽어 replay할 수 있다.

```
SIGKILL PS-1 후 상황:
  /tmp/ab/wal/{partition-id}/{lsn}  ← 파일이 디스크에 멀쩡히 존재

Failover 시 PS-2가 PreparePartition 처리:
  1. CheckpointStore에서 마지막 checkpoint 로드  ← OK
  2. WALStore.ReadFrom(lastCheckpointLSN+1)       ← 공유 WAL 디렉토리에서 읽음
  3. /tmp/ab/wal/{partition-id}의 WAL replay     ← OK, 데이터 손실 없음
```

따라서 fs WAL + 공유 디렉토리 환경에서 SIGKILL 후 failover도 완전한 정합성을 보장한다.
이것은 networked WAL (Redis Streams 등)과 동일한 시맨틱이다.

**실제 장비 장애(하드웨어 고장, OS 크래시) 시에는 데이터 손실 가능성이 있다.**
이 경우 로컬 파일 시스템 자체가 손상될 수 있어 WAL 파일이 유실된다.
프로덕션에서는 networked WALStore를 사용하거나 공유 스토리지(NFS 등)를 사용해야 한다.

### Chaos 이벤트 타이밍

chaos 이벤트가 kv_longrun의 연산 중간에 발생하면 일시적으로 ErrPartitionBusy /
ErrPartitionMoved가 반환된다. kv_longrun은 MaxRetries=5, RetryInterval=200ms로
재시도하므로 1초 내에 성공해야 한다. 재시도 초과 시 fail로 집계되고 정답지 기록을 건너뛴다.

---

## 테스트 결과 기록

### 버그 수정: ledger race condition (2026-03-08)

**증상**: 검증 시 19건 불일치. 서버가 ledger보다 더 최신값 보유.

```
key=key:00009517  expected=v9517-1772931533189124000  got=v9517-1772931543519328000
key=key:00005077  expected=<deleted>                  got=v5077-1772931543519706000
```

**원인**: duration 만료 시 ctx 취소 → 서버에서 이미 커밋된 in-flight 요청이 클라이언트에서
`ctx.Err()`로 실패 처리 → ledger 미갱신. 서버와 정답지 불일치.

**수정**: worker가 ctx 대신 별도 `stopCh`로 종료 판단.
- duration 만료 → `close(stopCh)` 신호
- `wg.Wait()`으로 모든 worker의 in-flight 요청 완료 대기
- 이후 ledger flush → 서버 상태와 완전 일치 보장

**수정 파일**:
- `examples/kv_longrun/worker.go`: `run(ctx, stopCh, stats)` 시그니처
- `examples/kv_longrun/main.go`: stopCh + WaitGroup 도입

---

### 1차 통과 (2026-03-08)

| 항목 | 값 |
|---|---|
| 환경 | macOS Darwin 25.3.0, Go 1.26, etcd v3.6.8 |
| 부하 duration | 8분 |
| 총 연산 수 | ~1,138,219 ops (fail=0) |
| 검증 키 수 | 10,000개 |
| 불일치 | **0건 (PASS)** |
| chaos 이벤트 | split×4, migrate×2, SIGKILL PS-3×1, SIGTERM PS-1×1 |
| 최종 파티션 수 | 3개 (PS-1×2, PS-3×1) |

**WAL replay 검증**: SIGKILL PS-3 직전 in-flight WAL 엔트리가 failover 후 PS-1에서 replay되어
정답지와 일치. 데이터 손실 없음 확인.

---

### chaos.sh 버그 수정 (2026-03-08)

**증상**: chaos 이벤트 중 일부 migrate/split이 "internal server error" 또는 "already on node"로 실패.

**원인 1 — `do_migrate` 파티션 선택 오류**: `get_first_partition`은 항상 라우팅 테이블 첫 번째 항목을 반환. split이 여러 번 일어난 후에는 그 파티션이 이미 target 노드에 있을 수 있어 "already on node ps-2" 오류 발생.

**원인 2 — `do_split` 하드코딩된 키**: `key:00007500` 같은 고정 키가 실제 파티션 범위를 벗어나 "splitKey must be less than partition end" 오류 발생.

**원인 3 — `fromGRPCStatus` 오류 마스킹**: `codes.Internal` 코드가 모두 `ErrActorPanicked`로 변환되어 실제 원인("already on node", "splitKey out of range")이 숨겨짐.

**수정**:
- `do_migrate`: 라우팅 테이블을 파싱하여 target 노드에 없는 파티션 선택
- `do_split`: 각 파티션의 start/end를 파싱하여 수치 중간점 동적 계산 (`printf "key:%08d"`)
- `fromGRPCStatus`: `codes.Internal`에서 메시지 문자열을 비교하여 `ErrActorPanicked`와 기타 오류 구분

**수정 파일**:
- `test/longrun/chaos.sh`: `get_routing_entries`, `key_to_num`, `do_split`, `do_migrate` 전면 재작성
- `internal/transport/errors.go`: `fromGRPCStatus` `codes.Internal` 처리 수정

---

### 2차 통과 (2026-03-08, chaos.sh 수정 후)

| 항목 | 값 |
|---|---|
| 환경 | macOS Darwin 25.3.0, Go 1.26, etcd v3.6.8 |
| 부하 duration | 8분 |
| 검증 키 수 | 10,000개 |
| 불일치 | **0건 (PASS)** |
| chaos 이벤트 | split×4, migrate×3, SIGKILL PS-3×1, SIGKILL PS-2×1, SIGTERM PS-1×1 |
| 최종 파티션 수 | 5개 |

**재현성 확인**: 1차, 2차 모두 PASS. chaos 이벤트 전체 스케줄(split 4회, migrate 3회, SIGKILL 2회, SIGTERM 1회, 재기동 3회) 정상 동작 확인.
