# Actorbase 웹 콘솔 설계

## 개요

PM에 내장된 HTTP 웹 콘솔. 브라우저에서 클러스터 상태 모니터링과 split/migrate 운영 작업을 수행할 수 있다.

- **인증**: 없음 (MVP)
- **실시간성**: 3초 폴링
- **KV Browser**: 제외 (actor 구현체 관심사)
- **PM HA 상태**: 제외 (MVP 스킵)

---

## 아키텍처

```
pm/
├── config.go          ← HTTPAddr string 필드
├── server.go          ← HTTPAddr != "" 이면 console.NewServer 시작
└── console/
    ├── server.go      ← HTTP 서버 (go:embed + SPA fallback)
    ├── api.go         ← REST 핸들러 (transport.PMClient → PM gRPC 호출)
    └── web/           ← React + Vite 소스
        ├── package.json
        ├── vite.config.ts
        ├── tsconfig.json
        ├── tailwind.config.js
        ├── postcss.config.js
        ├── index.html
        ├── src/
        │   ├── main.tsx
        │   ├── App.tsx              ← BrowserRouter + 사이드바 네비게이션
        │   ├── api.ts               ← fetch 래퍼 + TypeScript 타입
        │   ├── hooks/
        │   │   └── usePolling.ts    ← 폴링 커스텀 훅
        │   ├── pages/
        │   │   ├── Dashboard.tsx    ← 노드 카드 + RPS 차트 + 파티션 분포
        │   │   ├── Routing.tsx      ← 라우팅 테이블
        │   │   ├── Operations.tsx   ← Split + Migrate 탭
        │   │   └── Policy.tsx       ← 정책 상태
        │   └── components/
        │       └── SplitModal.tsx   ← Split 확인 모달
        └── dist/                   ← npm run build 결과 (go:embed 대상)
```

### 정적 파일 서빙

`//go:embed web/dist` 로 dist 디렉토리 전체를 PM 바이너리에 내장한다.
SPA fallback: 파일이 없는 경로는 `index.html`을 반환한다.

### 라우팅 테이블 캐싱

`console/api.go`의 `startRoutingWatcher`가 백그라운드에서 `WatchRouting` gRPC 스트림을 구독하여 최신 라우팅 테이블을 메모리에 캐싱한다. `GET /api/routing`은 이 캐시를 반환한다.

---

## HTTP API

모든 엔드포인트는 `/api/` prefix. PM의 gRPC API를 `transport.PMClient`로 내부 호출한다.

| Method | Path | 동작 |
|---|---|---|
| GET | `/api/members` | `ListMembers` → `[]Member` |
| GET | `/api/routing` | 캐시된 라우팅 테이블 스냅샷 |
| GET | `/api/stats` | `GetClusterStats("")` → 전체 노드 통계 |
| POST | `/api/split` | `{actor_type, partition_id, split_key}` → `RequestSplit` |
| POST | `/api/migrate` | `{actor_type, partition_id, target_node_id}` → `RequestMigrate` |
| GET | `/api/policy` | `GetPolicy` → `{active, yaml}` |

에러 응답: `{"error": "..."}`  + 적절한 HTTP status code.

정적 파일: `GET /` → embed.FS의 dist/ 서빙, SPA fallback → index.html.

---

## 화면 구성

### Dashboard
- 노드 카드: node_id, address, RPS, 파티션 수
- RPS 시계열 그래프 (recharts LineChart, 최근 20샘플)
- 파티션 분포 테이블 (노드별 파티션 수 / 총 RPS)

### Routing Table
- 파티션 ID, actor type, key range, 담당 노드 테이블
- 버전 표시, 3초 자동 갱신

### Operations

**Split 탭**
1. 라우팅 테이블 행마다 "Split" 버튼
2. 클릭 → SplitModal (partition 정보 + split key 입력, 비워두면 midpoint 자동)
3. 확인 → `POST /api/split`

**Migrate 탭**
1. 파티션을 DraggablePartition 카드로 표시, 노드 영역이 DroppableNode
2. 카드를 다른 노드 영역에 드롭 → 확인 모달
3. 확인 → `POST /api/migrate`
- 구현 라이브러리: `@dnd-kit/core`

### Policy
- AutoPolicy 활성/비활성 badge
- 활성인 경우 YAML 읽기 전용 표시

---

## 프론트엔드 기술 스택

| 항목 | 선택 |
|---|---|
| 빌드 도구 | Vite 6 |
| UI 프레임워크 | React 18 |
| 라우팅 | react-router-dom v6 |
| 스타일링 | Tailwind CSS v3 |
| 차트 | recharts |
| DnD | @dnd-kit/core |
| 언어 | TypeScript |

### usePolling 훅

```typescript
// fetcher ref 패턴으로 함수 레퍼런스 변경 시 effect 재실행 방지
function usePolling<T>(fetcher: () => Promise<T>, interval = 3000): { data: T | null, error: string | null }
```

---

## 설정 및 빌드

### PM 설정

```go
// pm/config.go
HTTPAddr string  // 웹 콘솔 HTTP 주소. "" 이면 비활성. 예: ":8080"
```

### 플래그

```
./bin/pm -addr :8000 -http :8080 -etcd localhost:2379 -actor-types kv
```

`-http` 플래그 미지정 시 콘솔 비활성 (기본값 "").

### 빌드 워크플로우

```bash
# 전체 빌드 (프론트엔드 → PM 바이너리)
make pm

# 프론트엔드만
make console
# = cd pm/console/web && npm install && npm run build

# go generate (console/server.go에 주석 포함)
//go:generate sh -c "cd web && npm install && npm run build"
```

`web/dist/`는 `.gitignore`에 추가하지 않고 커밋. `make clean` 시 placeholder `index.html`을 자동 복원하여 `go:embed`가 항상 유효하게 유지된다.

---

## 검증

```bash
# 1. 프론트엔드 빌드
cd pm/console/web && npm install && npm run build

# 2. PM 기동
./bin/pm -addr :8000 -http :8080 -etcd localhost:2379 -actor-types kv

# 3. PS 기동
./bin/kv_server -node-id ps-1 -addr localhost:8001 -etcd localhost:2379 \
  -wal-dir /tmp/ab/wal -checkpoint-dir /tmp/ab/ckpt

# 4. 브라우저 확인
open http://localhost:8080
```

| 항목 | 확인 내용 |
|---|---|
| Dashboard | ps-1 카드 표시, RPS 그래프 렌더링 |
| Routing | kv 파티션 1개 표시 |
| Operations > Split | "m"으로 split → Routing 2개로 갱신 확인 |
| Operations > Migrate | ps-2 기동 후 드래그 → ps-2로 이동 확인 |
| Policy | active=false, yaml="" 표시 |
