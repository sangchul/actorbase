package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/oomymy/actorbase/sdk"
)

// Stats는 전체 worker들의 집계 통계다.
type Stats struct {
	success atomic.Int64
	fail    atomic.Int64
}

// workerConfig는 연산 비율을 설정한다.
type workerConfig struct {
	setRatio float64 // set 연산 비율
	delRatio float64 // del 연산 비율 (나머지는 get)
}

// worker는 담당 키 범위([keyLow, keyHigh))에서 set/del/get을 수행한다.
// 서버 성공 응답 후 ledger를 업데이트한다.
type worker struct {
	id      int
	keyLow  int
	keyHigh int
	client  *sdk.Client[KVRequest, KVResponse]
	ledger  *Ledger
	cfg     workerConfig
}

// run은 stopCh가 닫힐 때까지 set/del/get을 반복한다.
// ctx는 gRPC 호출용으로만 사용한다 — in-flight 요청이 완료될 때까지 ctx는 살아있어야 한다.
// stopCh가 닫히면 새 요청을 시작하지 않고, 현재 진행 중인 요청은 ctx로 완료를 기다린다.
func (w *worker) run(ctx context.Context, stopCh <-chan struct{}, stats *Stats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w.id)*1000))
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		keyIdx := w.keyLow + rng.Intn(w.keyHigh-w.keyLow)
		key := fmt.Sprintf("key:%08d", keyIdx)

		r := rng.Float64()
		switch {
		case r < w.cfg.setRatio:
			value := fmt.Sprintf("v%d-%d", keyIdx, time.Now().UnixNano())
			_, err := w.client.Send(ctx, key, KVRequest{Op: "set", Key: key, Value: []byte(value)})
			if err == nil {
				w.ledger.Set(key, value)
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}

		case r < w.cfg.setRatio+w.cfg.delRatio:
			_, err := w.client.Send(ctx, key, KVRequest{Op: "del", Key: key})
			if err == nil {
				w.ledger.Del(key)
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}

		default:
			// get — live consistency 확인용, ledger는 변경하지 않음
			_, err := w.client.Send(ctx, key, KVRequest{Op: "get", Key: key})
			if err == nil {
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}
		}
	}
}
