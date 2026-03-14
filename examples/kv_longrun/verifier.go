package main

import (
	"context"
	"fmt"

	"github.com/sangchul/actorbase/sdk"
)

// Mismatch는 정답지와 서버 상태가 일치하지 않는 항목이다.
type Mismatch struct {
	Key      string
	Expected *string // nil = 삭제되어야 함
	Got      *string // nil = 서버에 존재하지 않음
}

// verify는 정답지의 모든 키를 서버에서 조회하여 불일치 목록을 반환한다.
func verify(ctx context.Context, client *sdk.Client[KVRequest, KVResponse], ledger *Ledger) ([]Mismatch, error) {
	snap := ledger.Snapshot()
	var mismatches []Mismatch

	for key, expected := range snap {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := client.Send(ctx, key, KVRequest{Op: "get", Key: key})
		if err != nil {
			return nil, fmt.Errorf("get %s: %w", key, err)
		}

		if expected == nil {
			// 삭제된 키인데 서버에 존재하면 불일치
			if resp.Found {
				got := string(resp.Value)
				mismatches = append(mismatches, Mismatch{Key: key, Expected: nil, Got: &got})
			}
		} else {
			// 존재해야 하는 키
			if !resp.Found {
				mismatches = append(mismatches, Mismatch{Key: key, Expected: expected, Got: nil})
			} else if string(resp.Value) != *expected {
				got := string(resp.Value)
				mismatches = append(mismatches, Mismatch{Key: key, Expected: expected, Got: &got})
			}
		}
	}

	return mismatches, nil
}
