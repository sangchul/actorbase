package main

import (
	"context"
	"fmt"

	"github.com/sangchul/actorbase/sdk"
)

// Mismatch is an entry where the ledger and server state do not agree.
type Mismatch struct {
	Key      string
	Expected *string // nil = should have been deleted
	Got      *string // nil = not found on server
}

// verify queries all keys in the ledger from the server and returns a list of mismatches.
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
			// Key should have been deleted but exists on server — mismatch
			if resp.Found {
				got := string(resp.Value)
				mismatches = append(mismatches, Mismatch{Key: key, Expected: nil, Got: &got})
			}
		} else {
			// Key should exist on server
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
