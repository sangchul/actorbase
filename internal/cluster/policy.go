package cluster

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const policyKey = "/actorbase/policy"

// SavePolicy는 policy YAML 문자열을 etcd에 저장한다.
func SavePolicy(ctx context.Context, cli *clientv3.Client, yamlStr string) error {
	_, err := cli.Put(ctx, policyKey, yamlStr)
	if err != nil {
		return fmt.Errorf("cluster: save policy: %w", err)
	}
	return nil
}

// LoadPolicy는 etcd에서 policy YAML 문자열을 로드한다.
// policy가 없으면 빈 문자열과 nil을 반환한다.
func LoadPolicy(ctx context.Context, cli *clientv3.Client) (string, error) {
	resp, err := cli.Get(ctx, policyKey)
	if err != nil {
		return "", fmt.Errorf("cluster: load policy: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// ClearPolicy는 etcd에서 policy를 삭제한다.
func ClearPolicy(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Delete(ctx, policyKey)
	if err != nil {
		return fmt.Errorf("cluster: clear policy: %w", err)
	}
	return nil
}
