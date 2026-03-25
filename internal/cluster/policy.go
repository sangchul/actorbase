package cluster

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const policyKey = "/actorbase/policy"

// SavePolicy saves a policy YAML string to etcd.
func SavePolicy(ctx context.Context, cli *clientv3.Client, yamlStr string) error {
	_, err := cli.Put(ctx, policyKey, yamlStr)
	if err != nil {
		return fmt.Errorf("cluster: save policy: %w", err)
	}
	return nil
}

// LoadPolicy loads the policy YAML string from etcd.
// Returns an empty string and nil if no policy is stored.
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

// ClearPolicy deletes the policy from etcd.
func ClearPolicy(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Delete(ctx, policyKey)
	if err != nil {
		return fmt.Errorf("cluster: clear policy: %w", err)
	}
	return nil
}
