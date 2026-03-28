package cluster

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

const redisPolicyKey = "actorbase:policy"

// SaveRedisPolicy saves a policy YAML string to Redis.
func SaveRedisPolicy(ctx context.Context, cli goredis.UniversalClient, yamlStr string) error {
	if err := cli.Set(ctx, redisPolicyKey, yamlStr, 0).Err(); err != nil {
		return fmt.Errorf("cluster: save policy to redis: %w", err)
	}
	return nil
}

// LoadRedisPolicy loads the policy YAML string from Redis.
// Returns an empty string and nil if no policy is stored.
func LoadRedisPolicy(ctx context.Context, cli goredis.UniversalClient) (string, error) {
	val, err := cli.Get(ctx, redisPolicyKey).Result()
	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("cluster: load policy from redis: %w", err)
	}
	return val, nil
}

// ClearRedisPolicy deletes the policy from Redis.
func ClearRedisPolicy(ctx context.Context, cli goredis.UniversalClient) error {
	if err := cli.Del(ctx, redisPolicyKey).Err(); err != nil {
		return fmt.Errorf("cluster: clear policy from redis: %w", err)
	}
	return nil
}
