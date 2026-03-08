// cmd/abctl은 actorbase 클러스터 운영 CLI다.
//
// 사용법:
//
//	abctl [-pm <addr>] <command> [args]
//
// 명령:
//
//	members                                            현재 PS 노드 목록 출력
//	routing                                            현재 라우팅 테이블 출력
//	split <actor-type> <partition-id> <split-key>      파티션 split 요청
//	migrate <actor-type> <partition-id> <node-id>      파티션 migrate 요청
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oomymy/actorbase/internal/transport"
)

func main() {
	pmFlag := flag.String("pm", "", "PM gRPC address (overrides config file and ACTORBASE_PM_ADDR env)")
	flag.Usage = printUsage
	flag.Parse()

	if flag.NArg() == 0 {
		printUsage()
		os.Exit(1)
	}

	cfg := loadConfig(*pmFlag)

	switch flag.Arg(0) {
	case "members":
		cmdMembers(cfg)
	case "routing":
		cmdRouting(cfg)
	case "split":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: abctl split <actor-type> <partition-id> <split-key>")
			os.Exit(1)
		}
		cmdSplit(cfg, flag.Arg(1), flag.Arg(2), flag.Arg(3))
	case "migrate":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: abctl migrate <actor-type> <partition-id> <target-node-id>")
			os.Exit(1)
		}
		cmdMigrate(cfg, flag.Arg(1), flag.Arg(2), flag.Arg(3))
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", flag.Arg(0))
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `Usage: abctl [-pm <addr>] <command> [args]

Global flags:
  -pm string   PM gRPC address (default from ~/.actorbase/config.json or ACTORBASE_PM_ADDR env)

Commands:
  members                                          List active PS nodes
  routing                                          Print current routing table
  split <actor-type> <partition-id> <split-key>    Split a partition at the given key
  migrate <actor-type> <partition-id> <node-id>    Migrate a partition to the target node

`)
}

func newPMClient(pmAddr string) *transport.PMClient {
	conn, err := grpc.NewClient(pmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to connect to PM", "addr", pmAddr, "err", err)
		os.Exit(1)
	}
	return transport.NewPMClient(conn)
}

func cmdMembers(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	members, err := client.ListMembers(ctx)
	if err != nil {
		slog.Error("list members failed", "err", err)
		os.Exit(1)
	}

	if len(members) == 0 {
		fmt.Println("no members registered")
		return
	}

	fmt.Printf("%-36s  %-20s  %s\n", "NODE-ID", "ADDRESS", "STATUS")
	fmt.Printf("%-36s  %-20s  %s\n",
		"------------------------------------", "--------------------", "------")
	for _, m := range members {
		status := "active"
		if m.Status != 0 {
			status = "draining"
		}
		fmt.Printf("%-36s  %-20s  %s\n", m.NodeID, m.Address, status)
	}
}

func cmdRouting(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	ch := client.WatchRouting(ctx, "abctl")

	rt, ok := <-ch
	if !ok || rt == nil {
		fmt.Fprintln(os.Stderr, "no routing table available (is PM running?)")
		os.Exit(1)
	}

	fmt.Printf("Version: %d\n\n", rt.Version())
	fmt.Printf("%-36s  %-16s  %-16s  %-36s  %s\n",
		"PARTITION-ID", "KEY-START", "KEY-END", "NODE-ID", "NODE-ADDR")
	fmt.Printf("%-36s  %-16s  %-16s  %-36s  %s\n",
		"------------------------------------", "----------------", "----------------",
		"------------------------------------", "-----------")
	for _, e := range rt.Entries() {
		start := e.Partition.KeyRange.Start
		end := e.Partition.KeyRange.End
		if start == "" {
			start = "(start)"
		}
		if end == "" {
			end = "(end)"
		}
		fmt.Printf("%-36s  %-16s  %-16s  %-36s  %s\n",
			e.Partition.ID, start, end, e.Node.ID, e.Node.Address)
	}
}

func cmdSplit(cfg *Config, actorType, partitionID, splitKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	newID, err := client.RequestSplit(ctx, actorType, partitionID, splitKey)
	if err != nil {
		slog.Error("split failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("split successful\nnew partition ID: %s\n", newID)
}

func cmdMigrate(cfg *Config, actorType, partitionID, targetNodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.RequestMigrate(ctx, actorType, partitionID, targetNodeID); err != nil {
		slog.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	fmt.Println("migrate successful")
}
