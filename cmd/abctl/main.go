// cmd/abctl is the actorbase cluster operations CLI.
//
// Usage:
//
//	abctl [-pm <addr>] <command> [args]
//
// Commands:
//
//	members                                                            List current PS nodes
//	routing                                                            Print current routing table
//	split <actor-type> <partition-id> <split-key>                      Request a partition split
//	migrate <actor-type> <partition-id> <node-id>                      Request a partition migrate
//	merge <actor-type> <lower-partition-id> <upper-partition-id>       Request a merge of adjacent partitions
//	stats [node-id]                                                    Print cluster (or specific node) statistics
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sangchul/actorbase/pm"
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
	case "stats":
		nodeID := ""
		if flag.NArg() >= 2 {
			nodeID = flag.Arg(1)
		}
		cmdStats(cfg, nodeID)
	case "policy":
		if flag.NArg() < 2 {
			fmt.Fprintln(os.Stderr, "usage: abctl policy <apply <file>|get|clear>")
			os.Exit(1)
		}
		switch flag.Arg(1) {
		case "apply":
			if flag.NArg() < 3 {
				fmt.Fprintln(os.Stderr, "usage: abctl policy apply <file>")
				os.Exit(1)
			}
			cmdPolicyApply(cfg, flag.Arg(2))
		case "get":
			cmdPolicyGet(cfg)
		case "clear":
			cmdPolicyClear(cfg)
		default:
			fmt.Fprintf(os.Stderr, "unknown policy subcommand: %s\n", flag.Arg(1))
			os.Exit(1)
		}
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
	case "merge":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: abctl merge <actor-type> <lower-partition-id> <upper-partition-id>")
			os.Exit(1)
		}
		cmdMerge(cfg, flag.Arg(1), flag.Arg(2), flag.Arg(3))
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
  merge <actor-type> <lower-id> <upper-id>         Merge two adjacent partitions
  stats [node-id]                                  Show cluster stats (or specific node)
  policy apply <file>                              Apply policy YAML (activate AutoPolicy)
  policy get                                       Show current policy
  policy clear                                     Remove policy (revert to ManualPolicy)

`)
}

func newPMClient(pmAddr string) *pm.Client {
	client, err := pm.NewClient(pmAddr)
	if err != nil {
		slog.Error("failed to connect to PM", "addr", pmAddr, "err", err)
		os.Exit(1)
	}
	return client
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
		fmt.Printf("%-36s  %-20s  %s\n", m.NodeID, m.Address, m.Status)
	}
}

func cmdRouting(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	ch := client.WatchRouting(ctx, "abctl")

	snap, ok := <-ch
	if !ok {
		fmt.Fprintln(os.Stderr, "no routing table available (is PM running?)")
		os.Exit(1)
	}

	fmt.Printf("Version: %d\n\n", snap.Version)
	fmt.Printf("%-36s  %-12s  %-16s  %-16s  %-36s  %s\n",
		"PARTITION-ID", "ACTOR-TYPE", "KEY-START", "KEY-END", "NODE-ID", "NODE-ADDR")
	fmt.Printf("%-36s  %-12s  %-16s  %-16s  %-36s  %s\n",
		"------------------------------------", "------------", "----------------", "----------------",
		"------------------------------------", "-----------")
	for _, e := range snap.Entries {
		start := e.KeyRangeStart
		end := e.KeyRangeEnd
		if start == "" {
			start = "(start)"
		}
		if end == "" {
			end = "(end)"
		}
		fmt.Printf("%-36s  %-12s  %-16s  %-16s  %-36s  %s\n",
			e.PartitionID, e.ActorType, start, end, e.NodeID, e.NodeAddr)
	}
}

func cmdSplit(cfg *Config, actorType, partitionID, splitKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	newID, err := client.RequestSplit(ctx, actorType, partitionID, splitKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "split failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("split successful\nnew partition ID: %s\n", newID)
}

func cmdMigrate(cfg *Config, actorType, partitionID, targetNodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.RequestMigrate(ctx, actorType, partitionID, targetNodeID); err != nil {
		fmt.Fprintf(os.Stderr, "migrate failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("migrate successful")
}

func cmdMerge(cfg *Config, actorType, lowerPartitionID, upperPartitionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.RequestMerge(ctx, actorType, lowerPartitionID, upperPartitionID); err != nil {
		fmt.Fprintf(os.Stderr, "merge failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("merge successful")
}

func cmdPolicyApply(cfg *Config, filePath string) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("read policy file failed", "file", filePath, "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.ApplyPolicy(ctx, string(data)); err != nil {
		slog.Error("apply policy failed", "err", err)
		os.Exit(1)
	}
	fmt.Println("AutoPolicy applied successfully")
}

func cmdPolicyGet(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	yamlStr, active, err := client.GetPolicy(ctx)
	if err != nil {
		slog.Error("get policy failed", "err", err)
		os.Exit(1)
	}
	if !active {
		fmt.Println("Mode: ManualPolicy (no AutoPolicy active)")
		return
	}
	fmt.Println("Mode: AutoPolicy (active)")
	fmt.Println("---")
	fmt.Print(yamlStr)
}

func cmdPolicyClear(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.ClearPolicy(ctx); err != nil {
		slog.Error("clear policy failed", "err", err)
		os.Exit(1)
	}
	fmt.Println("Policy cleared. Reverted to ManualPolicy.")
}

func cmdStats(cfg *Config, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	nodes, err := client.GetClusterStats(ctx, nodeID)
	if err != nil {
		slog.Error("get stats failed", "err", err)
		os.Exit(1)
	}

	if len(nodes) == 0 {
		fmt.Println("no stats available")
		return
	}

	for _, n := range nodes {
		fmt.Printf("Node: %s (%s)  rps=%.1f  partitions=%d\n",
			n.NodeID, n.NodeAddr, n.NodeRPS, n.PartitionCount)
		if len(n.Partitions) > 0 {
			fmt.Printf("  %-36s  %-12s  %10s  %8s\n", "PARTITION-ID", "ACTOR-TYPE", "KEY-COUNT", "RPS")
			fmt.Printf("  %-36s  %-12s  %10s  %8s\n",
				"------------------------------------", "------------", "----------", "--------")
			for _, p := range n.Partitions {
				keyCount := fmt.Sprintf("%d", p.KeyCount)
				if p.KeyCount < 0 {
					keyCount = "n/a"
				}
				fmt.Printf("  %-36s  %-12s  %10s  %8.1f\n",
					p.PartitionID, p.ActorType, keyCount, p.RPS)
			}
		}
		fmt.Println()
	}
}
