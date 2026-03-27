// cmd/abctl is the actorbase cluster operations CLI.
//
// Usage:
//
//	abctl [-pm <addr>] <command> [args]
//
// Commands:
//
//	members                                                            List all nodes (all states)
//	routing                                                            Print current routing table
//	split <actor-type> <partition-id> <split-key>                      Request a partition split
//	migrate <actor-type> <partition-id> <node-id>                      Request a partition migrate
//	merge <actor-type> <lower-partition-id> <upper-partition-id>       Request a merge of adjacent partitions
//	stats [node-id]                                                    Print cluster (or specific node) statistics
//	node add <node-id> <addr>                                          Pre-register a node (Waiting)
//	node remove <node-id>                                              Remove a Waiting or Failed node
//	node reset <node-id>                                               Reset a Failed node to Waiting
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sangchul/actorbase/internal/domain"
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
	case "node":
		if flag.NArg() < 2 {
			fmt.Fprintln(os.Stderr, "usage: abctl node <add <node-id> <addr>|remove <node-id>|reset <node-id>>")
			os.Exit(1)
		}
		switch flag.Arg(1) {
		case "add":
			if flag.NArg() < 4 {
				fmt.Fprintln(os.Stderr, "usage: abctl node add <node-id> <addr>")
				os.Exit(1)
			}
			cmdNodeAdd(cfg, flag.Arg(2), flag.Arg(3))
		case "remove":
			if flag.NArg() < 3 {
				fmt.Fprintln(os.Stderr, "usage: abctl node remove <node-id>")
				os.Exit(1)
			}
			cmdNodeRemove(cfg, flag.Arg(2))
		case "reset":
			if flag.NArg() < 3 {
				fmt.Fprintln(os.Stderr, "usage: abctl node reset <node-id>")
				os.Exit(1)
			}
			cmdNodeReset(cfg, flag.Arg(2))
		default:
			fmt.Fprintf(os.Stderr, "unknown node subcommand: %s\n", flag.Arg(1))
			os.Exit(1)
		}
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
  members                                          List all nodes (all states)
  routing                                          Print current routing table
  split <actor-type> <partition-id> <split-key>    Split a partition at the given key
  migrate <actor-type> <partition-id> <node-id>    Migrate a partition to the target node
  merge <actor-type> <lower-id> <upper-id>         Merge two adjacent partitions
  stats [node-id]                                  Show cluster stats (or specific node)
  policy apply <file>                              Apply policy YAML (activate AutoPolicy)
  policy get                                       Show current policy
  policy clear                                     Remove policy (revert to ManualPolicy)
  node add <node-id> <addr>                        Pre-register a node (Waiting state)
  node remove <node-id>                            Remove a Waiting or Failed node
  node reset <node-id>                             Reset a Failed node to Waiting (allow rejoin)

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
		"------------------------------------", "--------------------", "---------")
	for _, m := range members {
		fmt.Printf("%-36s  %-20s  %s\n", m.NodeID, m.Address, nodeStatusLabel(m.Status))
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

func cmdNodeAdd(cfg *Config, nodeID, addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.AddNode(ctx, nodeID, addr); err != nil {
		fmt.Fprintf(os.Stderr, "node add failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("node %q registered with Waiting status\n", nodeID)
}

func cmdNodeRemove(cfg *Config, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.RemoveNode(ctx, nodeID); err != nil {
		fmt.Fprintf(os.Stderr, "node remove failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("node %q removed from catalog\n", nodeID)
}

func cmdNodeReset(cfg *Config, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := newPMClient(cfg.PMAddr)
	if err := client.ResetNode(ctx, nodeID); err != nil {
		fmt.Fprintf(os.Stderr, "node reset failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("node %q reset to Waiting; it may now rejoin the cluster\n", nodeID)
}

func nodeStatusLabel(s domain.NodeStatus) string {
	switch s {
	case domain.NodeStatusWaiting:
		return "Waiting"
	case domain.NodeStatusActive:
		return "Active"
	case domain.NodeStatusDraining:
		return "Draining"
	case domain.NodeStatusFailed:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
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
