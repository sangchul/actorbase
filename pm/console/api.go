package console

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

// ─── JSON response types ───────────────────────────────────────────────────

type memberJSON struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

type routeEntryJSON struct {
	PartitionID   string `json:"partition_id"`
	ActorType     string `json:"actor_type"`
	KeyRangeStart string `json:"key_range_start"`
	KeyRangeEnd   string `json:"key_range_end"`
	NodeID        string `json:"node_id"`
	NodeAddress   string `json:"node_address"`
}

type routingTableJSON struct {
	Version int64            `json:"version"`
	Entries []routeEntryJSON `json:"entries"`
}

type partitionStatsJSON struct {
	PartitionID string  `json:"partition_id"`
	ActorType   string  `json:"actor_type"`
	KeyCount    int64   `json:"key_count"`
	RPS         float64 `json:"rps"`
}

type nodeStatsJSON struct {
	NodeID         string               `json:"node_id"`
	NodeAddr       string               `json:"node_addr"`
	NodeRPS        float64              `json:"node_rps"`
	PartitionCount int32                `json:"partition_count"`
	Partitions     []partitionStatsJSON `json:"partitions"`
}

type clusterStatsJSON struct {
	Nodes []nodeStatsJSON `json:"nodes"`
}

type policyJSON struct {
	Active bool   `json:"active"`
	YAML   string `json:"yaml"`
}

// ─── apiHandler ───────────────────────────────────────────────────────────

type apiHandler struct {
	pmAddr string
	pool   *transport.ConnPool

	routingMu sync.RWMutex
	cachedRT  *domain.RoutingTable
}

func newAPIHandler(pmAddr string) *apiHandler {
	return &apiHandler{
		pmAddr: pmAddr,
		pool:   transport.NewConnPool(),
	}
}

// startRoutingWatcher maintains a persistent WatchRouting subscription to
// keep cachedRT up-to-date. Runs until ctx is cancelled.
func (a *apiHandler) startRoutingWatcher(ctx context.Context) {
	conn, err := a.pool.Get(a.pmAddr)
	if err != nil {
		slog.Error("console: routing watcher connect failed", "err", err)
		return
	}
	client := transport.NewPMClient(conn)
	ch := client.WatchRouting(ctx, "console-routing-watcher")
	for {
		select {
		case rt, ok := <-ch:
			if !ok {
				return
			}
			if rt != nil {
				a.routingMu.Lock()
				a.cachedRT = rt
				a.routingMu.Unlock()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (a *apiHandler) pmClient() (*transport.PMClient, error) {
	conn, err := a.pool.Get(a.pmAddr)
	if err != nil {
		return nil, err
	}
	return transport.NewPMClient(conn), nil
}

func (a *apiHandler) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/members", a.handleMembers)
	mux.HandleFunc("GET /api/routing", a.handleRouting)
	mux.HandleFunc("GET /api/stats", a.handleStats)
	mux.HandleFunc("POST /api/split", a.handleSplit)
	mux.HandleFunc("POST /api/migrate", a.handleMigrate)
	mux.HandleFunc("GET /api/policy", a.handlePolicy)
}

// ─── helpers ─────────────────────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ─── handlers ─────────────────────────────────────────────────────────────

func (a *apiHandler) handleMembers(w http.ResponseWriter, r *http.Request) {
	client, err := a.pmClient()
	if err != nil {
		jsonErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	members, err := client.ListMembers(r.Context())
	if err != nil {
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := make([]memberJSON, len(members))
	for i, m := range members {
		status := "active"
		if m.Status == domain.NodeStatusDraining {
			status = "draining"
		}
		resp[i] = memberJSON{NodeID: m.NodeID, Address: m.Address, Status: status}
	}
	jsonOK(w, resp)
}

func (a *apiHandler) handleRouting(w http.ResponseWriter, r *http.Request) {
	a.routingMu.RLock()
	rt := a.cachedRT
	a.routingMu.RUnlock()

	if rt == nil {
		jsonErr(w, http.StatusServiceUnavailable, "routing table not yet available")
		return
	}

	entries := rt.Entries()
	resp := routingTableJSON{
		Version: rt.Version(),
		Entries: make([]routeEntryJSON, len(entries)),
	}
	for i, e := range entries {
		resp.Entries[i] = routeEntryJSON{
			PartitionID:   e.Partition.ID,
			ActorType:     e.Partition.ActorType,
			KeyRangeStart: e.Partition.KeyRange.Start,
			KeyRangeEnd:   e.Partition.KeyRange.End,
			NodeID:        e.Node.ID,
			NodeAddress:   e.Node.Address,
		}
	}
	jsonOK(w, resp)
}

func (a *apiHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	client, err := a.pmClient()
	if err != nil {
		jsonErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	nodeStats, err := client.GetClusterStats(r.Context(), "")
	if err != nil {
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	nodes := make([]nodeStatsJSON, len(nodeStats))
	for i, n := range nodeStats {
		partitions := make([]partitionStatsJSON, len(n.Partitions))
		for j, p := range n.Partitions {
			partitions[j] = partitionStatsJSON{
				PartitionID: p.PartitionID,
				ActorType:   p.ActorType,
				KeyCount:    p.KeyCount,
				RPS:         p.RPS,
			}
		}
		nodes[i] = nodeStatsJSON{
			NodeID:         n.NodeID,
			NodeAddr:       n.NodeAddr,
			NodeRPS:        n.NodeRPS,
			PartitionCount: n.PartitionCount,
			Partitions:     partitions,
		}
	}
	jsonOK(w, clusterStatsJSON{Nodes: nodes})
}

func (a *apiHandler) handleSplit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ActorType   string `json:"actor_type"`
		PartitionID string `json:"partition_id"`
		SplitKey    string `json:"split_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	client, err := a.pmClient()
	if err != nil {
		jsonErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	newID, err := client.RequestSplit(r.Context(), req.ActorType, req.PartitionID, req.SplitKey)
	if err != nil {
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]string{"new_partition_id": newID})
}

func (a *apiHandler) handleMigrate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ActorType    string `json:"actor_type"`
		PartitionID  string `json:"partition_id"`
		TargetNodeID string `json:"target_node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	client, err := a.pmClient()
	if err != nil {
		jsonErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if err := client.RequestMigrate(r.Context(), req.ActorType, req.PartitionID, req.TargetNodeID); err != nil {
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]string{"status": "ok"})
}

func (a *apiHandler) handlePolicy(w http.ResponseWriter, r *http.Request) {
	client, err := a.pmClient()
	if err != nil {
		jsonErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	yamlStr, active, err := client.GetPolicy(r.Context())
	if err != nil {
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, policyJSON{Active: active, YAML: yamlStr})
}
