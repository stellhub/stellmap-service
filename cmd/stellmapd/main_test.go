package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellhub/stellmap/internal/raftnode"
	"github.com/stellhub/stellmap/internal/registry"
	"github.com/stellhub/stellmap/internal/storage"
	httptransport "github.com/stellhub/stellmap/internal/transport/http"
)

type testSuccessEnvelope struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

const testAdminToken = "test-admin-token"

func TestStellmapdThreeNodeClusterWriteAndRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clusterID := uint64(300)
	peerIDs := "1,2,3"

	httpAddrs := map[uint64]string{
		1: reserveTCPAddress(t),
		2: reserveTCPAddress(t),
		3: reserveTCPAddress(t),
	}
	adminAddrs := map[uint64]string{
		1: reserveTCPAddress(t),
		2: reserveTCPAddress(t),
		3: reserveTCPAddress(t),
	}
	grpcAddrs := map[uint64]string{
		1: reserveTCPAddress(t),
		2: reserveTCPAddress(t),
		3: reserveTCPAddress(t),
	}

	apps := make(map[uint64]*serverApp, 3)
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		app := mustNewTestApp(t, daemonConfig{
			NodeID:         nodeID,
			ClusterID:      clusterID,
			DataDir:        filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", nodeID)),
			HTTPAddr:       httpAddrs[nodeID],
			AdminAddr:      adminAddrs[nodeID],
			AdminToken:     testAdminToken,
			GRPCAddr:       grpcAddrs[nodeID],
			PeerIDs:        peerIDs,
			PeerHTTPAddrs:  joinAddressMap(httpAddrs),
			PeerAdminAddrs: joinAddressMap(adminAddrs),
			PeerGRPCAddrs:  joinAddressMap(grpcAddrs),
		})
		if err := app.Start(ctx); err != nil {
			t.Fatalf("start node %d: %v", nodeID, err)
		}
		apps[nodeID] = app
	}
	defer stopApps(t, apps)

	leaderID := waitForClusterLeader(t, apps, 10*time.Second)
	leaderBaseURL := "http://" + httpAddrs[leaderID]

	if err := postJSON(leaderBaseURL+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "checkout-service",
		InstanceID: "checkout-1",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
		}},
	}, ""); err != nil {
		t.Fatalf("register first instance via leader %d: %v", leaderID, err)
	}
	for nodeID, addr := range httpAddrs {
		waitForRegistryInstanceIDs(t, "http://"+addr, url.Values{
			"namespace": []string{"prod"},
			"service":   []string{"checkout-service"},
		}, 10*time.Second, fmt.Sprintf("node-%d first registry instance", nodeID), "checkout-1")
	}

	restartNodeID := pickFollower(apps, leaderID)
	restartCfg := apps[restartNodeID].Config()
	if err := apps[restartNodeID].Stop(context.Background()); err != nil {
		t.Fatalf("stop follower %d: %v", restartNodeID, err)
	}
	delete(apps, restartNodeID)

	if err := postJSON(leaderBaseURL+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "checkout-service",
		InstanceID: "checkout-2",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8081,
		}},
	}, ""); err != nil {
		t.Fatalf("register second instance while follower down: %v", err)
	}
	for nodeID, addr := range httpAddrs {
		if nodeID == restartNodeID {
			continue
		}
		waitForRegistryInstanceIDs(t, "http://"+addr, url.Values{
			"namespace": []string{"prod"},
			"service":   []string{"checkout-service"},
		}, 10*time.Second, fmt.Sprintf("node-%d second registry instance before restart", nodeID), "checkout-1", "checkout-2")
	}

	restarted := mustNewTestApp(t, restartCfg)
	if err := restarted.Start(ctx); err != nil {
		t.Fatalf("restart follower %d: %v", restartNodeID, err)
	}
	apps[restartNodeID] = restarted

	waitForClusterLeader(t, apps, 10*time.Second)
	waitForRegistryInstanceIDs(t, "http://"+httpAddrs[restartNodeID], url.Values{
		"namespace": []string{"prod"},
		"service":   []string{"checkout-service"},
	}, 10*time.Second, fmt.Sprintf("node-%d recover registry instances", restartNodeID), "checkout-1", "checkout-2")
}

func TestStellmapdPersistDynamicMemberAddressesAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)
	dataDir := filepath.Join(t.TempDir(), "node-1")

	app := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  301,
		DataDir:    dataDir,
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	memberRequest := httptransport.MemberChangeRequestDTO{
		NodeID:    2,
		HTTPAddr:  "127.0.0.1:28080",
		GRPCAddr:  "127.0.0.1:29090",
		AdminAddr: "127.0.0.1:28081",
	}
	if err := postJSON("http://"+adminAddr+"/admin/v1/members/add-learner", memberRequest, testAdminToken); err != nil {
		t.Fatalf("add learner failed: %v", err)
	}
	waitForMemberAddress(t, "http://"+adminAddr, testAdminToken, 2, memberRequest.HTTPAddr, memberRequest.GRPCAddr, memberRequest.AdminAddr, 5*time.Second)

	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("stop first app: %v", err)
	}

	restarted := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  301,
		DataDir:    dataDir,
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := restarted.Start(ctx); err != nil {
		t.Fatalf("restart single node app: %v", err)
	}
	defer func() {
		if err := restarted.Stop(context.Background()); err != nil {
			t.Fatalf("stop restarted app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: restarted}, 5*time.Second)
	waitForMemberAddress(t, "http://"+adminAddr, testAdminToken, 2, memberRequest.HTTPAddr, memberRequest.GRPCAddr, memberRequest.AdminAddr, 5*time.Second)
}

func TestStellmapdAdminRequiresToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  302,
		DataDir:    filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForAdminStatusCode(t, "http://"+adminAddr+"/admin/v1/status", "", http.StatusUnauthorized, 5*time.Second)
}

func TestAdminAuthMiddlewareRejectsNonLocalhost(t *testing.T) {
	handler := adminAuthMiddleware(testAdminToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := httptest.NewRequest(http.MethodGet, "/admin/v1/status", nil)
	request.RemoteAddr = "192.168.1.10:34567"
	request.Header.Set("Authorization", "Bearer "+testAdminToken)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}

func TestRegisterAndHeartbeatPersistStructuredRegistryValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  303,
		DataDir:    filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	registerRequest := httptransport.RegisterRequestDTO{
		Namespace:  " prod ",
		Service:    " order-service ",
		InstanceID: " order-1 ",
		Zone:       " az1 ",
		Labels: map[string]string{
			"color":   " gray ",
			"version": " v2 ",
		},
		Metadata: map[string]string{
			"owner":     " trade-team ",
			"build_sha": " abc123 ",
		},
		Endpoints: []httptransport.EndpointDTO{
			{
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     8080,
			},
			{
				Name:     "grpc-main",
				Protocol: "grpc",
				Host:     "127.0.0.1",
				Port:     9090,
				Weight:   80,
			},
		},
	}
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", registerRequest, ""); err != nil {
		t.Fatalf("register instance failed: %v", err)
	}

	key := registry.Key("prod", "order-service", "order-1")
	stored := waitForRegistryValue(t, app, key, func(value registry.Value) bool {
		return value.Zone == "az1" && value.LeaseTTLSeconds == registry.DefaultLeaseTTLSeconds && len(value.Endpoints) == 2
	}, 5*time.Second, "register instance")
	if stored.Zone != "az1" {
		t.Fatalf("unexpected zone: %s", stored.Zone)
	}
	if stored.Labels["color"] != "gray" || stored.Labels["version"] != "v2" {
		t.Fatalf("unexpected labels: %+v", stored.Labels)
	}
	if stored.Metadata["owner"] != "trade-team" || stored.Metadata["build_sha"] != "abc123" {
		t.Fatalf("unexpected metadata: %+v", stored.Metadata)
	}
	if len(stored.Endpoints) != 2 {
		t.Fatalf("unexpected endpoints count: %d", len(stored.Endpoints))
	}
	if stored.Endpoints[0].Name != "http" || stored.Endpoints[0].Weight != registry.DefaultEndpointWeight {
		t.Fatalf("unexpected normalized endpoint: %+v", stored.Endpoints[0])
	}
	if stored.LeaseTTLSeconds != registry.DefaultLeaseTTLSeconds {
		t.Fatalf("unexpected default lease ttl: %d", stored.LeaseTTLSeconds)
	}
	registeredAt := stored.RegisteredAtUnix
	lastHeartbeat := stored.LastHeartbeatUnix

	secondRequest := httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-2",
		Zone:       "az2",
		Labels: map[string]string{
			"color": "blue",
		},
		Metadata: map[string]string{
			"owner": "checkout-team",
		},
		Endpoints: []httptransport.EndpointDTO{
			{
				Name:     "http",
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     8081,
			},
		},
		LeaseTTLSeconds: 60,
	}
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", secondRequest, ""); err != nil {
		t.Fatalf("register second instance failed: %v", err)
	}

	waitForRegistryValue(t, app, registry.Key("prod", "order-service", "order-2"), func(value registry.Value) bool {
		return value.Zone == "az2" && value.LeaseTTLSeconds == 60
	}, 5*time.Second, "register second instance")

	candidates := waitForRegistryInstances(t, "http://"+httpAddr, url.Values{
		"namespace": []string{"prod"},
		"service":   []string{"order-service"},
		"zone":      []string{"az1"},
		"endpoint":  []string{"http"},
		"selector":  []string{"color=gray,version in (v2),!deprecated"},
		"label":     []string{"color=gray"},
	}, 5*time.Second, func(items []httptransport.RegistryInstanceDTO) bool {
		return len(items) == 1
	})
	if candidates[0].InstanceID != "order-1" {
		t.Fatalf("unexpected candidate instance: %+v", candidates[0])
	}
	if len(candidates[0].Endpoints) != 1 || candidates[0].Endpoints[0].Name != "http" {
		t.Fatalf("unexpected filtered endpoints: %+v", candidates[0].Endpoints)
	}
	if candidates[0].LeaseTTLSeconds != registry.DefaultLeaseTTLSeconds {
		t.Fatalf("unexpected candidate lease ttl: %d", candidates[0].LeaseTTLSeconds)
	}

	time.Sleep(1 * time.Second)
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/heartbeat", httptransport.HeartbeatRequestDTO{
		Namespace:       "prod",
		Service:         "order-service",
		InstanceID:      "order-1",
		LeaseTTLSeconds: 45,
	}, ""); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	updated := waitForRegistryValue(t, app, key, func(value registry.Value) bool {
		return value.LeaseTTLSeconds == 45 && value.LastHeartbeatUnix > lastHeartbeat
	}, 5*time.Second, "heartbeat update")
	if updated.LeaseTTLSeconds != 45 {
		t.Fatalf("unexpected lease ttl after heartbeat: %d", updated.LeaseTTLSeconds)
	}
	if updated.RegisteredAtUnix != registeredAt {
		t.Fatalf("registered time changed after heartbeat: before=%d after=%d", registeredAt, updated.RegisteredAtUnix)
	}
	if updated.LastHeartbeatUnix <= lastHeartbeat {
		t.Fatalf("last heartbeat did not advance: before=%d after=%d", lastHeartbeat, updated.LastHeartbeatUnix)
	}
	if len(updated.Endpoints) != 2 || updated.Endpoints[0].Name != "http" || updated.Endpoints[1].Name != "grpc-main" {
		t.Fatalf("endpoints changed after heartbeat: %+v", updated.Endpoints)
	}
	if updated.Labels["color"] != "gray" || updated.Metadata["owner"] != "trade-team" {
		t.Fatalf("structured fields lost after heartbeat: labels=%+v metadata=%+v", updated.Labels, updated.Metadata)
	}
}

func TestRegistrySelectorSyntaxAndExpiredCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  304,
		DataDir:    filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	longLived := httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "payment-service",
		InstanceID: "payment-1",
		Zone:       "az1",
		Labels: map[string]string{
			"color":   "gray",
			"version": "v2",
		},
		Endpoints: []httptransport.EndpointDTO{
			{
				Name:     "http",
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     8080,
			},
			{
				Name:     "grpc",
				Protocol: "grpc",
				Host:     "127.0.0.1",
				Port:     9090,
			},
		},
	}
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", longLived, ""); err != nil {
		t.Fatalf("register long lived instance failed: %v", err)
	}

	expiring := httptransport.RegisterRequestDTO{
		Namespace:       "prod",
		Service:         "payment-service",
		InstanceID:      "payment-expired",
		Zone:            "az2",
		LeaseTTLSeconds: 1,
		Labels: map[string]string{
			"color":   "blue",
			"version": "v1",
		},
		Endpoints: []httptransport.EndpointDTO{
			{
				Name:     "http",
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     8081,
			},
		},
	}
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", expiring, ""); err != nil {
		t.Fatalf("register expiring instance failed: %v", err)
	}

	selected := waitForRegistryInstances(t, "http://"+httpAddr, url.Values{
		"namespace": []string{"prod"},
		"service":   []string{"payment-service"},
		"endpoint":  []string{"http"},
		"selector":  []string{"color=gray,version in (v2),!deprecated"},
	}, 5*time.Second, func(items []httptransport.RegistryInstanceDTO) bool {
		return len(items) == 1 && items[0].InstanceID == "payment-1"
	})
	if len(selected[0].Endpoints) != 1 || selected[0].Endpoints[0].Name != "http" {
		t.Fatalf("unexpected selected endpoints: %+v", selected[0].Endpoints)
	}

	waitForRegistryValueDeleted(t, app, registry.Key("prod", "payment-service", "payment-expired"), 8*time.Second, "expired registry cleanup")

	waitForRegistryInstances(t, "http://"+httpAddr, url.Values{
		"namespace": []string{"prod"},
		"service":   []string{"payment-service"},
	}, 5*time.Second, func(items []httptransport.RegistryInstanceDTO) bool {
		return len(items) == 1 && items[0].InstanceID == "payment-1"
	})
}

func TestRegistryWatchSSEPublishesSnapshotUpsertAndDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:     1,
		ClusterID:  307,
		DataDir:    filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:   httpAddr,
		AdminAddr:  adminAddr,
		AdminToken: testAdminToken,
		GRPCAddr:   grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	request, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/api/v1/registry/watch?namespace=prod&service=order-service", nil)
	if err != nil {
		t.Fatalf("new watch request failed: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("open watch stream failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("expected watch status 200, got %d body=%s", response.StatusCode, string(body))
	}

	reader := bufio.NewReader(response.Body)
	snapshotEvent := mustReadSSEEvent(t, reader, 5*time.Second)
	if snapshotEvent.Event != "snapshot" {
		t.Fatalf("expected first event snapshot, got %s", snapshotEvent.Event)
	}

	var snapshotPayload httptransport.RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(snapshotEvent.Data), &snapshotPayload); err != nil {
		t.Fatalf("unmarshal snapshot payload failed: %v", err)
	}
	if snapshotPayload.Type != "snapshot" || len(snapshotPayload.Instances) != 0 {
		t.Fatalf("unexpected snapshot payload: %+v", snapshotPayload)
	}

	registerRequest := httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-1",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
		}},
	}
	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", registerRequest, ""); err != nil {
		t.Fatalf("register instance failed: %v", err)
	}

	upsertEvent := mustReadSSEEvent(t, reader, 5*time.Second)
	if upsertEvent.Event != "upsert" {
		t.Fatalf("expected upsert event, got %s", upsertEvent.Event)
	}
	var upsertPayload httptransport.RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(upsertEvent.Data), &upsertPayload); err != nil {
		t.Fatalf("unmarshal upsert payload failed: %v", err)
	}
	if upsertPayload.Type != "upsert" || upsertPayload.Instance == nil || upsertPayload.Instance.InstanceID != "order-1" {
		t.Fatalf("unexpected upsert payload: %+v", upsertPayload)
	}

	if err := postJSON("http://"+httpAddr+"/api/v1/registry/deregister", httptransport.DeregisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-1",
	}, ""); err != nil {
		t.Fatalf("deregister instance failed: %v", err)
	}

	deleteEvent := mustReadSSEEvent(t, reader, 5*time.Second)
	if deleteEvent.Event != "delete" {
		t.Fatalf("expected delete event, got %s", deleteEvent.Event)
	}
	var deletePayload httptransport.RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(deleteEvent.Data), &deletePayload); err != nil {
		t.Fatalf("unmarshal delete payload failed: %v", err)
	}
	if deletePayload.Type != "delete" || deletePayload.InstanceID != "order-1" {
		t.Fatalf("unexpected delete payload: %+v", deletePayload)
	}
}

func TestCrossRegionReplicationSyncsDirectoryWithoutLeakingToPublicWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replicationToken := "test-replication-token"

	sourceHTTPAddr := reserveTCPAddress(t)
	sourceAdminAddr := reserveTCPAddress(t)
	sourceGRPCAddr := reserveTCPAddress(t)
	sourceApp := mustNewTestApp(t, daemonConfig{
		NodeID:           1,
		ClusterID:        401,
		Region:           "cn-sh",
		DataDir:          filepath.Join(t.TempDir(), "source-node-1"),
		HTTPAddr:         sourceHTTPAddr,
		AdminAddr:        sourceAdminAddr,
		AdminToken:       testAdminToken,
		ReplicationToken: replicationToken,
		GRPCAddr:         sourceGRPCAddr,
	})
	if err := sourceApp.Start(ctx); err != nil {
		t.Fatalf("start source app: %v", err)
	}
	defer func() {
		if err := sourceApp.Stop(context.Background()); err != nil {
			t.Fatalf("stop source app: %v", err)
		}
	}()

	targetHTTPAddr := reserveTCPAddress(t)
	targetAdminAddr := reserveTCPAddress(t)
	targetGRPCAddr := reserveTCPAddress(t)
	targetsFile := filepath.Join(t.TempDir(), "replication-targets.json")
	targetsJSON := fmt.Sprintf(`[{"sourceRegion":"cn-sh","sourceClusterId":"401","baseURL":"http://%s","services":[{"namespace":"prod","service":"order-service"}]}]`, sourceHTTPAddr)
	if err := os.WriteFile(targetsFile, []byte(targetsJSON), 0o644); err != nil {
		t.Fatalf("write replication targets file failed: %v", err)
	}
	targetApp := mustNewTestApp(t, daemonConfig{
		NodeID:                 1,
		ClusterID:              402,
		Region:                 "cn-bj",
		DataDir:                filepath.Join(t.TempDir(), "target-node-1"),
		HTTPAddr:               targetHTTPAddr,
		AdminAddr:              targetAdminAddr,
		AdminToken:             testAdminToken,
		ReplicationToken:       replicationToken,
		ReplicationTargetsFile: targetsFile,
		GRPCAddr:               targetGRPCAddr,
	})
	if err := targetApp.Start(ctx); err != nil {
		t.Fatalf("start target app: %v", err)
	}
	defer func() {
		if err := targetApp.Stop(context.Background()); err != nil {
			t.Fatalf("stop target app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: sourceApp}, 5*time.Second)
	waitForClusterLeader(t, map[uint64]*serverApp{1: targetApp}, 5*time.Second)

	request, err := http.NewRequest(http.MethodGet, "http://"+targetHTTPAddr+"/api/v1/registry/watch?namespace=prod&service=order-service", nil)
	if err != nil {
		t.Fatalf("new target public watch request failed: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("open target public watch failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("expected target public watch status 200, got %d body=%s", response.StatusCode, string(body))
	}

	reader := bufio.NewReader(response.Body)
	snapshotEvent := mustReadSSEEvent(t, reader, 5*time.Second)
	if snapshotEvent.Event != "snapshot" {
		t.Fatalf("expected target public watch snapshot, got %s", snapshotEvent.Event)
	}

	if err := postJSON("http://"+sourceHTTPAddr+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-source-1",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "10.0.1.23",
			Port:     8080,
		}},
	}, ""); err != nil {
		t.Fatalf("register source instance failed: %v", err)
	}

	waitForRegistryInstances(t, "http://"+targetHTTPAddr, url.Values{
		"namespace": []string{"prod"},
		"service":   []string{"order-service"},
	}, 8*time.Second, func(items []httptransport.RegistryInstanceDTO) bool {
		return len(items) == 1 && items[0].InstanceID == "order-source-1"
	})

	replicatedKey := registry.ReplicatedKey("cn-sh", "401", "prod", "order-service", "order-source-1")
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
		data, err := targetApp.Node().Get(readCtx, replicatedKey)
		readCancel()
		if err == nil && len(data) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if data, err := targetApp.Node().Get(context.Background(), replicatedKey); err != nil || len(data) == 0 {
		t.Fatalf("expected replicated value on target, err=%v len=%d", err, len(data))
	}

	statuses := waitForReplicationStatus(t, "http://"+targetAdminAddr, testAdminToken, 8*time.Second, func(items []httptransport.ReplicationStatusDTO) bool {
		return len(items) == 1 &&
			items[0].SourceRegion == "cn-sh" &&
			items[0].SourceClusterID == "401" &&
			items[0].Namespace == "prod" &&
			items[0].Service == "order-service" &&
			items[0].Connected &&
			items[0].LastAppliedRevision > 0
	})
	if statuses[0].LastAppliedRevision == 0 {
		t.Fatalf("expected replication status revision > 0, got %+v", statuses[0])
	}

	metricsBody := waitForMetricsBody(t, "http://"+targetHTTPAddr+"/metrics", 5*time.Second, func(body string) bool {
		return strings.Contains(body, "stellmap_replication_connected{") &&
			strings.Contains(body, `namespace="prod"`) &&
			strings.Contains(body, `service="order-service"`) &&
			strings.Contains(body, `source_cluster="401"`) &&
			strings.Contains(body, `source_region="cn-sh"`) &&
			strings.Contains(body, " 1")
	})
	if !strings.Contains(metricsBody, "stellmap_replication_last_applied_revision") {
		t.Fatalf("expected replication metrics to contain revision gauge, body=%s", metricsBody)
	}

	assertNoSSEEvent(t, reader, 1500*time.Millisecond)
}

func TestReplicationWatchSupportsSinceRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:           1,
		ClusterID:        403,
		Region:           "cn-sh",
		DataDir:          filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:         httpAddr,
		AdminAddr:        adminAddr,
		AdminToken:       testAdminToken,
		ReplicationToken: "test-replication-token",
		GRPCAddr:         grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	req, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/internal/v1/replication/watch?namespace=prod&service=order-service", nil)
	if err != nil {
		t.Fatalf("new replication watch request failed: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-replication-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open replication watch failed: %v", err)
	}
	reader := bufio.NewReader(resp.Body)
	defer resp.Body.Close()

	_ = mustReadSSEEvent(t, reader, 5*time.Second) // snapshot

	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-1",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
		}},
	}, ""); err != nil {
		t.Fatalf("register first instance failed: %v", err)
	}

	firstEvent := mustReadSSEEvent(t, reader, 5*time.Second)
	firstRevision := firstEvent.ID
	resp.Body.Close()

	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-2",
		Endpoints: []httptransport.EndpointDTO{{
			Name:     "http",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8081,
		}},
	}, ""); err != nil {
		t.Fatalf("register second instance failed: %v", err)
	}

	sinceReq, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/internal/v1/replication/watch?namespace=prod&service=order-service&sinceRevision="+url.QueryEscape(firstRevision), nil)
	if err != nil {
		t.Fatalf("new since replication watch request failed: %v", err)
	}
	sinceReq.Header.Set("Authorization", "Bearer test-replication-token")
	sinceResp, err := http.DefaultClient.Do(sinceReq)
	if err != nil {
		t.Fatalf("open replication watch with since failed: %v", err)
	}
	defer sinceResp.Body.Close()
	if sinceResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(sinceResp.Body)
		t.Fatalf("expected replication watch with since status 200, got %d body=%s", sinceResp.StatusCode, string(body))
	}

	sinceReader := bufio.NewReader(sinceResp.Body)
	replayedEvent := mustReadSSEEvent(t, sinceReader, 5*time.Second)
	if replayedEvent.Event != "upsert" {
		t.Fatalf("expected replayed upsert event, got %s", replayedEvent.Event)
	}
	if replayedEvent.ID == firstRevision {
		t.Fatalf("expected replayed event id to advance beyond first revision")
	}

	var replayedPayload httptransport.ReplicationWatchEventDTO
	if err := json.Unmarshal([]byte(replayedEvent.Data), &replayedPayload); err != nil {
		t.Fatalf("unmarshal replayed payload failed: %v", err)
	}
	if replayedPayload.Instance == nil || replayedPayload.Instance.InstanceID != "order-2" {
		t.Fatalf("unexpected replayed payload: %+v", replayedPayload)
	}
}

func TestPrometheusSDReturnsServiceTargetsAndOptionalSelfTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:            1,
		ClusterID:         402,
		DataDir:           filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:          httpAddr,
		AdminAddr:         adminAddr,
		AdminToken:        testAdminToken,
		ReplicationToken:  "test-replication-token",
		PrometheusSDToken: "test-prometheus-sd-token",
		GRPCAddr:          grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	if err := postJSON("http://"+httpAddr+"/api/v1/registry/register", httptransport.RegisterRequestDTO{
		Namespace:  "prod",
		Service:    "order-service",
		InstanceID: "order-1",
		Zone:       "az1",
		Labels: map[string]string{
			"color": "gray",
		},
		Endpoints: []httptransport.EndpointDTO{
			{
				Name:     "http",
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     8080,
			},
			{
				Name:     "metrics",
				Protocol: "http",
				Host:     "127.0.0.1",
				Port:     9090,
				Path:     "/metrics",
			},
		},
	}, ""); err != nil {
		t.Fatalf("register instance failed: %v", err)
	}

	targets := waitForPrometheusSDTargets(t, "http://"+httpAddr+"/internal/v1/prometheus/sd?namespace=prod&service=order-service", "test-prometheus-sd-token", 5*time.Second, func(items []httptransport.PrometheusSDTargetGroupDTO) bool {
		return len(items) == 1
	})
	if len(targets[0].Targets) != 1 || targets[0].Targets[0] != "127.0.0.1:9090" {
		t.Fatalf("unexpected prometheus sd targets: %+v", targets[0].Targets)
	}
	if targets[0].Labels["namespace"] != "prod" || targets[0].Labels["service"] != "order-service" {
		t.Fatalf("unexpected prometheus sd labels: %+v", targets[0].Labels)
	}
	if targets[0].Labels["target_kind"] != "service_instance" {
		t.Fatalf("unexpected target kind: %+v", targets[0].Labels)
	}
	if targets[0].Labels["stellmap_label_color"] != "gray" {
		t.Fatalf("expected transformed service label, got %+v", targets[0].Labels)
	}

	withSelf := waitForPrometheusSDTargets(t, "http://"+httpAddr+"/internal/v1/prometheus/sd?namespace=prod&service=order-service&includeSelf=true", "test-prometheus-sd-token", 5*time.Second, func(items []httptransport.PrometheusSDTargetGroupDTO) bool {
		return len(items) == 2
	})
	foundSelf := false
	foundService := false
	for _, item := range withSelf {
		if item.Labels["target_kind"] == "stellmapd" {
			foundSelf = true
			if len(item.Targets) != 1 || item.Targets[0] != httpAddr {
				t.Fatalf("unexpected stellmapd self target: %+v", item)
			}
		}
		if item.Labels["target_kind"] == "service_instance" {
			foundService = true
		}
	}
	if !foundSelf || !foundService {
		t.Fatalf("expected both self and service targets, got %+v", withSelf)
	}
}

func TestPrometheusSDSelfTargetPrefersConfiguredPeerHTTPAddress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	advertiseHTTPAddr := reserveTCPAddress(t)
	listenHTTPAddr := wildcardListenAddress(t, advertiseHTTPAddr)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:            1,
		ClusterID:         404,
		DataDir:           filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:          listenHTTPAddr,
		AdminAddr:         adminAddr,
		AdminToken:        testAdminToken,
		ReplicationToken:  "test-replication-token",
		PrometheusSDToken: "test-prometheus-sd-token",
		GRPCAddr:          grpcAddr,
		PeerHTTPAddrs:     "1=" + advertiseHTTPAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	withSelf := waitForPrometheusSDTargets(t, "http://"+advertiseHTTPAddr+"/internal/v1/prometheus/sd?includeSelf=true", "test-prometheus-sd-token", 5*time.Second, func(items []httptransport.PrometheusSDTargetGroupDTO) bool {
		return len(items) == 1
	})
	if len(withSelf) != 1 {
		t.Fatalf("expected one self target, got %+v", withSelf)
	}
	if len(withSelf[0].Targets) != 1 || withSelf[0].Targets[0] != advertiseHTTPAddr {
		t.Fatalf("unexpected self target: %+v", withSelf[0])
	}
}

func TestPrometheusSDRequiresToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:            1,
		ClusterID:         403,
		DataDir:           filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:          httpAddr,
		AdminAddr:         adminAddr,
		AdminToken:        testAdminToken,
		PrometheusSDToken: "test-prometheus-sd-token",
		GRPCAddr:          grpcAddr,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	statusCode, body, err := rawGet("http://"+httpAddr+"/internal/v1/prometheus/sd", "")
	if err != nil {
		t.Fatalf("call prometheus sd without token failed: %v", err)
	}
	if statusCode != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusUnauthorized, statusCode, body)
	}
}

func TestCleanupExpiredRegistryRespectsDeleteLimitAndAdvancesScanCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpAddr := reserveTCPAddress(t)
	adminAddr := reserveTCPAddress(t)
	grpcAddr := reserveTCPAddress(t)

	app := mustNewTestApp(t, daemonConfig{
		NodeID:                     1,
		ClusterID:                  305,
		DataDir:                    filepath.Join(t.TempDir(), "node-1"),
		HTTPAddr:                   httpAddr,
		AdminAddr:                  adminAddr,
		AdminToken:                 testAdminToken,
		GRPCAddr:                   grpcAddr,
		RegistryCleanupInterval:    time.Hour,
		RegistryCleanupDeleteLimit: 1,
	})
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start single node app: %v", err)
	}
	defer func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("stop app: %v", err)
		}
	}()

	waitForClusterLeader(t, map[uint64]*serverApp{1: app}, 5*time.Second)

	now := time.Now().Unix()
	alive := registry.Value{
		Namespace:         "prod",
		Service:           "payment-service",
		InstanceID:        "a-live",
		Endpoints:         nil,
		LeaseTTLSeconds:   30,
		RegisteredAtUnix:  now,
		LastHeartbeatUnix: now,
	}
	expiredFirst := registry.Value{
		Namespace:         "prod",
		Service:           "payment-service",
		InstanceID:        "b-expired",
		Endpoints:         nil,
		LeaseTTLSeconds:   1,
		RegisteredAtUnix:  now - 10,
		LastHeartbeatUnix: now - 10,
	}
	expiredSecond := registry.Value{
		Namespace:         "prod",
		Service:           "payment-service",
		InstanceID:        "c-expired",
		Endpoints:         nil,
		LeaseTTLSeconds:   1,
		RegisteredAtUnix:  now - 10,
		LastHeartbeatUnix: now - 10,
	}

	mustPutTestRegistryValue(t, app, alive)
	mustPutTestRegistryValue(t, app, expiredFirst)
	mustPutTestRegistryValue(t, app, expiredSecond)

	runCleanupRound(t, app)
	mustReadRegistryValue(t, app, registry.Key(alive.Namespace, alive.Service, alive.InstanceID))
	mustReadRegistryValue(t, app, registry.Key(expiredFirst.Namespace, expiredFirst.Service, expiredFirst.InstanceID))
	mustReadRegistryValue(t, app, registry.Key(expiredSecond.Namespace, expiredSecond.Service, expiredSecond.InstanceID))

	runCleanupRound(t, app)
	waitForRegistryValueDeleted(t, app, registry.Key(expiredFirst.Namespace, expiredFirst.Service, expiredFirst.InstanceID), 5*time.Second, "first expired registry cleanup")
	mustReadRegistryValue(t, app, registry.Key(expiredSecond.Namespace, expiredSecond.Service, expiredSecond.InstanceID))

	runCleanupRound(t, app)
	waitForRegistryValueDeleted(t, app, registry.Key(expiredSecond.Namespace, expiredSecond.Service, expiredSecond.InstanceID), 5*time.Second, "second expired registry cleanup")
	mustReadRegistryValue(t, app, registry.Key(alive.Namespace, alive.Service, alive.InstanceID))
}

func mustNewTestApp(t *testing.T, cfg daemonConfig) *serverApp {
	t.Helper()

	if cfg.Region == "" {
		cfg.Region = "default"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(t.TempDir(), "node-data")
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = reserveTCPAddress(t)
	}
	if cfg.AdminAddr == "" {
		cfg.AdminAddr = reserveTCPAddress(t)
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = reserveTCPAddress(t)
	}
	if cfg.AdminToken == "" {
		cfg.AdminToken = testAdminToken
	}
	if cfg.ReplicationToken == "" {
		cfg.ReplicationToken = cfg.AdminToken
	}
	if cfg.PrometheusSDToken == "" {
		cfg.PrometheusSDToken = cfg.ReplicationToken
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.RegistryCleanupInterval == 0 {
		cfg.RegistryCleanupInterval = time.Second
	}
	if cfg.RegistryCleanupDeleteLimit == 0 {
		cfg.RegistryCleanupDeleteLimit = 128
	}

	app, err := newServerApp(cfg)
	if err != nil {
		t.Fatalf("new server app failed: %v", err)
	}

	return app
}

func mustReadRegistryValue(t *testing.T, app *serverApp, key []byte) registry.Value {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := app.Node().Get(ctx, key)
	if err != nil {
		t.Fatalf("read registry value failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("registry value for key=%s is empty", string(key))
	}

	var value registry.Value
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("unmarshal registry value failed: %v", err)
	}

	return value
}

func mustPutTestRegistryValue(t *testing.T, app *serverApp, value registry.Value) {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal registry value failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := registry.Key(value.Namespace, value.Service, value.InstanceID)
	if err := app.Node().ProposeCommand(ctx, storage.Command{
		Operation: storage.OperationPut,
		Key:       key,
		Value:     data,
	}); err != nil {
		t.Fatalf("write registry value failed: %v", err)
	}

	waitForRegistryValue(t, app, key, func(current registry.Value) bool {
		return current.InstanceID == value.InstanceID
	}, 5*time.Second, "write registry value")
}

func runCleanupRound(t *testing.T, app *serverApp) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := app.CleanupExpiredRegistry(ctx); err != nil {
		t.Fatalf("cleanup expired registry failed: %v", err)
	}
}

func waitForRegistryValue(t *testing.T, app *serverApp, key []byte, predicate func(registry.Value) bool, timeout time.Duration, scene string) registry.Value {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last registry.Value
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		data, err := app.Node().Get(ctx, key)
		cancel()
		if err == nil && len(data) > 0 {
			if err := json.Unmarshal(data, &last); err != nil {
				t.Fatalf("unmarshal registry value failed: %v", err)
			}
		}
		if len(data) > 0 && predicate(last) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("%s did not observe expected registry value within %s, last=%+v", scene, timeout, last)
	return registry.Value{}
}

func waitForRegistryValueDeleted(t *testing.T, app *serverApp, key []byte, timeout time.Duration, scene string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		data, err := app.Node().Get(ctx, key)
		cancel()
		if err == nil && len(data) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("%s did not delete registry key=%s within %s", scene, string(key), timeout)
}

func stopApps(t *testing.T, apps map[uint64]*serverApp) {
	t.Helper()

	for nodeID, app := range apps {
		if app == nil {
			continue
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := app.Stop(stopCtx); err != nil {
			cancel()
			t.Fatalf("stop node %d failed: %v", nodeID, err)
		}
		cancel()
	}
}

func waitForClusterLeader(t *testing.T, apps map[uint64]*serverApp, timeout time.Duration) uint64 {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaderID uint64
		allReady := true

		for _, app := range apps {
			status := app.Node().Status()
			if status.LeaderID == 0 {
				allReady = false
				break
			}
			if leaderID == 0 {
				leaderID = status.LeaderID
			}
			if leaderID != status.LeaderID {
				allReady = false
				break
			}
		}

		if allReady && leaderID != 0 {
			leaderApp, ok := apps[leaderID]
			if ok {
				if leaderApp.Node().Status().Role == raftnode.RoleLeader {
					return leaderID
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("cluster did not elect a stable leader within %s", timeout)
	return 0
}

func waitForMemberAddress(t *testing.T, baseURL, token string, nodeID uint64, httpAddr, grpcAddr, adminAddr string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := getClusterStatus(baseURL, token)
		if err == nil {
			if status.HTTPAddrs[nodeID] == httpAddr && status.GRPCAddrs[nodeID] == grpcAddr && status.AdminAddrs[nodeID] == adminAddr {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("member address nodeId=%d http=%s grpc=%s admin=%s not observed within %s", nodeID, httpAddr, grpcAddr, adminAddr, timeout)
}

func getClusterStatus(baseURL, token string) (httptransport.ClusterStatusDTO, error) {
	request, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+"/admin/v1/status", nil)
	if err != nil {
		return httptransport.ClusterStatusDTO{}, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return httptransport.ClusterStatusDTO{}, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return httptransport.ClusterStatusDTO{}, err
	}
	if response.StatusCode != http.StatusOK {
		return httptransport.ClusterStatusDTO{}, fmt.Errorf("status=%d body=%s", response.StatusCode, string(body))
	}

	var envelope testSuccessEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return httptransport.ClusterStatusDTO{}, err
	}
	var status httptransport.ClusterStatusDTO
	if err := json.Unmarshal(envelope.Data, &status); err != nil {
		return httptransport.ClusterStatusDTO{}, err
	}

	return status, nil
}

func getReplicationStatus(baseURL, token string) ([]httptransport.ReplicationStatusDTO, error) {
	request, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+"/admin/v1/replication/status", nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", response.StatusCode, string(body))
	}

	var envelope testSuccessEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	var items []httptransport.ReplicationStatusDTO
	if err := json.Unmarshal(envelope.Data, &items); err != nil {
		return nil, err
	}

	return items, nil
}

func getRegistryInstances(baseURL string, values url.Values) ([]httptransport.RegistryInstanceDTO, error) {
	target := strings.TrimRight(baseURL, "/") + "/api/v1/registry/instances"
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}

	response, err := http.Get(target)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", response.StatusCode, string(body))
	}

	var envelope testSuccessEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	var instances []httptransport.RegistryInstanceDTO
	if err := json.Unmarshal(envelope.Data, &instances); err != nil {
		return nil, err
	}

	return instances, nil
}

func getPrometheusSDTargets(target, token string) ([]httptransport.PrometheusSDTargetGroupDTO, error) {
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", response.StatusCode, string(body))
	}

	var items []httptransport.PrometheusSDTargetGroupDTO
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func waitForReplicationStatus(t *testing.T, baseURL, token string, timeout time.Duration, predicate func([]httptransport.ReplicationStatusDTO) bool) []httptransport.ReplicationStatusDTO {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last []httptransport.ReplicationStatusDTO
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = getReplicationStatus(baseURL, token)
		if lastErr == nil && predicate(last) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("get replication status failed within %s: %v", timeout, lastErr)
	}
	t.Fatalf("replication status did not match predicate within %s, last=%+v", timeout, last)
	return nil
}

func waitForMetricsBody(t *testing.T, target string, timeout time.Duration, predicate func(string) bool) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last string
	var lastErr error
	for time.Now().Before(deadline) {
		statusCode, body, err := rawGet(target, "")
		last = body
		lastErr = err
		if err == nil && statusCode == http.StatusOK && predicate(body) {
			return body
		}
		time.Sleep(50 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("get metrics failed within %s: %v", timeout, lastErr)
	}
	t.Fatalf("metrics body did not match predicate within %s, last=%s", timeout, last)
	return ""
}

func waitForPrometheusSDTargets(t *testing.T, target, token string, timeout time.Duration, predicate func([]httptransport.PrometheusSDTargetGroupDTO) bool) []httptransport.PrometheusSDTargetGroupDTO {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last []httptransport.PrometheusSDTargetGroupDTO
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = getPrometheusSDTargets(target, token)
		if lastErr == nil && predicate(last) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("get prometheus sd targets failed within %s: %v", timeout, lastErr)
	}
	t.Fatalf("prometheus sd targets did not match predicate within %s, last=%+v", timeout, last)
	return nil
}

func waitForRegistryInstances(t *testing.T, baseURL string, params url.Values, timeout time.Duration, predicate func([]httptransport.RegistryInstanceDTO) bool) []httptransport.RegistryInstanceDTO {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last []httptransport.RegistryInstanceDTO
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = getRegistryInstances(baseURL, params)
		if lastErr == nil && predicate(last) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("query registry instances failed within %s: %v", timeout, lastErr)
	}
	t.Fatalf("query registry instances did not match predicate within %s, last=%+v", timeout, last)
	return nil
}

func waitForRegistryInstanceIDs(t *testing.T, baseURL string, params url.Values, timeout time.Duration, scene string, expectedIDs ...string) {
	t.Helper()
	_ = scene

	waitForRegistryInstances(t, baseURL, params, timeout, func(items []httptransport.RegistryInstanceDTO) bool {
		if len(items) != len(expectedIDs) {
			return false
		}

		seen := make(map[string]struct{}, len(items))
		for _, item := range items {
			seen[item.InstanceID] = struct{}{}
		}
		for _, id := range expectedIDs {
			if _, ok := seen[id]; !ok {
				return false
			}
		}

		return true
	})
}

func postJSON(target string, body interface{}, token string) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		return fmt.Errorf("status=%d body=%s", response.StatusCode, string(raw))
	}

	return nil
}

func rawGet(target, token string) (int, string, error) {
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return 0, "", err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return response.StatusCode, "", err
	}

	return response.StatusCode, string(body), nil
}

type sseEvent struct {
	ID    string
	Event string
	Data  string
}

func mustReadSSEEvent(t *testing.T, reader *bufio.Reader, timeout time.Duration) sseEvent {
	t.Helper()

	type result struct {
		event sseEvent
		err   error
	}
	done := make(chan result, 1)
	go func() {
		event, err := readSSEEvent(reader)
		done <- result{event: event, err: err}
	}()

	select {
	case <-time.After(timeout):
		t.Fatalf("did not receive sse event within %s", timeout)
	case result := <-done:
		if result.err != nil {
			t.Fatalf("read sse event failed: %v", result.err)
		}
		return result.event
	}

	return sseEvent{}
}

func assertNoSSEEvent(t *testing.T, reader *bufio.Reader, timeout time.Duration) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		_, err := readSSEEvent(reader)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("unexpectedly received sse event within %s", timeout)
		}
	case <-time.After(timeout):
	}
}

func readSSEEvent(reader *bufio.Reader) (sseEvent, error) {
	var event sseEvent

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return sseEvent{}, err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event.Event != "" || event.Data != "" || event.ID != "" {
				return event, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id: "))
		case strings.HasPrefix(line, "event: "):
			event.Event = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
		case strings.HasPrefix(line, "data: "):
			event.Data = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		}
	}
}

func waitForAdminStatusCode(t *testing.T, target, token string, expectedStatus int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastBody string
	var lastErr error
	var lastStatus int

	for time.Now().Before(deadline) {
		statusCode, body, err := rawGet(target, token)
		lastBody = body
		lastErr = err
		lastStatus = statusCode
		if err == nil && statusCode == expectedStatus {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("admin endpoint %s did not return status=%d within %s, last error=%v", target, expectedStatus, timeout, lastErr)
	}
	t.Fatalf("admin endpoint %s did not return status=%d within %s, last status=%d body=%s", target, expectedStatus, timeout, lastStatus, lastBody)
}

func pickFollower(apps map[uint64]*serverApp, leaderID uint64) uint64 {
	for nodeID := range apps {
		if nodeID != leaderID {
			return nodeID
		}
	}

	return 0
}

func joinAddressMap(items map[uint64]string) string {
	parts := make([]string, 0, len(items))
	for nodeID := uint64(1); nodeID <= uint64(len(items)); nodeID++ {
		if addr, ok := items[nodeID]; ok {
			parts = append(parts, fmt.Sprintf("%d=%s", nodeID, addr))
		}
	}

	return strings.Join(parts, ",")
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp address failed: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func wildcardListenAddress(t *testing.T, addr string) string {
	t.Helper()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port failed: %v", err)
	}
	return net.JoinHostPort("0.0.0.0", port)
}
