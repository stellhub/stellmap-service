package httptransport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellhub/stellmap/internal/raftnode"
	"github.com/stellhub/stellmap/internal/registry"
	"github.com/stellhub/stellmap/internal/runtime"
	"github.com/stellhub/stellmap/internal/storage"
)

type fakeRegistryNode struct {
	mu                    sync.Mutex
	status                raftnode.Status
	proposeCalls          int
	getCalls              int
	scanCalls             int
	linearizableReadCalls int
	lastGetKey            []byte
	lastScanStart         []byte
	lastScanEnd           []byte
	lastScanLimit         int
	lastProposedCommand   *storage.Command
	proposeFunc           func(ctx context.Context, cmd storage.Command) error
	getFunc               func(ctx context.Context, key []byte) ([]byte, error)
	scanFunc              func(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error)
	linearizableReadFunc  func(ctx context.Context, reqCtx []byte) error
}

func (f *fakeRegistryNode) ProposeCommand(ctx context.Context, cmd storage.Command) error {
	f.mu.Lock()
	f.proposeCalls++
	cloned := storage.Command{
		Operation: cmd.Operation,
		Key:       append([]byte(nil), cmd.Key...),
		Value:     append([]byte(nil), cmd.Value...),
	}
	f.lastProposedCommand = &cloned
	fn := f.proposeFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, cmd)
	}
	return nil
}

func (f *fakeRegistryNode) LinearizableRead(ctx context.Context, reqCtx []byte) error {
	f.mu.Lock()
	f.linearizableReadCalls++
	fn := f.linearizableReadFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, reqCtx)
	}
	return nil
}

func (f *fakeRegistryNode) Get(ctx context.Context, key []byte) ([]byte, error) {
	f.mu.Lock()
	f.getCalls++
	f.lastGetKey = append([]byte(nil), key...)
	fn := f.getFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, key)
	}
	return nil, nil
}

func (f *fakeRegistryNode) Scan(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error) {
	f.mu.Lock()
	f.scanCalls++
	f.lastScanStart = append([]byte(nil), start...)
	f.lastScanEnd = append([]byte(nil), end...)
	f.lastScanLimit = limit
	fn := f.scanFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, start, end, limit)
	}
	return nil, nil
}

func (f *fakeRegistryNode) Status() raftnode.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func TestRegisterSupportsStructuredServiceIdentityInputs(t *testing.T) {
	t.Parallel()

	canonicalService := "company.trade.order.order-center.api"
	leaderNode := raftnode.Status{
		NodeID:   1,
		LeaderID: 1,
		Role:     raftnode.RoleLeader,
		Started:  true,
		Stopped:  false,
	}

	testCases := []struct {
		name                 string
		body                 string
		expectedStatus       int
		expectedErrorCode    string
		expectedService      string
		expectedOrganization string
		expectedRole         string
		expectPropose        bool
	}{
		{
			name: "only structured identity",
			body: `{
  "namespace":"prod",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1",
  "endpoints":[{"protocol":"http","host":"127.0.0.1","port":8080}]
}`,
			expectedStatus:       http.StatusOK,
			expectedService:      canonicalService,
			expectedOrganization: "company",
			expectedRole:         "api",
			expectPropose:        true,
		},
		{
			name: "only canonical service",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1",
  "endpoints":[{"protocol":"http","host":"127.0.0.1","port":8080}]
}`,
			expectedStatus:       http.StatusOK,
			expectedService:      canonicalService,
			expectedOrganization: "company",
			expectedRole:         "api",
			expectPropose:        true,
		},
		{
			name: "service conflicts with structured identity",
			body: `{
  "namespace":"prod",
  "service":"company.trade.payment.order-center.api",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1",
  "endpoints":[{"protocol":"http","host":"127.0.0.1","port":8080}]
}`,
			expectedStatus:    http.StatusBadRequest,
			expectedErrorCode: "bad_request",
			expectPropose:     false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			node := &fakeRegistryNode{status: leaderNode}
			handler := &RegistryAPI{node: node, requestTimout: time.Second}

			response := performJSONHandlerRequest(t, http.MethodPost, "/api/v1/registry/register", testCase.body, handler.Register)
			if response.Code != testCase.expectedStatus {
				t.Fatalf("expected status %d, got %d", testCase.expectedStatus, response.Code)
			}

			if testCase.expectPropose {
				assertSuccessResponse(t, response.Body)
				node.mu.Lock()
				lastProposed := node.lastProposedCommand
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if proposeCalls != 1 || lastProposed == nil {
					t.Fatalf("expected one proposal, got proposeCalls=%d lastProposed=%v", proposeCalls, lastProposed)
				}
				if lastProposed.Operation != storage.OperationPut {
					t.Fatalf("expected put proposal, got %+v", lastProposed)
				}
				if string(lastProposed.Key) != string(registry.Key("prod", canonicalService, "order-center-api-1")) {
					t.Fatalf("expected canonical register key, got %s", string(lastProposed.Key))
				}

				var value registry.Value
				if err := json.Unmarshal(lastProposed.Value, &value); err != nil {
					t.Fatalf("expected valid proposed registry value: %v", err)
				}
				if value.Service != testCase.expectedService || value.Organization != testCase.expectedOrganization || value.Role != testCase.expectedRole {
					t.Fatalf("expected normalized structured identity, got %+v", value)
				}
			} else {
				payload := assertErrorResponse(t, response.Body)
				if payload.Code != testCase.expectedErrorCode {
					t.Fatalf("expected error code %s, got %s", testCase.expectedErrorCode, payload.Code)
				}
				node.mu.Lock()
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if proposeCalls != 0 {
					t.Fatalf("expected no proposal on invalid request, got %d", proposeCalls)
				}
			}
		})
	}
}

func TestDeregisterSupportsStructuredServiceIdentityInputs(t *testing.T) {
	t.Parallel()

	canonicalService := "company.trade.order.order-center.api"
	leaderNode := raftnode.Status{
		NodeID:   1,
		LeaderID: 1,
		Role:     raftnode.RoleLeader,
		Started:  true,
	}

	testCases := []struct {
		name              string
		body              string
		expectedStatus    int
		expectedErrorCode string
		expectPropose     bool
	}{
		{
			name: "only structured identity",
			body: `{
  "namespace":"prod",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1"
}`,
			expectedStatus: http.StatusOK,
			expectPropose:  true,
		},
		{
			name: "only canonical service",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1"
}`,
			expectedStatus: http.StatusOK,
			expectPropose:  true,
		},
		{
			name: "service conflicts with structured identity",
			body: `{
  "namespace":"prod",
  "service":"company.trade.payment.order-center.api",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1"
}`,
			expectedStatus:    http.StatusBadRequest,
			expectedErrorCode: "bad_request",
			expectPropose:     false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			node := &fakeRegistryNode{status: leaderNode}
			handler := &RegistryAPI{node: node, requestTimout: time.Second}

			response := performJSONHandlerRequest(t, http.MethodPost, "/api/v1/registry/deregister", testCase.body, handler.Deregister)
			if response.Code != testCase.expectedStatus {
				t.Fatalf("expected status %d, got %d", testCase.expectedStatus, response.Code)
			}

			if testCase.expectPropose {
				assertSuccessResponse(t, response.Body)
				node.mu.Lock()
				lastProposed := node.lastProposedCommand
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if proposeCalls != 1 || lastProposed == nil {
					t.Fatalf("expected one proposal, got proposeCalls=%d lastProposed=%v", proposeCalls, lastProposed)
				}
				if lastProposed.Operation != storage.OperationDelete {
					t.Fatalf("expected delete proposal, got %+v", lastProposed)
				}
				if string(lastProposed.Key) != string(registry.Key("prod", canonicalService, "order-center-api-1")) {
					t.Fatalf("expected canonical deregister key, got %s", string(lastProposed.Key))
				}
			} else {
				payload := assertErrorResponse(t, response.Body)
				if payload.Code != testCase.expectedErrorCode {
					t.Fatalf("expected error code %s, got %s", testCase.expectedErrorCode, payload.Code)
				}
				node.mu.Lock()
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if proposeCalls != 0 {
					t.Fatalf("expected no proposal on invalid request, got %d", proposeCalls)
				}
			}
		})
	}
}

func TestHeartbeatSupportsStructuredServiceIdentityInputs(t *testing.T) {
	t.Parallel()

	canonicalService := "company.trade.order.order-center.api"
	existingValue := registry.NewValue(registry.RegisterInput{
		Namespace:        "prod",
		Service:          canonicalService,
		Organization:     "company",
		BusinessDomain:   "trade",
		CapabilityDomain: "order",
		Application:      "order-center",
		Role:             "api",
		InstanceID:       "order-center-api-1",
		LeaseTTLSeconds:  30,
		Endpoints:        []registry.Endpoint{{Protocol: "http", Host: "127.0.0.1", Port: 8080, Name: "http", Weight: 100}},
	}, time.Now().Add(-time.Minute).Unix())
	existingPayload, err := json.Marshal(existingValue)
	if err != nil {
		t.Fatalf("expected existing registry value marshal success: %v", err)
	}

	leaderNode := raftnode.Status{
		NodeID:   1,
		LeaderID: 1,
		Role:     raftnode.RoleLeader,
		Started:  true,
	}

	testCases := []struct {
		name                 string
		body                 string
		expectedStatus       int
		expectedErrorCode    string
		expectRead           bool
		expectPropose        bool
		expectedOrganization string
		expectedRole         string
	}{
		{
			name: "only structured identity",
			body: `{
  "namespace":"prod",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1",
  "leaseTtlSeconds":45
}`,
			expectedStatus:       http.StatusOK,
			expectRead:           true,
			expectPropose:        true,
			expectedOrganization: "company",
			expectedRole:         "api",
		},
		{
			name: "only canonical service",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1",
  "leaseTtlSeconds":45
}`,
			expectedStatus:       http.StatusOK,
			expectRead:           true,
			expectPropose:        true,
			expectedOrganization: "company",
			expectedRole:         "api",
		},
		{
			name: "service conflicts with structured identity",
			body: `{
  "namespace":"prod",
  "service":"company.trade.payment.order-center.api",
  "organization":"company",
  "businessDomain":"trade",
  "capabilityDomain":"order",
  "application":"order-center",
  "role":"api",
  "instanceId":"order-center-api-1",
  "leaseTtlSeconds":45
}`,
			expectedStatus:    http.StatusBadRequest,
			expectedErrorCode: "bad_request",
			expectRead:        false,
			expectPropose:     false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			node := &fakeRegistryNode{
				status: leaderNode,
				getFunc: func(ctx context.Context, key []byte) ([]byte, error) {
					return append([]byte(nil), existingPayload...), nil
				},
			}
			handler := &RegistryAPI{node: node, requestTimout: time.Second}

			response := performJSONHandlerRequest(t, http.MethodPost, "/api/v1/registry/heartbeat", testCase.body, handler.Heartbeat)
			if response.Code != testCase.expectedStatus {
				t.Fatalf("expected status %d, got %d", testCase.expectedStatus, response.Code)
			}

			if testCase.expectPropose {
				assertSuccessResponse(t, response.Body)
				node.mu.Lock()
				lastGetKey := append([]byte(nil), node.lastGetKey...)
				lastProposed := node.lastProposedCommand
				getCalls := node.getCalls
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if getCalls != 1 || proposeCalls != 1 || lastProposed == nil {
					t.Fatalf("expected one read and one proposal, got getCalls=%d proposeCalls=%d lastProposed=%v", getCalls, proposeCalls, lastProposed)
				}
				expectedKey := string(registry.Key("prod", canonicalService, "order-center-api-1"))
				if string(lastGetKey) != expectedKey || string(lastProposed.Key) != expectedKey {
					t.Fatalf("expected canonical heartbeat key %s, got get=%s propose=%s", expectedKey, string(lastGetKey), string(lastProposed.Key))
				}

				var value registry.Value
				if err := json.Unmarshal(lastProposed.Value, &value); err != nil {
					t.Fatalf("expected valid heartbeat registry value: %v", err)
				}
				if value.Organization != testCase.expectedOrganization || value.Role != testCase.expectedRole || value.LeaseTTLSeconds != 45 {
					t.Fatalf("expected normalized heartbeat value, got %+v", value)
				}
			} else {
				payload := assertErrorResponse(t, response.Body)
				if payload.Code != testCase.expectedErrorCode {
					t.Fatalf("expected error code %s, got %s", testCase.expectedErrorCode, payload.Code)
				}
				node.mu.Lock()
				getCalls := node.getCalls
				proposeCalls := node.proposeCalls
				node.mu.Unlock()
				if getCalls != 0 || proposeCalls != 0 {
					t.Fatalf("expected invalid request to skip read/propose, got getCalls=%d proposeCalls=%d", getCalls, proposeCalls)
				}
			}
		})
	}
}

func TestWriteHandlersReturnNotLeaderWithLeaderInfo(t *testing.T) {
	t.Parallel()

	book := runtime.NewAddressBook(
		map[uint64]string{2: "10.0.0.2:8080"},
		nil,
		nil,
	)
	followerStatus := raftnode.Status{
		NodeID:   1,
		LeaderID: 2,
		Role:     raftnode.RoleFollower,
		Started:  true,
	}

	testCases := []struct {
		name    string
		target  string
		body    string
		handler func(*RegistryAPI) func(http.ResponseWriter, *http.Request)
	}{
		{
			name:   "register",
			target: "/api/v1/registry/register",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1",
  "endpoints":[{"protocol":"http","host":"127.0.0.1","port":8080}]
}`,
			handler: func(api *RegistryAPI) func(http.ResponseWriter, *http.Request) {
				return api.Register
			},
		},
		{
			name:   "deregister",
			target: "/api/v1/registry/deregister",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1"
}`,
			handler: func(api *RegistryAPI) func(http.ResponseWriter, *http.Request) {
				return api.Deregister
			},
		},
		{
			name:   "heartbeat",
			target: "/api/v1/registry/heartbeat",
			body: `{
  "namespace":"prod",
  "service":"company.trade.order.order-center.api",
  "instanceId":"order-center-api-1",
  "leaseTtlSeconds":30
}`,
			handler: func(api *RegistryAPI) func(http.ResponseWriter, *http.Request) {
				return api.Heartbeat
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			node := &fakeRegistryNode{
				status: followerStatus,
				getFunc: func(ctx context.Context, key []byte) ([]byte, error) {
					t.Fatalf("expected not_leader request to skip storage read, got key=%s", string(key))
					return nil, nil
				},
				proposeFunc: func(ctx context.Context, cmd storage.Command) error {
					t.Fatalf("expected not_leader request to skip proposal, got cmd=%+v", cmd)
					return nil
				},
			}
			handler := &RegistryAPI{
				node:          node,
				httpAddr:      "10.0.0.1:8080",
				book:          book,
				requestTimout: time.Second,
			}

			response := performJSONHandlerRequest(t, http.MethodPost, testCase.target, testCase.body, testCase.handler(handler))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected status 503, got %d", response.Code)
			}

			payload := assertErrorResponse(t, response.Body)
			if payload.Code != "not_leader" {
				t.Fatalf("expected error code not_leader, got %s", payload.Code)
			}
			if payload.LeaderID != 2 {
				t.Fatalf("expected leaderId 2, got %d", payload.LeaderID)
			}
			if payload.LeaderAddr != "10.0.0.2:8080" {
				t.Fatalf("expected leaderAddr 10.0.0.2:8080, got %s", payload.LeaderAddr)
			}
			if !strings.Contains(payload.Message, "leaderId=2") || !strings.Contains(payload.Message, "leaderAddr=10.0.0.2:8080") {
				t.Fatalf("expected not_leader message to include leader info, got %s", payload.Message)
			}

			node.mu.Lock()
			getCalls := node.getCalls
			proposeCalls := node.proposeCalls
			node.mu.Unlock()
			if getCalls != 0 || proposeCalls != 0 {
				t.Fatalf("expected not_leader request to skip read/propose, got getCalls=%d proposeCalls=%d", getCalls, proposeCalls)
			}
		})
	}
}

func TestQueryInstancesBuildsExactServiceFromStructuredIdentity(t *testing.T) {
	t.Parallel()

	exactService := "company.trade.order.order-center.api"
	now := time.Now().Unix()
	node := &fakeRegistryNode{
		scanFunc: func(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error) {
			return []storage.KV{
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          exactService,
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "api",
					InstanceID:       "order-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.worker",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "worker",
					InstanceID:       "order-center-worker-1",
					LeaseTTLSeconds:  30,
				}, now)),
			}, nil
		},
	}
	handler := &RegistryAPI{node: node, requestTimout: time.Second}

	server := httptest.NewServer(http.HandlerFunc(handler.QueryInstances))
	defer server.Close()

	response, err := server.Client().Get(
		server.URL + "?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order&application=order-center&role=api",
	)
	if err != nil {
		t.Fatalf("expected query request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	var payload SuccessResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("expected valid query payload: %v", err)
	}

	items, ok := payload.Data.([]interface{})
	if !ok {
		t.Fatalf("expected array payload, got %T", payload.Data)
	}
	if len(items) != 1 {
		t.Fatalf("expected one exact service instance, got %d", len(items))
	}

	raw, err := json.Marshal(items[0])
	if err != nil {
		t.Fatalf("expected marshal item success: %v", err)
	}
	var instance RegistryInstanceDTO
	if err := json.Unmarshal(raw, &instance); err != nil {
		t.Fatalf("expected valid instance payload: %v", err)
	}
	if instance.Service != exactService || instance.Role != "api" {
		t.Fatalf("expected exact service instance, got %+v", instance)
	}

	node.mu.Lock()
	lastScanStart := string(node.lastScanStart)
	lastScanLimit := node.lastScanLimit
	node.mu.Unlock()
	if lastScanStart != registry.ServicePrefix("prod", exactService) {
		t.Fatalf("expected exact scan prefix %s, got %s", registry.ServicePrefix("prod", exactService), lastScanStart)
	}
	if lastScanLimit != 0 {
		t.Fatalf("expected unlimited scan, got limit %d", lastScanLimit)
	}
}

func TestQueryInstancesBuildsServicePrefixFromStructuredHierarchy(t *testing.T) {
	t.Parallel()

	now := time.Now().Unix()
	node := &fakeRegistryNode{
		scanFunc: func(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error) {
			return []storage.KV{
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.api",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "api",
					InstanceID:       "order-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.worker",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "worker",
					InstanceID:       "order-center-worker-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.payment.payment-center.api",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "payment",
					Application:      "payment-center",
					Role:             "api",
					InstanceID:       "payment-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
			}, nil
		},
	}
	handler := &RegistryAPI{node: node, requestTimout: time.Second}

	server := httptest.NewServer(http.HandlerFunc(handler.QueryInstances))
	defer server.Close()

	response, err := server.Client().Get(
		server.URL + "?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order",
	)
	if err != nil {
		t.Fatalf("expected query request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	var payload SuccessResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("expected valid query payload: %v", err)
	}

	items, ok := payload.Data.([]interface{})
	if !ok {
		t.Fatalf("expected array payload, got %T", payload.Data)
	}
	if len(items) != 2 {
		t.Fatalf("expected two prefix-matched instances, got %d", len(items))
	}

	services := make([]string, 0, len(items))
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			t.Fatalf("expected marshal item success: %v", err)
		}
		var instance RegistryInstanceDTO
		if err := json.Unmarshal(raw, &instance); err != nil {
			t.Fatalf("expected valid instance payload: %v", err)
		}
		services = append(services, instance.Service)
	}
	for _, service := range services {
		if !strings.HasPrefix(service, "company.trade.order.") {
			t.Fatalf("expected prefix-matched service, got %s", service)
		}
	}

	node.mu.Lock()
	lastScanStart := string(node.lastScanStart)
	node.mu.Unlock()
	if lastScanStart != registry.NamespacePrefix("prod") {
		t.Fatalf("expected namespace scan prefix %s, got %s", registry.NamespacePrefix("prod"), lastScanStart)
	}
}

func TestWatchInstancesBuildsExactServiceSnapshotFromStructuredIdentity(t *testing.T) {
	t.Parallel()

	exactService := "company.trade.order.order-center.api"
	now := time.Now().Unix()
	node := &fakeRegistryNode{
		status: raftnode.Status{
			NodeID:       1,
			LeaderID:     1,
			Role:         raftnode.RoleLeader,
			Started:      true,
			AppliedIndex: 12,
			CommitIndex:  12,
		},
		scanFunc: func(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error) {
			return []storage.KV{
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          exactService,
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "api",
					InstanceID:       "order-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.worker",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "worker",
					InstanceID:       "order-center-worker-1",
					LeaseTTLSeconds:  30,
				}, now)),
			}, nil
		},
	}
	handler := &RegistryAPI{
		node:          node,
		httpAddr:      "127.0.0.1:8080",
		watchHub:      registry.NewWatchHub(),
		requestTimout: time.Second,
	}

	server := httptest.NewServer(http.HandlerFunc(handler.WatchInstances))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order&application=order-center&role=api",
		nil,
	)
	if err != nil {
		t.Fatalf("expected request creation success: %v", err)
	}

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("expected watch request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	eventID, eventName, data := readSSEEvent(t, response.Body)
	if eventID != "12" || eventName != "snapshot" {
		t.Fatalf("expected snapshot event 12, got id=%s event=%s", eventID, eventName)
	}

	var payload RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("expected valid watch payload: %v", err)
	}
	if payload.Service != exactService {
		t.Fatalf("expected exact snapshot service %s, got %s", exactService, payload.Service)
	}
	if len(payload.Instances) != 1 || payload.Instances[0].Role != "api" {
		t.Fatalf("expected exact snapshot instances, got %+v", payload.Instances)
	}

	node.mu.Lock()
	lastScanStart := string(node.lastScanStart)
	node.mu.Unlock()
	if lastScanStart != registry.ServicePrefix("prod", exactService) {
		t.Fatalf("expected exact scan prefix %s, got %s", registry.ServicePrefix("prod", exactService), lastScanStart)
	}
}

func TestWatchInstancesBuildsServicePrefixSnapshotFromStructuredHierarchy(t *testing.T) {
	t.Parallel()

	now := time.Now().Unix()
	node := &fakeRegistryNode{
		status: raftnode.Status{
			NodeID:       1,
			LeaderID:     1,
			Role:         raftnode.RoleLeader,
			Started:      true,
			AppliedIndex: 21,
			CommitIndex:  21,
		},
		scanFunc: func(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error) {
			return []storage.KV{
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.api",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "api",
					InstanceID:       "order-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.order.order-center.worker",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "order",
					Application:      "order-center",
					Role:             "worker",
					InstanceID:       "order-center-worker-1",
					LeaseTTLSeconds:  30,
				}, now)),
				mustRegistryKV(t, registry.NewValue(registry.RegisterInput{
					Namespace:        "prod",
					Service:          "company.trade.payment.payment-center.api",
					Organization:     "company",
					BusinessDomain:   "trade",
					CapabilityDomain: "payment",
					Application:      "payment-center",
					Role:             "api",
					InstanceID:       "payment-center-api-1",
					LeaseTTLSeconds:  30,
				}, now)),
			}, nil
		},
	}
	handler := &RegistryAPI{
		node:          node,
		httpAddr:      "127.0.0.1:8080",
		watchHub:      registry.NewWatchHub(),
		requestTimout: time.Second,
	}

	server := httptest.NewServer(http.HandlerFunc(handler.WatchInstances))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"?namespace=prod&organization=company&businessDomain=trade&capabilityDomain=order",
		nil,
	)
	if err != nil {
		t.Fatalf("expected request creation success: %v", err)
	}

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("expected watch request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	eventID, eventName, data := readSSEEvent(t, response.Body)
	if eventID != "21" || eventName != "snapshot" {
		t.Fatalf("expected snapshot event 21, got id=%s event=%s", eventID, eventName)
	}

	var payload RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("expected valid watch payload: %v", err)
	}
	if payload.Service != "" {
		t.Fatalf("expected aggregated prefix snapshot service to be empty, got %s", payload.Service)
	}
	if len(payload.Instances) != 2 {
		t.Fatalf("expected two prefix-matched instances, got %+v", payload.Instances)
	}
	for _, instance := range payload.Instances {
		if !strings.HasPrefix(instance.Service, "company.trade.order.") {
			t.Fatalf("expected prefix-matched service, got %+v", instance)
		}
	}

	node.mu.Lock()
	lastScanStart := string(node.lastScanStart)
	node.mu.Unlock()
	if lastScanStart != registry.NamespacePrefix("prod") {
		t.Fatalf("expected namespace scan prefix %s, got %s", registry.NamespacePrefix("prod"), lastScanStart)
	}
}

func TestWatchInstancesReplaysRetainedEventsSinceRevision(t *testing.T) {
	t.Parallel()

	service := "company.trade.order.order-center.api"
	now := time.Now().Unix()
	hub := registry.NewWatchHub()
	hub.Publish(registry.WatchEvent{
		Revision:   10,
		Type:       registry.WatchEventUpsert,
		Namespace:  "prod",
		Service:    service,
		InstanceID: "order-center-api-1",
	})
	hub.Publish(registry.WatchEvent{
		Revision:   11,
		Type:       registry.WatchEventUpsert,
		Namespace:  "prod",
		Service:    service,
		InstanceID: "order-center-api-2",
		Value: &registry.Value{
			Namespace:         "prod",
			Service:           service,
			Organization:      "company",
			BusinessDomain:    "trade",
			CapabilityDomain:  "order",
			Application:       "order-center",
			Role:              "api",
			InstanceID:        "order-center-api-2",
			LeaseTTLSeconds:   30,
			RegisteredAtUnix:  now,
			LastHeartbeatUnix: now,
		},
	})

	node := &fakeRegistryNode{
		status: raftnode.Status{
			NodeID:       1,
			LeaderID:     1,
			Role:         raftnode.RoleLeader,
			Started:      true,
			AppliedIndex: 11,
			CommitIndex:  11,
		},
	}
	handler := &RegistryAPI{
		node:          node,
		httpAddr:      "127.0.0.1:8080",
		watchHub:      hub,
		requestTimout: time.Second,
	}

	server := httptest.NewServer(http.HandlerFunc(handler.WatchInstances))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"?namespace=prod&service="+url.QueryEscape(service)+"&sinceRevision=10",
		nil,
	)
	if err != nil {
		t.Fatalf("expected request creation success: %v", err)
	}

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("expected watch request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	eventID, eventName, data := readSSEEvent(t, response.Body)
	if eventID != "11" {
		t.Fatalf("expected replay event id 11, got %s", eventID)
	}
	if eventName != "upsert" {
		t.Fatalf("expected replay event type upsert, got %s", eventName)
	}

	var payload RegistryWatchEventDTO
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("expected valid watch payload: %v", err)
	}
	if payload.Revision != 11 {
		t.Fatalf("expected payload revision 11, got %d", payload.Revision)
	}
	if payload.Service != service {
		t.Fatalf("expected payload service %s, got %s", service, payload.Service)
	}
	if payload.Organization != "company" || payload.BusinessDomain != "trade" || payload.CapabilityDomain != "order" || payload.Application != "order-center" || payload.Role != "api" {
		t.Fatalf("expected structured service identity in payload, got %+v", payload)
	}
	if payload.Instance == nil || payload.Instance.InstanceID != "order-center-api-2" {
		t.Fatalf("expected replayed instance payload, got %+v", payload.Instance)
	}

	node.mu.Lock()
	linearizableReadCalls := node.linearizableReadCalls
	scanCalls := node.scanCalls
	node.mu.Unlock()
	if linearizableReadCalls != 0 || scanCalls != 0 {
		t.Fatalf("expected retained replay path to avoid snapshot reads, got linearizableReadCalls=%d scanCalls=%d", linearizableReadCalls, scanCalls)
	}
}

func TestWatchInstancesReturnsRevisionExpiredWhenHistoryIsInsufficientWithoutSnapshot(t *testing.T) {
	t.Parallel()

	service := "company.trade.order.order-center.api"
	hub := registry.NewWatchHub()
	for revision := uint64(1); revision <= 2050; revision++ {
		hub.Publish(registry.WatchEvent{
			Revision:   revision,
			Type:       registry.WatchEventUpsert,
			Namespace:  "prod",
			Service:    service,
			InstanceID: fmt.Sprintf("order-center-api-%d", revision),
		})
	}

	node := &fakeRegistryNode{
		status: raftnode.Status{
			NodeID:       1,
			LeaderID:     1,
			Role:         raftnode.RoleLeader,
			Started:      true,
			AppliedIndex: 2050,
			CommitIndex:  2050,
		},
	}
	handler := &RegistryAPI{
		node:          node,
		httpAddr:      "127.0.0.1:8080",
		watchHub:      hub,
		requestTimout: time.Second,
	}

	server := httptest.NewServer(http.HandlerFunc(handler.WatchInstances))
	defer server.Close()

	response, err := server.Client().Get(
		server.URL + "?namespace=prod&service=" + url.QueryEscape(service) + "&sinceRevision=1&includeSnapshot=false",
	)
	if err != nil {
		t.Fatalf("expected watch request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusGone {
		t.Fatalf("expected status 410, got %d", response.StatusCode)
	}

	var payload ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("expected valid error payload: %v", err)
	}
	if payload.Code != "revision_expired" {
		t.Fatalf("expected revision_expired, got %s", payload.Code)
	}
	if !strings.Contains(payload.Message, "no longer retained") {
		t.Fatalf("expected retention hint in error message, got %s", payload.Message)
	}

	node.mu.Lock()
	linearizableReadCalls := node.linearizableReadCalls
	scanCalls := node.scanCalls
	node.mu.Unlock()
	if linearizableReadCalls != 0 || scanCalls != 0 {
		t.Fatalf("expected expired watch without snapshot to avoid snapshot reads, got linearizableReadCalls=%d scanCalls=%d", linearizableReadCalls, scanCalls)
	}
}

func TestWatchInstancesSendsPingWhenSubscribedTargetIsIdle(t *testing.T) {
	service := "company.trade.order.order-center.api"
	handler := &RegistryAPI{
		node:              &fakeRegistryNode{},
		watchHub:          registry.NewWatchHub(),
		requestTimout:     time.Second,
		watchPingInterval: 10 * time.Millisecond,
	}

	server := httptest.NewServer(http.HandlerFunc(handler.WatchInstances))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"?namespace=prod&service="+url.QueryEscape(service)+"&includeSnapshot=false",
		nil,
	)
	if err != nil {
		t.Fatalf("expected request creation success: %v", err)
	}

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("expected watch request success: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.StatusCode)
	}

	reader := bufio.NewReader(response.Body)
	if comment := readSSEComment(t, reader); comment != "ping" {
		t.Fatalf("expected initial ping comment, got %q", comment)
	}
	if comment := readSSEComment(t, reader); comment != "ping" {
		t.Fatalf("expected periodic ping comment, got %q", comment)
	}
}

func readSSEEvent(t *testing.T, body io.ReadCloser) (string, string, string) {
	t.Helper()

	reader := bufio.NewReader(body)
	var (
		eventID   string
		eventName string
		dataLines []string
	)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("expected SSE event line: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "id: ") {
			eventID = strings.TrimSpace(strings.TrimPrefix(line, "id: "))
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		}
	}

	return eventID, eventName, strings.Join(dataLines, "\n")
}

func readSSEComment(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	comment := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("expected SSE comment line: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return comment
		}
		if strings.HasPrefix(line, ":") {
			comment = strings.TrimSpace(strings.TrimPrefix(line, ":"))
			continue
		}
		t.Fatalf("expected SSE comment line, got %q", line)
	}
}

func mustRegistryKV(t *testing.T, value registry.Value) storage.KV {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("expected registry value marshal success: %v", err)
	}

	return storage.KV{
		Key:   registry.Key(value.Namespace, value.Service, value.InstanceID),
		Value: encoded,
	}
}

func performJSONHandlerRequest(t *testing.T, method, target, body string, handler func(http.ResponseWriter, *http.Request)) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler(response, request)
	return response
}

func assertSuccessResponse(t *testing.T, body *bytes.Buffer) SuccessResponse {
	t.Helper()

	var payload SuccessResponse
	if err := json.NewDecoder(bytes.NewReader(body.Bytes())).Decode(&payload); err != nil {
		t.Fatalf("expected valid success response: %v", err)
	}
	if payload.Code != "ok" {
		t.Fatalf("expected success code ok, got %s", payload.Code)
	}
	return payload
}

func assertErrorResponse(t *testing.T, body *bytes.Buffer) ErrorResponse {
	t.Helper()

	var payload ErrorResponse
	if err := json.NewDecoder(bytes.NewReader(body.Bytes())).Decode(&payload); err != nil {
		t.Fatalf("expected valid error response: %v", err)
	}
	return payload
}
