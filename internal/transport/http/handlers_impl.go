package httptransport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	internalmetrics "github.com/stellhub/stellmap/internal/metrics"
	"github.com/stellhub/stellmap/internal/raftnode"
	"github.com/stellhub/stellmap/internal/registry"
	"github.com/stellhub/stellmap/internal/replication"
	"github.com/stellhub/stellmap/internal/runtime"
	"github.com/stellhub/stellmap/internal/storage"
)

const defaultWatchPingInterval = 15 * time.Second

// RegistryAPI 实现对外 HTTP 数据面。
type RegistryAPI struct {
	node              registryNode
	httpAddr          string
	book              *runtime.AddressBook
	watchHub          *registry.WatchHub
	sourceRegion      string
	sourceCluster     string
	replicateAuth     string
	promSDAuth        string
	requestTimout     time.Duration
	registryMetrics   *internalmetrics.RegistryMetrics
	watchPingInterval time.Duration
}

type registryNode interface {
	ProposeCommand(ctx context.Context, cmd storage.Command) error
	LinearizableRead(ctx context.Context, reqCtx []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Scan(ctx context.Context, start, end []byte, limit int) ([]storage.KV, error)
	Status() raftnode.Status
}

// HealthAPI 实现对外健康检查。
type HealthAPI struct {
	node               *raftnode.RaftNode
	httpAddr           string
	book               *runtime.AddressBook
	replicationTracker *replication.Tracker
	metricsHandler     http.Handler
}

// ControlAPI 实现控制面 HTTP 接口。
type ControlAPI struct {
	node               *raftnode.RaftNode
	clusterID          uint64
	adminAddr          string
	book               *runtime.AddressBook
	peerTransport      *runtime.PeerTransport
	replicationTracker *replication.Tracker
	requestTimout      time.Duration
}

// NewRegistryHandler 创建注册中心 HTTP 数据面 handler。
func NewRegistryHandler(node *raftnode.RaftNode, httpAddr string, book *runtime.AddressBook, watchHub *registry.WatchHub, sourceRegion, sourceCluster, replicationToken, prometheusSDToken string, requestTimeout time.Duration) *RegistryAPI {
	return &RegistryAPI{
		node:          node,
		httpAddr:      httpAddr,
		book:          book,
		watchHub:      watchHub,
		sourceRegion:  sourceRegion,
		sourceCluster: sourceCluster,
		replicateAuth: replicationToken,
		promSDAuth:    prometheusSDToken,
		requestTimout: requestTimeout,
	}
}

// WithRegistryMetrics 为注册中心 HTTP handler 增加客户端画像和 watch 治理指标。
func (h *RegistryAPI) WithRegistryMetrics(metrics *internalmetrics.RegistryMetrics) *RegistryAPI {
	if h == nil {
		return nil
	}
	h.registryMetrics = metrics
	return h
}

// NewHealthHandler 创建健康检查 handler。
func NewHealthHandler(node *raftnode.RaftNode, httpAddr string, book *runtime.AddressBook, replicationTracker *replication.Tracker, metricsHandler http.Handler) *HealthAPI {
	return &HealthAPI{
		node:               node,
		httpAddr:           httpAddr,
		book:               book,
		replicationTracker: replicationTracker,
		metricsHandler:     metricsHandler,
	}
}

// NewControlHandler 创建控制面 handler。
func NewControlHandler(node *raftnode.RaftNode, clusterID uint64, adminAddr string, book *runtime.AddressBook, peerTransport *runtime.PeerTransport, replicationTracker *replication.Tracker, requestTimeout time.Duration) *ControlAPI {
	return &ControlAPI{
		node:               node,
		clusterID:          clusterID,
		adminAddr:          adminAddr,
		book:               book,
		peerTransport:      peerTransport,
		replicationTracker: replicationTracker,
		requestTimout:      requestTimeout,
	}
}

// Register 处理实例注册。
func (h *RegistryAPI) Register(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}

	var request RegisterRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	input := registryRegisterInputFromDTO(request)
	if err := registry.NormalizeRegisterInput(&input); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if input.Namespace == "" || input.Service == "" || input.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "namespace, service and instanceId are required")
		return
	}
	statusCode := http.StatusOK
	defer func() {
		h.observeRegisterRequestMetrics(registryIdentityFromRegisterInput(input), statusCode)
	}()
	if !h.ensureWritable(w) {
		statusCode = http.StatusServiceUnavailable
		return
	}

	now := time.Now().Unix()
	value, err := json.Marshal(registry.NewValue(input, now))
	if err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "marshal_failed", err.Error())
		return
	}

	if err := h.propose(r.Context(), storage.Command{
		Operation: storage.OperationPut,
		Key:       registry.Key(input.Namespace, input.Service, input.InstanceID),
		Value:     value,
	}); err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "propose_failed", err.Error())
		return
	}
	log.Printf(
		"registry register accepted namespace=%s service=%s instance_id=%s remote=%s",
		input.Namespace,
		input.Service,
		input.InstanceID,
		r.RemoteAddr,
	)

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code:    "ok",
		Message: "instance registered",
	})
}

// Deregister 处理实例注销。
func (h *RegistryAPI) Deregister(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}

	var request DeregisterRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.InstanceID = strings.TrimSpace(request.InstanceID)
	if err := registry.NormalizeStructuredServiceIdentity(
		&request.Service,
		&request.Organization,
		&request.BusinessDomain,
		&request.CapabilityDomain,
		&request.Application,
		&request.Role,
	); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.Namespace == "" || request.Service == "" || request.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "namespace, service and instanceId are required")
		return
	}
	statusCode := http.StatusOK
	defer func() {
		h.observeDeregisterRequestMetrics(registryIdentityFromDeregisterRequest(request), statusCode)
	}()
	if !h.ensureWritable(w) {
		statusCode = http.StatusServiceUnavailable
		return
	}

	if err := h.propose(r.Context(), storage.Command{
		Operation: storage.OperationDelete,
		Key:       registry.Key(request.Namespace, request.Service, request.InstanceID),
	}); err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "propose_failed", err.Error())
		return
	}
	log.Printf(
		"registry deregister accepted namespace=%s service=%s instance_id=%s remote=%s",
		request.Namespace,
		request.Service,
		request.InstanceID,
		r.RemoteAddr,
	)

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code:    "ok",
		Message: "instance deregistered",
	})
}

// Heartbeat 处理实例续约。
func (h *RegistryAPI) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}

	var request HeartbeatRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.InstanceID = strings.TrimSpace(request.InstanceID)
	if err := registry.NormalizeStructuredServiceIdentity(
		&request.Service,
		&request.Organization,
		&request.BusinessDomain,
		&request.CapabilityDomain,
		&request.Application,
		&request.Role,
	); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.Namespace == "" || request.Service == "" || request.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "namespace, service and instanceId are required")
		return
	}
	if request.LeaseTTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "leaseTtlSeconds must be greater than or equal to 0")
		return
	}
	statusCode := http.StatusOK
	defer func() {
		h.observeHeartbeatRequestMetrics(registryIdentityFromHeartbeatRequest(request), statusCode)
	}()
	if !h.ensureWritable(w) {
		statusCode = http.StatusServiceUnavailable
		return
	}

	key := registry.Key(request.Namespace, request.Service, request.InstanceID)
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()

	current, err := h.node.Get(ctx, key)
	if err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "read_failed", err.Error())
		return
	}
	if len(current) == 0 {
		statusCode = http.StatusNotFound
		writeError(w, http.StatusNotFound, "not_found", "instance not found")
		return
	}

	var existing registry.Value
	if err := json.Unmarshal(current, &existing); err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "unmarshal_failed", err.Error())
		return
	}
	if request.LeaseTTLSeconds > 0 {
		existing.LeaseTTLSeconds = request.LeaseTTLSeconds
	} else if existing.LeaseTTLSeconds <= 0 {
		existing.LeaseTTLSeconds = registry.EffectiveLeaseTTLSeconds(existing.LeaseTTLSeconds)
	}
	existing.LastHeartbeatUnix = time.Now().Unix()

	value, err := json.Marshal(existing)
	if err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "marshal_failed", err.Error())
		return
	}
	if err := h.propose(r.Context(), storage.Command{
		Operation: storage.OperationPut,
		Key:       key,
		Value:     value,
	}); err != nil {
		statusCode = http.StatusInternalServerError
		writeError(w, http.StatusInternalServerError, "propose_failed", err.Error())
		return
	}
	log.Printf(
		"registry heartbeat accepted namespace=%s service=%s instance_id=%s lease_ttl_seconds=%d remote=%s",
		request.Namespace,
		request.Service,
		request.InstanceID,
		existing.LeaseTTLSeconds,
		r.RemoteAddr,
	)

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code:    "ok",
		Message: "heartbeat accepted",
	})
}

// QueryInstances 按条件过滤实例候选集。
func (h *RegistryAPI) QueryInstances(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}

	query, err := registry.ParseQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	scanPrefix := registry.NamespacePrefix(query.Namespace)
	if query.Service != "" && len(query.Services) <= 1 && len(query.ServicePrefixes) == 0 {
		scanPrefix = registry.ServicePrefix(query.Namespace, query.Service)
	}
	start, end := prefixRange(scanPrefix)
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()

	items, err := h.node.Scan(ctx, start, end, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "scan_failed", err.Error())
		return
	}

	now := time.Now().Unix()
	result := make([]RegistryInstanceDTO, 0, len(items))
	for _, item := range items {
		var value registry.Value
		if err := json.Unmarshal(item.Value, &value); err != nil {
			continue
		}
		instance, ok := registryInstanceDTOFromValue(value, query, now)
		if !ok {
			continue
		}
		result = append(result, instance)
		if query.Limit > 0 && len(result) >= query.Limit {
			break
		}
	}

	if len(result) == 0 {
		result, err = h.queryReplicatedInstances(ctx, query, now)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "scan_failed", err.Error())
			return
		}
	}

	writeJSON(w, http.StatusOK, SuccessResponse{Code: "ok", Data: result})
}

// WatchInstances 以 SSE 方式持续推送实例变化事件。
func (h *RegistryAPI) WatchInstances(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if h.watchHub == nil {
		writeError(w, http.StatusServiceUnavailable, "watch_unavailable", "registry watch hub is not configured")
		return
	}

	query, err := registry.ParseQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	caller, err := parseRegistryCallerIdentity(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	sinceRevision, err := parseSinceRevision(r.URL.Query().Get("sinceRevision"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	includeSnapshot := true
	if raw := strings.TrimSpace(r.URL.Query().Get("includeSnapshot")); raw != "" {
		includeSnapshot, err = strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid includeSnapshot %q", raw))
			return
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_not_supported", "response writer does not support streaming")
		return
	}

	_, events, replay, exact, unsubscribe := h.watchHub.SubscribeSince(128, sinceRevision)
	defer unsubscribe()
	closeWatchMetrics := h.trackWatchSessionMetrics("instances", caller, registryWatchTargetFromQuery(query))
	defer closeWatchMetrics()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	currentRevision := sinceRevision
	emittedInitialFrame := false
	if sinceRevision > 0 && !exact && !includeSnapshot {
		writeError(w, http.StatusGone, "revision_expired", "watch revision is no longer retained")
		return
	}
	if sinceRevision == 0 || !exact {
		if includeSnapshot {
			snapshotItems, snapshotRevision, err := h.registryWatchSnapshot(r.Context(), query)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "snapshot_failed", err.Error())
				return
			}
			if err := writeSSEEvent(w, flusher, snapshotRevision, "snapshot", RegistryWatchEventDTO{
				Revision:  snapshotRevision,
				Type:      "snapshot",
				Namespace: query.Namespace,
				Service:   query.Service,
				Instances: snapshotItems,
			}); err != nil {
				return
			}
			currentRevision = snapshotRevision
			emittedInitialFrame = true
		}
	} else {
		for _, event := range replay {
			payload, emit := registryWatchEventDTO(query, event)
			if !emit {
				continue
			}
			if err := writeSSEEvent(w, flusher, event.Revision, payload.Type, payload); err != nil {
				return
			}
			currentRevision = event.Revision
			emittedInitialFrame = true
		}
	}
	if !emittedInitialFrame {
		if err := writeSSEPing(w, flusher); err != nil {
			return
		}
	}

	pingTicker := time.NewTicker(h.effectiveWatchPingInterval())
	defer pingTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-pingTicker.C:
			if err := writeSSEPing(w, flusher); err != nil {
				return
			}
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Revision <= currentRevision {
				continue
			}

			payload, emit := registryWatchEventDTO(query, event)
			if !emit {
				continue
			}
			if err := writeSSEEvent(w, flusher, event.Revision, payload.Type, payload); err != nil {
				return
			}
			currentRevision = event.Revision
		}
	}
}

// WatchReplication 以内部同步专用 SSE 通道持续推送原生目录变化。
func (h *RegistryAPI) WatchReplication(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if h.watchHub == nil {
		writeError(w, http.StatusServiceUnavailable, "watch_unavailable", "registry watch hub is not configured")
		return
	}
	if !h.isAuthorizedReplicationRequest(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid replication token")
		return
	}

	query, err := registry.ParseQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	sinceRevision, err := parseSinceRevision(r.URL.Query().Get("sinceRevision"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_not_supported", "response writer does not support streaming")
		return
	}

	_, events, replay, exact, unsubscribe := h.watchHub.SubscribeSince(128, sinceRevision)
	defer unsubscribe()
	closeWatchMetrics := h.trackWatchSessionMetrics("replication", internalmetrics.RegistryIdentity{}, registryWatchTargetFromQuery(query))
	defer closeWatchMetrics()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	currentRevision := sinceRevision
	emittedInitialFrame := false
	if sinceRevision == 0 || !exact {
		snapshotItems, snapshotRevision, err := h.registryWatchSnapshot(r.Context(), query)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "snapshot_failed", err.Error())
			return
		}

		if err := writeSSEEvent(w, flusher, snapshotRevision, "snapshot", ReplicationWatchEventDTO{
			Revision:        snapshotRevision,
			Type:            "snapshot",
			Namespace:       query.Namespace,
			Service:         query.Service,
			SourceRegion:    h.sourceRegion,
			SourceClusterID: h.sourceCluster,
			ExportedAtUnix:  time.Now().Unix(),
			Instances:       snapshotItems,
		}); err != nil {
			return
		}
		currentRevision = snapshotRevision
		emittedInitialFrame = true
	} else {
		for _, event := range replay {
			payload, emit := replicationWatchEventDTO(query, h.sourceRegion, h.sourceCluster, event)
			if !emit {
				continue
			}
			if err := writeSSEEvent(w, flusher, event.Revision, payload.Type, payload); err != nil {
				return
			}
			currentRevision = event.Revision
			emittedInitialFrame = true
		}
	}
	if !emittedInitialFrame {
		if err := writeSSEPing(w, flusher); err != nil {
			return
		}
	}

	pingTicker := time.NewTicker(h.effectiveWatchPingInterval())
	defer pingTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-pingTicker.C:
			if err := writeSSEPing(w, flusher); err != nil {
				return
			}
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Revision <= currentRevision {
				continue
			}

			payload, emit := replicationWatchEventDTO(query, h.sourceRegion, h.sourceCluster, event)
			if !emit {
				continue
			}
			if err := writeSSEEvent(w, flusher, event.Revision, payload.Type, payload); err != nil {
				return
			}
			currentRevision = event.Revision
		}
	}
}

// PrometheusSD 以 Prometheus HTTP SD 兼容格式返回当前可抓取的 target 列表。
func (h *RegistryAPI) PrometheusSD(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if !h.isAuthorizedPrometheusSDRequest(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid prometheus sd token")
		return
	}

	query, err := parsePrometheusSDQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()

	items, err := h.prometheusSDTargetGroups(ctx, query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "scan_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, items)
}

func registryRegisterInputFromDTO(request RegisterRequestDTO) registry.RegisterInput {
	return registry.RegisterInput{
		Namespace:        request.Namespace,
		Service:          request.Service,
		Organization:     request.Organization,
		BusinessDomain:   request.BusinessDomain,
		CapabilityDomain: request.CapabilityDomain,
		Application:      request.Application,
		Role:             request.Role,
		InstanceID:       request.InstanceID,
		Zone:             request.Zone,
		Labels:           request.Labels,
		Metadata:         request.Metadata,
		Endpoints:        registryEndpointsFromDTO(request.Endpoints),
		LeaseTTLSeconds:  request.LeaseTTLSeconds,
	}
}

func registryEndpointsFromDTO(items []EndpointDTO) []registry.Endpoint {
	if len(items) == 0 {
		return nil
	}

	result := make([]registry.Endpoint, 0, len(items))
	for _, item := range items {
		result = append(result, registry.Endpoint{
			Name:     item.Name,
			Protocol: item.Protocol,
			Host:     item.Host,
			Port:     item.Port,
			Path:     item.Path,
			Weight:   item.Weight,
		})
	}

	return result
}

func endpointDTOsFromRegistry(items []registry.Endpoint) []EndpointDTO {
	if len(items) == 0 {
		return nil
	}

	result := make([]EndpointDTO, 0, len(items))
	for _, item := range items {
		result = append(result, EndpointDTO{
			Name:     item.Name,
			Protocol: item.Protocol,
			Host:     item.Host,
			Port:     item.Port,
			Path:     item.Path,
			Weight:   item.Weight,
		})
	}

	return result
}

func registryInstanceDTOFromValue(value registry.Value, query registry.Query, now int64) (RegistryInstanceDTO, bool) {
	if !registry.IsAlive(value, now) || !registry.MatchQuery(value, query) {
		return RegistryInstanceDTO{}, false
	}

	endpoints := registry.CloneEndpoints(value.Endpoints)
	if query.Endpoint != "" {
		endpoints = registry.FilterEndpoints(value.Endpoints, query.Endpoint)
		if len(endpoints) == 0 {
			return RegistryInstanceDTO{}, false
		}
	}

	return RegistryInstanceDTO{
		Namespace:         value.Namespace,
		Service:           value.Service,
		Organization:      value.Organization,
		BusinessDomain:    value.BusinessDomain,
		CapabilityDomain:  value.CapabilityDomain,
		Application:       value.Application,
		Role:              value.Role,
		InstanceID:        value.InstanceID,
		Zone:              value.Zone,
		Labels:            registry.CloneStringMap(value.Labels),
		Metadata:          registry.CloneStringMap(value.Metadata),
		Endpoints:         endpointDTOsFromRegistry(endpoints),
		LeaseTTLSeconds:   registry.EffectiveLeaseTTLSeconds(value.LeaseTTLSeconds),
		RegisteredAtUnix:  value.RegisteredAtUnix,
		LastHeartbeatUnix: value.LastHeartbeatUnix,
	}, true
}

func registryWatchEventDTO(query registry.Query, event registry.WatchEvent) (RegistryWatchEventDTO, bool) {
	if event.Namespace != query.Namespace || !registry.MatchServiceQuery(event.Service, query) {
		return RegistryWatchEventDTO{}, false
	}
	organization, businessDomain, capabilityDomain, application, role, _ := registry.ParseServiceName(event.Service)

	switch event.Type {
	case registry.WatchEventUpsert:
		if event.Value != nil {
			if instance, ok := registryInstanceDTOFromValue(*event.Value, query, time.Now().Unix()); ok {
				return RegistryWatchEventDTO{
					Revision:         event.Revision,
					Type:             string(registry.WatchEventUpsert),
					Namespace:        event.Namespace,
					Service:          event.Service,
					Organization:     instance.Organization,
					BusinessDomain:   instance.BusinessDomain,
					CapabilityDomain: instance.CapabilityDomain,
					Application:      instance.Application,
					Role:             instance.Role,
					InstanceID:       event.InstanceID,
					Instance:         &instance,
				}, true
			}
		}
		return RegistryWatchEventDTO{
			Revision:         event.Revision,
			Type:             string(registry.WatchEventDelete),
			Namespace:        event.Namespace,
			Service:          event.Service,
			Organization:     organization,
			BusinessDomain:   businessDomain,
			CapabilityDomain: capabilityDomain,
			Application:      application,
			Role:             role,
			InstanceID:       event.InstanceID,
		}, true
	case registry.WatchEventDelete:
		return RegistryWatchEventDTO{
			Revision:         event.Revision,
			Type:             string(registry.WatchEventDelete),
			Namespace:        event.Namespace,
			Service:          event.Service,
			Organization:     organization,
			BusinessDomain:   businessDomain,
			CapabilityDomain: capabilityDomain,
			Application:      application,
			Role:             role,
			InstanceID:       event.InstanceID,
		}, true
	default:
		return RegistryWatchEventDTO{}, false
	}
}

func replicationWatchEventDTO(query registry.Query, sourceRegion, sourceCluster string, event registry.WatchEvent) (ReplicationWatchEventDTO, bool) {
	if event.Namespace != query.Namespace || !registry.MatchServiceQuery(event.Service, query) {
		return ReplicationWatchEventDTO{}, false
	}

	exportedAtUnix := time.Now().Unix()
	organization, businessDomain, capabilityDomain, application, role, _ := registry.ParseServiceName(event.Service)
	switch event.Type {
	case registry.WatchEventUpsert:
		if event.Value != nil {
			if instance, ok := registryInstanceDTOFromValue(*event.Value, query, time.Now().Unix()); ok {
				return ReplicationWatchEventDTO{
					Revision:         event.Revision,
					Type:             string(registry.WatchEventUpsert),
					Namespace:        event.Namespace,
					Service:          event.Service,
					Organization:     instance.Organization,
					BusinessDomain:   instance.BusinessDomain,
					CapabilityDomain: instance.CapabilityDomain,
					Application:      instance.Application,
					Role:             instance.Role,
					InstanceID:       event.InstanceID,
					SourceRegion:     sourceRegion,
					SourceClusterID:  sourceCluster,
					ExportedAtUnix:   exportedAtUnix,
					Instance:         &instance,
				}, true
			}
		}
		return ReplicationWatchEventDTO{
			Revision:         event.Revision,
			Type:             string(registry.WatchEventDelete),
			Namespace:        event.Namespace,
			Service:          event.Service,
			Organization:     organization,
			BusinessDomain:   businessDomain,
			CapabilityDomain: capabilityDomain,
			Application:      application,
			Role:             role,
			InstanceID:       event.InstanceID,
			SourceRegion:     sourceRegion,
			SourceClusterID:  sourceCluster,
			ExportedAtUnix:   exportedAtUnix,
		}, true
	case registry.WatchEventDelete:
		return ReplicationWatchEventDTO{
			Revision:         event.Revision,
			Type:             string(registry.WatchEventDelete),
			Namespace:        event.Namespace,
			Service:          event.Service,
			Organization:     organization,
			BusinessDomain:   businessDomain,
			CapabilityDomain: capabilityDomain,
			Application:      application,
			Role:             role,
			InstanceID:       event.InstanceID,
			SourceRegion:     sourceRegion,
			SourceClusterID:  sourceCluster,
			ExportedAtUnix:   exportedAtUnix,
		}, true
	default:
		return ReplicationWatchEventDTO{}, false
	}
}

func (h *RegistryAPI) registryWatchSnapshot(parent context.Context, query registry.Query) ([]RegistryInstanceDTO, uint64, error) {
	ctx, cancel := context.WithTimeout(parent, h.requestTimout)
	defer cancel()

	if err := h.node.LinearizableRead(ctx, []byte(fmt.Sprintf("registry-watch-%s-%s-%d", query.Namespace, query.Service, time.Now().UnixNano()))); err != nil {
		return nil, 0, err
	}
	snapshotRevision := h.node.Status().AppliedIndex

	scanPrefix := registry.NamespacePrefix(query.Namespace)
	if query.Service != "" && len(query.Services) <= 1 && len(query.ServicePrefixes) == 0 {
		scanPrefix = registry.ServicePrefix(query.Namespace, query.Service)
	}
	start, end := prefixRange(scanPrefix)
	items, err := h.node.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, 0, err
	}

	now := time.Now().Unix()
	result := make([]RegistryInstanceDTO, 0, len(items))
	for _, item := range items {
		var value registry.Value
		if err := json.Unmarshal(item.Value, &value); err != nil {
			continue
		}
		instance, ok := registryInstanceDTOFromValue(value, query, now)
		if !ok {
			continue
		}
		result = append(result, instance)
		if query.Limit > 0 && len(result) >= query.Limit {
			break
		}
	}

	return result, snapshotRevision, nil
}

func (h *RegistryAPI) queryReplicatedInstances(ctx context.Context, query registry.Query, now int64) ([]RegistryInstanceDTO, error) {
	items, err := h.node.Scan(ctx, []byte(registry.ReplicationRootPrefix), prefixUpperBound(registry.ReplicationRootPrefix), 0)
	if err != nil {
		return nil, err
	}

	result := make([]RegistryInstanceDTO, 0)
	for _, item := range items {
		_, _, namespace, service, _, ok := registry.ParseReplicatedKey(item.Key)
		if !ok || namespace != query.Namespace || !registry.MatchServiceQuery(service, query) {
			continue
		}

		var replicated registry.ReplicatedValue
		if err := json.Unmarshal(item.Value, &replicated); err != nil {
			continue
		}
		instance, ok := registryInstanceDTOFromValue(replicated.ToValue(), query, now)
		if !ok {
			continue
		}
		result = append(result, instance)
		if query.Limit > 0 && len(result) >= query.Limit {
			break
		}
	}

	return result, nil
}

// Healthz 返回进程存活状态。
func (h *HealthAPI) Healthz(w http.ResponseWriter, r *http.Request) {
	status := h.node.Status()
	writeJSON(w, http.StatusOK, SuccessResponse{
		Code: "ok",
		Data: HealthResponseDTO{
			Status:      "ok",
			LeaderID:    status.LeaderID,
			LocalNodeID: status.NodeID,
			LeaderAddr:  h.leaderHTTPAddr(status.LeaderID),
		},
	})
}

// Readyz 返回服务就绪状态。
func (h *HealthAPI) Readyz(w http.ResponseWriter, r *http.Request) {
	status := h.node.Status()
	if !status.Started || status.Stopped || (status.LeaderID == 0 && status.Role != raftnode.RoleLeader) {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "raft node is not ready")
		return
	}

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code: "ok",
		Data: HealthResponseDTO{
			Status:      "ready",
			LeaderID:    status.LeaderID,
			LocalNodeID: status.NodeID,
			LeaderAddr:  h.leaderHTTPAddr(status.LeaderID),
		},
	})
}

// Metrics 返回基础 Prometheus 文本指标。
func (h *HealthAPI) Metrics(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}

	if h.metricsHandler != nil {
		h.metricsHandler.ServeHTTP(w, r)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Status 返回当前节点视角下的集群状态。
func (h *ControlAPI) Status(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}

	status := h.node.Status()
	hardState := h.node.HardState()
	confState := h.node.ConfState()

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code: "ok",
		Data: ClusterStatusDTO{
			ClusterID:      h.clusterID,
			LocalNodeID:    status.NodeID,
			LeaderID:       status.LeaderID,
			Role:           string(status.Role),
			Term:           hardState.Term,
			Vote:           hardState.Vote,
			CommitIndex:    status.CommitIndex,
			AppliedIndex:   status.AppliedIndex,
			Started:        status.Started,
			Stopped:        status.Stopped,
			Voters:         append([]uint64(nil), confState.Voters...),
			VotersOutgoing: append([]uint64(nil), confState.VotersOutgoing...),
			Learners:       append([]uint64(nil), confState.Learners...),
			LearnersNext:   append([]uint64(nil), confState.LearnersNext...),
			AutoLeave:      confState.AutoLeave,
			HTTPAddrs:      h.book.SnapshotHTTP(),
			GRPCAddrs:      h.book.SnapshotGRPC(),
			AdminAddrs:     h.book.SnapshotAdmin(),
		},
	})
}

// ReplicationStatus 返回当前复制任务状态。
func (h *ControlAPI) ReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}

	items := make([]ReplicationStatusDTO, 0)
	if h.replicationTracker != nil {
		for _, item := range h.replicationTracker.List() {
			items = append(items, ReplicationStatusDTO{
				SourceRegion:         item.SourceRegion,
				SourceClusterID:      item.SourceClusterID,
				Namespace:            item.Namespace,
				Service:              item.Service,
				Connected:            item.Connected,
				LastAppliedRevision:  item.LastAppliedRevision,
				LastSnapshotRevision: item.LastSnapshotRevision,
				LastSyncUnix:         item.LastSyncUnix,
				ErrorCount:           item.ErrorCount,
				LastError:            item.LastError,
			})
		}
	}

	writeJSON(w, http.StatusOK, SuccessResponse{
		Code: "ok",
		Data: items,
	})
}

// AddLearner 触发一次 learner 加入。
func (h *ControlAPI) AddLearner(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if !h.ensureLeader(w) {
		return
	}

	var request MemberChangeRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.NodeID == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "nodeId is required")
		return
	}

	if err := h.peerTransport.UpsertPeer(request.NodeID, request.HTTPAddr, request.GRPCAddr, request.AdminAddr); err != nil {
		writeError(w, http.StatusInternalServerError, "peer_update_failed", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()
	if err := h.node.ApplyConfChange(ctx, raftnode.ConfChangeRequest{
		Type:    raftnode.ConfChangeAddLearner,
		NodeID:  request.NodeID,
		Context: mustMarshalJSON(request),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "conf_change_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, SuccessResponse{Code: "ok", Message: "learner add requested"})
}

// PromoteLearner 提升 learner 为 voter。
func (h *ControlAPI) PromoteLearner(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if !h.ensureLeader(w) {
		return
	}

	var request MemberChangeRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.NodeID == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "nodeId is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()
	if err := h.node.ApplyConfChange(ctx, raftnode.ConfChangeRequest{
		Type:    raftnode.ConfChangePromoteLearner,
		NodeID:  request.NodeID,
		Context: mustMarshalJSON(request),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "conf_change_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, SuccessResponse{Code: "ok", Message: "learner promote requested"})
}

// RemoveMember 移除集群成员。
func (h *ControlAPI) RemoveMember(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if !h.ensureLeader(w) {
		return
	}

	var request MemberChangeRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.NodeID == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "nodeId is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()
	if err := h.node.ApplyConfChange(ctx, raftnode.ConfChangeRequest{
		Type:    raftnode.ConfChangeRemoveNode,
		NodeID:  request.NodeID,
		Context: mustMarshalJSON(request),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "conf_change_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, SuccessResponse{Code: "ok", Message: "member remove requested"})
}

// TransferLeader 主动把 leader 转移到目标节点。
func (h *ControlAPI) TransferLeader(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if !h.ensureLeader(w) {
		return
	}

	var request LeaderTransferRequestDTO
	if err := decodeJSONBody(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if request.TargetNodeID == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "targetNodeId is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimout)
	defer cancel()
	if err := h.node.TransferLeadership(ctx, request.TargetNodeID); err != nil {
		writeError(w, http.StatusInternalServerError, "transfer_leader_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, SuccessResponse{Code: "ok", Message: "leader transfer requested"})
}

func (h *RegistryAPI) propose(parent context.Context, cmd storage.Command) error {
	ctx, cancel := context.WithTimeout(parent, h.requestTimout)
	defer cancel()
	return h.node.ProposeCommand(ctx, cmd)
}

func (h *RegistryAPI) ensureWritable(w http.ResponseWriter) bool {
	status := h.node.Status()
	if status.Role == raftnode.RoleLeader && status.LeaderID == status.NodeID {
		return true
	}

	writeNotLeader(w, status.LeaderID, h.leaderHTTPAddr(status.LeaderID))
	return false
}

func (h *RegistryAPI) leaderHTTPAddr(leaderID uint64) string {
	if leaderID == 0 {
		return ""
	}
	if leaderID == h.node.Status().NodeID {
		if h.book != nil {
			if addr := strings.TrimSpace(h.book.HTTPAddr(leaderID)); addr != "" {
				return addr
			}
		}
		return h.httpAddr
	}

	return h.book.HTTPAddr(leaderID)
}

func (h *RegistryAPI) isAuthorizedReplicationRequest(r *http.Request) bool {
	token := strings.TrimSpace(h.replicateAuth)
	if token == "" {
		return false
	}

	return strings.TrimSpace(r.Header.Get("Authorization")) == "Bearer "+token
}

func (h *RegistryAPI) isAuthorizedPrometheusSDRequest(r *http.Request) bool {
	token := strings.TrimSpace(h.promSDAuth)
	if token == "" {
		return false
	}

	return strings.TrimSpace(r.Header.Get("Authorization")) == "Bearer "+token
}

type prometheusSDQuery struct {
	Namespace   string
	Service     string
	Zone        string
	Endpoint    string
	Scope       string
	IncludeSelf bool
	Selector    registry.Selector
}

func parsePrometheusSDQuery(values map[string][]string) (prometheusSDQuery, error) {
	query := prometheusSDQuery{
		Namespace: strings.TrimSpace(firstQueryValue(values, "namespace")),
		Service:   strings.TrimSpace(firstQueryValue(values, "service")),
		Zone:      strings.TrimSpace(firstQueryValue(values, "zone")),
		Endpoint:  strings.TrimSpace(firstQueryValue(values, "endpoint")),
		Scope:     strings.TrimSpace(firstQueryValue(values, "scope")),
	}
	if query.Endpoint == "" {
		query.Endpoint = "metrics"
	}
	if query.Scope == "" {
		query.Scope = "local"
	}
	if query.Service != "" && query.Namespace == "" {
		return prometheusSDQuery{}, fmt.Errorf("namespace is required when service is specified")
	}
	switch query.Scope {
	case "local", "merged":
	default:
		return prometheusSDQuery{}, fmt.Errorf("scope must be one of local or merged")
	}

	includeSelfRaw := strings.TrimSpace(firstQueryValue(values, "includeSelf"))
	if includeSelfRaw != "" {
		includeSelf, err := strconv.ParseBool(includeSelfRaw)
		if err != nil {
			return prometheusSDQuery{}, fmt.Errorf("invalid includeSelf %q", includeSelfRaw)
		}
		query.IncludeSelf = includeSelf
	}

	selector, err := registry.ParseLabelSelectorFilters(values["selector"], values["label"])
	if err != nil {
		return prometheusSDQuery{}, err
	}
	query.Selector = selector

	return query, nil
}

func firstQueryValue(values map[string][]string, key string) string {
	items := values[key]
	if len(items) == 0 {
		return ""
	}
	return items[0]
}

func (h *RegistryAPI) prometheusSDTargetGroups(ctx context.Context, query prometheusSDQuery) ([]PrometheusSDTargetGroupDTO, error) {
	result := make([]PrometheusSDTargetGroupDTO, 0)

	localItems, err := h.localPrometheusSDTargetGroups(ctx, query)
	if err != nil {
		return nil, err
	}
	result = append(result, localItems...)

	if query.Scope == "merged" {
		replicatedItems, err := h.replicatedPrometheusSDTargetGroups(ctx, query)
		if err != nil {
			return nil, err
		}
		result = append(result, replicatedItems...)
	}

	if query.IncludeSelf {
		result = append(result, h.selfPrometheusSDTargetGroups()...)
	}

	return result, nil
}

func (h *RegistryAPI) localPrometheusSDTargetGroups(ctx context.Context, query prometheusSDQuery) ([]PrometheusSDTargetGroupDTO, error) {
	items, err := h.node.Scan(ctx, []byte(registry.RootPrefix), prefixUpperBound(registry.RootPrefix), 0)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	result := make([]PrometheusSDTargetGroupDTO, 0)
	for _, item := range items {
		var value registry.Value
		if err := json.Unmarshal(item.Value, &value); err != nil {
			continue
		}
		if !registry.IsAlive(value, now) {
			continue
		}
		if !matchPrometheusSDQuery(value, query) {
			continue
		}

		group, ok := prometheusSDTargetGroupFromValue(value, query.Endpoint, "local", h.sourceRegion, h.sourceCluster)
		if !ok {
			continue
		}
		result = append(result, group)
	}

	return result, nil
}

func (h *RegistryAPI) replicatedPrometheusSDTargetGroups(ctx context.Context, query prometheusSDQuery) ([]PrometheusSDTargetGroupDTO, error) {
	items, err := h.node.Scan(ctx, []byte(registry.ReplicationRootPrefix), prefixUpperBound(registry.ReplicationRootPrefix), 0)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	result := make([]PrometheusSDTargetGroupDTO, 0)
	for _, item := range items {
		var replicated registry.ReplicatedValue
		if err := json.Unmarshal(item.Value, &replicated); err != nil {
			continue
		}
		value := replicated.ToValue()
		if !registry.IsAlive(value, now) {
			continue
		}
		if !matchPrometheusSDQuery(value, query) {
			continue
		}

		group, ok := prometheusSDTargetGroupFromValue(value, query.Endpoint, "replicated", replicated.SourceRegion, replicated.SourceClusterID)
		if !ok {
			continue
		}
		result = append(result, group)
	}

	return result, nil
}

func matchPrometheusSDQuery(value registry.Value, query prometheusSDQuery) bool {
	return registry.MatchQuery(value, registry.Query{
		Namespace: query.Namespace,
		Service:   query.Service,
		Zone:      query.Zone,
		Selector:  query.Selector,
	})
}

func prometheusSDTargetGroupFromValue(value registry.Value, endpointName, origin, sourceRegion, sourceCluster string) (PrometheusSDTargetGroupDTO, bool) {
	endpoint, ok := findPrometheusEndpoint(value.Endpoints, endpointName)
	if !ok {
		return PrometheusSDTargetGroupDTO{}, false
	}

	address, ok := joinPrometheusTargetAddress(endpoint.Host, endpoint.Port)
	if !ok {
		return PrometheusSDTargetGroupDTO{}, false
	}

	labels := map[string]string{
		"namespace":        value.Namespace,
		"service":          value.Service,
		"instance_id":      value.InstanceID,
		"region":           sourceRegion,
		"zone":             value.Zone,
		"cluster_id":       sourceCluster,
		"target_origin":    origin,
		"target_kind":      "service_instance",
		"endpoint_name":    endpoint.Name,
		"endpoint_proto":   endpoint.Protocol,
		"__scheme__":       normalizePrometheusScheme(endpoint.Protocol),
		"__metrics_path__": normalizePrometheusPath(endpoint.Path),
	}
	addPrefixedPrometheusLabels(labels, "stellmap_label_", value.Labels)
	addPrefixedPrometheusLabels(labels, "stellmap_meta_", value.Metadata)
	deleteEmptyLabels(labels)

	return PrometheusSDTargetGroupDTO{
		Targets: []string{address},
		Labels:  labels,
	}, true
}

func findPrometheusEndpoint(endpoints []registry.Endpoint, expected string) (registry.Endpoint, bool) {
	for _, endpoint := range endpoints {
		if endpoint.Name == expected || endpoint.Protocol == expected {
			if scheme := normalizePrometheusScheme(endpoint.Protocol); scheme == "http" || scheme == "https" {
				return endpoint, true
			}
		}
	}
	return registry.Endpoint{}, false
}

func normalizePrometheusScheme(protocol string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "https":
		return "https"
	default:
		return "http"
	}
}

func normalizePrometheusPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/metrics"
	}
	if strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}

func joinPrometheusTargetAddress(host string, port int32) (string, bool) {
	host = strings.TrimSpace(host)
	if host == "" || port <= 0 {
		return "", false
	}
	if host == "0.0.0.0" || host == "::" || host == "[::]" {
		return "", false
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port))), true
}

func addPrefixedPrometheusLabels(target map[string]string, prefix string, items map[string]string) {
	for key, value := range items {
		sanitizedKey := sanitizePrometheusLabelName(prefix + key)
		if sanitizedKey == "" {
			continue
		}
		target[sanitizedKey] = strings.TrimSpace(value)
	}
}

func sanitizePrometheusLabelName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	builder := strings.Builder{}
	for index, r := range raw {
		allowed := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (r >= '0' && r <= '9' && index > 0)
		if allowed {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}

	sanitized := builder.String()
	if sanitized == "" {
		return ""
	}
	first := sanitized[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		sanitized = "_" + sanitized
	}
	return sanitized
}

func deleteEmptyLabels(labels map[string]string) {
	for key, value := range labels {
		if strings.TrimSpace(value) == "" {
			delete(labels, key)
		}
	}
}

func (h *RegistryAPI) selfPrometheusSDTargetGroups() []PrometheusSDTargetGroupDTO {
	httpAddrs := h.book.SnapshotHTTP()
	status := h.node.Status()
	if currentAddr := strings.TrimSpace(h.httpAddr); currentAddr != "" && strings.TrimSpace(httpAddrs[status.NodeID]) == "" {
		httpAddrs[status.NodeID] = currentAddr
	}

	result := make([]PrometheusSDTargetGroupDTO, 0, len(httpAddrs))
	for nodeID, addr := range httpAddrs {
		target, ok := normalizePrometheusDiscoveryAddress(addr)
		if !ok {
			continue
		}
		result = append(result, PrometheusSDTargetGroupDTO{
			Targets: []string{target},
			Labels: map[string]string{
				"namespace":        "system",
				"service":          "stellmapd",
				"instance_id":      fmt.Sprintf("node-%d", nodeID),
				"node_id":          strconv.FormatUint(nodeID, 10),
				"region":           h.sourceRegion,
				"cluster_id":       h.sourceCluster,
				"target_origin":    "self",
				"target_kind":      "stellmapd",
				"component":        "stellmapd",
				"__scheme__":       "http",
				"__metrics_path__": "/metrics",
			},
		})
	}

	return result
}

// 过滤没有意义的ip地址，这种地址本来Prometheus也抓不到
func normalizePrometheusDiscoveryAddress(addr string) (string, bool) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", false
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", false
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}

func (h *HealthAPI) leaderHTTPAddr(leaderID uint64) string {
	if leaderID == 0 {
		return ""
	}
	if leaderID == h.node.Status().NodeID {
		if h.book != nil {
			if addr := strings.TrimSpace(h.book.HTTPAddr(leaderID)); addr != "" {
				return addr
			}
		}
		return h.httpAddr
	}

	return h.book.HTTPAddr(leaderID)
}

func (h *ControlAPI) ensureLeader(w http.ResponseWriter) bool {
	status := h.node.Status()
	if status.Role == raftnode.RoleLeader && status.LeaderID == status.NodeID {
		return true
	}

	writeNotLeader(w, status.LeaderID, h.leaderAdminAddr(status.LeaderID))
	return false
}

func (h *ControlAPI) leaderAdminAddr(leaderID uint64) string {
	if leaderID == 0 {
		return ""
	}
	if leaderID == h.node.Status().NodeID {
		return h.adminAddr
	}

	return h.book.AdminAddr(leaderID)
}

func (h *RegistryAPI) observeRegisterRequestMetrics(identity internalmetrics.RegistryIdentity, statusCode int) {
	if h == nil || h.registryMetrics == nil {
		return
	}
	h.registryMetrics.ObserveRegister(identity, statusCode)
}

func (h *RegistryAPI) observeHeartbeatRequestMetrics(identity internalmetrics.RegistryIdentity, statusCode int) {
	if h == nil || h.registryMetrics == nil {
		return
	}
	h.registryMetrics.ObserveHeartbeat(identity, statusCode)
}

func (h *RegistryAPI) observeDeregisterRequestMetrics(identity internalmetrics.RegistryIdentity, statusCode int) {
	if h == nil || h.registryMetrics == nil {
		return
	}
	h.registryMetrics.ObserveDeregister(identity, statusCode)
}

func (h *RegistryAPI) trackWatchSessionMetrics(watchKind string, caller internalmetrics.RegistryIdentity, target internalmetrics.RegistryWatchTarget) func() {
	if h == nil || h.registryMetrics == nil {
		return func() {}
	}
	return h.registryMetrics.TrackWatchSession(watchKind, caller, target)
}

func (h *RegistryAPI) effectiveWatchPingInterval() time.Duration {
	if h != nil && h.watchPingInterval > 0 {
		return h.watchPingInterval
	}
	return defaultWatchPingInterval
}

func registryIdentityFromRegisterInput(input registry.RegisterInput) internalmetrics.RegistryIdentity {
	return internalmetrics.RegistryIdentity{
		Namespace:        input.Namespace,
		Service:          input.Service,
		Organization:     input.Organization,
		BusinessDomain:   input.BusinessDomain,
		CapabilityDomain: input.CapabilityDomain,
		Application:      input.Application,
		Role:             input.Role,
		Zone:             input.Zone,
	}
}

func registryIdentityFromDeregisterRequest(request DeregisterRequestDTO) internalmetrics.RegistryIdentity {
	return internalmetrics.RegistryIdentity{
		Namespace:        request.Namespace,
		Service:          request.Service,
		Organization:     request.Organization,
		BusinessDomain:   request.BusinessDomain,
		CapabilityDomain: request.CapabilityDomain,
		Application:      request.Application,
		Role:             request.Role,
	}
}

func registryIdentityFromHeartbeatRequest(request HeartbeatRequestDTO) internalmetrics.RegistryIdentity {
	return internalmetrics.RegistryIdentity{
		Namespace:        request.Namespace,
		Service:          request.Service,
		Organization:     request.Organization,
		BusinessDomain:   request.BusinessDomain,
		CapabilityDomain: request.CapabilityDomain,
		Application:      request.Application,
		Role:             request.Role,
	}
}

func parseRegistryCallerIdentity(r *http.Request) (internalmetrics.RegistryIdentity, error) {
	if r == nil {
		return internalmetrics.RegistryIdentity{}, nil
	}

	values := r.URL.Query()
	namespace := callerField(values, r.Header, "callerNamespace", "X-StellMap-Caller-Namespace", "X-Caller-Namespace")
	service := callerField(values, r.Header, "callerService", "X-StellMap-Caller-Service", "X-Caller-Service")
	organization := callerField(values, r.Header, "callerOrganization", "X-StellMap-Caller-Organization", "X-Caller-Organization")
	businessDomain := callerField(values, r.Header, "callerBusinessDomain", "X-StellMap-Caller-Business-Domain", "X-Caller-Business-Domain")
	capabilityDomain := callerField(values, r.Header, "callerCapabilityDomain", "X-StellMap-Caller-Capability-Domain", "X-Caller-Capability-Domain")
	application := callerField(values, r.Header, "callerApplication", "X-StellMap-Caller-Application", "X-Caller-Application")
	role := callerField(values, r.Header, "callerRole", "X-StellMap-Caller-Role", "X-Caller-Role")

	namespace = strings.TrimSpace(namespace)
	service = strings.TrimSpace(service)
	organization = strings.TrimSpace(organization)
	businessDomain = strings.TrimSpace(businessDomain)
	capabilityDomain = strings.TrimSpace(capabilityDomain)
	application = strings.TrimSpace(application)
	role = strings.TrimSpace(role)

	if namespace == "" && service == "" && organization == "" && businessDomain == "" && capabilityDomain == "" && application == "" && role == "" {
		return internalmetrics.RegistryIdentity{}, nil
	}

	if err := registry.NormalizeStructuredServiceIdentity(
		&service,
		&organization,
		&businessDomain,
		&capabilityDomain,
		&application,
		&role,
	); err != nil {
		return internalmetrics.RegistryIdentity{}, fmt.Errorf("invalid caller identity: %w", err)
	}

	return internalmetrics.RegistryIdentity{
		Namespace:        namespace,
		Service:          service,
		Organization:     organization,
		BusinessDomain:   businessDomain,
		CapabilityDomain: capabilityDomain,
		Application:      application,
		Role:             role,
	}, nil
}

func callerField(values map[string][]string, headers http.Header, queryKey string, headerKeys ...string) string {
	if value := strings.TrimSpace(firstQueryValue(values, queryKey)); value != "" {
		return value
	}
	for _, key := range headerKeys {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			return value
		}
	}
	return ""
}

func registryWatchTargetFromQuery(query registry.Query) internalmetrics.RegistryWatchTarget {
	target := internalmetrics.RegistryWatchTarget{
		Namespace: query.Namespace,
		Scope:     "namespace",
	}

	switch {
	case query.Service != "" && len(query.Services) <= 1 && len(query.ServicePrefixes) == 0:
		target.Scope = "service"
		target.Service = query.Service
	case len(query.Services) > 1:
		target.Scope = "service_set"
		target.Service = "multiple"
	case len(query.ServicePrefixes) == 1:
		target.Scope = "service_prefix"
		target.Service = query.ServicePrefixes[0]
	case len(query.ServicePrefixes) > 1:
		target.Scope = "service_prefix"
		target.Service = "multiple"
	}

	return target
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}

	w.Header().Set("Allow", method)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func decodeJSONBody(r *http.Request, target interface{}) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Code: code, Message: message})
}

func writeNotLeader(w http.ResponseWriter, leaderID uint64, leaderAddr string) {
	writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
		Code:       "not_leader",
		Message:    fmt.Sprintf("current node is not leader, leaderId=%d leaderAddr=%s", leaderID, leaderAddr),
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	})
}

func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, revision uint64, event string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\n", revision); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeSSEPing(w http.ResponseWriter, flusher http.Flusher) error {
	if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func prefixRange(prefix string) ([]byte, []byte) {
	if prefix == "" {
		return nil, nil
	}

	start := []byte(prefix)
	end := prefixUpperBound(prefix)
	return start, end
}

func prefixUpperBound(prefix string) []byte {
	if prefix == "" {
		return nil
	}

	return append(append([]byte(nil), []byte(prefix)...), 0xFF)
}

func parseSinceRevision(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sinceRevision %q", raw)
	}
	return value, nil
}

func mustMarshalJSON(value interface{}) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return data
}
