package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	daemonapp "github.com/stellhub/stellmap/internal/app"
	daemonlogging "github.com/stellhub/stellmap/internal/logging"
	internalmetrics "github.com/stellhub/stellmap/internal/metrics"
	"github.com/stellhub/stellmap/internal/raftnode"
	"github.com/stellhub/stellmap/internal/registry"
	"github.com/stellhub/stellmap/internal/replication"
	"github.com/stellhub/stellmap/internal/runtime"
	"github.com/stellhub/stellmap/internal/snapshot"
	grpctransport "github.com/stellhub/stellmap/internal/transport/grpc"
	httptransport "github.com/stellhub/stellmap/internal/transport/http"
	"google.golang.org/grpc"
)

type daemonConfig = daemonapp.Config
type serverApp = daemonapp.App

// main 是 stellmapd 的服务进程入口。
//
// 启动前提：
// 1. 推荐通过 `--config` 指向 TOML 配置文件提供完整启动配置。
// 2. 命令行参数会覆盖配置文件中的同名字段。
// 3. admin listener 当前只允许 `127.0.0.1` 访问，因此控制面运维通常需要在节点本机执行。
//
// 单节点启动示例：
//
//	go run ./cmd/stellmapd --config=./config/stellmapd.toml
//
// 三节点启动示例中的单个节点：
//
//	go run ./cmd/stellmapd --config=/etc/stellmapd/stellmapd-node-1.toml
//
// 当前职责：
// 1. 解析启动配置。
// 2. 装配 raftnode、runtime、grpc/http server。
// 3. 启动应用并处理优雅退出。
func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatalf("parse flags failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	daemonLogger, err := daemonlogging.New(ctx, cfg)
	if err != nil {
		log.Fatalf("init stellmapd logger failed: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), daemonlogging.EffectiveShutdownTimeout(cfg))
		defer cancel()
		if err := daemonLogger.Close(shutdownCtx); err != nil {
			log.Printf("close stellmapd logger failed: %v", err)
		}
	}()

	app, err := newServerApp(cfg)
	if err != nil {
		log.Fatalf("create stellmapd failed: %v", err)
	}

	if err := app.Start(ctx); err != nil {
		log.Fatalf("start stellmapd failed: %v", err)
	}

	effectiveCfg := app.Config()
	log.Printf("stellmapd started, node_id=%d cluster_id=%d http=%s admin=%s grpc=%s data_dir=%s", effectiveCfg.NodeID, effectiveCfg.ClusterID, effectiveCfg.HTTPAddr, effectiveCfg.AdminAddr, effectiveCfg.GRPCAddr, effectiveCfg.DataDir)

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-app.Errors():
		if err != nil {
			log.Printf("server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), effectiveCfg.ShutdownTimeout)
	defer cancel()
	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("stop stellmapd failed: %v", err)
	}
}

func newServerApp(cfg daemonConfig) (*serverApp, error) {
	validated, err := daemonapp.ValidateConfig(cfg)
	if err != nil {
		return nil, err
	}

	peers, err := parsePeerIDs(validated.NodeID, validated.PeerIDs)
	if err != nil {
		return nil, err
	}
	httpAddrByID, err := parseAddressMap(validated.PeerHTTPAddrs)
	if err != nil {
		return nil, err
	}
	grpcAddrByID, err := parseAddressMap(validated.PeerGRPCAddrs)
	if err != nil {
		return nil, err
	}
	adminAddrByID, err := parseAddressMap(validated.PeerAdminAddrs)
	if err != nil {
		return nil, err
	}

	book := runtime.NewAddressBook(httpAddrByID, grpcAddrByID, adminAddrByID)
	book.Set(
		validated.NodeID,
		preferAdvertiseAddr(httpAddrByID, validated.NodeID, validated.HTTPAddr),
		preferAdvertiseAddr(grpcAddrByID, validated.NodeID, validated.GRPCAddr),
		preferAdvertiseAddr(adminAddrByID, validated.NodeID, validated.AdminAddr),
	)

	node, err := raftnode.New(raftnode.Config{
		NodeID:    validated.NodeID,
		ClusterID: validated.ClusterID,
		DataDir:   validated.DataDir,
		Peers:     peers,
	})
	if err != nil {
		return nil, err
	}

	internalService := runtime.NewInternalTransportService(node, snapshot.NewFileStore(filepath.Join(validated.DataDir, "snapshot")))
	grpcServer := grpc.NewServer()

	peerTransport := runtime.NewPeerTransport(validated.NodeID, book, runtime.DefaultSnapshotChunk)
	watchHub := registry.NewWatchHub()
	replicationTracker := replication.NewTracker()
	transportMetrics := internalmetrics.NewTransportMetrics()
	registryMetrics := internalmetrics.NewRegistryMetrics(node)
	metricsRegistry := prometheus.NewRegistry()
	_ = metricsRegistry.Register(collectors.NewGoCollector())
	_ = metricsRegistry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	_ = metricsRegistry.Register(replication.NewCollector(replicationTracker))
	_ = transportMetrics.Register(metricsRegistry)
	_ = registryMetrics.Register(metricsRegistry)
	metricsHandler := promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})
	grpctransport.NewServer(internalService).WithMetrics(transportMetrics.GRPC()).RegisterHandlers(grpcServer)
	registryHandler := httptransport.NewRegistryHandler(node, validated.HTTPAddr, book, watchHub, validated.Region, fmt.Sprintf("%d", validated.ClusterID), validated.ReplicationToken, validated.PrometheusSDToken, validated.RequestTimeout).WithRegistryMetrics(registryMetrics)
	health := httptransport.NewHealthHandler(node, validated.HTTPAddr, book, replicationTracker, metricsHandler)
	control := httptransport.NewControlHandler(node, validated.ClusterID, validated.AdminAddr, book, peerTransport, replicationTracker, validated.RequestTimeout)
	httpServer := &http.Server{
		Addr:    validated.HTTPAddr,
		Handler: httptransport.NewPublicServer(registryHandler, health).WithMetrics(transportMetrics.HTTP()).Handler(),
	}
	adminServer := &http.Server{
		Addr:    validated.AdminAddr,
		Handler: adminAuthMiddleware(validated.AdminToken, httptransport.NewAdminServer(control).WithMetrics(transportMetrics.HTTP()).Handler()),
	}

	return daemonapp.New(validated, daemonapp.Components{
		Node:               node,
		HTTPServer:         httpServer,
		AdminServer:        adminServer,
		GRPCServer:         grpcServer,
		AddressBook:        book,
		PeerTransport:      peerTransport,
		TransportService:   internalService,
		RegistryWatchHub:   watchHub,
		ReplicationTracker: replicationTracker,
	})
}

// adminAuthMiddleware 为独立 admin listener 提供最小控制面防护。
func adminAuthMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLocalAdminRequest(r.RemoteAddr) {
			writeError(w, http.StatusForbidden, "forbidden", "admin listener only allows 127.0.0.1")
			return
		}
		if !isAuthorizedAdminRequest(r, token) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLocalAdminRequest(remoteAddr string) bool {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return false
	}

	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	return host == "127.0.0.1"
}

func isAuthorizedAdminRequest(r *http.Request, token string) bool {
	if strings.TrimSpace(token) == "" {
		return false
	}

	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	expected := "Bearer " + token
	return authorization == expected
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json response failed: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, httptransport.ErrorResponse{
		Code:    code,
		Message: message,
	})
}

func preferAdvertiseAddr(addrs map[uint64]string, nodeID uint64, fallback string) string {
	if addr, ok := addrs[nodeID]; ok {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			return addr
		}
	}
	return strings.TrimSpace(fallback)
}
