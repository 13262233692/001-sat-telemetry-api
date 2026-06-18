package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/anomaly"
	"github.com/aerospace/sat-telemetry-api/internal/api"
	"github.com/aerospace/sat-telemetry-api/internal/ingest"
	"github.com/aerospace/sat-telemetry-api/internal/pipeline"
	"github.com/aerospace/sat-telemetry-api/internal/store"
)

func main() {
	tcpAddr := envOr("TCP_ADDR", "0.0.0.0:9090")
	httpAddr := envOr("HTTP_ADDR", "0.0.0.0:8080")
	dbConnStr := envOr("DATABASE_URL", "postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable")
	alertGRPCAddr := envOr("ALERT_GRPC_ADDR", "localhost:50051")
	useAlertMock := envOr("ALERT_GRPC_MOCK", "true") == "true"

	frameSize := envIntOr("FRAME_SIZE", 1024)
	bufferSize := envIntOr("BUFFER_SIZE", 10000)
	batchSize := envIntOr("BATCH_SIZE", 500)
	flushMs := envIntOr("FLUSH_INTERVAL_MS", 200)
	workerCount := envIntOr("WORKER_COUNT", 4)

	zscoreThr := envFloatOr("ANOMALY_ZSCORE", 3.5)
	gradThr := envFloatOr("ANOMALY_GRADIENT", 5.0)
	windowMin := envIntOr("ANOMALY_WINDOW_MIN", 10)
	cooldownSec := envIntOr("ANOMALY_COOLDOWN_SEC", 30)
	thrusterSensorIDs := envIntListOr("ANOMALY_THRUSTER_SENSORS", []int{101, 102, 103, 201, 202, 203})
	thrusterAPIDMin := envIntOr("ANOMALY_APID_MIN", 0)
	thrusterAPIDMax := envIntOr("ANOMALY_APID_MAX", 0)
	anomalyWorkers := envIntOr("ANOMALY_WORKERS", 2)

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("[gateway] starting satellite telemetry gateway")

	writer, err := store.NewWriter(store.WriterConfig{
		ConnString:    dbConnStr,
		BufferSize:    bufferSize,
		BatchSize:     batchSize,
		FlushInterval: time.Duration(flushMs) * time.Millisecond,
		WorkerCount:   workerCount,
	})
	if err != nil {
		log.Fatalf("[gateway] failed to create writer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := writer.InitSchema(ctx); err != nil {
		log.Fatalf("[gateway] failed to init schema: %v", err)
	}
	cancel()

	writer.Start()

	pipe := pipeline.New(frameSize, writer)

	var alertClient anomaly.AlertClient
	if useAlertMock {
		alertClient = anomaly.NewMockAlertClient()
		log.Printf("[gateway] using MOCK alert client (ALERT_GRPC_MOCK=true)")
	} else {
		grpcClient, err := anomaly.NewGRPCAlertClient(anomaly.GRPCAlertClientConfig{
			Addr:        alertGRPCAddr,
			DialTimeout: 5 * time.Second,
			PushTimeout: 500 * time.Millisecond,
		})
		if err != nil {
			log.Printf("[gateway] WARNING: failed to connect alert gRPC %s, falling back to mock: %v",
				alertGRPCAddr, err)
			alertClient = anomaly.NewMockAlertClient()
		} else {
			alertClient = grpcClient
		}
	}

	thrusterSet := make(map[int]struct{}, len(thrusterSensorIDs))
	for _, id := range thrusterSensorIDs {
		thrusterSet[id] = struct{}{}
	}

	detector := anomaly.NewDetector(anomaly.DetectorConfig{
		WindowDuration:    time.Duration(windowMin) * time.Minute,
		ZScoreThreshold:   zscoreThr,
		GradientThreshold: gradThr,
		CooldownDuration:  time.Duration(cooldownSec) * time.Second,
		APIDMin:           thrusterAPIDMin,
		APIDMax:           thrusterAPIDMax,
		ThrusterSensors:   thrusterSet,
	}, alertClient)
	detector.Start(anomalyWorkers)
	pipe.SetAnomalyDetector(detector)

	tcpServer := ingest.NewServer(tcpAddr, frameSize, func(data []byte) {
		pipe.ProcessChunk(data)
	},
		ingest.WithReadTimeout(60*time.Second),
		ingest.WithMaxConnections(50),
	)

	if err := tcpServer.Start(); err != nil {
		log.Fatalf("[gateway] failed to start TCP server: %v", err)
	}

	handler := api.NewHandler(writer.DB(), writer)
	httpServer := api.NewServer(httpAddr, handler)

	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatalf("[gateway] HTTP server error: %v", err)
		}
	}()

	log.Printf("[gateway] all services started, TCP=%s HTTP=%s alert=%s",
		tcpAddr, httpAddr, describeAlertClient(alertClient, useAlertMock, alertGRPCAddr))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[gateway] received signal %v, shutting down", sig)

	tcpServer.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)

	detector.Stop()
	writer.Stop()

	frameCount, parseErrors := pipe.Stats()
	pending, dropped := writer.Stats()
	retried, deadDropped := writer.DeadLetterStats()
	alertCount, alertSkips := detector.Stats()

	log.Printf("[gateway] stats: frames=%d parseErrors=%d pending=%d dropped=%d retried=%d deadDropped=%d",
		frameCount, parseErrors, pending, dropped, retried, deadDropped)
	log.Printf("[gateway] anomaly stats: alerts=%d detectorSkips=%d activeWindows=%d",
		alertCount, alertSkips, detector.WindowCount())
	log.Printf("[gateway] shutdown complete")
}

func describeAlertClient(client anomaly.AlertClient, isMock bool, addr string) string {
	if isMock {
		return "mock"
	}
	if ac, ok := client.(*anomaly.GRPCAlertClient); ok {
		sent, failed, dropped := ac.Stats()
		return fmt.Sprintf("grpc://%s (sent=%d failed=%d dropped=%d)", addr, sent, failed, dropped)
	}
	return addr
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envFloatOr(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envIntListOr(key string, fallback []int) []int {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		result := make([]int, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if n, err := strconv.Atoi(p); err == nil {
				result = append(result, n)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return fallback
}
