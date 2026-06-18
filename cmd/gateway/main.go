package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/api"
	"github.com/aerospace/sat-telemetry-api/internal/ingest"
	"github.com/aerospace/sat-telemetry-api/internal/pipeline"
	"github.com/aerospace/sat-telemetry-api/internal/store"
)

func main() {
	tcpAddr := envOr("TCP_ADDR", "0.0.0.0:9090")
	httpAddr := envOr("HTTP_ADDR", "0.0.0.0:8080")
	dbConnStr := envOr("DATABASE_URL", "postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable")
	frameSize := envIntOr("FRAME_SIZE", 1024)
	bufferSize := envIntOr("BUFFER_SIZE", 10000)
	batchSize := envIntOr("BATCH_SIZE", 500)
	flushMs := envIntOr("FLUSH_INTERVAL_MS", 200)
	workerCount := envIntOr("WORKER_COUNT", 4)

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

	log.Printf("[gateway] all services started, TCP=%s HTTP=%s", tcpAddr, httpAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[gateway] received signal %v, shutting down", sig)

	tcpServer.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)

	writer.Stop()

	frameCount, parseErrors := pipe.Stats()
	pending, dropped := writer.Stats()
	log.Printf("[gateway] stats: frames=%d parseErrors=%d pending=%d dropped=%d",
		frameCount, parseErrors, pending, dropped)
	log.Printf("[gateway] shutdown complete")
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
