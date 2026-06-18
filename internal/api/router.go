package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

type Server struct {
	httpServer *http.Server
	handler    *Handler
}

func NewServer(addr string, handler *Handler) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/telemetry", handler.QueryTelemetry)
	mux.HandleFunc("/api/v1/telemetry/latest", handler.GetLatestTelemetry)
	mux.HandleFunc("/api/v1/stats", handler.GetStats)
	mux.HandleFunc("/health", handler.HealthCheck)

	s := &Server{
		handler: handler,
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      corsMiddleware(mux),
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}

	return s
}

func (s *Server) Start() error {
	log.Printf("[api] HTTP server starting on %s", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api: server error: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Printf("[api] HTTP server shutting down")
	return s.httpServer.Shutdown(ctx)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
