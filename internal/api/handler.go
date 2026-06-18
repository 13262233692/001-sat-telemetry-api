package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/store"
)

type Handler struct {
	db     *sql.DB
	writer *store.Writer
}

func NewHandler(db *sql.DB, writer *store.Writer) *Handler {
	return &Handler{db: db, writer: writer}
}

func (h *Handler) QueryTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	params, err := h.parseQueryParams(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := h.executeQuery(r, params)
	if err != nil {
		log.Printf("[api] query error: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) parseQueryParams(r *http.Request) (*store.QueryParams, error) {
	q := r.URL.Query()
	params := &store.QueryParams{
		Limit:  1000,
		Offset: 0,
	}

	startStr := q.Get("start")
	if startStr == "" {
		return nil, fmt.Errorf("start parameter is required (RFC3339 format)")
	}
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return nil, fmt.Errorf("invalid start time: %w", err)
	}
	params.Start = start.UTC()

	endStr := q.Get("end")
	if endStr == "" {
		params.End = time.Now().UTC()
	} else {
		end, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			return nil, fmt.Errorf("invalid end time: %w", err)
		}
		params.End = end.UTC()
	}

	if apidStr := q.Get("apid"); apidStr != "" {
		apid, err := strconv.Atoi(apidStr)
		if err != nil {
			return nil, fmt.Errorf("invalid apid: %w", err)
		}
		params.APID = apid
	}

	if sensorStr := q.Get("sensor_id"); sensorStr != "" {
		sensorID, err := strconv.Atoi(sensorStr)
		if err != nil {
			return nil, fmt.Errorf("invalid sensor_id: %w", err)
		}
		params.SensorID = sensorID
	}

	if limitStr := q.Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 || limit > 10000 {
			return nil, fmt.Errorf("limit must be between 1 and 10000")
		}
		params.Limit = limit
	}

	if offsetStr := q.Get("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("offset must be >= 0")
		}
		params.Offset = offset
	}

	return params, nil
}

func (h *Handler) executeQuery(r *http.Request, params *store.QueryParams) (*store.QueryResult, error) {
	var conditions []string
	var args []interface{}
	argIdx := 1

	conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIdx))
	args = append(args, params.Start)
	argIdx++

	conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIdx))
	args = append(args, params.End)
	argIdx++

	if params.APID > 0 {
		conditions = append(conditions, fmt.Sprintf("apid = $%d", argIdx))
		args = append(args, params.APID)
		argIdx++
	}

	if params.SensorID > 0 {
		conditions = append(conditions, fmt.Sprintf("sensor_id = $%d", argIdx))
		args = append(args, params.SensorID)
		argIdx++
	}

	where := strings.Join(conditions, " AND ")

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM telemetry_raw WHERE %s", where)
	var total int
	if err := h.db.QueryRowContext(r.Context(), countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count query: %w", err)
	}

	dataQuery := fmt.Sprintf(
		"SELECT timestamp, apid, sensor_id, raw_value, value, unit, quality FROM telemetry_raw WHERE %s ORDER BY timestamp DESC LIMIT $%d OFFSET $%d",
		where, argIdx, argIdx+1,
	)
	args = append(args, params.Limit, params.Offset)

	rows, err := h.db.QueryContext(r.Context(), dataQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("data query: %w", err)
	}
	defer rows.Close()

	var points []store.TelemetryPoint
	for rows.Next() {
		var p store.TelemetryPoint
		if err := rows.Scan(&p.Timestamp, &p.APID, &p.SensorID, &p.RawValue, &p.Value, &p.Unit, &p.Quality); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		points = append(points, p)
	}

	if points == nil {
		points = []store.TelemetryPoint{}
	}

	return &store.QueryResult{
		Points: points,
		Total:  total,
	}, nil
}

func (h *Handler) GetLatestTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	apidStr := q.Get("apid")
	if apidStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "apid parameter is required"})
		return
	}
	apid, err := strconv.Atoi(apidStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid apid"})
		return
	}

	sensorStr := q.Get("sensor_id")
	sensorID := 0
	if sensorStr != "" {
		sensorID, err = strconv.Atoi(sensorStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid sensor_id"})
			return
		}
	}

	query := "SELECT timestamp, apid, sensor_id, raw_value, value, unit, quality FROM telemetry_raw WHERE apid = $1"
	args := []interface{}{apid}

	if sensorID > 0 {
		query += " AND sensor_id = $2 ORDER BY timestamp DESC LIMIT 1"
		args = append(args, sensorID)
	} else {
		query += " ORDER BY timestamp DESC LIMIT 1"
	}

	var p store.TelemetryPoint
	err = h.db.QueryRowContext(r.Context(), query, args...).Scan(
		&p.Timestamp, &p.APID, &p.SensorID, &p.RawValue, &p.Value, &p.Unit, &p.Quality,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no data found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}

	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pending, dropped := h.writer.Stats()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pending_points":  pending,
		"dropped_points":  dropped,
		"buffer_capacity": h.writer.BufferCapacity(),
	})
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	err := h.db.PingContext(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
