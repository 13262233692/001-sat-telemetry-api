package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

const (
	defaultBufferSize    = 10000
	defaultBatchSize     = 500
	defaultFlushInterval = 200 * time.Millisecond
	defaultWorkerCount   = 4
)

type WriterConfig struct {
	ConnString    string
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
}

type Writer struct {
	cfg     WriterConfig
	db      *sql.DB
	ch      chan TelemetryPoint
	done    chan struct{}
	wg      sync.WaitGroup
	dropped atomic.Int64
	pending atomic.Int64
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewWriter(cfg WriterConfig) (*Writer, error) {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}

	db, err := sql.Open("postgres", cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("store: open db failed: %w", err)
	}

	db.SetMaxOpenConns(cfg.WorkerCount + 2)
	db.SetMaxIdleConns(cfg.WorkerCount)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())

	w := &Writer{
		cfg:    cfg,
		db:     db,
		ch:     make(chan TelemetryPoint, cfg.BufferSize),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	return w, nil
}

func (w *Writer) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS telemetry_raw (
		timestamp TIMESTAMPTZ NOT NULL,
		apid INTEGER NOT NULL,
		sensor_id INTEGER NOT NULL,
		raw_value DOUBLE PRECISION NOT NULL,
		value DOUBLE PRECISION NOT NULL,
		unit TEXT NOT NULL DEFAULT '',
		quality INTEGER NOT NULL DEFAULT 0
	);

	SELECT create_hypertable_if_not_exists('telemetry_raw', 'timestamp',
		chunk_time_interval => INTERVAL '1 hour',
		migrate_data => true
	);

	CREATE INDEX IF NOT EXISTS idx_telemetry_apid_sensor ON telemetry_raw (apid, sensor_id, timestamp DESC);

	CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry_raw (timestamp DESC);
	`
	_, err := w.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("store: init schema failed: %w", err)
	}
	log.Printf("[store] schema initialized")
	return nil
}

func (w *Writer) Start() {
	for i := 0; i < w.cfg.WorkerCount; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}
	log.Printf("[store] writer started with %d workers, buffer=%d, batch=%d",
		w.cfg.WorkerCount, w.cfg.BufferSize, w.cfg.BatchSize)
}

func (w *Writer) Stop() {
	w.cancel()
	close(w.done)
	w.wg.Wait()
	w.db.Close()
	log.Printf("[store] writer stopped, dropped=%d", w.dropped.Load())
}

func (w *Writer) Write(point TelemetryPoint) bool {
	w.pending.Add(1)
	select {
	case w.ch <- point:
		return true
	default:
		w.pending.Add(-1)
		w.dropped.Add(1)
		if w.dropped.Load()%1000 == 0 {
			log.Printf("[store] WARNING: backpressure dropped %d points", w.dropped.Load())
		}
		return false
	}
}

func (w *Writer) WriteBatch(points []TelemetryPoint) int {
	written := 0
	for _, p := range points {
		if w.Write(p) {
			written++
		}
	}
	return written
}

func (w *Writer) worker(id int) {
	defer w.wg.Done()

	batch := make([]TelemetryPoint, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.insertBatch(w.ctx, batch); err != nil {
			log.Printf("[store] worker-%d insert error: %v", id, err)
		}
		w.pending.Add(-int64(len(batch)))
		batch = batch[:0]
	}

	for {
		select {
		case <-w.done:
			for len(w.ch) > 0 {
				p := <-w.ch
				batch = append(batch, p)
				if len(batch) >= w.cfg.BatchSize {
					flush()
				}
			}
			flush()
			return

		case p, ok := <-w.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, p)
			if len(batch) >= w.cfg.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

func (w *Writer) insertBatch(ctx context.Context, batch []TelemetryPoint) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO telemetry_raw (timestamp, apid, sensor_id, raw_value, value, unit, quality)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, p := range batch {
		_, err := stmt.ExecContext(ctx, p.Timestamp, p.APID, p.SensorID, p.RawValue, p.Value, p.Unit, p.Quality)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("exec: %w", err)
		}
	}

	return tx.Commit()
}

func (w *Writer) Stats() (pending int64, dropped int64) {
	return w.pending.Load(), w.dropped.Load()
}

func (w *Writer) DB() *sql.DB {
	return w.db
}

func (w *Writer) BufferCapacity() int {
	return cap(w.ch)
}

func CleanPoint(raw TelemetryPoint) TelemetryPoint {
	result := raw

	if math.IsNaN(result.RawValue) || math.IsInf(result.RawValue, 0) {
		result.Quality = 2
		result.Value = 0
		return result
	}

	result.Value = result.RawValue

	if result.Quality == 0 {
		result.Quality = 1
	}

	if result.Timestamp.IsZero() {
		result.Timestamp = time.Now().UTC()
	}

	return result
}
