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
	maxRetryAttempts     = 3
	initialRetryDelay    = 100 * time.Millisecond
	maxRetryDelay        = 5 * time.Second
)

type WriterConfig struct {
	ConnString    string
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
}

type deadLetterEntry struct {
	batch   []TelemetryPoint
	attempt int
	nextAt  time.Time
}

type Writer struct {
	cfg        WriterConfig
	db         *sql.DB
	ch         chan TelemetryPoint
	done       chan struct{}
	wg         sync.WaitGroup
	dropped    atomic.Int64
	pending    atomic.Int64
	retried    atomic.Int64
	deadDropped atomic.Int64
	deadMu     sync.Mutex
	deadLetters []deadLetterEntry
	ctx        context.Context
	cancel     context.CancelFunc
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
		cfg:         cfg,
		db:          db,
		ch:          make(chan TelemetryPoint, cfg.BufferSize),
		done:        make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		deadLetters: make([]deadLetterEntry, 0, 64),
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
		quality INTEGER NOT NULL DEFAULT 0,
		CONSTRAINT chk_value_range CHECK (value >= -1e8 AND value <= 1e8),
		CONSTRAINT chk_quality CHECK (quality >= 0 AND quality <= 3)
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
	w.wg.Add(1)
	go w.deadLetterWorker()
	log.Printf("[store] writer started with %d workers, buffer=%d, batch=%d",
		w.cfg.WorkerCount, w.cfg.BufferSize, w.cfg.BatchSize)
}

func (w *Writer) Stop() {
	w.cancel()
	close(w.done)
	w.wg.Wait()
	w.db.Close()
	log.Printf("[store] writer stopped, dropped=%d, retried=%d, deadDropped=%d",
		w.dropped.Load(), w.retried.Load(), w.deadDropped.Load())
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
			log.Printf("[store] worker-%d insert error: %v, sending %d points to dead letter queue",
				id, err, len(batch))
			w.enqueueDeadLetter(batch)
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

func (w *Writer) enqueueDeadLetter(batch []TelemetryPoint) {
	entry := deadLetterEntry{
		batch:   make([]TelemetryPoint, len(batch)),
		attempt: 0,
		nextAt:  time.Now().Add(initialRetryDelay),
	}
	copy(entry.batch, batch)

	w.deadMu.Lock()
	w.deadLetters = append(w.deadLetters, entry)
	w.deadMu.Unlock()
}

func (w *Writer) deadLetterWorker() {
	defer w.wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			w.flushDeadLetters()
			return
		case <-ticker.C:
			w.processDeadLetters()
		}
	}
}

func (w *Writer) processDeadLetters() {
	now := time.Now()
	var retryBatches []deadLetterEntry
	var remaining []deadLetterEntry

	w.deadMu.Lock()
	for _, entry := range w.deadLetters {
		if now.After(entry.nextAt) || now.Equal(entry.nextAt) {
			retryBatches = append(retryBatches, entry)
		} else {
			remaining = append(remaining, entry)
		}
	}
	w.deadLetters = remaining
	w.deadMu.Unlock()

	for i, entry := range retryBatches {
		filtered := w.filterValidPoints(entry.batch)
		if len(filtered) == 0 {
			continue
		}

		err := w.insertBatch(w.ctx, filtered)
		if err != nil {
			entry.attempt++
			if entry.attempt >= maxRetryAttempts {
				w.deadDropped.Add(int64(len(entry.batch)))
				log.Printf("[store] DEAD LETTER: discarding %d points after %d retries (last error: %v)",
					len(entry.batch), entry.attempt, err)
				continue
			}

			delay := w.backoffDelay(entry.attempt)
			entry.nextAt = now.Add(delay)
			log.Printf("[store] DEAD LETTER: retry %d/%d scheduled in %v for %d points",
				entry.attempt, maxRetryAttempts, delay, len(entry.batch))

			retryBatches[i] = entry
			w.deadMu.Lock()
			w.deadLetters = append(w.deadLetters, entry)
			w.deadMu.Unlock()
		} else {
			w.retried.Add(int64(len(filtered)))
			log.Printf("[store] DEAD LETTER: successfully retried %d points on attempt %d",
				len(filtered), entry.attempt+1)
		}
	}
}

func (w *Writer) filterValidPoints(batch []TelemetryPoint) []TelemetryPoint {
	valid := make([]TelemetryPoint, 0, len(batch))
	for _, p := range batch {
		if IsValidValue(p.Value) && IsValidValue(p.RawValue) {
			valid = append(valid, p)
		}
	}
	return valid
}

func (w *Writer) backoffDelay(attempt int) time.Duration {
	delay := initialRetryDelay * time.Duration(1<<uint(attempt))
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	return delay
}

func (w *Writer) flushDeadLetters() {
	w.processDeadLetters()
	w.deadMu.Lock()
	count := len(w.deadLetters)
	w.deadMu.Unlock()
	if count > 0 {
		log.Printf("[store] WARNING: %d dead letter batches remain unprocessed at shutdown", count)
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

func (w *Writer) DeadLetterStats() (retried int64, deadDropped int64) {
	return w.retried.Load(), w.deadDropped.Load()
}

func (w *Writer) DB() *sql.DB {
	return w.db
}

func (w *Writer) BufferCapacity() int {
	return cap(w.ch)
}

const (
	MaxTelemetryValue = 1e8
	MinTelemetryValue = -1e8
)

func IsValidValue(v float64) bool {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return false
	}
	if v > MaxTelemetryValue || v < MinTelemetryValue {
		return false
	}
	return true
}

func CleanPoint(raw TelemetryPoint) TelemetryPoint {
	result := raw

	if !IsValidValue(result.RawValue) {
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

	if !IsValidValue(result.Value) {
		result.Quality = 3
		result.Value = 0
	}

	return result
}
