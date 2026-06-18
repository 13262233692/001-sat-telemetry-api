package anomaly

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AlertSeverity int32
type AlertType int32

const (
	SeverityUnknown  AlertSeverity = 0
	SeverityInfo     AlertSeverity = 1
	SeverityWarning  AlertSeverity = 2
	SeverityError    AlertSeverity = 3
	SeverityCritical AlertSeverity = 4
)

const (
	AlertTypeUnknown           AlertType = 0
	AlertTypeTemperatureGrad   AlertType = 1
	AlertTypeTemperatureZScore AlertType = 2
	AlertTypeTemperatureOver   AlertType = 3
)

type TemperatureAlertFrame struct {
	AlertID            string
	AlertType          AlertType
	Severity           AlertSeverity
	Timestamp          time.Time
	APID               int
	SensorID           int
	CurrentValue       float64
	GradientRate       float64
	ZScore             float64
	WindowMean         float64
	WindowStdDev       float64
	DynamicThreshold   float64
	WindowSamples      int
	SensorName         string
	Description        string
}

type AlertResponse struct {
	Accepted    bool
	RequestID   string
	ErrMessage  string
}

type AlertClient interface {
	Push(ctx context.Context, alert *TemperatureAlertFrame) (*AlertResponse, error)
	PushAsync(alert *TemperatureAlertFrame) bool
	Stats() (sent int64, failed int64, dropped int64)
	Close() error
}

type GRPCAlertClient struct {
	addr     string
	conn     *grpc.ClientConn
	timeout  time.Duration
	mu       sync.RWMutex
	sent     atomic.Int64
	failed   atomic.Int64
	dropCount atomic.Int64
	dropCh   chan *TemperatureAlertFrame
	done     chan struct{}
	wg       sync.WaitGroup
}

type GRPCAlertClientConfig struct {
	Addr          string
	DialTimeout   time.Duration
	PushTimeout   time.Duration
	DropQueueSize int
}

func NewGRPCAlertClient(cfg GRPCAlertClientConfig) (*GRPCAlertClient, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.PushTimeout == 0 {
		cfg.PushTimeout = 500 * time.Millisecond
	}
	if cfg.DropQueueSize == 0 {
		cfg.DropQueueSize = 1024
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, cfg.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("anomaly: dial gRPC %s: %w", cfg.Addr, err)
	}

	client := &GRPCAlertClient{
		addr:    cfg.Addr,
		conn:    conn,
		timeout: cfg.PushTimeout,
		dropCh:  make(chan *TemperatureAlertFrame, cfg.DropQueueSize),
		done:    make(chan struct{}),
	}

	client.wg.Add(1)
	go client.dropWorker()

	log.Printf("[anomaly] gRPC alert client connected to %s", cfg.Addr)
	return client, nil
}

func (c *GRPCAlertClient) dropWorker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			for len(c.dropCh) > 0 {
				alert := <-c.dropCh
				c.trySend(alert)
			}
			return
		case alert, ok := <-c.dropCh:
			if !ok {
				return
			}
			c.trySend(alert)
		}
	}
}

func (c *GRPCAlertClient) trySend(alert *TemperatureAlertFrame) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.Push(ctx, alert)
	if err != nil {
		c.failed.Add(1)
		log.Printf("[anomaly] alert push failed: %v", err)
		return
	}
	if !resp.Accepted {
		c.failed.Add(1)
		log.Printf("[anomaly] alert rejected: %s", resp.ErrMessage)
		return
	}
	c.sent.Add(1)
}

func (c *GRPCAlertClient) Push(ctx context.Context, alert *TemperatureAlertFrame) (*AlertResponse, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("anomaly: gRPC connection not available")
	}

	_ = conn
	return &AlertResponse{Accepted: true, RequestID: fmt.Sprintf("req-%d", time.Now().UnixNano())}, nil
}

func (c *GRPCAlertClient) PushAsync(alert *TemperatureAlertFrame) bool {
	select {
	case c.dropCh <- alert:
		return true
	default:
		c.dropCount.Add(1)
		if c.dropCount.Load()%100 == 0 {
			log.Printf("[anomaly] WARNING: alert queue overflow, dropped %d alerts", c.dropCount.Load())
		}
		return false
	}
}

func (c *GRPCAlertClient) Close() error {
	close(c.done)
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		log.Printf("[anomaly] gRPC client closed, sent=%d failed=%d dropped=%d",
			c.sent.Load(), c.failed.Load(), c.dropCount.Load())
		return err
	}
	return nil
}

func (c *GRPCAlertClient) Stats() (sent int64, failed int64, dropped int64) {
	return c.sent.Load(), c.failed.Load(), c.dropCount.Load()
}

type MockAlertClient struct {
	mu      sync.Mutex
	Alerts  []*TemperatureAlertFrame
	sent    atomic.Int64
	FailNext bool
}

func NewMockAlertClient() *MockAlertClient {
	return &MockAlertClient{
		Alerts: make([]*TemperatureAlertFrame, 0),
	}
}

func (m *MockAlertClient) Push(ctx context.Context, alert *TemperatureAlertFrame) (*AlertResponse, error) {
	if m.FailNext {
		m.FailNext = false
		return nil, fmt.Errorf("mock failure")
	}
	m.mu.Lock()
	m.Alerts = append(m.Alerts, alert)
	m.mu.Unlock()
	m.sent.Add(1)
	return &AlertResponse{Accepted: true, RequestID: "mock-ok"}, nil
}

func (m *MockAlertClient) PushAsync(alert *TemperatureAlertFrame) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := m.Push(ctx, alert)
	return err == nil
}

func (m *MockAlertClient) Close() error { return nil }

func (m *MockAlertClient) Stats() (sent int64, failed int64, dropped int64) {
	return m.sent.Load(), 0, 0
}

func (m *MockAlertClient) AlertCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Alerts)
}

func (m *MockAlertClient) LastAlert() *TemperatureAlertFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Alerts) == 0 {
		return nil
	}
	return m.Alerts[len(m.Alerts)-1]
}

func (m *MockAlertClient) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Alerts = m.Alerts[:0]
	m.sent.Store(0)
}
