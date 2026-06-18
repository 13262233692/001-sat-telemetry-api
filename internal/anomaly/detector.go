package anomaly

import (
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/store"
)

const (
	DefaultWindowSize    = 10 * time.Minute
	DefaultCooldownTime  = 30 * time.Second
	DefaultZScoreThreshold = 3.5
	DefaultGradientThreshold = 5.0
	MinWindowSamples     = 30
)

type tempSample struct {
	Timestamp time.Time
	Value     float64
}

type sensorWindow struct {
	sync.Mutex
	apid      int
	sensorID  int
	samples   []tempSample
	lastAlert time.Time
	mean      float64
	stddev    float64
	count     int
}

type DetectorConfig struct {
	WindowDuration    time.Duration
	ZScoreThreshold   float64
	GradientThreshold float64
	CooldownDuration  time.Duration
	APIDMin           int
	APIDMax           int
	ThrusterSensors   map[int]struct{}
}

type Detector struct {
	cfg      DetectorConfig
	client   AlertClient
	ch       chan *store.TelemetryPoint
	done     chan struct{}
	wg       sync.WaitGroup
	windows  sync.Map
	alerts   int64
	skips    int64
	mu       sync.Mutex
}

func NewDetector(cfg DetectorConfig, client AlertClient) *Detector {
	if cfg.WindowDuration == 0 {
		cfg.WindowDuration = DefaultWindowSize
	}
	if cfg.ZScoreThreshold == 0 {
		cfg.ZScoreThreshold = DefaultZScoreThreshold
	}
	if cfg.GradientThreshold == 0 {
		cfg.GradientThreshold = DefaultGradientThreshold
	}
	if cfg.CooldownDuration == 0 {
		cfg.CooldownDuration = DefaultCooldownTime
	}
	if cfg.ThrusterSensors == nil {
		cfg.ThrusterSensors = map[int]struct{}{}
	}

	return &Detector{
		cfg:    cfg,
		client: client,
		ch:     make(chan *store.TelemetryPoint, 4096),
		done:   make(chan struct{}),
	}
}

func (d *Detector) Start(workers int) {
	if workers <= 0 {
		workers = 2
	}
	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go d.worker(i)
	}
	log.Printf("[anomaly] detector started with %d workers, window=%v zscore=%.2f gradient=%.2f",
		workers, d.cfg.WindowDuration, d.cfg.ZScoreThreshold, d.cfg.GradientThreshold)
}

func (d *Detector) Stop() {
	close(d.done)
	d.wg.Wait()
	if d.client != nil {
		d.client.Close()
	}
	log.Printf("[anomaly] detector stopped, alerts=%d skips=%d", d.alerts, d.skips)
}

func (d *Detector) Submit(point *store.TelemetryPoint) bool {
	if !d.isThrusterPoint(point) {
		d.skips++
		return false
	}
	select {
	case d.ch <- point:
		return true
	default:
		d.skips++
		if d.skips%10000 == 0 {
			log.Printf("[anomaly] WARNING: detector channel overflow, skipped %d points", d.skips)
		}
		return false
	}
}

func (d *Detector) isThrusterPoint(pt *store.TelemetryPoint) bool {
	if pt == nil {
		return false
	}
	if pt.Quality >= 2 {
		return false
	}
	if d.cfg.APIDMin > 0 && pt.APID < d.cfg.APIDMin {
		return false
	}
	if d.cfg.APIDMax > 0 && pt.APID > d.cfg.APIDMax {
		return false
	}
	if len(d.cfg.ThrusterSensors) > 0 {
		_, ok := d.cfg.ThrusterSensors[pt.SensorID]
		return ok
	}
	return true
}

func (d *Detector) worker(id int) {
	defer d.wg.Done()
	for {
		select {
		case <-d.done:
			return
		case pt, ok := <-d.ch:
			if !ok {
				return
			}
			d.processPoint(pt)
		}
	}
}

func (d *Detector) processPoint(pt *store.TelemetryPoint) {
	win := d.getOrCreateWindow(pt.APID, pt.SensorID)
	d.processSample(win, pt)
}

func (d *Detector) getOrCreateWindow(apid, sensorID int) *sensorWindow {
	key := int64(apid)<<32 | int64(uint32(sensorID))
	winIface, loaded := d.windows.LoadOrStore(key, &sensorWindow{
		apid:     apid,
		sensorID: sensorID,
		samples:  make([]tempSample, 0, 1024),
	})
	_ = loaded
	return winIface.(*sensorWindow)
}

func (d *Detector) processSample(win *sensorWindow, pt *store.TelemetryPoint) {
	win.Lock()
	defer win.Unlock()

	sample := tempSample{
		Timestamp: pt.Timestamp,
		Value:     pt.Value,
	}
	win.samples = append(win.samples, sample)
	win.count++

	cutoff := pt.Timestamp.Add(-d.cfg.WindowDuration)
	trimmed := 0
	for _, s := range win.samples {
		if s.Timestamp.After(cutoff) || s.Timestamp.Equal(cutoff) {
			break
		}
		trimmed++
	}
	if trimmed > 0 {
		win.samples = win.samples[trimmed:]
	}

	gradient, _ := d.computeGradient(win)

	if len(win.samples) < MinWindowSamples {
		return
	}

	mean, stddev := computeMeanStddev(win.samples)
	win.mean = mean
	win.stddev = stddev

	var zScore float64
	if stddev > 1e-9 {
		zScore = math.Abs(pt.Value - mean) / stddev
	}

	shouldAlert := false
	alertType := AlertTypeUnknown
	severity := SeverityWarning
	dynThreshold := d.cfg.GradientThreshold

	if zScore > d.cfg.ZScoreThreshold {
		shouldAlert = true
		alertType = AlertTypeTemperatureZScore
		dynThreshold = d.cfg.ZScoreThreshold
		if zScore > d.cfg.ZScoreThreshold*1.5 {
			severity = SeverityError
		}
		if zScore > d.cfg.ZScoreThreshold*2.5 {
			severity = SeverityCritical
		}
	}

	if !shouldAlert && math.Abs(gradient) > d.cfg.GradientThreshold {
		shouldAlert = true
		alertType = AlertTypeTemperatureGrad
		dynThreshold = d.cfg.GradientThreshold
		if math.Abs(gradient) > d.cfg.GradientThreshold*2 {
			severity = SeverityError
		}
		if math.Abs(gradient) > d.cfg.GradientThreshold*4 {
			severity = SeverityCritical
		}
	}

	if !shouldAlert {
		return
	}

	now := time.Now()
	if !win.lastAlert.IsZero() && now.Sub(win.lastAlert) < d.cfg.CooldownDuration {
		return
	}
	win.lastAlert = now

	alert := &TemperatureAlertFrame{
		AlertID:          genAlertID(pt.APID, pt.SensorID, sample.Timestamp),
		AlertType:        alertType,
		Severity:         severity,
		Timestamp:        sample.Timestamp,
		APID:             pt.APID,
		SensorID:         pt.SensorID,
		CurrentValue:     pt.Value,
		GradientRate:     gradient,
		ZScore:           zScore,
		WindowMean:       mean,
		WindowStdDev:     stddev,
		DynamicThreshold: dynThreshold,
		WindowSamples:    len(win.samples),
		SensorName:       buildSensorName(pt.APID, pt.SensorID),
		Description:      buildDescription(alertType, gradient, zScore, d.cfg),
	}

	if d.client != nil {
		d.client.PushAsync(alert)
	}
	d.alerts++
}

func (d *Detector) computeGradient(win *sensorWindow) (float64, bool) {
	n := len(win.samples)
	if n < 2 {
		return 0, false
	}

	recentN := 10
	if recentN > n {
		recentN = n
	}

	recent := win.samples[n-recentN:]

	values := make([]float64, recentN)
	for i, s := range recent {
		values[i] = s.Value
	}

	sort.Float64s(values)
	median := values[recentN/2]
	first := recent[0]
	last := recent[len(recent)-1]

	dt := last.Timestamp.Sub(first.Timestamp).Seconds()
	if dt < 0.001 {
		return 0, false
	}

	dv := last.Value - first.Value
	gradient := dv / dt
	if recentN > 3 {
		q1 := values[recentN/4]
		q3 := values[3*recentN/4]
		iqr := q3 - q1
		if math.Abs(median) > 1e-6 && iqr > 0.1*math.Abs(median) {
			gradient = (last.Value - median) / dt
		}
	}

	return gradient, true
}

func computeMeanStddev(samples []tempSample) (mean, stddev float64) {
	n := len(samples)
	if n == 0 {
		return 0, 0
	}

	sum := 0.0
	for _, s := range samples {
		sum += s.Value
	}
	mean = sum / float64(n)

	if n < 2 {
		return mean, 0
	}

	varSum := 0.0
	for _, s := range samples {
		diff := s.Value - mean
		varSum += diff * diff
	}
	stddev = math.Sqrt(varSum / float64(n-1))
	return
}

func genAlertID(apid, sensorID int, ts time.Time) string {
	return fmtAlertID(apid, sensorID, ts)
}

func fmtAlertID(apid, sensorID int, ts time.Time) string {
	return string(appendID(apid, sensorID, ts.UnixNano()))
}

func appendID(apid, sensorID int, ns int64) []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, 'a', 'l', 't', '-')
	buf = appendNum(buf, int64(apid))
	buf = append(buf, '-')
	buf = appendNum(buf, int64(sensorID))
	buf = append(buf, '-')
	buf = appendNum(buf, ns)
	return buf
}

func appendNum(buf []byte, n int64) []byte {
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	if n == 0 {
		return append(buf, '0')
	}
	rev := make([]byte, 0, 20)
	for n > 0 {
		rev = append(rev, byte('0'+n%10))
		n /= 10
	}
	for i := len(rev) - 1; i >= 0; i-- {
		buf = append(buf, rev[i])
	}
	return buf
}

func buildSensorName(apid, sensorID int) string {
	return string(appendID(apid, sensorID, 0)[:len(appendID(apid, sensorID, 0))-2])
}

func buildDescription(alertType AlertType, gradient, zscore float64, cfg DetectorConfig) string {
	switch alertType {
	case AlertTypeTemperatureGrad:
		return "thruster temperature gradient exceeded threshold: " +
			formatFloat(gradient) + " > " + formatFloat(cfg.GradientThreshold) + " deg/s"
	case AlertTypeTemperatureZScore:
		return "thruster temperature z-score anomaly: " +
			formatFloat(zscore) + " > " + formatFloat(cfg.ZScoreThreshold)
	default:
		return "thruster temperature anomaly detected"
	}
}

func formatFloat(v float64) string {
	return string(appendFloat(v))
}

func appendFloat(v float64) []byte {
	if math.IsNaN(v) {
		return []byte("NaN")
	}
	abs := math.Abs(v)
	neg := v < 0
	buf := make([]byte, 0, 24)
	if neg {
		buf = append(buf, '-')
	}

	intPart := int64(abs)
	buf = appendNum(buf, intPart)

	frac := abs - float64(intPart)
	if frac > 0 {
		buf = append(buf, '.')
		for i := 0; i < 4; i++ {
			frac *= 10
			digit := int64(frac)
			buf = append(buf, byte('0'+digit))
			frac -= float64(digit)
			if frac < 1e-9 {
				break
			}
		}
	}
	return buf
}

func (d *Detector) Stats() (alerts int64, skips int64) {
	return d.alerts, d.skips
}

func (d *Detector) WindowCount() int {
	count := 0
	d.windows.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}
