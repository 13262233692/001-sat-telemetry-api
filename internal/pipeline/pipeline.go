package pipeline

import (
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/anomaly"
	"github.com/aerospace/sat-telemetry-api/internal/ccsds"
	"github.com/aerospace/sat-telemetry-api/internal/store"
)

const (
	CCSDSEpochOffset = 315532800
)

type ByteOrder int

const (
	OrderBigEndian    ByteOrder = iota
	OrderLittleEndian
)

type apidByteOrderEntry struct {
	apidMin  uint16
	apidMax  uint16
	byteOrder ByteOrder
}

var apidByteOrderTable = []apidByteOrderEntry{
	{0x000, 0x4FF, OrderBigEndian},
	{0x500, 0x5FF, OrderBigEndian},
	{0x600, 0x7FF, OrderLittleEndian},
	{0x800, 0x9FF, OrderBigEndian},
	{0xA00, 0xBFF, OrderLittleEndian},
	{0xC00, 0xFFF, OrderBigEndian},
}

type PointWriter interface {
	Write(point TelemetryPoint) bool
}

type TelemetryPoint = store.TelemetryPoint

type AnomalySubmitter interface {
	Submit(point *store.TelemetryPoint) bool
}

type Pipeline struct {
	scanner    *ccsds.FrameScanner
	writer     PointWriter
	anomaly    AnomalySubmitter
	mu         sync.Mutex
	frameCount int64
	parseErrs  int64
	ordErrs    int64
}

func New(frameSize int, writer PointWriter) *Pipeline {
	return &Pipeline{
		scanner: ccsds.NewFrameScanner(frameSize),
		writer:  writer,
	}
}

func (p *Pipeline) SetAnomalyDetector(detector AnomalySubmitter) {
	p.anomaly = detector
}

func (p *Pipeline) WithAnomalyDetector(detector *anomaly.Detector) *Pipeline {
	p.anomaly = detector
	return p
}

func resolveByteOrder(apid uint16) ByteOrder {
	for _, entry := range apidByteOrderTable {
		if apid >= entry.apidMin && apid <= entry.apidMax {
			return entry.byteOrder
		}
	}
	return OrderBigEndian
}

func (p *Pipeline) ProcessChunk(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.scanner.Feed(data)

	for {
		frameData, ok := p.scanner.NextFrame()
		if !ok {
			break
		}
		p.frameCount++
		p.processFrame(frameData)
	}
}

func (p *Pipeline) processFrame(data []byte) {
	frame, err := ccsds.ParseTransferFrame(data)
	if err != nil {
		p.parseErrs++
		if p.parseErrs%1000 == 0 {
			log.Printf("[pipeline] parse errors: %d (last: %v)", p.parseErrs, err)
		}
		return
	}

	for _, pkt := range frame.Packets {
		points := p.extractTelemetry(pkt)
		for i := range points {
			cleaned := store.CleanPoint(points[i])
			p.writer.Write(cleaned)
			if p.anomaly != nil {
				ptCopy := cleaned
				p.anomaly.Submit(&ptCopy)
			}
		}
	}
}

func (p *Pipeline) extractTelemetry(pkt ccsds.SourcePacket) []TelemetryPoint {
	var points []TelemetryPoint

	apid := int(pkt.Header.APID)
	timestamp := ccsdsEpochToTime(pkt.Timestamp)

	if len(pkt.Payload) < 4 {
		return points
	}

	byteOrder := resolveByteOrder(pkt.Header.APID)

	offset := 0
	for offset+4 <= len(pkt.Payload) {
		sensorID, rawValue := readSensorReading(pkt.Payload[offset:offset+4], byteOrder)

		points = append(points, store.TelemetryPoint{
			Timestamp: timestamp,
			APID:      apid,
			SensorID:  sensorID,
			RawValue:  rawValue,
			Value:     rawValue,
			Unit:      "",
			Quality:   0,
		})

		offset += 4

		if offset+6 <= len(pkt.Payload) {
			extValue, unitLen, unit := readExtendedField(pkt.Payload[offset:], byteOrder)
			if len(points) > 0 {
				points[len(points)-1].Value = extValue
				points[len(points)-1].Unit = unit
			}
			offset += 5 + unitLen
		}
	}

	return points
}

func readSensorReading(data []byte, order ByteOrder) (sensorID int, rawValue float64) {
	if order == OrderLittleEndian {
		sensorID = int(binary.LittleEndian.Uint16(data[0:2]))
		rawValue = float64(int16(binary.LittleEndian.Uint16(data[2:4])))
	} else {
		sensorID = int(binary.BigEndian.Uint16(data[0:2]))
		rawValue = float64(int16(binary.BigEndian.Uint16(data[2:4])))
	}
	return
}

func readExtendedField(data []byte, order ByteOrder) (value float64, unitLen int, unit string) {
	if order == OrderLittleEndian {
		value = float64(binary.LittleEndian.Uint32(data[0:4])) / 1000.0
	} else {
		value = float64(binary.BigEndian.Uint32(data[0:4])) / 1000.0
	}
	unitLen = int(data[4])
	if unitLen > 0 && 5+unitLen <= len(data) {
		unit = string(data[5 : 5+unitLen])
	}
	return
}

func RegisterAPIDByteOrder(apidMin, apidMax uint16, order ByteOrder) {
	for i, entry := range apidByteOrderTable {
		if entry.apidMin == apidMin && entry.apidMax == apidMax {
			apidByteOrderTable[i].byteOrder = order
			return
		}
	}
	apidByteOrderTable = append(apidByteOrderTable, apidByteOrderEntry{
		apidMin:   apidMin,
		apidMax:   apidMax,
		byteOrder: order,
	})
}

func ccsdsEpochToTime(cdsTime uint64) time.Time {
	if cdsTime == 0 {
		return time.Now().UTC()
	}

	days := int(cdsTime >> 32)
	ms := int(cdsTime & 0xFFFFFFFF)

	epoch := time.Date(1958, 1, 1, 0, 0, 0, 0, time.UTC)
	t := epoch.AddDate(0, 0, days).Add(time.Duration(ms) * time.Millisecond)
	return t.UTC()
}

func (p *Pipeline) Stats() (frameCount int64, parseErrors int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.frameCount, p.parseErrs
}
