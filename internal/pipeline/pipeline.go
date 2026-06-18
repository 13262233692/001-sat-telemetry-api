package pipeline

import (
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/ccsds"
	"github.com/aerospace/sat-telemetry-api/internal/store"
)

const (
	CCSDSEpochOffset = 315532800
)

type PointWriter interface {
	Write(point TelemetryPoint) bool
}

type TelemetryPoint = store.TelemetryPoint

type Pipeline struct {
	scanner    *ccsds.FrameScanner
	writer     PointWriter
	mu         sync.Mutex
	frameCount int64
	parseErrs  int64
}

func New(frameSize int, writer PointWriter) *Pipeline {
	return &Pipeline{
		scanner: ccsds.NewFrameScanner(frameSize),
		writer:  writer,
	}
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
		for _, pt := range points {
			cleaned := store.CleanPoint(pt)
			p.writer.Write(cleaned)
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

	offset := 0
	for offset+4 <= len(pkt.Payload) {
		sensorID := int(binary.BigEndian.Uint16(pkt.Payload[offset : offset+2]))
		rawValue := float64(int16(binary.BigEndian.Uint16(pkt.Payload[offset+2 : offset+4])))

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
			extValue := float64(binary.BigEndian.Uint32(pkt.Payload[offset:offset+4])) / 1000.0
			unitLen := int(pkt.Payload[offset+4])
			var unit string
			if offset+5+unitLen <= len(pkt.Payload) {
				unit = string(pkt.Payload[offset+5 : offset+5+unitLen])
			}
			if len(points) > 0 {
				points[len(points)-1].Value = extValue
				points[len(points)-1].Unit = unit
			}
			offset += 5 + unitLen
		}
	}

	return points
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
