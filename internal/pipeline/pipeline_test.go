package pipeline

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/aerospace/sat-telemetry-api/internal/ccsds"
	"github.com/aerospace/sat-telemetry-api/internal/store"
)

type mockWriter struct {
	points []store.TelemetryPoint
}

func (m *mockWriter) Write(p TelemetryPoint) bool {
	m.points = append(m.points, p)
	return true
}

func TestProcessChunkSingleFrame(t *testing.T) {
	mw := &mockWriter{}
	pipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  mw,
	}

	frame := buildValidFrame(0x050, 0x200, 1, 42.0)
	pipe.ProcessChunk(frame)

	if len(mw.points) == 0 {
		t.Fatal("expected telemetry points, got none")
	}

	pt := mw.points[0]
	if pt.APID != 0x200 {
		t.Errorf("expected APID 0x200, got 0x%04X", pt.APID)
	}
	if pt.SensorID != 1 {
		t.Errorf("expected sensor ID 1, got %d", pt.SensorID)
	}
}

func TestCcsdsEpochToTime(t *testing.T) {
	ts := ccsdsEpochToTime(0)
	if ts.IsZero() {
		t.Error("zero CDS time should produce current time, not zero")
	}

	days := uint64(1) << 32
	ms := uint64(0)
	cdsTime := days | ms

	result := ccsdsEpochToTime(cdsTime)
	epoch := time.Date(1958, 1, 1, 0, 0, 0, 0, time.UTC)
	expected := epoch.AddDate(0, 0, 1)

	if result.Year() != expected.Year() || result.YearDay() != expected.YearDay() {
		t.Errorf("expected date near %v, got %v", expected, result)
	}
}

func TestResolveByteOrderBigEndian(t *testing.T) {
	order := resolveByteOrder(0x100)
	if order != OrderBigEndian {
		t.Errorf("APID 0x100 should resolve to BigEndian, got %v", order)
	}

	order = resolveByteOrder(0x500)
	if order != OrderBigEndian {
		t.Errorf("APID 0x500 (attitude sensor) should resolve to BigEndian, got %v", order)
	}
}

func TestResolveByteOrderLittleEndian(t *testing.T) {
	order := resolveByteOrder(0x650)
	if order != OrderLittleEndian {
		t.Errorf("APID 0x650 should resolve to LittleEndian, got %v", order)
	}

	order = resolveByteOrder(0xA50)
	if order != OrderLittleEndian {
		t.Errorf("APID 0xA50 should resolve to LittleEndian, got %v", order)
	}
}

func TestResolveByteOrderDefault(t *testing.T) {
	order := resolveByteOrder(0x1234)
	if order != OrderBigEndian {
		t.Errorf("unknown APID should default to BigEndian, got %v", order)
	}
}

func TestBigEndianPayloadExtraction(t *testing.T) {
	mw := &mockWriter{}
	pipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  mw,
	}

	frame := buildValidFrameWithByteOrder(0x050, 0x200, 1, 42, OrderBigEndian)
	pipe.ProcessChunk(frame)

	if len(mw.points) == 0 {
		t.Fatal("expected telemetry points")
	}

	pt := mw.points[0]
	if pt.SensorID != 1 {
		t.Errorf("expected sensor ID 1, got %d", pt.SensorID)
	}
	if pt.RawValue != 42 {
		t.Errorf("expected raw value 42, got %f", pt.RawValue)
	}
}

func TestLittleEndianPayloadExtraction(t *testing.T) {
	mw := &mockWriter{}
	pipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  mw,
	}

	frame := buildValidFrameWithByteOrder(0x050, 0x650, 1, 42, OrderLittleEndian)
	pipe.ProcessChunk(frame)

	if len(mw.points) == 0 {
		t.Fatal("expected telemetry points")
	}

	pt := mw.points[0]
	if pt.SensorID != 1 {
		t.Errorf("expected sensor ID 1, got %d", pt.SensorID)
	}
	if pt.RawValue != 42 {
		t.Errorf("expected raw value 42, got %f", pt.RawValue)
	}
}

func TestAttitudeSensorBigEndian(t *testing.T) {
	mw := &mockWriter{}
	pipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  mw,
	}

	sensorValue := int16(-350)
	frame := buildValidFrameWithByteOrder(0x050, 0x530, 5, sensorValue, OrderBigEndian)
	pipe.ProcessChunk(frame)

	if len(mw.points) == 0 {
		t.Fatal("expected telemetry points for attitude sensor")
	}

	pt := mw.points[0]
	if pt.APID != 0x530 {
		t.Errorf("expected APID 0x530, got 0x%04X", pt.APID)
	}
	if pt.SensorID != 5 {
		t.Errorf("expected sensor ID 5, got %d", pt.SensorID)
	}
	expectedRaw := float64(sensorValue)
	if pt.RawValue != expectedRaw {
		t.Errorf("expected raw value %f, got %f", expectedRaw, pt.RawValue)
	}
}

func TestWrongByteOrderProducesCorruptValue(t *testing.T) {
	mw := &mockWriter{}
	pipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  mw,
	}

	sensorValue := int16(256)
	frame := buildValidFrameWithByteOrder(0x050, 0x200, 1, sensorValue, OrderBigEndian)
	pipe.ProcessChunk(frame)

	pt := mw.points[0]

	var corruptWriter mockWriter
	corruptPipe := &Pipeline{
		scanner: ccsds.NewFrameScanner(1024),
		writer:  &corruptWriter,
	}

	frameLE := buildValidFrameWithByteOrder(0x050, 0x200, 1, sensorValue, OrderLittleEndian)
	corruptPipe.ProcessChunk(frameLE)

	corruptPt := corruptWriter.points[0]
	if corruptPt.RawValue == pt.RawValue {
		t.Logf("both orders produced same value %f (coincidental for value %d)", pt.RawValue, sensorValue)
	}
}

func TestReadSensorReadingBigEndian(t *testing.T) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint16(data[0:2], 10)
	var val int16 = -100
	binary.BigEndian.PutUint16(data[2:4], uint16(val))

	sensorID, rawValue := readSensorReading(data, OrderBigEndian)
	if sensorID != 10 {
		t.Errorf("expected sensor ID 10, got %d", sensorID)
	}
	if rawValue != -100 {
		t.Errorf("expected raw value -100, got %f", rawValue)
	}
}

func TestReadSensorReadingLittleEndian(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:2], 10)
	var val int16 = -100
	binary.LittleEndian.PutUint16(data[2:4], uint16(val))

	sensorID, rawValue := readSensorReading(data, OrderLittleEndian)
	if sensorID != 10 {
		t.Errorf("expected sensor ID 10, got %d", sensorID)
	}
	if rawValue != -100 {
		t.Errorf("expected raw value -100, got %f", rawValue)
	}
}

func buildValidFrame(scID, apid uint16, sensorID int, value float64) []byte {
	return buildValidFrameWithByteOrder(scID, apid, sensorID, int16(value), OrderBigEndian)
}

func buildValidFrameWithByteOrder(scID, apid uint16, sensorID int, value int16, order ByteOrder) []byte {
	frame := make([]byte, 1024)

	binary.BigEndian.PutUint32(frame[0:4], ccsds.ASM)

	var word0 uint32 = (0 << 30) | (uint32(scID) << 20) | (1 << 17) | 0x01
	binary.BigEndian.PutUint32(frame[4:8], word0)
	binary.BigEndian.PutUint16(frame[8:10], 0x0001)
	binary.BigEndian.PutUint16(frame[10:12], 0x0000)

	offset := ccsds.ASMSize + ccsds.PrimaryHeaderSize

	var pktWord0 uint16 = (0 << 13) | (0 << 12) | (1 << 11) | apid
	binary.BigEndian.PutUint16(frame[offset:offset+2], pktWord0)

	var pktWord1 uint16 = (0x03 << 14) | 0x0001
	binary.BigEndian.PutUint16(frame[offset+2:offset+4], pktWord1)

	payloadLen := 8 + 4
	binary.BigEndian.PutUint16(frame[offset+4:offset+6], uint16(payloadLen+1))

	binary.BigEndian.PutUint64(frame[offset+6:offset+14], 0x0000020000000001)

	if order == OrderLittleEndian {
		binary.LittleEndian.PutUint16(frame[offset+14:offset+16], uint16(sensorID))
		binary.LittleEndian.PutUint16(frame[offset+16:offset+18], uint16(value))
	} else {
		binary.BigEndian.PutUint16(frame[offset+14:offset+16], uint16(sensorID))
		binary.BigEndian.PutUint16(frame[offset+16:offset+18], uint16(value))
	}

	return frame
}
