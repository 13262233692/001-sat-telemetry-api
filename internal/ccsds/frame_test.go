package ccsds

import (
	"encoding/binary"
	"testing"
)

func TestParseASM(t *testing.T) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, ASM)

	offset, err := ParseASM(data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if offset != ASMSize {
		t.Fatalf("expected offset %d, got %d", ASMSize, offset)
	}

	invalidData := []byte{0x00, 0x00, 0x00, 0x00}
	_, err = ParseASM(invalidData)
	if err != ErrInvalidASM {
		t.Fatalf("expected ErrInvalidASM, got %v", err)
	}
}

func TestFindASMBoundary(t *testing.T) {
	prefix := []byte{0xAA, 0xBB, 0xCC}
	asm := make([]byte, 4)
	binary.BigEndian.PutUint32(asm, ASM)
	suffix := []byte{0x01, 0x02}

	data := append(prefix, asm...)
	data = append(data, suffix...)

	idx := FindASMBoundary(data)
	if idx != len(prefix) {
		t.Fatalf("expected ASM at offset %d, got %d", len(prefix), idx)
	}

	idx = FindASMBoundary([]byte{0x00, 0x00, 0x00})
	if idx != -1 {
		t.Fatalf("expected -1 for no ASM, got %d", idx)
	}
}

func TestParsePrimaryHeader(t *testing.T) {
	header := make([]byte, PrimaryHeaderSize)
	var word0 uint32 = (1 << 30) | (0x050 << 20) | (0x3 << 17) | (1 << 16) | 0x2A
	binary.BigEndian.PutUint32(header[0:4], word0)

	var word1 uint16 = (0x10 << 8) | 0xFF
	binary.BigEndian.PutUint16(header[4:6], word1)

	h, err := ParsePrimaryHeader(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if h.VersionNumber != 1 {
		t.Errorf("expected version 1, got %d", h.VersionNumber)
	}
	if h.SpacecraftID != 0x050 {
		t.Errorf("expected spacecraft ID 0x050, got 0x%04X", h.SpacecraftID)
	}
	if h.VirtualChannelID != 0x3 {
		t.Errorf("expected VC ID 3, got %d", h.VirtualChannelID)
	}
	if !h.OCFFlag {
		t.Error("expected OCF flag true")
	}
	if h.MasterFrameCount != 0x2A {
		t.Errorf("expected master frame count 0x2A, got 0x%02X", h.MasterFrameCount)
	}
}

func TestParseSourcePacket(t *testing.T) {
	packetData := make([]byte, PacketPrimaryHeaderSize+8+4)

	var word0 uint16 = (0 << 13) | (0 << 12) | (1 << 11) | 0x123
	binary.BigEndian.PutUint16(packetData[0:2], word0)

	var word1 uint16 = (0x03 << 14) | 0x0001
	binary.BigEndian.PutUint16(packetData[2:4], word1)

	binary.BigEndian.PutUint16(packetData[4:6], 11)

	binary.BigEndian.PutUint64(packetData[6:14], 0x00000123456789AB)

	copy(packetData[14:], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	pkt, err := ParseSourcePacket(packetData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pkt.Header.APID != 0x123 {
		t.Errorf("expected APID 0x123, got 0x%04X", pkt.Header.APID)
	}
	if pkt.Header.SecondaryHeaderFlag != true {
		t.Error("expected secondary header flag true")
	}
	if pkt.Header.SequenceFlags != 0x03 {
		t.Errorf("expected sequence flags 3, got %d", pkt.Header.SequenceFlags)
	}
	if pkt.Header.DataLength != 11 {
		t.Errorf("expected data length 11, got %d", pkt.Header.DataLength)
	}
	if pkt.Timestamp != 0x00000123456789AB {
		t.Errorf("expected timestamp 0x00000123456789AB, got 0x%016X", pkt.Timestamp)
	}
	if len(pkt.Payload) != 4 {
		t.Fatalf("expected payload length 4, got %d", len(pkt.Payload))
	}
}

func TestParseTransferFrame(t *testing.T) {
	frame := buildTestFrame(0x100, []uint16{0x200})

	parsed, err := ParseTransferFrame(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Header.SpacecraftID != 0x100 {
		t.Errorf("expected spacecraft ID 0x100, got 0x%04X", parsed.Header.SpacecraftID)
	}
	if len(parsed.Packets) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(parsed.Packets))
	}
	if parsed.Packets[0].Header.APID != 0x200 {
		t.Errorf("expected APID 0x200, got 0x%04X", parsed.Packets[0].Header.APID)
	}
}

func TestParseTransferFrameTooShort(t *testing.T) {
	_, err := ParseTransferFrame([]byte{0x1A, 0xCF})
	if err != ErrFrameTooShort {
		t.Fatalf("expected ErrFrameTooShort, got %v", err)
	}
}

func TestParseTransferFrameInvalidASM(t *testing.T) {
	data := make([]byte, MinFrameSize)
	_, err := ParseTransferFrame(data)
	if err != ErrInvalidASM {
		t.Fatalf("expected ErrInvalidASM, got %v", err)
	}
}

func TestFrameScanner(t *testing.T) {
	scanner := NewFrameScanner(64)

	frame := buildTestFrame(0x050, []uint16{0x100})

	scanner.Feed(frame[:10])
	if _, ok := scanner.NextFrame(); ok {
		t.Fatal("expected no frame yet")
	}

	scanner.Feed(frame[10:30])
	if _, ok := scanner.NextFrame(); ok {
		t.Fatal("expected no frame yet")
	}

	scanner.Feed(frame[30:])
	result, ok := scanner.NextFrame()
	if !ok {
		t.Fatal("expected frame")
	}
	if len(result) != 64 {
		t.Fatalf("expected frame size 64, got %d", len(result))
	}
}

func buildTestFrame(spacecraftID uint16, apids []uint16) []byte {
	frame := make([]byte, 1024)

	binary.BigEndian.PutUint32(frame[0:4], ASM)

	var word0 uint32 = (0 << 30) | (uint32(spacecraftID) << 20) | (1 << 17) | 0x01
	binary.BigEndian.PutUint32(frame[4:8], word0)

	binary.BigEndian.PutUint16(frame[8:10], 0x0001)
	binary.BigEndian.PutUint16(frame[10:12], 0x0000)

	offset := ASMSize + PrimaryHeaderSize

	for _, apid := range apids {
		var pktWord0 uint16 = (0 << 13) | (0 << 12) | (1 << 11) | apid
		binary.BigEndian.PutUint16(frame[offset:offset+2], pktWord0)

		var pktWord1 uint16 = (0x03 << 14) | 0x0001
		binary.BigEndian.PutUint16(frame[offset+2:offset+4], pktWord1)

		payloadLen := 8 + 4
		binary.BigEndian.PutUint16(frame[offset+4:offset+6], uint16(payloadLen+1))

		binary.BigEndian.PutUint64(frame[offset+6:offset+14], 0x0000010000000001)
		copy(frame[offset+14:offset+18], []byte{0xCA, 0xFE, 0xBA, 0xBE})

		offset += PacketPrimaryHeaderSize + payloadLen + 1
	}

	return frame[:1024]
}
