package ccsds

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	ASM             = 0x1ACFFC1D
	ASMSize         = 4
	PrimaryHeaderSize = 6
	PacketPrimaryHeaderSize = 6
	MaxFrameSize    = 2048
	MinFrameSize    = ASMSize + PrimaryHeaderSize
)

var (
	ErrInvalidASM        = errors.New("ccsds: invalid attached sync marker")
	ErrFrameTooShort     = errors.New("ccsds: frame too short")
	ErrInvalidFrameLen   = errors.New("ccsds: declared frame length exceeds buffer")
	ErrInvalidPacket     = errors.New("ccsds: invalid source packet")
)

type TransferFrameHeader struct {
	VersionNumber    uint8
	SpacecraftID    uint16
	VirtualChannelID uint8
	OCFFlag         bool
	MasterFrameCount uint8
	VCFrameCount    uint8
	SecondaryHeaderFlag bool
	SyncFlag        bool
	PacketOrderFlag bool
	SegmentLengthID uint8
	FirstHeaderPointer uint16
	DataFieldStatus uint16
}

type SourcePacketHeader struct {
	VersionNumber   uint8
	Type            uint8
	SecondaryHeaderFlag bool
	APID            uint16
	SequenceFlags   uint8
	SequenceCount   uint16
	DataLength      uint16
}

type SourcePacket struct {
	Header    SourcePacketHeader
	Timestamp uint64
	Payload   []byte
}

type TransferFrame struct {
	Header  TransferFrameHeader
	Packets []SourcePacket
	RawData []byte
}

func ParseASM(data []byte) (int, error) {
	if len(data) < ASMSize {
		return 0, ErrFrameTooShort
	}
	marker := binary.BigEndian.Uint32(data[0:ASMSize])
	if marker != ASM {
		return 0, ErrInvalidASM
	}
	return ASMSize, nil
}

func FindASMBoundary(data []byte) int {
	for i := 0; i <= len(data)-ASMSize; i++ {
		marker := binary.BigEndian.Uint32(data[i : i+ASMSize])
		if marker == ASM {
			return i
		}
	}
	return -1
}

func ParsePrimaryHeader(data []byte) (*TransferFrameHeader, error) {
	if len(data) < PrimaryHeaderSize {
		return nil, ErrFrameTooShort
	}

	word0 := binary.BigEndian.Uint32(data[0:4])
	word1 := binary.BigEndian.Uint16(data[4:6])

	h := &TransferFrameHeader{
		VersionNumber:    uint8(word0 >> 30 & 0x03),
		SpacecraftID:     uint16(word0 >> 20 & 0x3FF),
		VirtualChannelID: uint8(word0 >> 17 & 0x07),
		OCFFlag:          word0&0x10000 != 0,
		MasterFrameCount: uint8(word0 & 0xFF),
		VCFrameCount:     uint8(word1 >> 8),
		DataFieldStatus:  uint16(word1 & 0xFF),
	}

	status := h.DataFieldStatus
	h.SecondaryHeaderFlag = status&0x8000 != 0
	h.SyncFlag = status&0x4000 != 0
	h.PacketOrderFlag = status&0x2000 != 0
	h.SegmentLengthID = uint8(status >> 11 & 0x03)
	h.FirstHeaderPointer = status & 0x07FF

	return h, nil
}

func ParseSourcePacket(data []byte) (*SourcePacket, error) {
	if len(data) < PacketPrimaryHeaderSize {
		return nil, ErrInvalidPacket
	}

	word0 := binary.BigEndian.Uint16(data[0:2])
	word1 := binary.BigEndian.Uint16(data[2:4])
	word2 := binary.BigEndian.Uint16(data[4:6])

	h := SourcePacketHeader{
		VersionNumber:      uint8(word0 >> 13 & 0x07),
		Type:              uint8(word0 >> 12 & 0x01),
		SecondaryHeaderFlag: word0&0x0800 != 0,
		APID:              word0 & 0x07FF,
		SequenceFlags:     uint8(word1 >> 14),
		SequenceCount:     word1 & 0x3FFF,
		DataLength:        word2,
	}

	totalLen := PacketPrimaryHeaderSize + int(h.DataLength) + 1
	if len(data) < totalLen {
		return nil, fmt.Errorf("%w: need %d bytes, have %d", ErrInvalidPacket, totalLen, len(data))
	}

	packetData := data[PacketPrimaryHeaderSize:totalLen]

	var timestamp uint64
	payloadStart := 0
	if h.SecondaryHeaderFlag && len(packetData) >= 8 {
		timestamp = binary.BigEndian.Uint64(packetData[0:8])
		payloadStart = 8
	}

	payload := make([]byte, len(packetData)-payloadStart)
	copy(payload, packetData[payloadStart:])

	return &SourcePacket{
		Header:    h,
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}

func ParseTransferFrame(data []byte) (*TransferFrame, error) {
	if len(data) < MinFrameSize {
		return nil, ErrFrameTooShort
	}

	_, err := ParseASM(data)
	if err != nil {
		return nil, err
	}

	header, err := ParsePrimaryHeader(data[ASMSize:])
	if err != nil {
		return nil, err
	}

	frameDataStart := ASMSize + PrimaryHeaderSize
	if header.SecondaryHeaderFlag {
		if len(data) < frameDataStart+1 {
			return nil, ErrFrameTooShort
		}
		secHeaderLen := int(data[frameDataStart])
		if secHeaderLen > 0 {
			frameDataStart += secHeaderLen
		}
	}

	if frameDataStart >= len(data) {
		return &TransferFrame{
			Header:  *header,
			Packets: nil,
			RawData: data,
		}, nil
	}

	frameData := data[frameDataStart:]
	var packets []SourcePacket

	if !header.SyncFlag {
		offset := 0
		for offset < len(frameData) {
			if offset+PacketPrimaryHeaderSize > len(frameData) {
				break
			}
			if isIdleData(frameData[offset:]) {
				break
			}
			pkt, err := ParseSourcePacket(frameData[offset:])
			if err != nil {
				offset++
				continue
			}
			pktLen := PacketPrimaryHeaderSize + int(pkt.Header.DataLength) + 1
			if pktLen <= 0 || offset+pktLen > len(frameData) {
				break
			}
			packets = append(packets, *pkt)
			offset += pktLen
		}
	}

	return &TransferFrame{
		Header:  *header,
		Packets: packets,
		RawData: data,
	}, nil
}

func isIdleData(data []byte) bool {
	if len(data) < PacketPrimaryHeaderSize {
		return true
	}
	for i := 0; i < PacketPrimaryHeaderSize; i++ {
		if data[i] != 0x00 {
			return false
		}
	}
	return true
}

type FrameScanner struct {
	buffer    []byte
	frameSize int
}

func NewFrameScanner(frameSize int) *FrameScanner {
	return &FrameScanner{
		buffer:    make([]byte, 0, MaxFrameSize*2),
		frameSize: frameSize,
	}
}

func (s *FrameScanner) Feed(data []byte) {
	s.buffer = append(s.buffer, data...)
}

func (s *FrameScanner) NextFrame() ([]byte, bool) {
	idx := FindASMBoundary(s.buffer)
	if idx < 0 {
		if len(s.buffer) > ASMSize {
			s.buffer = s.buffer[len(s.buffer)-ASMSize+1:]
		}
		return nil, false
	}

	if idx > 0 {
		s.buffer = s.buffer[idx:]
	}

	need := s.frameSize
	if need <= 0 {
		need = MaxFrameSize
	}

	if len(s.buffer) < need {
		return nil, false
	}

	frame := make([]byte, need)
	copy(frame, s.buffer[:need])
	s.buffer = s.buffer[need:]
	return frame, true
}

func (s *FrameScanner) Reset() {
	s.buffer = s.buffer[:0]
}
