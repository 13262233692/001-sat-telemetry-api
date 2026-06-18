package store

import (
	"math"
	"testing"
	"time"
)

func TestCleanPointNormal(t *testing.T) {
	now := time.Now().UTC()
	raw := TelemetryPoint{
		Timestamp: now,
		APID:      100,
		SensorID:  1,
		RawValue:  42.5,
		Value:     0,
		Unit:      "deg",
		Quality:   0,
	}

	cleaned := CleanPoint(raw)
	if cleaned.Value != 42.5 {
		t.Errorf("expected value 42.5, got %f", cleaned.Value)
	}
	if cleaned.Quality != 1 {
		t.Errorf("expected quality 1, got %d", cleaned.Quality)
	}
	if !cleaned.Timestamp.Equal(now) {
		t.Error("timestamp should be preserved")
	}
}

func TestCleanPointNaN(t *testing.T) {
	raw := TelemetryPoint{
		Timestamp: time.Now().UTC(),
		APID:      100,
		SensorID:  1,
		RawValue:  math.NaN(),
		Value:     0,
		Quality:   0,
	}

	cleaned := CleanPoint(raw)
	if cleaned.Quality != 2 {
		t.Errorf("expected quality 2 for NaN, got %d", cleaned.Quality)
	}
	if cleaned.Value != 0 {
		t.Errorf("expected value 0 for NaN, got %f", cleaned.Value)
	}
}

func TestCleanPointInf(t *testing.T) {
	raw := TelemetryPoint{
		Timestamp: time.Now().UTC(),
		APID:      100,
		SensorID:  1,
		RawValue:  math.Inf(1),
		Value:     0,
		Quality:   0,
	}

	cleaned := CleanPoint(raw)
	if cleaned.Quality != 2 {
		t.Errorf("expected quality 2 for Inf, got %d", cleaned.Quality)
	}
}

func TestCleanPointZeroTimestamp(t *testing.T) {
	before := time.Now().UTC()
	raw := TelemetryPoint{
		Timestamp: time.Time{},
		APID:      100,
		SensorID:  1,
		RawValue:  10.0,
		Quality:   0,
	}

	cleaned := CleanPoint(raw)
	after := time.Now().UTC()

	if cleaned.Timestamp.Before(before) || cleaned.Timestamp.After(after) {
		t.Error("zero timestamp should be replaced with current time")
	}
}
