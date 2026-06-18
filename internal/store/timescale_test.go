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

func TestIsValidValueNormal(t *testing.T) {
	if !IsValidValue(0) {
		t.Error("0 should be valid")
	}
	if !IsValidValue(42.5) {
		t.Error("42.5 should be valid")
	}
	if !IsValidValue(-999.9) {
		t.Error("-999.9 should be valid")
	}
	if !IsValidValue(1e7) {
		t.Error("1e7 should be valid")
	}
}

func TestIsValidValueNaN(t *testing.T) {
	if IsValidValue(math.NaN()) {
		t.Error("NaN should be invalid")
	}
}

func TestIsValidValueInf(t *testing.T) {
	if IsValidValue(math.Inf(1)) {
		t.Error("+Inf should be invalid")
	}
	if IsValidValue(math.Inf(-1)) {
		t.Error("-Inf should be invalid")
	}
}

func TestIsValidValueOutOfRange(t *testing.T) {
	if IsValidValue(1e9) {
		t.Error("1e9 exceeds MaxTelemetryValue and should be invalid")
	}
	if IsValidValue(-1e9) {
		t.Error("-1e9 below MinTelemetryValue and should be invalid")
	}
	if !IsValidValue(MaxTelemetryValue) {
		t.Error("exactly MaxTelemetryValue should be valid")
	}
	if !IsValidValue(MinTelemetryValue) {
		t.Error("exactly MinTelemetryValue should be valid")
	}
}

func TestCleanPointOutOfRangeRawValue(t *testing.T) {
	raw := TelemetryPoint{
		Timestamp: time.Now().UTC(),
		APID:      100,
		SensorID:  1,
		RawValue:  9e8,
		Value:     0,
		Quality:   0,
	}

	cleaned := CleanPoint(raw)
	if cleaned.Quality != 2 {
		t.Errorf("expected quality 2 for out-of-range raw, got %d", cleaned.Quality)
	}
	if cleaned.Value != 0 {
		t.Errorf("expected value 0 for out-of-range raw, got %f", cleaned.Value)
	}
}

func TestCleanPointOutOfRangeEngineeredValue(t *testing.T) {
	raw := TelemetryPoint{
		Timestamp: time.Now().UTC(),
		APID:      100,
		SensorID:  1,
		RawValue:  -5e8,
		Value:     -5e8,
		Quality:   1,
	}

	cleaned := CleanPoint(raw)
	if cleaned.Quality != 2 {
		t.Errorf("expected quality 2 for out-of-range raw value, got %d", cleaned.Quality)
	}
	if cleaned.Value != 0 {
		t.Errorf("expected value 0 for out-of-range raw, got %f", cleaned.Value)
	}
}
