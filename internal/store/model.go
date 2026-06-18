package store

import "time"

type TelemetryPoint struct {
	Timestamp time.Time `json:"timestamp" db:"timestamp"`
	APID      int       `json:"apid" db:"apid"`
	SensorID  int       `json:"sensor_id" db:"sensor_id"`
	RawValue  float64   `json:"raw_value" db:"raw_value"`
	Value     float64   `json:"value" db:"value"`
	Unit      string    `json:"unit" db:"unit"`
	Quality   int       `json:"quality" db:"quality"`
}

type QueryParams struct {
	APID     int       `json:"apid"`
	SensorID int       `json:"sensor_id"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Limit    int       `json:"limit"`
	Offset   int       `json:"offset"`
}

type QueryResult struct {
	Points []TelemetryPoint `json:"points"`
	Total  int              `json:"total"`
}
