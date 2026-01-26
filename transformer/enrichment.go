package transformer

import (
	"numaflow_gtfs_udf/duckdb"
)

type EnrichStopTimesByTripID struct {
	stopTimes map[string]StopTimeDB
	StopTime  StopTimeDB
	StopID    string
}

type StopTimeDB struct {
	TripID        string `json:"trip_id"`
	ArrivalTime   string `json:"arrival_time"`
	DepartureTime string `json:"departure_time"`
	StopID        string `json:"stop_id"`
	StopSequence  int    `json:"stop_sequence"`
	PickupType    int    `json:"pickup_type"`
	DropOffType   int    `json:"drop_off_type"`
}

func FetchStopTimesByTripID(feedVersion string, tripID string) (map[string]StopTimeDB, error) {

	db, err := duckdb.GetDuckDB(feedVersion)

	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type FROM stop_times WHERE trip_id == ?", tripID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stopTimes := make(map[string]StopTimeDB)

	for rows.Next() {
		var stopTime StopTimeDB

		err := rows.Scan(&stopTime.TripID, &stopTime.ArrivalTime, &stopTime.DepartureTime, &stopTime.StopID, &stopTime.StopSequence, &stopTime.PickupType, &stopTime.DropOffType)
		if err != nil {
			return nil, err
		}
		stopTimes[stopTime.StopID] = stopTime
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return stopTimes, nil
}

func NewEnrichStopTimeByTripID(stopTimes map[string]StopTimeDB, stopID string, feedVersion string) *EnrichStopTimesByTripID {
	return &EnrichStopTimesByTripID{stopTimes: stopTimes, StopID: stopID}
}

func (e *EnrichStopTimesByTripID) Transform() {
	stopTime := e.stopTimes[e.StopID]

	e.StopTime = stopTime
}
