package transformer

import (
	"numaflow_gtfs_udf/duckdb"
	"strings"
)

type EnrichStopTimesByTripID struct {
	stopTimes map[string]StopTimeDB
	StopTime  *StopTimeDB
	StopID    string
}

type EnrichStopNameByID struct {
	feedVersion string
	StopID      string
	Stop        *StopDB
}

type EnrichRouteByID struct {
	feedVersion string
	RouteID     string
	Route       *RouteDB
}

type EnrichTripByID struct {
	feedVersion string
	TripID      string
	Trip        *TripDB
}

type StopTimeDB struct {
	TripID        string `json:"trip_id"`
	ArrivalTime   string `json:"arrival_time"`
	DepartureTime string `json:"departure_time"`
	StopID        string `json:"stop_id"`
	StopSequence  string `json:"stop_sequence"`
	PickupType    string `json:"pickup_type"`
	DropOffType   string `json:"drop_off_type"`
}

type RouteDB struct {
	RouteID        string `json:"route_id"`
	AgencyID       string `json:"agency_id"`
	RouteShortName string `json:"route_short_name"`
	RouteLongName  string `json:"route_long_name"`
	RouteDesc      string `json:"route_desc"`
	RouteType      string `json:"route_type"`
}

type StopDB struct {
	StopID        string  `json:"stop_id"`
	StopName      string  `json:"stop_name"`
	Lat           float64 `json:"stop_lat"`
	Lon           float64 `json:"stop_lon"`
	LocationType  string  `json:"location_type"`
	ParentStation string  `json:"parent_station"`
	PlatformCode  string  `json:"platform_code"`
}

type TripDB struct {
	RouteID        string `json:"route_id"`
	ServiceID      string `json:"service_id"`
	TripID         string `json:"trip_id"`
	TripHeadsign   string `json:"trip_headsign"`
	TripShortName  string `json:"trip_short_name"`
	DirectionID    string `json:"direction_id"`
	BlockID        string `json:"block_id"`
	OriginalTripID string `json:"original_trip_id"`
	Hints          string `json:"hints"`
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

		stopIDParts := strings.Split(stopTime.StopID, ":")

		stopTimes[stopIDParts[0]] = stopTime
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return stopTimes, nil
}

func FetchRouteByID(feedVersion string, routeID string) (*RouteDB, error) {
	var route RouteDB

	db, err := duckdb.GetDuckDB(feedVersion)

	if err != nil {
		return nil, err
	}

	row := db.QueryRow("SELECT route_id, agency_id, route_short_name, route_long_name, route_desc, route_type FROM routes WHERE route_id = '" + routeID + "' LIMIT 1;")

	err = row.Scan(
		&route.RouteID,
		&route.AgencyID,
		&route.RouteShortName,
		&route.RouteLongName,
		&route.RouteDesc,
		&route.RouteType,
	)

	if err != nil {
		return nil, err
	}

	return &route, nil
}

func FetchStopByID(feedVersion string, sourceStopID string) (*StopDB, error) {
	var stop StopDB

	db, err := duckdb.GetDuckDB(feedVersion)

	if err != nil {
		return nil, err
	}

	row := db.QueryRow("SELECT stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, platform_code FROM stops WHERE stop_id = 'Parent" + sourceStopID + "' LIMIT 1;")

	err = row.Scan(
		&stop.StopID,
		&stop.StopName,
		&stop.Lat,
		&stop.Lon,
		&stop.LocationType,
		&stop.ParentStation,
		&stop.PlatformCode,
	)

	if err != nil {
		return nil, err
	}

	return &stop, nil
}

func FetchTripByID(feedVersion string, tripID string) (*TripDB, error) {
	var trip TripDB

	db, err := duckdb.GetDuckDB(feedVersion)

	if err != nil {
		return nil, err
	}

	row := db.QueryRow("SELECT route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id, block_id, original_trip_id, hints FROM trips WHERE trip_id = '" + tripID + "' LIMIT 1;")

	err = row.Scan(
		&trip.RouteID,
		&trip.ServiceID,
		&trip.TripID,
		&trip.TripHeadsign,
		&trip.TripShortName,
		&trip.DirectionID,
		&trip.BlockID,
		&trip.OriginalTripID,
		&trip.Hints,
	)

	if err != nil {
		return nil, err
	}

	return &trip, nil
}

// Contruct type
func NewEnrichStopTimeByTripID(stopTimes map[string]StopTimeDB, stopID string, feedVersion string) *EnrichStopTimesByTripID {
	return &EnrichStopTimesByTripID{stopTimes: stopTimes, StopID: stopID}
}

func NewEnrichStopByID(stopID string, feedVersion string) *EnrichStopNameByID {
	return &EnrichStopNameByID{StopID: stopID, feedVersion: feedVersion}
}

func NewEnrichRouteByID(routeID string, feedVersion string) *EnrichRouteByID {
	return &EnrichRouteByID{RouteID: routeID, feedVersion: feedVersion}
}

func NewEnrichTripByID(tripID string, feedVersion string) *EnrichTripByID {
	return &EnrichTripByID{TripID: tripID, feedVersion: feedVersion}
}

func (e *EnrichStopTimesByTripID) Transform() {
	stopTime := e.stopTimes[e.StopID]

	e.StopTime = &stopTime
}

func (e *EnrichStopNameByID) Transform() {
	stop, err := FetchStopByID(e.feedVersion, e.StopID)

	if err != nil {
		return
	}
	e.Stop = stop
}

func (e *EnrichRouteByID) Transform() {
	route, err := FetchRouteByID(e.feedVersion, e.RouteID)

	if err != nil {
		return
	}
	e.Route = route
}

func (e *EnrichTripByID) Transform() {
	trip, err := FetchTripByID(e.feedVersion, e.TripID)

	if err != nil {
		return
	}
	e.Trip = trip
}
