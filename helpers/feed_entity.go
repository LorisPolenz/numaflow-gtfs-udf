package helpers

import (
	"numaflow_gtfs_udf/transformer"
)

type EnrichedFeedMessage struct {
	*FeedMessage
	EnrichedFeedEntities []EnrichedFeedEntity `json:"entity"`
}

type EnrichedFeedEntity struct {
	*TransitFeedEntity
	EnrichedTripUpdate EnrichedTripUpdate `json:"trip_update"`
}

type EnrichedTripUpdate struct {
	*TripUpdate
	EnrichedStopTimeUpdates []EnrichedStopTimeUpdate `json:"stop_time_update"`
	EnrichedRoute           transformer.RouteDB      `json:"route"`
	EnrichedTrip            transformer.TripDB       `json:"trip_enriched"`
}

type EnrichedStopTimeUpdate struct {
	*TripUpdate_StopTimeUpdate
	Stop                       transformer.StopDB     `json:"stop"`
	StopTime                   transformer.StopTimeDB `json:"stop_time"`
	ScheduleRelationShipString string                 `json:"schedule_relationship_string"`
}

func NewEnrichedFeedMessage(fm *FeedMessage, efe []EnrichedFeedEntity) *EnrichedFeedMessage {
	return &EnrichedFeedMessage{
		FeedMessage:          fm,
		EnrichedFeedEntities: efe,
	}
}

func NewEnrichedFeedEntity(fe *TransitFeedEntity, etu EnrichedTripUpdate) *EnrichedFeedEntity {
	return &EnrichedFeedEntity{
		TransitFeedEntity:  fe,
		EnrichedTripUpdate: etu,
	}
}

func NewEnrichedTripUpdate(tu *TripUpdate, estu []EnrichedStopTimeUpdate, eru *transformer.RouteDB, et *transformer.TripDB) *EnrichedTripUpdate {
	return &EnrichedTripUpdate{
		TripUpdate:              tu,
		EnrichedStopTimeUpdates: estu,
		EnrichedRoute:           *eru,
		EnrichedTrip:            *et,
	}
}

func NewEnrichedStopTimeUpdate(stu *TripUpdate_StopTimeUpdate, stop transformer.StopDB, stopTime transformer.StopTimeDB, srs string) *EnrichedStopTimeUpdate {
	return &EnrichedStopTimeUpdate{
		TripUpdate_StopTimeUpdate:  stu,
		Stop:                       stop,
		StopTime:                   stopTime,
		ScheduleRelationShipString: srs,
	}
}
