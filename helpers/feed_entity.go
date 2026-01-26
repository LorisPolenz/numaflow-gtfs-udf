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
}

type EnrichedStopTimeUpdate struct {
	*TripUpdate_StopTimeUpdate
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

func NewEnrichedTripUpdate(tu *TripUpdate, estu []EnrichedStopTimeUpdate) *EnrichedTripUpdate {
	return &EnrichedTripUpdate{
		TripUpdate:              tu,
		EnrichedStopTimeUpdates: estu,
	}
}

func NewEnrichedStopTimeUpdate(stu *TripUpdate_StopTimeUpdate, stopTime transformer.StopTimeDB, srs string) *EnrichedStopTimeUpdate {
	return &EnrichedStopTimeUpdate{
		TripUpdate_StopTimeUpdate:  stu,
		StopTime:                   stopTime,
		ScheduleRelationShipString: srs,
	}
}
