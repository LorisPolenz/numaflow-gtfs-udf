package enrichment

import (
	"encoding/json"
	"log/slog"
	"numaflow_gtfs_udf/helpers"
	"numaflow_gtfs_udf/pipeline"
	"numaflow_gtfs_udf/transformer"

	"github.com/numaproj/numaflow-go/pkg/mapper"
)

func EnrichFeedEntity(feedEntity helpers.TransitFeedEntity) mapper.Messages {

	if feedEntity.GetTripUpdate() == nil {
		slog.Debug("No TripUpdate found in FeedEntity")
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	if feedEntity.TripUpdate.GetStopTimeUpdate() == nil {
		slog.Debug("No StopTimeUpdate found in TripUpdate")
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	enrichedStopTimeUpdates := []helpers.EnrichedStopTimeUpdate{}

	stopTimes, err := transformer.FetchStopTimesByTripID(feedEntity.GetFeedVersion(), feedEntity.TripUpdate.Trip.GetTripId())

	for _, stu := range feedEntity.TripUpdate.StopTimeUpdate {
		p1 := pipeline.NewPipeline("Process Stop Time Update")

		splitStopID := transformer.NewSplit(*stu.StopId, ":")

		p1.
			AddStage("split Stop ID by ':'", splitStopID)

		p1.Run()

		p2 := pipeline.NewPipeline("Enrich Stop Name Pipeline")

		enrichTripTime := transformer.NewEnrichStopTimeByTripID(stopTimes, splitStopID.Parts[0], feedEntity.GetFeedVersion())

		p2.
			AddStage("enrich Stop Times by Trip ID", enrichTripTime)

		p2.Run()

		slog.Info("Enriched Stop Time", "stop_time", enrichTripTime.StopTime.StopID, "stop_id", enrichTripTime.StopTime.StopID)

		enrichedStopTimeUpdate := helpers.NewEnrichedStopTimeUpdate(stu, enrichTripTime.StopTime, stu.GetScheduleRelationship().String())

		enrichedStopTimeUpdates = append(enrichedStopTimeUpdates, *enrichedStopTimeUpdate)
	}

	enrichedTripUpdate := helpers.NewEnrichedTripUpdate(feedEntity.TripUpdate, enrichedStopTimeUpdates)
	enrichedFeedEntity := helpers.NewEnrichedFeedEntity(&feedEntity, *enrichedTripUpdate)

	enrichedFeedEntityJson, err := json.Marshal(enrichedFeedEntity)

	if err != nil {
		slog.Error("Failed to marshal enriched feed entity", "error", err)
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	return mapper.MessagesBuilder().Append(mapper.NewMessage(enrichedFeedEntityJson))

	// slog.Info(fmt.Sprintf("Processing record with FeedVersion: %s", feedEntity.GetFeedVersion()))

	// id, name, err := duckdb.TestDBConnection(feedEntity.GetFeedVersion())

	// slog.Info(fmt.Sprintf("From DuckDB - id: %s, name: %s\n", id, name))

	// if err != nil {
	// 	slog.Error(fmt.Sprintf("Failed to unmarshal feed entity: %s", err))
	// 	return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	// }

	// slog.Info(fmt.Sprintf("Entity - id: %s", feedEntity.GetId()))
	// slog.Info(fmt.Sprintf("StopTimeUpdate - Count %d, ", len(feedEntity.TripUpdate.GetStopTimeUpdate())))
}
