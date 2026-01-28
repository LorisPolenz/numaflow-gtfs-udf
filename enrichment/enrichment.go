package enrichment

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"numaflow_gtfs_udf/helpers"
	"numaflow_gtfs_udf/pipeline"
	"numaflow_gtfs_udf/transformer"
	"time"

	"github.com/numaproj/numaflow-go/pkg/mapper"
)

type StopLocation struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type IndexDocument struct {
	helpers.EnrichedStopTimeUpdate
	Route        transformer.RouteDB     `json:"route"`
	TripEnriched transformer.TripDB      `json:"trip_enriched"`
	Trip         *helpers.TripDescriptor `json:"trip"`
	StopLocation StopLocation            `json:"stop_location"`
	Timestamp    string                  `json:"@timestamp"`
}

func EnrichFeedEntity(feedEntity helpers.TransitFeedEntity) mapper.Messages {

	if feedEntity.GetTripUpdate() == nil {
		slog.Debug("No TripUpdate found in FeedEntity")
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	if feedEntity.TripUpdate.GetStopTimeUpdate() == nil {
		slog.Debug("No StopTimeUpdate found in TripUpdate")
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	if feedEntity.TripUpdate.GetTrip() == nil {
		slog.Debug("No Trip found in TripUpdate")
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	p0 := pipeline.NewPipeline("Enrich Feed Entity Pipeline")

	enrichRoute := transformer.NewEnrichRouteByID(feedEntity.TripUpdate.Trip.GetRouteId(), feedEntity.GetFeedVersion())
	enrichTrip := transformer.NewEnrichTripByID(feedEntity.TripUpdate.Trip.GetTripId(), feedEntity.GetFeedVersion())

	p0.
		AddStage("enrich Route by ID", enrichRoute).
		AddStage("enrich Trip by ID", enrichTrip)

	now := time.Now()
	p0.Run()
	slog.Info(fmt.Sprintf("Enrichment Pipeline p0 completed in %s", time.Since(now)))

	if enrichRoute.Route == nil {
		slog.Error(fmt.Sprintf("Failed to enrich Route for Route ID %s", feedEntity.TripUpdate.Trip.GetRouteId()))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	if enrichTrip.Trip == nil {
		slog.Error(fmt.Sprintf("Failed to enrich Trip for Trip ID %s", feedEntity.TripUpdate.Trip.GetTripId()))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	enrichedStopTimeUpdates := []helpers.EnrichedStopTimeUpdate{}

	now = time.Now()

	stopTimes, err := transformer.FetchStopTimesByTripID(feedEntity.GetFeedVersion(), feedEntity.TripUpdate.Trip.GetTripId())

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to fetch Stop Times for Trip ID %s: %s", feedEntity.TripUpdate.Trip.GetTripId(), err))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	slog.Info(fmt.Sprintf("Enrichment stoptimes enrichment completed in %s", time.Since(now)))

	stops, err := transformer.FetchStopByStopTimes(feedEntity.GetFeedVersion(), stopTimes)

	now = time.Now()

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to fetch Stops for Stop Times of Trip ID %s: %s", feedEntity.TripUpdate.Trip.GetTripId(), err))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	slog.Info(fmt.Sprintf("Enrichment stops enrichment completed in %s", time.Since(now)))

	for _, stu := range feedEntity.TripUpdate.StopTimeUpdate {
		p1 := pipeline.NewPipeline("Process Stop Time Update")

		splitStopID := transformer.NewSplit(*stu.StopId, ":")

		p1.
			AddStage("split Stop ID by ':'", splitStopID)
		p1.Run()

		p2 := pipeline.NewPipeline("Enrich Stop Name Pipeline")

		enrichStopName := transformer.NewEnrichStopByID(stops, splitStopID.Parts[0])
		enrichStopTime := transformer.NewEnrichStopTimeByTripID(stopTimes, splitStopID.Parts[0])

		p2.
			AddStage("enrich Stop Name by ID", enrichStopName).
			AddStage("enrich Stop Times by Trip ID", enrichStopTime)

		now = time.Now()
		p2.Run()
		slog.Info(fmt.Sprintf("Enrichment pipline p2 completed in %s", time.Since(now)))

		slog.Debug("Enriched Stop Time", "stop_time", enrichStopTime.StopTime.StopID, "stop_id", enrichStopTime.StopTime.StopID)

		enrichedStopTimeUpdate := helpers.NewEnrichedStopTimeUpdate(stu, *enrichStopName.Stop, *enrichStopTime.StopTime, stu.GetScheduleRelationship().String())

		enrichedStopTimeUpdates = append(enrichedStopTimeUpdates, *enrichedStopTimeUpdate)
	}

	enrichedTripUpdate := helpers.NewEnrichedTripUpdate(feedEntity.TripUpdate, enrichedStopTimeUpdates, enrichRoute.Route, enrichTrip.Trip)
	enrichedFeedEntity := helpers.NewEnrichedFeedEntity(&feedEntity, *enrichedTripUpdate)

	now = time.Now()

	var messagesBuilder = mapper.MessagesBuilder()

	for _, stu := range enrichedFeedEntity.EnrichedTripUpdate.EnrichedStopTimeUpdates {
		slog.Debug("Preparing Index Document for Stop Time Update", "stop_time", stu.StopTime.StopID, "stop_id", stu.Stop.StopID)
		doc := IndexDocument{
			EnrichedStopTimeUpdate: stu,
			Route:                  enrichedFeedEntity.EnrichedTripUpdate.EnrichedRoute,
			TripEnriched:           enrichedFeedEntity.EnrichedTripUpdate.EnrichedTrip,
			Trip:                   feedEntity.GetTripUpdate().GetTrip(),
			Timestamp:              time.Now().UTC().Format(time.RFC3339),
			StopLocation: StopLocation{
				Lat: stu.Stop.Lat,
				Lon: stu.Stop.Lon,
			},
		}

		docJson, err := json.Marshal(doc)

		if err != nil {
			slog.Error("Failed to marshal index document", "error", err)
			continue
		}

		messagesBuilder = messagesBuilder.Append(mapper.NewMessage(docJson))
	}

	slog.Info(fmt.Sprintf("Time building took in %s", time.Since(now)))

	return messagesBuilder

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
