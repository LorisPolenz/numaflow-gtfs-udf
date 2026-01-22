package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"numaflow_gtfs_udf/duckdb"
	"numaflow_gtfs_udf/helpers"
	"os"

	"github.com/numaproj/numaflow-go/pkg/mapper"
)

func mapFn(_ context.Context, _ []string, d mapper.Datum) mapper.Messages {
	msg := d.Value()

	slog.Info(fmt.Sprintf("Processing msg with len: %d", len(msg)))

	feedEntity, err := helpers.UnmarshallFeedEntity(msg)

	slog.Info(fmt.Sprintf("Processing record with FeedVersion: %s", feedEntity.GetFeedVersion()))

	id, name, err := duckdb.TestDBConnection(feedEntity.GetFeedVersion())

	slog.Info(fmt.Sprintf("From DuckDB - id: %s, name: %s\n", id, name))

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to unmarshal feed entity: %s", err))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	slog.Info(fmt.Sprintf("Entity - id: %s", feedEntity.GetId()))
	slog.Info(fmt.Sprintf("StopTimeUpdate - Count %d, ", len(feedEntity.TripUpdate.GetStopTimeUpdate())))

	feedEntityJson, err := json.Marshal(feedEntity)

	if err != nil {
		slog.Info(fmt.Sprintf("Failed to marshal feed entity: %s", err))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	return mapper.MessagesBuilder().Append(mapper.NewMessage(feedEntityJson))
}

func main() {
	helpers.VerifyEnv()
	duckdb.InitDBHousekeeper()

	err := mapper.NewServer(mapper.MapperFunc(mapFn)).Start(context.Background())

	if err != nil {
		slog.Error(fmt.Sprintf("Mapper server failed to start: %s", err))
		os.Exit(1)
	}
}
