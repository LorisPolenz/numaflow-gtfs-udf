package main

import (
	"context"
	"fmt"
	"log/slog"
	"numaflow_gtfs_udf/duckdb"
	"numaflow_gtfs_udf/enrichment"
	"numaflow_gtfs_udf/helpers"
	"os"

	"github.com/numaproj/numaflow-go/pkg/mapper"
)

func mapFn(_ context.Context, _ []string, d mapper.Datum) mapper.Messages {
	msg := d.Value()

	slog.Info(fmt.Sprintf("Processing msg with len: %d", len(msg)))

	feedEntity, err := helpers.UnmarshallFeedEntity(msg)

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to unmarshal feed entity: %s", err))
		return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
	}

	messages := enrichment.EnrichFeedEntity(*feedEntity)

	return messages
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
