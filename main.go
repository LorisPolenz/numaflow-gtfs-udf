package main

import (
	"context"
	"encoding/json"
	"log"
	"numaflow_gtfs_udf/duckdb"
	"numaflow_gtfs_udf/helpers"

	"github.com/numaproj/numaflow-go/pkg/mapper"
)

type FeedMetadata struct {
	FeedName    string
	LastUpdated string
}

type RecentFeeds struct {
	Feeds []FeedMetadata
}

func mapFn(_ context.Context, _ []string, d mapper.Datum) mapper.Messages {
	msg := d.Value()

	id, name := duckdb.TestDBConnection(string(msg))

	log.Printf("From DuckDB - id: %s, name: %s\n", id, name)

	feedEntity := helpers.UnmarshallFeedEntity(msg)

	log.Printf("Entity - id: %s", feedEntity.GetId())
	log.Printf("StopTimeUpdate - Count %d, ", len(feedEntity.TripUpdate.GetStopTimeUpdate()))

	feedEntityJson, err := json.Marshal(feedEntity)

	if err != nil {
		log.Panic("Failed to marshal feed entity: ", err)
	}

	return mapper.MessagesBuilder().Append(mapper.NewMessage(feedEntityJson))
}

func main() {
	helpers.VerifyEnv()
	duckdb.InitDBHousekeeper()

	err := mapper.NewServer(mapper.MapperFunc(mapFn)).Start(context.Background())

	if err != nil {
		log.Panic("Failed to start map function server: ", err)
	}
}
