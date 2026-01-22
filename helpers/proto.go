package helpers

import (
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

func UnmarshallFeedEntity(data []byte) *FeedEntity {
	feedEntity := &FeedEntity{}

	if err := proto.Unmarshal(data, feedEntity); err != nil {
		log.Fatalln("Failed to unmarshal feed entity:", err)
		os.Exit(1)
	}

	return feedEntity
}
