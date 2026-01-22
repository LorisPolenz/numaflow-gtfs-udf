package helpers

import (
	"log"

	"google.golang.org/protobuf/proto"
)

func UnmarshallFeedEntity(data []byte) (*FeedEntity, error) {
	feedEntity := &FeedEntity{}

	if err := proto.Unmarshal(data, feedEntity); err != nil {
		log.Fatalln("Failed to unmarshal feed entity:", err)
		return nil, err
	}

	return feedEntity, nil
}
