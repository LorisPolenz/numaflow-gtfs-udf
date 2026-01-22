package helpers

import (
	"bytes"
	"encoding/gob"
	"log"
)

type TransitFeedEntity struct {
	FeedVersion string
	*FeedEntity
}

func UnmarshallFeedEntity(data []byte) (*FeedEntity, error) {
	var feedEntity FeedEntity

	gobDec := gob.NewDecoder(bytes.NewBuffer(data))

	if err := gobDec.Decode(&feedEntity); err != nil {
		log.Fatalln("Failed to unmarshal feed entity:", err)
		return nil, err
	}

	return &feedEntity, nil
}
