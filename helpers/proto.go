package helpers

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
)

type TransitFeedEntity struct {
	FeedVersion *string
	*FeedEntity
}

func (x *TransitFeedEntity) GetFeedVersion() string {
	if x != nil && x.FeedVersion != nil {
		return *x.FeedVersion
	}
	return ""
}

func UnmarshallFeedEntity(data []byte) (*TransitFeedEntity, error) {
	var feedEntity TransitFeedEntity

	gobDec := gob.NewDecoder(bytes.NewBuffer(data))

	if err := gobDec.Decode(&feedEntity); err != nil {
		slog.Error(fmt.Sprintf("Failed to unmarshal feed entity: %s", err))
		return nil, err
	}

	return &feedEntity, nil
}
