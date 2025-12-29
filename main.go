package main

import (
	"context"
	"log"
	"numaflow_gtfs_udf/duckdb"
	"os"
	"path"
	"sync"

	"github.com/fsnotify/fsnotify"

	"github.com/numaproj/numaflow-go/pkg/mapper"
	"github.com/numaproj/numaflow-go/pkg/sideinput"
)

var sideInputName = "gtfs-version"
var sideInputData []byte
var mu sync.RWMutex

type FeedMetadata struct {
	FeedName    string
	LastUpdated string
}

type RecentFeeds struct {
	Feeds []FeedMetadata
}

func mapFn(_ context.Context, _ []string, d mapper.Datum) mapper.Messages {
	msg := d.Value()

	// read side input data with read lock
	mu.RLock()
	siData := sideInputData
	mu.RUnlock()

	// // Decode side input data
	// var recentFeeds RecentFeeds
	// decoder := gob.NewDecoder(bytes.NewReader(siData))

	// err := decoder.Decode(&recentFeeds)

	// if err != nil {
	// 	fmt.Println("Error decoding recent feeds:", err)
	// 	os.Exit(1)
	// }

	// fmt.Println("Decoded RecentFeeds data:", recentFeeds)

	id, name := duckdb.TestDBConnection("20251002")

	log.Printf("From DuckDB - id: %d, name: %s\n", id, name)
	log.Printf("Processing message: %s\n", string(msg))
	log.Printf("Side input data: %s\n", string(siData))

	if len(siData) > 0 {
		if string(siData) == "even" {
			return mapper.MessagesBuilder().Append(mapper.NewMessage([]byte(string(msg) + "-even-data")))
		} else if string(siData) == "odd" {
			return mapper.MessagesBuilder().Append(mapper.NewMessage([]byte(string(msg) + "-odd-data")))
		} else {
			log.Printf("Unknown side input data: %s\n", string(siData))
			return mapper.MessagesBuilder().Append(mapper.NewMessage([]byte(string(msg) + "-unknown-data")))
		}
	}

	return mapper.MessagesBuilder().Append(mapper.MessageToDrop())
}

func main() {
	duckdb.InitDBHousekeeper()

	// Create a new fsnotify watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	// Add a path to the watcher
	err = watcher.Add(sideinput.DirPath)
	if err != nil {
		log.Fatal(err)
	}

	// Start a goroutine to listen for events from the watcher
	go fileWatcher(watcher, sideInputName)

	err = mapper.NewServer(mapper.MapperFunc(mapFn)).Start(context.Background())
	if err != nil {
		log.Panic("Failed to start map function server: ", err)
	}
}

// fileWatcher will watch for any changes in side input file and set data in
// sideInputData global variable.
func fileWatcher(watcher *fsnotify.Watcher, sideInputName string) {
	log.Println("Watching for changes in side input file: ", sideinput.DirPath)
	p := path.Join(sideinput.DirPath, sideInputName)
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				log.Println("watcher.Events channel closed")
				return
			}
			if event.Op&fsnotify.Create == fsnotify.Create && event.Name == p {
				log.Println("Side input file has been created:", event.Name)
				b, err := os.ReadFile(p)
				if err != nil {
					log.Println("Failed to read side input file: ", err)
				}

				// take a lock before updating sideInputData data to prevent race condition
				mu.Lock()
				sideInputData = b[:]
				mu.Unlock()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				log.Println("watcher.Errors channel closed")
				return
			}
			log.Println("error:", err)
		}
	}
}
