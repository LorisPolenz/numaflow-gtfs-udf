package duckdb

import (
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type cachedDuckDB struct {
	db         *sql.DB
	lastAccess time.Time
}

var (
	dbMutex sync.RWMutex
	dbMap   = make(map[string]*cachedDuckDB)
)

func InitDBHousekeeper() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for range ticker.C {
			dbMutex.Lock()
			for feedVersion, cachedDB := range dbMap {
				if time.Since(cachedDB.lastAccess) > 24*time.Hour {
					log.Printf("Closing idle DuckDB connection for key: %s\n", feedVersion)
					cachedDB.db.Close()
					delete(dbMap, feedVersion)
					// Delete DB File as well
					err := os.RemoveAll(feedVersion + "_feed.db")

					if err != nil {
						log.Printf("Error deleting DB file for key %s: %v\n", feedVersion, err)
					}
				}
			}
			dbMutex.Unlock()
		}
	}()
}

func TestDBConnection(feedVersion string) (string, string) {
	db := GetDuckDB(feedVersion)

	var (
		trip_id        string
		arrival_time   string
		departure_time string
		stop_id        string
		stop_sequence  int
		pickup_type    int
		drop_off_type  int
	)

	row := db.QueryRow(`SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type FROM stop_times USING SAMPLE 1;`)
	err := row.Scan(&trip_id, &arrival_time, &departure_time, &stop_id, &stop_sequence, &pickup_type, &drop_off_type)
	if errors.Is(err, sql.ErrNoRows) {
		log.Println("no rows")
	} else if err != nil {
		log.Fatal(err)
	}

	return trip_id, stop_id
}

func GetDuckDB(feedVersion string) *sql.DB {

	dbMutex.Lock()
	defer dbMutex.Unlock()

	// If conn for feedVersion exists, return it
	if cachedDB, exists := dbMap[feedVersion]; exists {
		slog.Debug("Returning cached DB connection", "feedVersion", feedVersion)
		cachedDB.lastAccess = time.Now()
		return cachedDB.db
	}

	// Create new connection
	slog.Info("Creating new DB connection", "feedVersion", feedVersion)

	dbPath := BuildNewFeedVersion(feedVersion)

	duckDBConn, err := sql.Open("duckdb", dbPath)

	if err != nil {
		log.Fatal(err)
	}

	err = duckDBConn.Ping()

	if err != nil {
		log.Fatal(err)
	}

	// Store in map
	dbMap[feedVersion] = &cachedDuckDB{
		db:         duckDBConn,
		lastAccess: time.Now(),
	}

	return duckDBConn
}
