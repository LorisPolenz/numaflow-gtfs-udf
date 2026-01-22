package duckdb

import (
	"database/sql"
	"errors"
	"fmt"
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
					slog.Info(fmt.Sprintf("Closing idle DuckDB connection for key: %s\n", feedVersion))
					cachedDB.db.Close()
					delete(dbMap, feedVersion)
					// Delete DB File as well
					err := os.RemoveAll(feedVersion + "_feed.db")

					if err != nil {
						slog.Error(fmt.Sprintf("Error deleting DB file for key %s: %v\n", feedVersion, err))
					}
				}
			}
			dbMutex.Unlock()
		}
	}()
}

func TestDBConnection(feedVersion string) (string, string, error) {
	db, err := GetDuckDB(feedVersion)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not get DuckDB connection: %s", err))
		return "", "", err
	}

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
	err = row.Scan(&trip_id, &arrival_time, &departure_time, &stop_id, &stop_sequence, &pickup_type, &drop_off_type)
	if errors.Is(err, sql.ErrNoRows) {
		slog.Info("No rows returned from test query")
	} else if err != nil {
		slog.Error(fmt.Sprintf("Found error in query: %s", err))
		return "", "", err
	}

	return trip_id, stop_id, nil
}

func GetDuckDB(feedVersion string) (*sql.DB, error) {

	dbMutex.Lock()
	defer dbMutex.Unlock()

	// If conn for feedVersion exists, return it
	if cachedDB, exists := dbMap[feedVersion]; exists {
		slog.Debug("Returning cached DB connection", "feedVersion", feedVersion)
		cachedDB.lastAccess = time.Now()
		return cachedDB.db, nil
	}

	// Create new connection
	slog.Info("Creating new DB connection", "feedVersion", feedVersion)

	dbPath, err := BuildNewFeedVersion(feedVersion)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not build new feed version: %s", err))
		return nil, err
	}

	duckDBConn, err := sql.Open("duckdb", dbPath)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not establish DuckDB connection: %s", err))
		return nil, err
	}

	err = duckDBConn.Ping()

	if err != nil {
		slog.Error(fmt.Sprintf("Could not ping DuckDB: %s", err))
		return nil, err
	}

	// Store in map
	dbMap[feedVersion] = &cachedDuckDB{
		db:         duckDBConn,
		lastAccess: time.Now(),
	}

	return duckDBConn, nil
}
