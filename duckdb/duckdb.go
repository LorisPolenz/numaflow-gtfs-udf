package duckdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
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
				}
			}
			dbMutex.Unlock()
		}
	}()
}

func TestDBConnection(feedVersion string) (int, string) {
	db := GetDuckDB(feedVersion)

	var (
		id   int
		name string
	)

	row := db.QueryRow(`SELECT id, name FROM people`)
	err := row.Scan(&id, &name)
	if errors.Is(err, sql.ErrNoRows) {
		log.Println("no rows")
	} else if err != nil {
		log.Fatal(err)
	}

	return id, name
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

	dbPath := fmt.Sprintf("%s_feed.db", feedVersion)
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
