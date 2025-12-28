package duckdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
)

var (
	duckDBConn *sql.DB
	dbOnce     sync.Once
)

func TestDBConnection() {
	db := GetDuckDB()
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE people (id INTEGER, name VARCHAR)`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO people VALUES (42, 'John')`)
	if err != nil {
		log.Fatal(err)
	}

	var (
		id   int
		name string
	)
	row := db.QueryRow(`SELECT id, name FROM people`)
	err = row.Scan(&id, &name)
	if errors.Is(err, sql.ErrNoRows) {
		log.Println("no rows")
	} else if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("id: %d, name: %s\n", id, name)
}

func GetDuckDB() *sql.DB {

	dbOnce.Do(func() {
		fmt.Println("Connecting to DuckDB...")
		var err error

		//connect to duckdb
		duckDBConn, err = sql.Open("duckdb", "")

		err = duckDBConn.Ping()

		if err != nil {
			log.Fatal(err)
		}

		// duckDBConn.Exec("ATTACH DATABASE 'feeds.db' AS persistent_db;")
		// duckDBConn.Exec("COPY FROM DATABASE persistent_db TO memory;")
		// duckDBConn.Exec("DETACH persistent_db;")
	})

	return duckDBConn
}
