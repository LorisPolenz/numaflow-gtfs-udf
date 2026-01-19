package duckdb

import (
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func fetch_gtfs_fp(feedVersion string) string {

	endpoint := os.Getenv("S3_ENDPOINT")
	accessKeyID := os.Getenv("S3_ACCESS_KEY")
	secretAccessKey := os.Getenv("S3_SECRET_KEY")
	useSSL := true

	fmt.Println(endpoint)
	fmt.Println(accessKeyID)
	fmt.Println(secretAccessKey)

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		log.Fatalln(err)
	}

	bucketName := os.Getenv("GTFS_FP_BUCKET")
	objectName := fmt.Sprintf("%s_feed.zip", feedVersion)
	localFilePath := fmt.Sprintf("./tmp/%s", objectName)

	// Download the object from the bucket
	err = minioClient.FGetObject(context.Background(), bucketName, objectName, localFilePath, minio.GetObjectOptions{})

	if err != nil {
		log.Fatalln(err)
	}

	return localFilePath

}

func extractCSVFiles(localPath string, relevantFiles []string) {
	reader, err := zip.OpenReader(localPath)

	if err != nil {
		log.Fatalln(err)
	}

	for _, file := range reader.File {
		for _, relevantFile := range relevantFiles {
			if !strings.EqualFold(file.Name, relevantFile) {
				continue
			}

			fmt.Println("Unpacking file:", file.Name)

			destPath := fmt.Sprintf("./tmp/%s", file.Name)
			destFile, err := os.Create(destPath)

			if err != nil {
				log.Fatalln(err)
			}

			srcFile, err := file.Open()

			if err != nil {
				log.Fatalln(err)
			}

			_, err = destFile.ReadFrom(srcFile)

			if err != nil {
				log.Fatalln(err)
			}

			destFile.Close()
			srcFile.Close()
		}
	}

	reader.Close()
}

func buildDuckDB(feedVersion string, localPath string) string {
	// Placeholder for building DuckDB from GTFS feed
	fmt.Printf("Building DuckDB for feed version %s from file %s\n", feedVersion, localPath)

	relevantFiles := []string{
		"stop_times.txt",
		"stops.txt",
	}

	extractCSVFiles(localPath, relevantFiles)

	dbPath := fmt.Sprintf("%s_feed.db", feedVersion)
	duckDBConn, err := sql.Open("duckdb", dbPath)

	if err != nil {
		log.Fatalln(err)
	}

	defer duckDBConn.Close()

	for _, fileName := range relevantFiles {
		duckDBConn.Exec(
			fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv('./tmp/%s');", strings.Split(fileName, ".")[0], fileName),
		)
	}

	err = os.RemoveAll("./tmp/")

	if err != nil {
		log.Fatalln(err)
	}

	return dbPath
}

func BuildNewFeedVersion(feedVersion string) string {
	localPath := fetch_gtfs_fp(feedVersion)

	log.Printf("Downloaded GTFS feed to: %s\n", localPath)

	dbPath := buildDuckDB(feedVersion, localPath)

	fmt.Printf("DuckDB built at path: %s\n", dbPath)

	return dbPath
}
