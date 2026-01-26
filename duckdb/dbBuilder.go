package duckdb

import (
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func fetch_gtfs_fp(feedVersion string) (string, error) {

	endpoint := os.Getenv("S3_ENDPOINT")
	accessKeyID := os.Getenv("S3_ACCESS_KEY")
	secretAccessKey := os.Getenv("S3_SECRET_KEY")
	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		slog.Error(fmt.Sprintf("Could not create Minio Client %s", err))
		return "", err
	}

	bucketName := os.Getenv("GTFS_FP_BUCKET")
	objectName := fmt.Sprintf("%s_feed.zip", feedVersion)
	localFilePath := fmt.Sprintf("./tmp/%s", objectName)

	// Download the object from the bucket
	err = minioClient.FGetObject(context.Background(), bucketName, objectName, localFilePath, minio.GetObjectOptions{})

	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch S3 Object %s", err))
		return "", err
	}

	return localFilePath, nil

}

func extractCSVFiles(localPath string, relevantFiles []string) error {
	reader, err := zip.OpenReader(localPath)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not open CSV File %s", err))
		return err
	}

	for _, file := range reader.File {
		for _, relevantFile := range relevantFiles {
			if !strings.EqualFold(file.Name, relevantFile) {
				continue
			}

			slog.Info(fmt.Sprintf("Unpacking file: %s", file.Name))

			destPath := fmt.Sprintf("./tmp/%s", file.Name)
			destFile, err := os.Create(destPath)

			if err != nil {
				slog.Error(fmt.Sprintf("Could not write extracted file: %s", err))
				return err
			}

			srcFile, err := file.Open()

			if err != nil {
				slog.Error(fmt.Sprintf("Could not read extracted file: %s", err))
				return err
			}

			_, err = destFile.ReadFrom(srcFile)

			if err != nil {
				slog.Error(fmt.Sprintf("Could not read extracted file: %s", err))
				return err
			}

			destFile.Close()
			srcFile.Close()
		}
	}

	reader.Close()
	return nil
}

func buildDuckDB(feedVersion string, localPath string) (string, error) {
	// Placeholder for building DuckDB from GTFS feed
	slog.Info(fmt.Sprintf("Building DuckDB for feed version %s from file %s\n", feedVersion, localPath))

	relevantFiles := []string{
		"stop_times.txt",
		"stops.txt",
		"trips.txt",
		"routes.txt",
	}

	err := extractCSVFiles(localPath, relevantFiles)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not extract CSV files: %s", err))
		return "", err
	}

	dbPath := fmt.Sprintf("%s_feed.db", feedVersion)
	duckDBConn, err := sql.Open("duckdb", dbPath)

	if err != nil {
		slog.Error(fmt.Sprintf("Could not open DuckDB: %s", err))
		return "", err
	}

	defer duckDBConn.Close()

	for _, fileName := range relevantFiles {
		duckDBConn.Exec(
			fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv('./tmp/%s');", strings.Split(fileName, ".")[0], fileName),
		)
	}

	err = os.RemoveAll("./tmp/")

	if err != nil {
		slog.Error(fmt.Sprintf("Could not remove tmp dir: %s", err))
		return "", err
	}

	return dbPath, nil
}

func BuildNewFeedVersion(feedVersion string) (string, error) {
	localPath, err := fetch_gtfs_fp(feedVersion)

	if err != nil {
		slog.Error(fmt.Sprintf("Error fetching GTFS feed for version %s: %v\n", feedVersion, err))
		return "", err
	}

	slog.Info(fmt.Sprintf("Downloaded GTFS feed to: %s\n", localPath))

	dbPath, err := buildDuckDB(feedVersion, localPath)

	if err != nil {
		slog.Error(fmt.Sprintf("Error building DuckDB for feed version %s: %v\n", feedVersion, err))
		return "", err
	}

	slog.Info(fmt.Sprintf("DuckDB built at path: %s\n", dbPath))

	return dbPath, nil
}
