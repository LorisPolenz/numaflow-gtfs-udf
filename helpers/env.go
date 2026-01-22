package helpers

import (
	"fmt"
	"log/slog"
	"os"
)

func VerifyEnv() {
	requiredVars := []string{
		"S3_ENDPOINT",
		"S3_ACCESS_KEY",
		"S3_SECRET_KEY",
		"GTFS_FP_BUCKET",
	}

	for _, v := range requiredVars {
		if _, exists := os.LookupEnv(v); !exists {
			slog.Error(fmt.Sprintf("Environment variable %s is not set\n", v))
			os.Exit(1)
		}
	}
}
