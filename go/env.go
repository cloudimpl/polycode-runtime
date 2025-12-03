package runtime

import (
	"fmt"
	"os"
)

type ClientEnv struct {
	AppName       string `json:"appName"`
	AppPort       int64  `json:"appPort"`
	SidecarApi    string `json:"sidecarApi"`
	CatalogId     string `json:"catalogId"`
	FileStorePath string `json:"s3FilesBucket"`
}

func initClientEnv() (ClientEnv, error) {
	var appName string
	var appPort int
	var err error

	appName = os.Getenv("polycode_APP_NAME")
	if appName == "" {
		appName, err = GetWorkingDirName()
		if err != nil {
			return ClientEnv{}, fmt.Errorf("failed to get working directory: %w", err)
		}
		fmt.Printf("app name decided: %s", appName)
	}

	appPort, err = GetFreePortInRange(7001, 7999, 300)
	if err != nil {
		return ClientEnv{}, fmt.Errorf("failed to find free port: %w", err)
	}
	fmt.Printf("free port found: %d", appPort)

	sidecarApi := os.Getenv("polycode_SIDECAR_API")
	if sidecarApi == "" {
		sidecarApi = "http://localhost:9999"
	}

	catalogId := os.Getenv("polycode_CATALOG_ID")
	fileStorePath := os.Getenv("polycode_FILE_STORE_PATH")

	return ClientEnv{
		AppName:       appName,
		AppPort:       int64(appPort),
		SidecarApi:    sidecarApi,
		CatalogId:     catalogId,
		FileStorePath: fileStorePath,
	}, nil
}
