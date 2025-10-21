package runtime

import (
	"fmt"
	"os"
)

type ClientEnv struct {
	AppName    string `json:"appName"`
	AppPort    int64  `json:"appPort"`
	SidecarApi string `json:"sidecarApi"`
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

	return ClientEnv{
		AppName:    appName,
		AppPort:    int64(appPort),
		SidecarApi: sidecarApi,
	}, nil
}
