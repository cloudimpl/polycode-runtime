package sdk

type Controller interface {
	Call(options TaskOptions, path string, apiReq ApiRequest) (ApiResponse, error)
}

type ControllerBuilder interface {
	WithEnvId(envId string) ControllerBuilder
	Get() Controller
}
