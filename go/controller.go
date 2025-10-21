package runtime

import (
	"context"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"net/http"
)

type ControllerBuilder struct {
	ctx           context.Context
	sessionId     string
	envId         string
	controller    string
	serviceClient ServiceClient
}

func (r *ControllerBuilder) WithEnvId(envId string) sdk.ControllerBuilder {
	r.envId = envId
	return r
}

func (r *ControllerBuilder) Get() sdk.Controller {
	return &Controller{
		ctx:           r.ctx,
		sessionId:     r.sessionId,
		envId:         r.envId,
		controller:    r.controller,
		serviceClient: r.serviceClient,
	}
}

type Controller struct {
	ctx           context.Context
	sessionId     string
	envId         string
	controller    string
	serviceClient ServiceClient
}

func (r *Controller) Call(options sdk.TaskOptions, path string, apiReq sdk.ApiRequest) (sdk.ApiResponse, error) {
	req := ExecApiRequest{
		EnvId:      r.envId,
		Controller: r.controller,
		Path:       path,
		Options:    options,
		Request:    apiReq,
	}

	output, err := r.serviceClient.CallApi(r.sessionId, req)
	if err != nil {
		return sdk.ApiResponse{}, err
	}

	if output.IsError {
		return sdk.ApiResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       output.Error.ToJson(),
		}, nil
	}

	return output.Response, nil
}
