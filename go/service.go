package runtime

import (
	"context"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
)

type ServiceBuilder struct {
	ctx           context.Context
	sessionId     string
	envId         string
	service       string
	serviceClient ServiceClient
}

func (r *ServiceBuilder) WithEnvId(envId string) sdk.ServiceBuilder {
	r.envId = envId
	return r
}

func (r *ServiceBuilder) Get() sdk.Service {
	return &Service{
		ctx:           r.ctx,
		sessionId:     r.sessionId,
		envId:         r.envId,
		service:       r.service,
		serviceClient: r.serviceClient,
	}
}

type Service struct {
	ctx           context.Context
	sessionId     string
	envId         string
	service       string
	serviceClient ServiceClient
}

func (r *Service) RequestReply(options sdk.TaskOptions, method string, input any) (sdk.Response, error) {
	req := ExecServiceRequest{
		EnvId:   r.envId,
		Service: r.service,
		Method:  method,
		Options: options,
		Input:   input,
	}

	output, err := r.serviceClient.CallService(r.sessionId, req)
	if err != nil {
		fmt.Printf("client: exec service error: %v\n", err)
		return nil, err
	}

	return &Response{
		output:  output.Output,
		isError: output.IsError,
		error:   output.Error,
	}, nil
}

func (r *Service) Send(options sdk.TaskOptions, method string, input any) error {
	req := ExecServiceRequest{
		EnvId:   r.envId,
		Service: r.service,
		Method:  method,
		Options: options,
		Input:   input,
	}

	return r.serviceClient.SendService(r.sessionId, req)
}

type AppServiceBuilder struct {
	ctx           context.Context
	sessionId     string
	envId         string
	appName       string
	serviceClient ServiceClient
}

func (r *AppServiceBuilder) WithEnvId(envId string) sdk.ServiceBuilder {
	r.envId = envId
	return r
}

func (r *AppServiceBuilder) Get() sdk.Service {
	return &AppService{
		ctx:           r.ctx,
		sessionId:     r.sessionId,
		envId:         r.envId,
		appName:       r.appName,
		serviceClient: r.serviceClient,
	}
}

type AppService struct {
	ctx           context.Context
	sessionId     string
	envId         string
	appName       string
	serviceClient ServiceClient
}

func (r *AppService) RequestReply(options sdk.TaskOptions, method string, input any) (sdk.Response, error) {
	req := ExecAppRequest{
		EnvId:   r.envId,
		AppName: r.appName,
		Method:  method,
		Options: options,
		Input:   input,
	}

	output, err := r.serviceClient.CallApp(r.sessionId, req)
	if err != nil {
		fmt.Printf("client: exec service error: %v\n", err)
		return nil, err
	}

	return &Response{
		output:  output.Output,
		isError: output.IsError,
		error:   output.Error,
	}, nil
}

func (r *AppService) Send(options sdk.TaskOptions, method string, input any) error {
	req := ExecAppRequest{
		EnvId:   r.envId,
		AppName: r.appName,
		Method:  method,
		Options: options,
		Input:   input,
	}

	return r.serviceClient.SendApp(r.sessionId, req)
}
