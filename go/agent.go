package runtime

import (
	"context"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
)

type AgentBuilder struct {
	ctx           context.Context
	sessionId     string
	envId         string
	agent         string
	serviceClient ServiceClient
}

func (r *AgentBuilder) WithEnvId(envId string) sdk.AgentBuilder {
	r.envId = envId
	return r
}

func (r *AgentBuilder) Get() sdk.Agent {
	return &Agent{
		ctx:           r.ctx,
		sessionId:     r.sessionId,
		envId:         r.envId,
		agent:         r.agent,
		serviceClient: r.serviceClient,
	}
}

var _ sdk.AgentBuilder = (*AgentBuilder)(nil)

type Agent struct {
	ctx           context.Context
	sessionId     string
	envId         string
	agent         string
	serviceClient ServiceClient
}

func (r *Agent) Call(options sdk.TaskOptions, input sdk.AgentInput) (sdk.Response, error) {
	req := ExecAgentRequest{
		EnvId:     r.envId,
		AgentName: r.agent,
		Options:   options,
		Input:     input,
	}

	output, err := r.serviceClient.CallAgent(r.sessionId, req)
	if err != nil {
		fmt.Printf("client: exec task error: %v\n", err)
		return nil, err
	}

	fmt.Printf("client: exec task output: %v\n", output)
	return &Response{
		output:  output.Output,
		isError: output.IsError,
		error:   output.Error,
	}, nil
}

var _ sdk.Agent = (*Agent)(nil)
