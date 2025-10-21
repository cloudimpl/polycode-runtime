package sdk

type Agent interface {
	Call(options TaskOptions, input AgentInput) (Response, error)
}

type AgentBuilder interface {
	WithEnvId(envId string) AgentBuilder
	Get() Agent
}
