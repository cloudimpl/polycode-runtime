package sdk

import (
	"context"
)

// unexported key type to avoid collisions in context values
type ctxKey struct{}

// WithContext attaches your *ContextImpl to a parent context.
// Call this once at the edge (HTTP middleware, gRPC interceptor, Lambda handler, etc.)
func WithContext(parent context.Context, impl PolycodeContext) context.Context {
	return context.WithValue(parent, ctxKey{}, impl)
}

// getImpl is the internal helper that extracts the *ContextImpl from a context.
// It should never be exported; callers should use typed accessors like ApiContextFrom or RawContextFrom.
func getImpl(ctx context.Context) (PolycodeContext, bool) {
	v := ctx.Value(ctxKey{})
	if v == nil {
		return nil, false
	}
	impl, ok := v.(PolycodeContext)
	return impl, ok
}

func ApiContextFrom(ctx context.Context) (ApiContext, bool) {
	value := ctx.Value(ctxKey{})
	if value == nil {
		return nil, false
	}

	return value.(ApiContext), true
}

type BaseContext interface {
	context.Context
	Meta() TaskMeta
	Logger() Logger
	Validator() Validator
}

type ServiceContext interface {
	BaseContext
	Db() DataStoreBuilder
	FileStore() FileStoreBuilder
}

type WorkflowContext interface {
	BaseContext
	ReadOnlyDb() ReadOnlyDataStoreBuilder
	ReadOnlyFileStore() ReadOnlyFileStoreBuilder
	Service(service string) ServiceBuilder
	Agent(agent string) AgentBuilder
	App(appName string) ServiceBuilder
	Memo(getter func() (any, error)) Response
	Signal(signalName string) Signal
	ClientChannel(channelName string) ClientChannel
	Lock(key string) Lock
}

type ApiContext interface {
	BaseContext
	Service(service string) ServiceBuilder
	Controller(controller string) ControllerBuilder
	Agent(agent string) AgentBuilder
	App(appName string) ServiceBuilder
}

//type RawContext interface {
//	BaseContext
//	GetMeta(group string, typeName string, key string) (map[string]interface{}, error)
//}

type PolycodeContext interface {
	BaseContext
	ServiceContext
	WorkflowContext
	ApiContext
}
