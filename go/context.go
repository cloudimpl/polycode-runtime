package runtime

import (
	"context"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"time"
)

type Context struct {
	ctx           context.Context
	sessionId     string
	client        ServiceClient
	modelRegistry *ModelRegistry
	meta          sdk.TaskMeta
	validator     sdk.Validator
}

func (c Context) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c Context) Err() error {
	return c.ctx.Err()
}

func (c Context) Value(key any) any {
	return c.ctx.Value(key)
}

func (c Context) Meta() sdk.TaskMeta {
	return c.meta
}

func (c Context) Logger() sdk.Logger {
	return &JsonLogger{
		section: "task",
	}
}

func (c Context) Validator() sdk.Validator {
	return c.validator
}

func (c Context) ReadOnlyDb() sdk.ReadOnlyDataStoreBuilder {
	return &ReadOnlyDataStoreBuilder{
		client:        c.client,
		sessionId:     c.sessionId,
		modelRegistry: c.modelRegistry,
	}
}

func (c Context) Db() sdk.DataStoreBuilder {
	return &DataStoreBuilder{
		client:        c.client,
		sessionId:     c.sessionId,
		modelRegistry: c.modelRegistry,
	}
}

func (c Context) FileStore() sdk.FileStoreBuilder {
	return &FileStoreBuilder{
		client:    c.client,
		sessionId: c.sessionId,
	}
}

func (c Context) ReadOnlyFileStore() sdk.ReadOnlyFileStoreBuilder {
	return &ReadOnlyFileStoreBuilder{
		client:    c.client,
		sessionId: c.sessionId,
	}
}

func (c Context) Service(service string) sdk.ServiceBuilder {
	return &ServiceBuilder{
		ctx:           c.ctx,
		sessionId:     c.sessionId,
		service:       service,
		serviceClient: c.client,
	}
}

func (c Context) Agent(agent string) sdk.AgentBuilder {
	return &AgentBuilder{
		ctx:           c.ctx,
		sessionId:     c.sessionId,
		agent:         agent,
		serviceClient: c.client,
	}
}

func (c Context) Controller(controller string) sdk.ControllerBuilder {
	return &ControllerBuilder{
		ctx:           c.ctx,
		sessionId:     c.sessionId,
		controller:    controller,
		serviceClient: c.client,
	}
}

func (c Context) App(appName string) sdk.ServiceBuilder {
	return &AppServiceBuilder{
		ctx:           c.ctx,
		sessionId:     c.sessionId,
		appName:       appName,
		serviceClient: c.client,
	}
}

func (c Context) Memo(getter func() (any, error)) sdk.Response {
	return Memo{
		ctx:           c.ctx,
		sessionId:     c.sessionId,
		serviceClient: c.client,
		getter:        getter,
	}.Get()
}

func (c Context) Signal(signalName string) sdk.Signal {
	// not implemented, old architecture is not good
	panic("implement me")
}

func (c Context) ClientChannel(channelName string) sdk.ClientChannel {
	return &ClientChannel{
		name:          channelName,
		sessionId:     c.sessionId,
		serviceClient: c.client,
	}
}

func (c Context) Lock(key string) sdk.Lock {
	return &Lock{
		client:    c.client,
		sessionId: c.sessionId,
		key:       key,
	}
}
