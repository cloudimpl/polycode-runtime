package runtime

import (
	"github.com/cloudimpl/polycode-runtime/go/sdk"
)

type Signal struct {
	client    ServiceClient
	sessionId string
	name      string
}

func (s *Signal) Await() sdk.Response {
	output, err := s.client.WaitForSignal(s.sessionId, SignalWaitRequest{
		SignalName: s.name,
	})
	if err != nil {
		return &Response{
			output:  nil,
			isError: true,
			error:   ErrInternal.Wrap(err),
		}
	}

	return &Response{
		output:  output.Output,
		isError: output.IsError,
		error:   output.Error,
	}
}

func (s *Signal) EmitValue(taskId string, data any) error {
	return s.client.EmitSignal(s.sessionId, SignalEmitRequest{
		TaskId:     taskId,
		SignalName: s.name,
		Output:     data,
		IsError:    false,
		Error:      sdk.Error{},
	})
}

func (s *Signal) EmitError(taskId string, err sdk.Error) error {
	return s.client.EmitSignal(s.sessionId, SignalEmitRequest{
		TaskId:     taskId,
		SignalName: s.name,
		Output:     nil,
		IsError:    true,
		Error:      err,
	})
}
