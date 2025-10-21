package sdk

type Signal interface {
	Await() (Response, error)
	EmitValue(taskId string, data any) error
	EmitError(taskId string, err Error) error
}
