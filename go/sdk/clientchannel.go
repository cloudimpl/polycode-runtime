package sdk

type ClientChannel interface {
	Emit(data any) error
}
