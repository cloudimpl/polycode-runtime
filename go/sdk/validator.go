package sdk

type Validator interface {
	Validate(obj any) error
}
