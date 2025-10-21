package runtime

type DummyValidator struct {
}

func (v DummyValidator) Validate(obj any) error {
	return nil
}
