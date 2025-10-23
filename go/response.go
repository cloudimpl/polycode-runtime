package runtime

import "github.com/cloudimpl/polycode-runtime/go/sdk"

type Response struct {
	output  any
	isError bool
	error   sdk.Error
}

func (r *Response) Get(ret interface{}) error {
	if r.isError {
		return r.error
	}

	err := ConvertType(r.output, ret)
	if err != nil {
		return err
	}

	return nil
}

func (r *Response) GetAny() (any, error) {
	if r.isError {
		return nil, r.error
	} else {
		return r.output, nil
	}
}

func (r *Response) HasResult() bool {
	return r.output != nil
}

func (r *Response) IsError() bool {
	return r.isError
}

func (r *Response) Output() any {
	return r.output
}

func (r *Response) Error() sdk.Error {
	return r.error
}
