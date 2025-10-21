package apicontext

import (
	"context"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
)

func FromContext(ctx context.Context) (sdk.ApiContext, error) {
	value := ctx.Value("sdk.context")
	if value == nil {
		return nil, sdk.ErrContextNotFound
	}

	return value.(sdk.ApiContext), nil
}
