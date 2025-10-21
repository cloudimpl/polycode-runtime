package runtime

import "github.com/cloudimpl/polycode-runtime/go/sdk"

var ErrInternal = sdk.DefineError("sdk.client.runtime", 1, "internal error")
var ErrSidecarClientFailed = sdk.DefineError("sdk.client.runtime", 2, "sidecar client failed, reason: [%s]")
var ErrServiceExecError = sdk.DefineError("sdk.client", 3, "service exec error")
var ErrApiExecError = sdk.DefineError("sdk.client", 4, "api exec error")
var ErrBadRequest = sdk.DefineError("sdk.client", 5, "bad request")
var ErrTaskExecError = sdk.DefineError("sdk.client", 6, "task execution error")
