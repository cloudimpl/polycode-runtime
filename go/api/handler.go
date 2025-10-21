package api

import (
	"github.com/cloudimpl/polycode-runtime/go/apicontext"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"github.com/gin-gonic/gin"
	"net/http"
)

func FromWorkflow[Input any, Output any](f func(sdk.WorkflowContext, Input) (Output, error)) func(c *gin.Context) {
	return func(c *gin.Context) {
		apiCtx, err := apicontext.FromContext(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to execute workflow: " + err.Error(),
			})
			return
		}

		var input Input
		if err = c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid request",
			})
			return
		}

		err = apiCtx.Validator().Validate(input)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		output, err := f(apiCtx.(sdk.WorkflowContext), input)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to execute workflow: " + err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, output)
	}
}

func ExecService(c *gin.Context, service string, method string,
	options sdk.TaskOptions, input any, outputTransform func(any) (any, error)) {
	apiCtx, err := apicontext.FromContext(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to execute controller: " + err.Error(),
		})
		return
	}

	s := apiCtx.Service(service).Get()
	res, err := s.RequestReply(options, method, input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to execute controller: " + err.Error(),
		})
		return
	}

	output, err := res.GetAny()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to execute controller: " + err.Error(),
		})
		return
	}

	transformedOutput, err := outputTransform(output)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to execute controller: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, transformedOutput)
}
