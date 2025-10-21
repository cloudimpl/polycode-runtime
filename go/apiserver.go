package runtime

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

type ApiServerListener interface {
	RunService(ctx context.Context, event ServiceStartEvent) (evt ServiceCompleteEvent)
	RunApi(ctx context.Context, event ApiStartEvent) (evt ApiCompleteEvent)
}

func NewApiServer(port int64) ApiServer {
	return &ApiServerImpl{
		port: port,
	}
}

type ApiServer interface {
	Start(listener ApiServerListener)
}

type ApiServerImpl struct {
	port      int64
	listener  ApiServerListener
	ginEngine *gin.Engine
}

func (s *ApiServerImpl) Start(listener ApiServerListener) {
	s.listener = listener

	// Create a Gin router
	s.ginEngine = gin.Default()

	s.ginEngine.GET("/v1/health", s.invokeHealthCheck)
	s.ginEngine.POST("/v1/invoke/api", s.invokeApiHandler)
	s.ginEngine.POST("/v1/invoke/service", s.invokeServiceHandler)

	go func() {
		// Start the Gin server
		err := s.ginEngine.Run(fmt.Sprintf(":%d", s.port))
		if err != nil {
			log.Fatalf("Failed to start api server: %s", err.Error())
		}
	}()
}

func (s *ApiServerImpl) invokeHealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *ApiServerImpl) invokeApiHandler(c *gin.Context) {
	var input ApiStartEvent
	var output ApiCompleteEvent

	fmt.Println("api task received")
	if err := c.ShouldBindJSON(&input); err != nil {
		output = ErrorToApiComplete(ErrInternal.Wrap(err))
		fmt.Printf("api task failed %s\n", err.Error())
	} else {
		output = s.listener.RunApi(c, input)
		fmt.Println("api task success")
	}

	c.JSON(http.StatusOK, output)
}

func (s *ApiServerImpl) invokeServiceHandler(c *gin.Context) {
	var input ServiceStartEvent
	var output ServiceCompleteEvent

	fmt.Println("service task received")
	if err := c.ShouldBindJSON(&input); err != nil {
		output = ErrorToServiceComplete(ErrInternal.Wrap(err), "")
		fmt.Printf("service task failed %s\n", err.Error())
	} else {
		output = s.listener.RunService(c, input)
		fmt.Println("service task success")
	}

	c.JSON(http.StatusOK, output)
}
