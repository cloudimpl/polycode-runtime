package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"github.com/gin-gonic/gin"
	"log"
	"runtime/debug"
)

type haltType struct {
}

var HaltExecution = haltType{}

var serviceMap map[string]ClientService
var modelMap map[string]*ModelRegistry

func init() {
	serviceMap = make(map[string]ClientService)
	modelMap = make(map[string]*ModelRegistry)
}

type ModelRegistry struct {
	modelMap map[string]sdk.CollectionDescription
}

func (m *ModelRegistry) Get(name string) sdk.CollectionDescription {
	return m.modelMap[name]
}

func (m *ModelRegistry) List() []sdk.CollectionDescription {
	var models []sdk.CollectionDescription
	for _, model := range m.modelMap {
		models = append(models, model)
	}
	return models
}

func (m *ModelRegistry) Register(name string, modelType interface{}) error {
	if !IsPointer(modelType) {
		return errors.New("provide pointer of the struct to register")
	}

	typeName := GetTypeName(modelType)
	typeSchema, _, err := getSchema(modelType)
	if err != nil {
		return err
	}

	_, ok := m.modelMap[name]
	if ok {
		return errors.New("collection already registered")
	}

	m.modelMap[name] = sdk.CollectionDescription{
		Name:     name,
		TypeName: typeName,
		Schema:   typeSchema,
	}
	return nil
}

type MethodStartEvent struct {
	SessionId string       `json:"sessionId"`
	Method    string       `json:"method"`
	Meta      sdk.TaskMeta `json:"meta"`
	Input     any          `json:"input"`
}

type ServiceStartEvent struct {
	SessionId string       `json:"sessionId"`
	Service   string       `json:"service"`
	Method    string       `json:"method"`
	Meta      sdk.TaskMeta `json:"meta"`
	Input     any          `json:"input"`
}

type ServiceCompleteEvent struct {
	IsError    bool           `json:"isError"`
	Output     any            `json:"output"`
	Error      sdk.Error      `json:"error"`
	Stacktrace sdk.Stacktrace `json:"stacktrace"`
	Logs       []LogMsg       `json:"logs"`
}

type ApiStartEvent struct {
	SessionId string         `json:"sessionId"`
	Meta      sdk.TaskMeta   `json:"meta"`
	Request   sdk.ApiRequest `json:"request"`
}

type ApiCompleteEvent struct {
	Path     string          `json:"path"`
	Response sdk.ApiResponse `json:"response"`
	Logs     []LogMsg        `json:"logs"`
}

type ClientService interface {
	GetName() string
	GetDescription(method string) (string, error)
	GetInputType(method string) (any, error)
	GetOutputType(method string) (any, error)
	IsWorkflow(method string) bool
	ExecuteService(ctx sdk.ServiceContext, method string, input any) (any, error)
	ExecuteWorkflow(ctx sdk.WorkflowContext, method string, input any) (any, error)
}

type Runtime interface {
	GetValidator() sdk.Validator
	RunService(ctx context.Context, event ServiceStartEvent) (evt ServiceCompleteEvent)
	RunApi(ctx context.Context, event ApiStartEvent) (evt ApiCompleteEvent)
	Start() error
}

type ClientRuntime struct {
	env         ClientEnv
	client      ServiceClient
	apiServer   ApiServer
	serviceMap  map[string]ClientService
	modelMap    map[string]*ModelRegistry
	httpHandler *gin.Engine
	validator   sdk.Validator
}

func (c ClientRuntime) getService(serviceName string) (ClientService, error) {
	service := c.serviceMap[serviceName]
	if service == nil {
		return nil, fmt.Errorf("client: service %s not registered", serviceName)
	}
	return service, nil
}

func (c ClientRuntime) getApi() (*gin.Engine, error) {
	if c.httpHandler == nil {
		return nil, errors.New("client: api not registered")
	}
	return c.httpHandler, nil
}

func (c ClientRuntime) RegisterService(service ClientService) error {
	log.Println("client: register service ", service.GetName())

	if c.serviceMap[service.GetName()] != nil {
		return fmt.Errorf("client: service %s already registered", service.GetName())
	} else {
		c.serviceMap[service.GetName()] = service
		return nil
	}
}

func (c ClientRuntime) RegisterApi(httpHandler *gin.Engine) error {
	if c.httpHandler != nil {
		return errors.New("client: api already registered")
	}

	c.httpHandler = httpHandler
	return nil
}

func (c ClientRuntime) RegisterValidator(validator sdk.Validator) error {
	if c.httpHandler != nil {
		return errors.New("client: validator already registered")
	}

	c.validator = validator
	return nil
}

func (c ClientRuntime) GetValidator() sdk.Validator {
	return c.validator
}

func (c ClientRuntime) Start() error {
	c.apiServer.Start(c)

	services, err := ExtractServiceDescription(c.serviceMap, c.modelMap)
	if err != nil {
		return fmt.Errorf("client: failed to extract service description: %w", err)
	}

	req := StartAppRequest{
		AppName:     c.env.AppName,
		AppEndpoint: fmt.Sprintf("http://127.0.0.1:%d", c.env.AppPort),
		Services:    services,
		Routes:      LoadRoutes(c.httpHandler),
	}

	for {
		err = c.client.StartApp(req)
		if err == nil {
			break
		}
	}

	return nil
}

func (c ClientRuntime) RunService(ctx context.Context, event ServiceStartEvent) (evt ServiceCompleteEvent) {
	fmt.Printf("service started %s.%s", event.Service, event.Method)

	defer func() {
		// Recover from panic and check for a specific error
		if r := recover(); r != nil {
			switch r.(type) {
			case haltType:
				fmt.Printf("service stopped %s.%s", event.Service, event.Method)
				evt = ValueToServiceComplete(nil)
			default:
				stackTrace := string(debug.Stack())
				fmt.Printf("stack trace %s\n", stackTrace)

				if err, ok := r.(error); ok {
					evt = ErrorToServiceComplete(ErrInternal.Wrap(err), stackTrace)
				} else {
					evt = ErrorToServiceComplete(ErrInternal.Wrap(fmt.Errorf("recovered %v", r)), stackTrace)
				}
			}
		}
	}()

	service, err := c.getService(event.Service)
	if err != nil {
		err2 := ErrServiceExecError.Wrap(err)
		fmt.Printf("failed to get service %s\n", err.Error())
		return ErrorToServiceComplete(err2, "")
	}

	inputObj, err := service.GetInputType(event.Method)
	if err != nil {
		err2 := ErrServiceExecError.Wrap(err)
		fmt.Printf("failed to get input type %s\n", err.Error())
		return ErrorToServiceComplete(err2, "")
	}

	err = ConvertType(event.Input, inputObj)
	if err != nil {
		err2 := ErrBadRequest.Wrap(err)
		fmt.Printf("failed to convert input %s\n", err.Error())
		return ErrorToServiceComplete(err2, "")
	}

	err = c.validator.Validate(inputObj)
	if err != nil {
		err2 := ErrBadRequest.Wrap(err)
		fmt.Printf("failed to validate input %s\n", err.Error())
		return ErrorToServiceComplete(err2, "")
	}

	ctxImpl := &Context{
		ctx:           ctx,
		sessionId:     event.SessionId,
		client:        c.client,
		modelRegistry: GetModelRegistry(event.Service),
		meta:          event.Meta,
		validator:     c.validator,
	}

	var ret any
	if service.IsWorkflow(event.Method) {
		fmt.Printf("service %s exec workflow %s with session id %s", event.Service, event.Method, event.SessionId)
		ret, err = service.ExecuteWorkflow(ctxImpl, event.Method, inputObj)
	} else {
		fmt.Printf("service %s exec handler %s with session id %s", event.Service, event.Method, event.SessionId)
		ret, err = service.ExecuteService(ctxImpl, event.Method, inputObj)
	}

	if err != nil {
		err2 := ErrServiceExecError.Wrap(err)
		fmt.Printf("failed to execute service %s\n", err.Error())
		return ErrorToServiceComplete(err2, "")
	}

	fmt.Printf("service %s exec success %s\n", event.Service, event.Method)
	serviceCompleteEvent := ValueToServiceComplete(ret)
	return serviceCompleteEvent
}

func (c ClientRuntime) RunApi(ctx context.Context, event ApiStartEvent) (evt ApiCompleteEvent) {
	fmt.Printf("api started %s %s\n", event.Request.Method, event.Request.Path)

	defer func() {
		// Recover from panic and check for a specific error
		if r := recover(); r != nil {
			switch r.(type) {
			case haltType:
				fmt.Printf("api stopped %s %s", event.Request.Method, event.Request.Path)
				evt = ApiCompleteEvent{
					Response: sdk.ApiResponse{
						StatusCode:      202,
						Header:          make(map[string]string),
						Body:            "",
						IsBase64Encoded: false,
					},
				}
			default:
				stackTrace := string(debug.Stack())
				fmt.Printf("stack trace %s\n", stackTrace)

				if err, ok := r.(error); ok {
					evt = ApiCompleteEvent{
						Response: sdk.ApiResponse{
							StatusCode:      500,
							Header:          make(map[string]string),
							Body:            ErrInternal.Wrap(err).ToJson(),
							IsBase64Encoded: false,
						},
					}
				} else {
					evt = ApiCompleteEvent{
						Response: sdk.ApiResponse{
							StatusCode:      500,
							Header:          make(map[string]string),
							Body:            ErrInternal.Wrap(fmt.Errorf("recovered %v", r)).ToJson(),
							IsBase64Encoded: false,
						},
					}
				}
			}
		}
	}()

	if c.httpHandler == nil {
		err2 := ErrApiExecError.Wrap(errors.New("http handler not set"))
		fmt.Printf("api stopped %s %s, reason: %s", event.Request.Method, event.Request.Path, err2.Error())
		return ErrorToApiComplete(err2)
	}

	ctxImpl := &Context{
		ctx:           ctx,
		sessionId:     event.SessionId,
		client:        c.client,
		modelRegistry: GetModelRegistry("_nil_"),
		meta:          event.Meta,
		validator:     c.validator,
	}

	newCtx := context.WithValue(ctx, "sdk.context", ctxImpl)
	httpReq, err := ConvertToHttpRequest(newCtx, event.Request)
	if err != nil {
		err2 := ErrApiExecError.Wrap(err)
		fmt.Printf("failed to convert http request %s\n", err.Error())
		return ErrorToApiComplete(err2)
	}

	res := ManualInvokeHandler(c.httpHandler, httpReq)
	fmt.Printf("api completed %s %s\n", event.Request.Method, event.Request.Path)
	return ApiCompleteEvent{
		Response: res,
	}
}

func RegisterService(service ClientService) error {
	_, ok := serviceMap[service.GetName()]
	if ok {
		return errors.New("service already registered")
	}

	serviceMap[service.GetName()] = service
	return nil
}

func GetModelRegistry(serviceName string) *ModelRegistry {
	registry, ok := modelMap[serviceName]
	if !ok {
		registry = &ModelRegistry{
			modelMap: make(map[string]sdk.CollectionDescription),
		}
		modelMap[serviceName] = registry
	}

	return registry
}

type StartConfig struct {
	httpHandler *gin.Engine
	validator   sdk.Validator
}

type StartOption func(*StartConfig)

func WithHttpHandler(httpHandler *gin.Engine) StartOption {
	return func(config *StartConfig) {
		config.httpHandler = httpHandler
	}
}

func WithValidator(validator sdk.Validator) StartOption {
	return func(config *StartConfig) {
		config.validator = validator
	}
}

func Start(opts ...StartOption) error {
	clientEnv, err := initClientEnv()
	if err != nil {
		return err
	}

	serviceClient := NewServiceClient(clientEnv.SidecarApi)
	apiServer := NewApiServer(clientEnv.AppPort)

	cfg := &StartConfig{
		httpHandler: nil,
		validator:   DummyValidator{},
	}
	for _, opt := range opts {
		opt(cfg)
	}

	runtime := &ClientRuntime{
		env:         clientEnv,
		client:      serviceClient,
		apiServer:   apiServer,
		serviceMap:  serviceMap,
		modelMap:    modelMap,
		httpHandler: cfg.httpHandler,
		validator:   cfg.validator,
	}

	err = runtime.Start()
	if err != nil {
		return err
	}

	select {}
}
