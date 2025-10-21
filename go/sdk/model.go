package sdk

import (
	"time"
)

type LogLevel string

type BackoffStrategy struct {
	InitialInterval time.Duration `json:"initialInterval"`
	MaxInterval     time.Duration `json:"maxInterval"`
	Multiplier      float64       `json:"multiplier"`
}

type TaskOptions struct {
	Timeout         time.Duration   `json:"timeout"`
	Retries         int             `json:"retries"`
	RetryOnFail     bool            `json:"retryOnFail"`
	BackoffStrategy BackoffStrategy `json:"backoffStrategy"`
	SequenceKey     string          `json:"sequenceKey"`
}

func (t TaskOptions) WithTimeout(timeout time.Duration) TaskOptions {
	t.Timeout = timeout
	return t
}

func (t TaskOptions) WithSequenceKey(key string) TaskOptions {
	t.SequenceKey = key
	return t
}

type AuthContext struct {
	Claims map[string]interface{} `json:"claims"`
}

func (a AuthContext) Sub() string {
	return a.Claims["sub"].(string)
}

func (a AuthContext) TenantId() string {
	return a.Claims["sub"].(string)
}

type ApiRequest struct {
	Id              string            `json:"id"`
	Host            string            `json:"host"`
	Method          string            `json:"method"`
	Path            string            `json:"path"`
	Query           map[string]string `json:"query"`
	Header          map[string]string `json:"header"`
	Body            string            `json:"body"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`
}

type ApiResponse struct {
	StatusCode      int               `json:"statusCode"`
	Header          map[string]string `json:"header"`
	Body            string            `json:"body"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`
}

type AgentInput struct {
	SessionKey string            `json:"sessionKey"`
	TaskId     int64             `json:"taskId"`
	LLMInput   LLMInput          `json:"llmInput"`
	Labels     map[string]string `json:"labels"`
	ChannelId  string            `json:"channelId"`
}

type LLMInput struct {
	Text string `json:"text"`
}

type Meta struct {
	EnvId     string `json:"envId"`
	AppName   string `json:"appName"`
	AppId     string `json:"appId"`
	TaskGroup string `json:"taskGroup"`
	TaskName  string `json:"taskName"`
}

type TaskMeta struct {
	EnvId     string     `json:"envId"`
	TaskGroup string     `json:"taskGroup"`
	TaskName  string     `json:"taskName"`
	TaskId    string     `json:"taskId"`
	Input     InputMeta  `json:"input"`
	Parent    ParentMeta `json:"parent"`
}

type InputMeta struct {
	TraceId string `json:"traceId"`
}

type ParentMeta struct {
	EnvId     string `json:"envId"`
	TaskGroup string `json:"taskGroup"`
	TaskName  string `json:"taskName"`
	TaskId    string `json:"taskId"`
	Step      int64  `json:"step"`
}

type DataScope string

const (
	DataScopeService DataScope = "Service"
	DataScopeApp     DataScope = "App"
)

type RouteData struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type ServiceDescription struct {
	Name        string                  `json:"name"`
	Methods     []MethodDescription     `json:"tasks"`
	Collections []CollectionDescription `json:"collections"`
}

type MethodDescription struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	IsWorkflow  bool        `json:"isWorkflow"`
	Input       interface{} `json:"input"`
}

type CollectionDescription struct {
	Name     string      `json:"name"`
	TypeName string      `json:"typeName"`
	Schema   interface{} `json:"schema"`
}
