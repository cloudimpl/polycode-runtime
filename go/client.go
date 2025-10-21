package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"log"
	"net/http"
	"time"
)

const SessionIdHeader = "x-polycode-task-session-id"

type StartAppRequest struct {
	AppName     string                   `json:"appName"`
	AppEndpoint string                   `json:"appEndpoint"`
	Services    []sdk.ServiceDescription `json:"services"`
	Routes      []sdk.RouteData          `json:"routes"`
}

type ExecAgentRequest struct {
	EnvId      string          `json:"envId"`
	AgentName  string          `json:"service"`
	SessionKey string          `json:"sessionKey"`
	Options    sdk.TaskOptions `json:"options"`
	Input      sdk.AgentInput  `json:"input"`
}

type ExecServiceRequest struct {
	EnvId   string            `json:"envId"`
	Service string            `json:"service"`
	Method  string            `json:"method"`
	Options sdk.TaskOptions   `json:"options"`
	Headers map[string]string `json:"headers"`
	Input   any               `json:"input"`
}

type ExecServiceResponse struct {
	Output  any       `json:"output"`
	IsError bool      `json:"isError"`
	Error   sdk.Error `json:"error"`
}

type ExecAgentResponse struct {
	Output  any       `json:"output"`
	IsError bool      `json:"isError"`
	Error   sdk.Error `json:"error"`
}

type ExecApiRequest struct {
	EnvId      string          `json:"envId"`
	Controller string          `json:"controller"`
	Path       string          `json:"path"`
	Options    sdk.TaskOptions `json:"options"`
	Request    sdk.ApiRequest  `json:"request"`
}

type ExecApiResponse struct {
	Response sdk.ApiResponse `json:"response"`
	IsError  bool            `json:"isError"`
	Error    sdk.Error       `json:"error"`
}

type ExecAppRequest struct {
	EnvId   string            `json:"envId"`
	AppName string            `json:"appName"`
	Method  string            `json:"method"`
	Options sdk.TaskOptions   `json:"options"`
	Headers map[string]string `json:"headers"`
	Input   any               `json:"input"`
}

type ExecAppResponse struct {
	Output  any       `json:"output"`
	IsError bool      `json:"isError"`
	Error   sdk.Error `json:"error"`
}

type ExecFuncRequest struct {
	Input any `json:"input"`
}

type ExecFuncResult struct {
	Input   any       `json:"input"`
	Output  any       `json:"output"`
	IsError bool      `json:"isError"`
	Error   sdk.Error `json:"error"`
}

type ExecFuncResponse struct {
	IsCompleted bool      `json:"isCompleted"`
	Output      any       `json:"output"`
	IsError     bool      `json:"isError"`
	Error       sdk.Error `json:"error"`
}

type GetDataRequest struct {
	Scope    sdk.DataScope `json:"scope"`
	TenantId string        `json:"tenantId"`
	Path     string        `json:"path"`
}

type GetDataResponse struct {
	Path    string                 `json:"path"`
	Exist   bool                   `json:"exist"`
	Version int64                  `json:"version"`
	Data    map[string]interface{} `json:"data"`
}

type QueryDataRequest struct {
	Scope          sdk.DataScope `json:"scope"`
	TenantId       string        `json:"tenantId"`
	CollectionPath string        `json:"collectionPath"`
	Filter         string        `json:"filter"`
	Args           []interface{} `json:"args"`
	OffsetToken    string        `json:"offsetToken"`
	Limit          int           `json:"limit"`
}

type QueryDataResponse struct {
	Data      []GetDataResponse `json:"data"`
	NextToken string            `json:"nextToken"`
}

type InsertDataRequest struct {
	Scope          sdk.DataScope          `json:"scope"`
	TenantId       string                 `json:"tenantId"`
	Path           string                 `json:"path"`
	ParentPath     string                 `json:"parentPath"`
	CollectionPath string                 `json:"collectionPath"`
	Type           string                 `json:"type"`
	Id             string                 `json:"id"`
	Item           map[string]interface{} `json:"item"`
	Cfg            sdk.WriteConfig        `json:"cfg"`
}

type UpdateDataRequest struct {
	Scope    sdk.DataScope          `json:"scope"`
	TenantId string                 `json:"tenantId"`
	Path     string                 `json:"path"`
	Item     map[string]interface{} `json:"item"`
	Cfg      sdk.WriteConfig        `json:"cfg"`
}

type DeleteDataRequest struct {
	Scope    sdk.DataScope   `json:"scope"`
	TenantId string          `json:"tenantId"`
	Path     string          `json:"path"`
	Cfg      sdk.WriteConfig `json:"cfg"`
}

type UpdateTTLRequest struct {
	Scope    sdk.DataScope   `json:"scope"`
	TenantId string          `json:"tenantId"`
	Path     string          `json:"path"`
	Cfg      sdk.WriteConfig `json:"cfg"`
}

type GetScopeFileRequest struct {
	Scope   sdk.DataScope  `json:"scope"`
	Request GetFileRequest `json:"request"`
}

// GetFileRequest represents the JSON structure for get file operations
type GetFileRequest struct {
	TenantId string `json:"tenantId"`
	Path     string `json:"path"`
}

// GetFileResponse represents the JSON structure for get file response
type GetFileResponse struct {
	Path     string           `json:"path"`
	Metadata sdk.FileMetaData `json:"metadata"`
}

type ReadScopeFileContentRequest struct {
	Scope   sdk.DataScope          `json:"scope"`
	Request ReadFileContentRequest `json:"request"`
}

type ReadFileContentRequest struct {
	TenantId string `json:"tenantId"`
	Path     string `json:"path"`
}

type ReadFileContentResponse struct {
	Content string `json:"content"`
}

type GetScopeLinkRequest struct {
	Scope   sdk.DataScope  `json:"scope"`
	Request GetLinkRequest `json:"request"`
}

type GetLinkRequest struct {
	TenantId string `json:"tenantId"`
	Path     string `json:"path"`
}

type GetLinkResponse struct {
	Link string `json:"link"`
}

type PutScopeFileRequest struct {
	Scope   sdk.DataScope  `json:"scope"`
	Request PutFileRequest `json:"request"`
}

// PutFileRequest represents the JSON structure for put file operations
type PutFileRequest struct {
	TenantId      string `json:"tenantId"`
	Path          string `json:"path"`
	Content       string `json:"content"`
	LocalFilePath string `json:"filePath"`
}

type DeleteScopeFileRequest struct {
	Scope   sdk.DataScope     `json:"scope"`
	Request DeleteFileRequest `json:"request"`
}

type DeleteFileRequest struct {
	TenantId string `json:"tenantId"`
	Path     string `json:"path"`
}

type RenameScopeFileRequest struct {
	Scope   sdk.DataScope     `json:"scope"`
	Request RenameFileRequest `json:"request"`
}

type RenameFileRequest struct {
	TenantId string `json:"tenantId"`
	OldPath  string `json:"oldPath"`
	NewPath  string `json:"newPath"`
}

type CreateScopeFolderRequest struct {
	Scope   sdk.DataScope       `json:"scope"`
	Request CreateFolderRequest `json:"request"`
}

type CreateFolderRequest struct {
	TenantId   string `json:"tenantId"`
	FolderPath string `json:"folderPath"`
}

type ListScopeFolderRequest struct {
	Scope   sdk.DataScope     `json:"scope"`
	Request ListFolderRequest `json:"request"`
}

type ListFolderRequest struct {
	TenantId    string  `json:"tenantId"`
	FolderPath  string  `json:"folderPath"`
	OffsetToken *string `json:"offsetToken"`
	Limit       int32   `json:"limit"`
}

type ListFolderResponse struct {
	Files     []GetFileResponse `json:"files"`
	NextToken *string           `json:"nextContinuationToken"`
}

type SignalEmitRequest struct {
	TaskId     string    `json:"taskId"`
	SignalName string    `json:"signalName"`
	Output     any       `json:"output"`
	IsError    bool      `json:"isError"`
	Error      sdk.Error `json:"error"`
}

type RealtimeEventEmitRequest struct {
	Channel string `json:"channel"`
	Input   any    `json:"input"`
}

type SignalWaitRequest struct {
	SignalName string `json:"signalName"`
}

type SignalWaitResponse struct {
	IsAsync bool      `json:"isAsync"`
	Output  any       `json:"output"`
	IsError bool      `json:"isError"`
	Error   sdk.Error `json:"error"`
}

type AcquireLockRequest struct {
	Key string `json:"key"`
	TTL int64  `json:"TTL"`
}

type ReleaseLockRequest struct {
	Key string `json:"key"`
}

type ErrorEvent struct {
	Error sdk.Error `json:"error"`
}

// NewServiceClient creates a new ServiceClient with a reusable HTTP client
func NewServiceClient(baseURL string) ServiceClient {
	return &ServiceClientImpl{
		httpClient: &http.Client{
			Timeout: time.Second * 30, // Set a reasonable timeout for HTTP requests
		},
		baseURL: baseURL,
	}
}

type ServiceClient interface {
	StartApp(req StartAppRequest) error

	CallService(sessionId string, req ExecServiceRequest) (ExecServiceResponse, error)
	SendService(sessionId string, req ExecServiceRequest) error
	CallAgent(sessionId string, req ExecAgentRequest) (ExecAgentResponse, error)
	CallApi(sessionId string, req ExecApiRequest) (ExecApiResponse, error)
	CallApp(sessionId string, req ExecAppRequest) (ExecAppResponse, error)
	SendApp(sessionId string, req ExecAppRequest) error
	ExecFunc(sessionId string, req ExecFuncRequest) (ExecFuncResponse, error)
	ExecFuncResult(sessionId string, req ExecFuncResult) error

	GetData(sessionId string, req GetDataRequest) (GetDataResponse, error)
	QueryData(sessionId string, req QueryDataRequest) (QueryDataResponse, error)
	InsertData(sessionId string, req InsertDataRequest) error
	UpdateData(sessionId string, req UpdateDataRequest) error
	DeleteData(sessionId string, req DeleteDataRequest) error
	UpdateTTL(sessionId string, req UpdateTTLRequest) error

	GetFile(sessionId string, req GetScopeFileRequest) (GetFileResponse, error)
	PutFile(sessionId string, req PutScopeFileRequest) error
	DeleteFile(sessionId string, req DeleteScopeFileRequest) error
	RenameFile(sessionId string, req RenameScopeFileRequest) error
	GetFileDownloadLink(sessionId string, req GetScopeFileRequest) (GetLinkResponse, error)
	GetFileUploadLink(sessionId string, req GetScopeFileRequest) (GetLinkResponse, error)
	ListFolder(sessionId string, req ListScopeFolderRequest) (ListFolderResponse, error)
	CreateFolder(sessionId string, req CreateScopeFolderRequest) error

	EmitSignal(sessionId string, req SignalEmitRequest) error
	WaitForSignal(sessionId string, req SignalWaitRequest) (SignalWaitResponse, error)
	EmitRealtimeEvent(sessionId string, req RealtimeEventEmitRequest) error

	AcquireLock(sessionId string, req AcquireLockRequest) error
	ReleaseLock(sessionId string, req ReleaseLockRequest) error
}

// ServiceClientImpl is a reusable client for calling the service API
type ServiceClientImpl struct {
	httpClient *http.Client
	baseURL    string
}

func (sc *ServiceClientImpl) StartApp(req StartAppRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, "", "v1/system/app/start", req)
}

func (sc *ServiceClientImpl) CallService(sessionId string, req ExecServiceRequest) (ExecServiceResponse, error) {
	var res ExecServiceResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/service/call", req, &res)
	if err != nil {
		return ExecServiceResponse{}, err
	}

	return res, nil
}

func (sc *ServiceClientImpl) SendService(sessionId string, req ExecServiceRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/service/send", req)
}

func (sc *ServiceClientImpl) CallAgent(sessionId string, req ExecAgentRequest) (ExecAgentResponse, error) {
	var res ExecAgentResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/agent/call", req, &res)
	if err != nil {
		return ExecAgentResponse{}, err
	}

	return res, nil
}

func (sc *ServiceClientImpl) CallApi(sessionId string, req ExecApiRequest) (ExecApiResponse, error) {
	var res ExecApiResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/api/call", req, &res)
	if err != nil {
		return ExecApiResponse{}, err
	}

	return res, nil
}

func (sc *ServiceClientImpl) CallApp(sessionId string, req ExecAppRequest) (ExecAppResponse, error) {
	var res ExecAppResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/app/call", req, &res)
	if err != nil {
		return ExecAppResponse{}, err
	}

	return res, nil
}

func (sc *ServiceClientImpl) SendApp(sessionId string, req ExecAppRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/app/send", req)
}

func (sc *ServiceClientImpl) ExecFunc(sessionId string, req ExecFuncRequest) (ExecFuncResponse, error) {
	var res ExecFuncResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/func/exec", req, &res)
	if err != nil {
		return ExecFuncResponse{}, err
	}

	return res, nil
}

func (sc *ServiceClientImpl) ExecFuncResult(sessionId string, req ExecFuncResult) error {
	var res ExecFuncResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/func/result", req, &res)
	if err != nil {
		return err
	}

	return nil
}

func (sc *ServiceClientImpl) GetData(sessionId string, req GetDataRequest) (GetDataResponse, error) {
	var res GetDataResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/get", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) QueryData(sessionId string, req QueryDataRequest) (QueryDataResponse, error) {
	var res QueryDataResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/query", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) InsertData(sessionId string, req InsertDataRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/insert", req)
}

func (sc *ServiceClientImpl) UpdateData(sessionId string, req UpdateDataRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/update", req)
}

func (sc *ServiceClientImpl) DeleteData(sessionId string, req DeleteDataRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/delete", req)
}

func (sc *ServiceClientImpl) UpdateTTL(sessionId string, req UpdateTTLRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/db/update-ttl", req)
}

func (sc *ServiceClientImpl) GetFile(sessionId string, req GetScopeFileRequest) (GetFileResponse, error) {
	var res GetFileResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/get", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) GetFileDownloadLink(sessionId string, req GetScopeFileRequest) (GetLinkResponse, error) {
	var res GetLinkResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/get-download-link", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) PutFile(sessionId string, req PutScopeFileRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/put", req)
}

func (sc *ServiceClientImpl) GetFileUploadLink(sessionId string, req GetScopeFileRequest) (GetLinkResponse, error) {
	var res GetLinkResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/get-upload-link", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) DeleteFile(sessionId string, req DeleteScopeFileRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/delete", req)
}

func (sc *ServiceClientImpl) RenameFile(sessionId string, req RenameScopeFileRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/rename", req)
}

func (sc *ServiceClientImpl) ListFolder(sessionId string, req ListScopeFolderRequest) (ListFolderResponse, error) {
	var res ListFolderResponse
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/list", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) CreateFolder(sessionId string, req CreateScopeFolderRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/file/create-folder", req)
}

func (sc *ServiceClientImpl) EmitSignal(sessionId string, req SignalEmitRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/signal/emit", req)
}

func (sc *ServiceClientImpl) WaitForSignal(sessionId string, req SignalWaitRequest) (SignalWaitResponse, error) {
	res := SignalWaitResponse{}
	err := executeApiWithResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/signal/await", req, &res)
	return res, err
}

func (sc *ServiceClientImpl) EmitRealtimeEvent(sessionId string, req RealtimeEventEmitRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/realtime/event/emit", req)
}

func (sc *ServiceClientImpl) AcquireLock(sessionId string, req AcquireLockRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/lock/acquire", req)
}

func (sc *ServiceClientImpl) ReleaseLock(sessionId string, req ReleaseLockRequest) error {
	return executeApiWithoutResponse(sc.httpClient, sc.baseURL, sessionId, "v1/context/lock/release", req)
}

func executeApiWithoutResponse(httpClient *http.Client, baseUrl string, sessionId string, path string, req any) error {
	log.Printf("client: exec api without response from %s with session id %s", path, sessionId)

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s", baseUrl, path), bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-polycode-task-session-id", sessionId)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	} else if resp.StatusCode == http.StatusAccepted {
		// Task paused
		panic(HaltExecution)
	} else {
		errorEvent := ErrorEvent{}
		err = json.NewDecoder(resp.Body).Decode(&errorEvent)
		if err != nil {
			return err
		}
		return errorEvent.Error
	}
}

func executeApiWithResponse[T any](httpClient *http.Client, baseUrl string, sessionId string, path string, req any, res *T) error {
	log.Printf("client: exec api with response from %s with session id %s\n", path, sessionId)

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s", baseUrl, path), bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(SessionIdHeader, sessionId)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if res == nil {
		return ErrSidecarClientFailed.With("response is null")
	}

	if resp.StatusCode == http.StatusOK {
		err = json.NewDecoder(resp.Body).Decode(res)
		if err != nil {
			return err
		}
		return nil
	} else if resp.StatusCode == http.StatusAccepted {
		// Task paused
		panic(HaltExecution)
	} else {
		errorEvent := ErrorEvent{}
		err = json.NewDecoder(resp.Body).Decode(&errorEvent)
		if err != nil {
			return err
		}
		return errorEvent.Error
	}
}
