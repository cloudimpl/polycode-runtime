package runtime

//import (
//	"context"
//	"reflect"
//	"testing"
//
//	"github.com/cloudimpl/polycode-runtime-go/sdk"
//)
//
//// ---- Mock client that satisfies the full ServiceClient interface ----
//
//type mockServiceClient struct {
//	t *testing.T
//	// Expectations used by ExecService assertions:
//	wantSessionId string
//	wantReq       ExecServiceRequest
//
//	// Return values for ExecService
//	out ExecServiceResponse
//	err error
//
//	called bool
//}
//
//func (m *mockServiceClient) ExecService(sessionId string, req ExecServiceRequest) (ExecServiceResponse, error) {
//	m.called = true
//
//	if sessionId != m.wantSessionId {
//		m.t.Fatalf("ExecService: sessionId mismatch: got %q want %q", sessionId, m.wantSessionId)
//	}
//	if req.EnvId != m.wantReq.EnvId {
//		m.t.Fatalf("ExecService: EnvId mismatch: got %q want %q", req.EnvId, m.wantReq.EnvId)
//	}
//	if req.Service != m.wantReq.Service {
//		m.t.Fatalf("ExecService: Service mismatch: got %q want %q", req.Service, m.wantReq.Service)
//	}
//	if req.TenantId != m.wantReq.TenantId {
//		m.t.Fatalf("ExecService: TenantId mismatch: got %q want %q", req.TenantId, m.wantReq.TenantId)
//	}
//	if req.Method != m.wantReq.Method {
//		m.t.Fatalf("ExecService: Method mismatch: got %q want %q", req.Method, m.wantReq.Method)
//	}
//	if req.PartitionKey != m.wantReq.PartitionKey {
//		m.t.Fatalf("ExecService: PartitionKey mismatch: got %q want %q", req.PartitionKey, m.wantReq.PartitionKey)
//	}
//	if got, want := req.Headers[AgentNameHeader], m.wantReq.Headers[AgentNameHeader]; got != want {
//		m.t.Fatalf("ExecService: Headers[%s] mismatch: got %q want %q", AgentNameHeader, got, want)
//	}
//	if !reflect.DeepEqual(req.Options, m.wantReq.Options) {
//		m.t.Fatalf("ExecService: Options mismatch: got %#v want %#v", req.Options, m.wantReq.Options)
//	}
//	if !reflect.DeepEqual(req.Input, m.wantReq.Input) {
//		m.t.Fatalf("ExecService: Input mismatch: got %#v want %#v", req.Input, m.wantReq.Input)
//	}
//	return m.out, m.err
//}
//
//// --- Unused interface methods: return zero values so the type compiles ---
//
//func (*mockServiceClient) StartApp(req StartAppRequest) error { return nil }
//func (*mockServiceClient) ExecApp(string, ExecAppRequest) (ExecAppResponse, error) {
//	return ExecAppResponse{}, nil
//}
//func (*mockServiceClient) ExecApi(string, ExecApiRequest) (ExecApiResponse, error) {
//	return ExecApiResponse{}, nil
//}
//func (*mockServiceClient) ExecFunc(string, ExecFuncRequest) (ExecFuncResponse, error) {
//	return ExecFuncResponse{}, nil
//}
//func (*mockServiceClient) ExecFuncResult(string, ExecFuncResult) error { return nil }
//func (*mockServiceClient) GetItem(string, QueryRequest) (map[string]interface{}, error) {
//	return nil, nil
//}
//func (*mockServiceClient) UnsafeGetItem(string, UnsafeQueryRequest) (map[string]interface{}, error) {
//	return nil, nil
//}
//func (*mockServiceClient) QueryItems(string, QueryRequest) ([]map[string]interface{}, error) {
//	return nil, nil
//}
//func (*mockServiceClient) UnsafeQueryItems(string, UnsafeQueryRequest) ([]map[string]interface{}, error) {
//	return nil, nil
//}
//func (*mockServiceClient) PutItem(string, PutRequest) error             { return nil }
//func (*mockServiceClient) UnsafePutItem(string, UnsafePutRequest) error { return nil }
//func (*mockServiceClient) GetFile(string, GetFileRequest) (GetFileResponse, error) {
//	return GetFileResponse{}, nil
//}
//func (*mockServiceClient) GetFileDownloadLink(string, GetFileRequest) (GetLinkResponse, error) {
//	return GetLinkResponse{}, nil
//}
//func (*mockServiceClient) PutFile(string, PutFileRequest) error { return nil }
//func (*mockServiceClient) GetFileUploadLink(string, GetUploadLinkRequest) (GetLinkResponse, error) {
//	return GetLinkResponse{}, nil
//}
//func (*mockServiceClient) DeleteFile(string, DeleteFileRequest) error { return nil }
//func (*mockServiceClient) RenameFile(string, RenameFileRequest) error { return nil }
//func (*mockServiceClient) ListFile(string, ListFilePageRequest) (ListFilePageResponse, error) {
//	return ListFilePageResponse{}, nil
//}
//func (*mockServiceClient) CreateFolder(string, CreateFolderRequest) error { return nil }
//func (*mockServiceClient) EmitSignal(string, SignalEmitRequest) error     { return nil }
//func (*mockServiceClient) WaitForSignal(string, SignalWaitRequest) (SignalWaitResponse, error) {
//	return SignalWaitResponse{}, nil
//}
//func (*mockServiceClient) EmitRealtimeEvent(string, RealtimeEventEmitRequest) error { return nil }
//func (*mockServiceClient) AcquireLock(string, AcquireLockRequest) error             { return nil }
//func (*mockServiceClient) ReleaseLock(string, ReleaseLockRequest) error             { return nil }
//func (*mockServiceClient) IncrementCounter(string, IncrementCounterRequest) (IncrementCounterResponse, error) {
//	return IncrementCounterResponse{}, nil
//}
//func (*mockServiceClient) GetMeta(string, GetMetaDataRequest) (map[string]interface{}, error) {
//	return nil, nil
//}
//func (*mockServiceClient) Acknowledge(string) error { return nil }
//
//// ---- Tests ----
//
//func TestAgentBuilder_WithTenantIdAndGet(t *testing.T) {
//	ctx := context.Background()
//	mock := &mockServiceClient{t: t}
//
//	b := AgentBuilder{
//		ctx:           ctx,
//		sessionId:     "sess-123",
//		envId:         "env-1",
//		agent:         "echo",
//		serviceClient: mock,
//	}
//
//	aIface := b.WithTenantId("tenant-xyz").Get()
//	a, ok := aIface.(Agent)
//	if !ok {
//		t.Fatalf("Get() did not return concrete Agent type")
//	}
//
//	if a.tenantId != "tenant-xyz" || a.envId != "env-1" || a.agent != "echo" || a.sessionId != "sess-123" || a.ctx != ctx {
//		t.Fatalf("Agent fields not propagated correctly: %+v", a)
//	}
//}
//
//func TestAgentCall_Success(t *testing.T) {
//	ctx := context.Background()
//	agentName := "echo"
//	input := sdk.AgentInput{SessionKey: "user-session-42"}
//	var opts sdk.TaskOptions
//
//	wantReq := ExecServiceRequest{
//		EnvId:        "env-1",
//		Service:      "agent-service",
//		TenantId:     "tenant-xyz",
//		PartitionKey: agentName + ":" + input.SessionKey,
//		Method:       "CallAgent",
//		Options:      opts,
//		Headers:      map[string]string{AgentNameHeader: agentName},
//		Input:        input,
//	}
//
//	mock := &mockServiceClient{
//		t:             t,
//		wantSessionId: "sess-123",
//		wantReq:       wantReq,
//		out: ExecServiceResponse{
//			IsError: false,
//			Output:  map[string]any{"ok": true, "msg": "pong"},
//			// Error zero-value
//		},
//		err: nil,
//	}
//
//	a := Agent{
//		ctx:           ctx,
//		sessionId:     "sess-123",
//		envId:         "env-1",
//		agent:         agentName,
//		serviceClient: mock,
//		tenantId:      "tenant-xyz",
//	}
//
//	respIface := a.Call(opts, input)
//	resp, ok := respIface.(Response)
//	if !ok {
//		t.Fatalf("Call() did not return concrete Response type")
//	}
//
//	if !mock.called {
//		t.Fatalf("ExecService was not called")
//	}
//	if resp.isError {
//		t.Fatalf("expected isError=false, got true (err=%+v)", resp.error)
//	}
//	if !reflect.DeepEqual(resp.output, mock.out.Output) {
//		t.Fatalf("output mismatch: got %#v want %#v", resp.output, mock.out.Output)
//	}
//}
//
//func TestAgentCall_ExecError(t *testing.T) {
//	ctx := context.Background()
//	agentName := "alpha"
//	input := sdk.AgentInput{SessionKey: "s-1"}
//	var opts sdk.TaskOptions
//
//	wantReq := ExecServiceRequest{
//		EnvId:        "env-A",
//		Service:      "agent-service",
//		TenantId:     "tenant-A",
//		PartitionKey: agentName + ":" + input.SessionKey,
//		Method:       "CallAgent",
//		Options:      opts,
//		Headers:      map[string]string{AgentNameHeader: agentName},
//		Input:        input,
//	}
//
//	mock := &mockServiceClient{
//		t:             t,
//		wantSessionId: "sess-A",
//		wantReq:       wantReq,
//		out:           ExecServiceResponse{},       // ignored on error
//		err:           assertableError{"boom-123"}, // triggers error path
//	}
//
//	a := Agent{
//		ctx:           ctx,
//		sessionId:     "sess-A",
//		envId:         "env-A",
//		agent:         agentName,
//		serviceClient: mock,
//		tenantId:      "tenant-A",
//	}
//
//	respIface := a.Call(opts, input)
//	resp, ok := respIface.(Response)
//	if !ok {
//		t.Fatalf("Call() did not return concrete Response type")
//	}
//
//	if !mock.called {
//		t.Fatalf("ExecService was not called")
//	}
//	if !resp.isError {
//		t.Fatalf("expected isError=true, got false")
//	}
//}
//
//// Tiny error type for the error-path test
//type assertableError struct{ msg string }
//
//func (e assertableError) Error() string { return e.msg }
