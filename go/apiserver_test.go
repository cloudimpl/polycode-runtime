package runtime

//import (
//	"bytes"
//	"context"
//	"encoding/json"
//	"fmt"
//	"github.com/cloudimpl/polycode-runtime-go/sdk"
//	"github.com/stretchr/testify/assert"
//	"io"
//	"net"
//	"net/http"
//	"testing"
//	"time"
//)
//
//// ---------- Mock Runtime ----------
//
//type mockRuntime struct{}
//
//func (m mockRuntime) RunApi(ctx context.Context, input ApiStartEvent) ApiCompleteEvent {
//	return ApiCompleteEvent{
//		Path: "/test",
//		Response: sdk.ApiResponse{
//			StatusCode:      200,
//			Header:          map[string]string{"Content-Type": "application/json"},
//			Body:            `{"message":"ok"}`,
//			IsBase64Encoded: false,
//		},
//		Logs: []LogMsg{
//			{
//				Level:     InfoLevel,
//				Section:   "auth",
//				Tags:      map[string]interface{}{"user": "abc123"},
//				Timestamp: time.Now().Unix(),
//				Message:   "Authentication successful",
//			},
//		},
//	}
//}
//
//func (m mockRuntime) RunService(ctx context.Context, input ServiceStartEvent) ServiceCompleteEvent {
//	return ServiceCompleteEvent{
//		IsError: false,
//		Output:  map[string]string{"result": "success"},
//	}
//}
//
//// ---------- Helper: Get Free Port ----------
//
//func getFreePort() (int, error) {
//	l, err := net.Listen("tcp", ":0")
//	if err != nil {
//		return 0, err
//	}
//	defer l.Close()
//	return l.Addr().(*net.TCPAddr).Port, nil
//}
//
//// ---------- Helper: Start Server ----------
//
//func startTestServer(t *testing.T, r ApiServerListener) string {
//	port, err := getFreePort()
//	assert.NoError(t, err)
//
//	server := &ApiServer{listener: r}
//	go server.Start(port)
//
//	url := fmt.Sprintf("http://localhost:%d", port)
//
//	// Wait until server is ready
//	for i := 0; i < 30; i++ {
//		resp, err := http.Get(url + "/v1/health")
//		if err == nil && resp.StatusCode == http.StatusOK {
//			resp.Body.Close()
//			return url
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//
//	t.Fatalf("server failed to start on port %d", port)
//	return ""
//}
//
//// ---------- Tests ----------
//
//func TestHealthCheck_RealServer(t *testing.T) {
//	baseURL := startTestServer(t, mockRuntime{})
//
//	resp, err := http.Get(baseURL + "/v1/health")
//	assert.NoError(t, err)
//	defer resp.Body.Close()
//
//	body, _ := io.ReadAll(resp.Body)
//	assert.Equal(t, http.StatusOK, resp.StatusCode)
//	assert.Contains(t, string(body), `"status":"ok"`)
//}
//
//func TestInvokeApiHandler_RealServer(t *testing.T) {
//	baseURL := startTestServer(t, mockRuntime{})
//
//	req := ApiStartEvent{
//		SessionId: "sess-1",
//		Meta: sdk.HandlerContextMeta{
//			OrgId:        "org-1",
//			EnvId:        "dev",
//			AppName:      "testApp",
//			AppId:        "app-123",
//			TenantId:     "tenant-abc",
//			PartitionKey: "user-1",
//			TaskGroup:    "tg",
//			TaskName:     "tn",
//			TaskId:       "task-123",
//			ParentId:     "parent",
//			TraceId:      "trace",
//			InputId:      "input",
//			Caller: sdk.CallerContextMeta{
//				AppName:  "callerApp",
//				AppId:    "cid",
//				TaskName: "callerTask",
//				TaskId:   "ctid",
//			},
//		},
//		AuthContext: sdk.AuthContext{
//			Claims: map[string]interface{}{"sub": "user-123"},
//		},
//		Request: sdk.ApiRequest{
//			Id:     "req-id",
//			Host:   "localhost",
//			Method: "POST",
//			Path:   "/test",
//			Query:  map[string]string{"q": "1"},
//			Header: map[string]string{"X-Test": "true"},
//			Body:   `{"hello":"world"}`,
//		},
//	}
//
//	data, _ := json.Marshal(req)
//	resp, err := http.Post(baseURL+"/v1/invoke/api", "application/json", bytes.NewBuffer(data))
//	assert.NoError(t, err)
//	defer resp.Body.Close()
//
//	respBody, _ := io.ReadAll(resp.Body)
//	assert.Equal(t, http.StatusOK, resp.StatusCode)
//	assert.Contains(t, string(respBody), `"statusCode":200`)
//	assert.Contains(t, string(respBody), `"message":"Authentication successful"`)
//	assert.Contains(t, string(respBody), `"body":"{\"message\":\"ok\"}"`)
//}
//
//func TestInvokeServiceHandler_RealServer(t *testing.T) {
//	baseURL := startTestServer(t, mockRuntime{})
//
//	req := ServiceStartEvent{
//		SessionId: "sess-2",
//		Service:   "my-service",
//		Method:    "doStuff",
//		Meta: sdk.HandlerContextMeta{
//			OrgId:        "org-xyz",
//			EnvId:        "prod",
//			AppName:      "realApp",
//			AppId:        "appid-999",
//			TenantId:     "tenant-z",
//			PartitionKey: "user-abc",
//			TaskGroup:    "group",
//			TaskName:     "task",
//			TaskId:       "id123",
//			ParentId:     "pid",
//			TraceId:      "trid",
//			InputId:      "inpid",
//			Caller: sdk.CallerContextMeta{
//				AppName:  "callerX",
//				AppId:    "callerX-id",
//				TaskName: "cTask",
//				TaskId:   "ctid",
//			},
//		},
//		AuthContext: sdk.AuthContext{
//			Claims: map[string]interface{}{"role": "admin"},
//		},
//		Input: map[string]interface{}{"foo": "bar"},
//	}
//
//	data, _ := json.Marshal(req)
//	resp, err := http.Post(baseURL+"/v1/invoke/service", "application/json", bytes.NewBuffer(data))
//	assert.NoError(t, err)
//	defer resp.Body.Close()
//
//	respBody, _ := io.ReadAll(resp.Body)
//	assert.Equal(t, http.StatusOK, resp.StatusCode)
//	assert.Contains(t, string(respBody), `"result":"success"`)
//}
