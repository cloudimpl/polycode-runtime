package runtime

//import (
//	"fmt"
//	"github.com/cloudimpl/polycode-runtime-go/sdk"
//	"github.com/gin-gonic/gin"
//	"net/http"
//	"time"
//)
//
//func startMockBackendServer(port int) {
//	router := gin.Default()
//
//	// Health check
//	router.GET("/v1/health", func(c *gin.Context) {
//		c.JSON(http.StatusOK, gin.H{"status": "ok"})
//	})
//
//	// --- App & Service Execution ---
//	router.POST("/v1/system/app/start", empty200)
//	router.POST("/v1/context/service/exec", func(c *gin.Context) {
//		c.JSON(200, ExecServiceResponse{
//			Output: map[string]any{"service": "ok"},
//		})
//	})
//	router.POST("/v1/context/api/exec", func(c *gin.Context) {
//		c.JSON(200, ExecApiResponse{
//			Response: sdk.ApiResponse{
//				StatusCode: 200,
//				Header:     map[string]string{"X-Mock": "true"},
//				Body:       `{"result":"api-ok"}`,
//			},
//		})
//	})
//	router.POST("/v1/context/func/exec", func(c *gin.Context) {
//		c.JSON(200, ExecFuncResponse{
//			Output: "func-output",
//		})
//	})
//	router.POST("/v1/context/func/exec/result", empty200)
//
//	// --- DB ---
//	router.POST("/v1/context/db/get", mockJSON)
//	router.POST("/v1/context/db/unsafe-get", mockJSON)
//	router.POST("/v1/context/db/query", func(c *gin.Context) {
//		c.JSON(200, []map[string]any{{"key": "val"}})
//	})
//	router.POST("/v1/context/db/unsafe-query", func(c *gin.Context) {
//		c.JSON(200, []map[string]any{{"key": "unsafe"}})
//	})
//	router.POST("/v1/context/db/put", empty200)
//	router.POST("/v1/context/db/unsafe-put", empty200)
//
//	// --- File ---
//	router.POST("/v1/context/file/get", func(c *gin.Context) {
//		c.JSON(200, GetFileResponse{Content: "mock-content"})
//	})
//	router.POST("/v1/context/file/get-download-link", func(c *gin.Context) {
//		c.JSON(200, GetLinkResponse{Link: "http://mock.link/download"})
//	})
//	router.POST("/v1/context/file/put", empty200)
//	router.POST("/v1/context/file/get-upload-link", func(c *gin.Context) {
//		c.JSON(200, GetLinkResponse{Link: "http://mock.link/upload"})
//	})
//	router.POST("/v1/context/file/delete", empty200)
//	router.POST("/v1/context/file/rename", empty200)
//	router.POST("/v1/context/file/list", func(c *gin.Context) {
//		c.JSON(200, ListFolderResponse{
//			Files: []ListFileResponse{{
//				Key:          "file1.txt",
//				Size:         123,
//				LastModified: time.Now(),
//			}},
//			IsTruncated:           false,
//			NextContinuationToken: nil,
//		})
//	})
//	router.POST("/v1/context/file/create-folder", empty200)
//
//	// --- Signal & Event ---
//	router.POST("/v1/context/signal/emit", empty200)
//	router.POST("/v1/context/signal/await", func(c *gin.Context) {
//		c.JSON(200, SignalWaitResponse{
//			IsAsync: false,
//			Output:  "signal-output",
//		})
//	})
//	router.POST("/v1/context/realtime/event/emit", empty200)
//
//	// --- Lock ---
//	router.POST("/v1/context/lock/acquire", empty200)
//	router.POST("/v1/context/lock/release", empty200)
//
//	// --- Meta & Counter ---
//	router.POST("/v1/elevated/context/counter/increment", func(c *gin.Context) {
//		c.JSON(200, IncrementCounterResponse{
//			Value:       42,
//			Incremented: true,
//		})
//	})
//	router.POST("/v1/elevated/context/meta/get", func(c *gin.Context) {
//		c.JSON(200, map[string]interface{}{"meta": "value"})
//	})
//
//	// --- Misc ---
//	router.POST("/v1/context/acknowledge", empty200)
//
//	router.Run(getAddr(port))
//}
//
//func getAddr(port int) string {
//	if port == 0 {
//		port = 8080
//	}
//	return ":" + fmt.Sprint(port)
//}
//
//func empty200(c *gin.Context) {
//	c.Status(http.StatusOK)
//}
//
//func mockJSON(c *gin.Context) {
//	c.JSON(200, map[string]interface{}{
//		"key":   "value",
//		"value": 123,
//	})
//}
