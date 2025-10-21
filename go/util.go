package runtime

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"github.com/gin-gonic/gin"
	"github.com/invopop/jsonschema"
	"log"
	"math"
	"math/big"
	mathrand "math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

func ValueToServiceComplete(output any) ServiceCompleteEvent {
	return ServiceCompleteEvent{
		Output:  output,
		IsError: false,
		Error:   sdk.Error{},
	}
}

func ErrorToServiceComplete(err sdk.Error, stacktraceStr string) ServiceCompleteEvent {
	var stacktrace sdk.Stacktrace
	if stacktraceStr != "" {
		stacktrace = sdk.Stacktrace{
			Stacktrace:   stacktraceStr,
			IsAvailable:  true,
			IsCompressed: false,
		}
		_ = stacktrace.Compress()
	}

	return ServiceCompleteEvent{
		Output:     nil,
		IsError:    true,
		Error:      err,
		Stacktrace: stacktrace,
	}
}

func ErrorToApiComplete(err sdk.Error) ApiCompleteEvent {
	return ApiCompleteEvent{
		Response: sdk.ApiResponse{
			StatusCode:      500,
			Header:          make(map[string]string),
			Body:            err.ToJson(),
			IsBase64Encoded: false,
		},
	}
}

func ExtractServiceDescription(serviceMap map[string]ClientService, modelMap map[string]*ModelRegistry) ([]sdk.ServiceDescription, error) {
	var services []sdk.ServiceDescription
	for srvName, srv := range serviceMap {
		modelReg, ok := modelMap[srvName]
		var collections []sdk.CollectionDescription
		if ok {
			collections = modelReg.List()
		}

		serviceData := sdk.ServiceDescription{
			Name:        srvName,
			Methods:     make([]sdk.MethodDescription, 0),
			Collections: collections,
		}

		res, err := srv.ExecuteService(nil, "@definition", nil)
		if err != nil {
			return nil, err
		}

		taskList := res.([]string)
		for _, taskName := range taskList {
			description, err := GetMethodDescription(srv, taskName)
			if err != nil {
				return nil, err
			}

			serviceData.Methods = append(serviceData.Methods, description)
		}

		services = append(services, serviceData)
	}

	return services, nil
}

func GetMethodDescription(service ClientService, method string) (sdk.MethodDescription, error) {
	description, err := service.GetDescription(method)
	if err != nil {
		return sdk.MethodDescription{}, err
	}

	isWorkflow := service.IsWorkflow(method)

	inputType, err := service.GetInputType(method)
	if err != nil {
		return sdk.MethodDescription{}, err
	}

	inputSchema, _, err := getSchema(inputType)
	if err != nil {
		log.Printf("Error getting method description: %s\n", err.Error())
		// skip schema extract errors
		//return MethodDescription{}, err
	}

	return sdk.MethodDescription{
		Name:        method,
		Description: description,
		IsWorkflow:  isWorkflow,
		Input:       inputSchema,
	}, nil
}

func getSchema(obj interface{}) (interface{}, any, error) {
	var schema interface{}
	for _, v := range jsonschema.Reflect(obj).Definitions {
		schema = v
	}

	if reflect.ValueOf(obj).Kind() != reflect.Ptr {
		return nil, nil, errors.New("object must be a pointer")
	}

	pointsToValue := reflect.Indirect(reflect.ValueOf(obj))

	if pointsToValue.Kind() == reflect.Struct {
		return schema, obj, nil
	}

	if pointsToValue.Kind() == reflect.Slice {
		return nil, nil, errors.New("slice not supported as an input")
	}

	return schema, obj, nil
}

func LoadRoutes(httpHandler *gin.Engine) []sdk.RouteData {
	var routes = make([]sdk.RouteData, 0)
	if httpHandler != nil {
		for _, route := range httpHandler.Routes() {
			log.Printf("client: route found %s %s\n", route.Method, route.Path)

			routes = append(routes, sdk.RouteData{
				Method: route.Method,
				Path:   route.Path,
			})
		}
	}
	return routes
}

func ConvertType(input any, output any) error {
	in, err := json.Marshal(input)
	if err != nil {
		return err
	}

	return json.Unmarshal(in, output)
}

func ParseIntSafe(s string) int64 {
	s = strings.TrimSpace(s)
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func GetTypeName[T any](value T) string {
	t := reflect.TypeOf(value)

	// Handling for pointer types to get the base type
	if t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// For named structs, this should now return the struct name
	//fmt.Println("The type name is:", t.Name())
	return t.Name()
}

func IsPointer[T any](value T) bool {
	return reflect.TypeOf(value).Kind() == reflect.Pointer
}

// GetFreePortInRange returns a free TCP port within [start, end] by
// attempting up to maxAttempts random ports in that range.
// NOTE: this function closes the test socket before returning the port,
// so a small race window exists where another process could grab it.
func GetFreePortInRange(start, end, maxAttempts int) (int, error) {
	l, port, err := reservePortInRange(start, end, maxAttempts)
	if err != nil {
		return 0, err
	}
	_ = l.Close() // release immediately (racy)
	return port, nil
}

// ReservePortInRange binds and returns a listener holding a free port
// within [start, end]. Caller is responsible for closing it.
func ReservePortInRange(start, end, maxAttempts int) (net.Listener, int, error) {
	return reservePortInRange(start, end, maxAttempts)
}

// internal: picks a free port within [start, end]
func reservePortInRange(start, end, maxAttempts int) (net.Listener, int, error) {
	if start <= 0 || end < start {
		return nil, 0, fmt.Errorf("invalid range: %d–%d", start, end)
	}
	if maxAttempts <= 0 {
		maxAttempts = end - start + 1
	}

	// seed math/rand from crypto source
	seed, _ := cryptoSeed()
	mathrand.Seed(seed)

	total := end - start + 1
	if maxAttempts > total {
		maxAttempts = total
	}

	// choose ports randomly without repetition
	tried := make(map[int]bool)
	for i := 0; i < maxAttempts; i++ {
		port := start + mathrand.Intn(total)
		if tried[port] {
			i--
			continue
		}
		tried[port] = true

		addr := fmt.Sprintf("127.0.0.1:%d", port)
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l, port, nil
		}
	}
	return nil, 0, errors.New("no free port found in given range")
}

func GetWorkingDirName() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %w", err)
	}
	return filepath.Base(wd), nil
}

func cryptoSeed() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return int64(binary.LittleEndian.Uint64(b[:])), nil
	}
	n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}
