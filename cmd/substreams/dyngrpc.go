package main

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/second-state/WasmEdge-go/wasmedge"
	bindgen "github.com/second-state/wasmedge-bindgen/host/go"
	"github.com/spf13/cobra"
	"github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/standard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

var dynCmd = &cobra.Command{
	Use:          "dyngrpc <package>",
	Short:        "Test a dynamic gRPC server",
	RunE:         runDyn,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(dynCmd)
}

func runDyn(cmd *cobra.Command, args []string) error {
	encoding.RegisterCodec(passthroughCodec{})
	s := standard.NewServer(server.NewOptions())
	srv := s.GrpcServer()
	service, err := NewService("./cmd/substreams/dyntest/eth_xfer.wasm")
	if err != nil {
		return fmt.Errorf("init service: %w", err)
	}

	// TODO: generate this ServiceDesc struct from the
	// loaded .proto files we'd receive (from within the Substreams `spkg` ?)
	// or reuse the dynamic protobuf loading we use in `substreams`.
	var v interface{}
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "sf.mycustomer.v1.Eth",
		HandlerType: service,
		Methods:     []grpc.MethodDesc{},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Transfers",
				Handler:       service.New("sf.mycustomer.v1.Eth.Transfers").handle,
				ServerStreams: true,
			},
		},
		Metadata: "sf/mycustomer/v1/eth.proto",
	}, v)

	fmt.Println("Listening on :7878")
	s.Launch(":7878")

	return nil
}

type Service struct {
	bg *bindgen.Bindgen
	vm *wasmedge.VM

	kv map[string]string
}

func NewService(wasmFile string) (*Service, error) {
	srv := &Service{kv: map[string]string{
		"key1": "value1",
	}}
	// See: https://github.com/second-state/WasmEdge-go-examples/blob/master/wasmedge-bindgen/go_BindgenFuncs/bindgen_funcs.go
	wasmedge.SetLogErrorLevel()
	conf := wasmedge.NewConfigure(wasmedge.WASI)
	vm := wasmedge.NewVMWithConfig(conf)

	impobj := wasmedge.NewModule("host")
	hostftype := wasmedge.NewFunctionType(
		[]wasmedge.ValType{
			wasmedge.ValType_I32,
			wasmedge.ValType_I32,
			wasmedge.ValType_I32,
		},
		[]wasmedge.ValType{
			wasmedge.ValType_I32,
		})
	hostprint := wasmedge.NewFunction(hostftype, srv.getKey, nil, 0)
	hostftype.Release()
	impobj.AddFunction("get_key", hostprint)
	vm.RegisterModule(impobj)

	wasi := vm.GetImportModule(wasmedge.WASI)
	wasi.InitWasi(nil, nil, nil)
	if err := vm.LoadWasmFile(wasmFile); err != nil {
		return nil, fmt.Errorf("load wasm: %w", err)
	}
	if err := vm.Validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	bg := bindgen.New(vm)
	if err := bg.GetVm().Instantiate(); err != nil {
		return nil, fmt.Errorf("error instantiating VM: %w", err)
	}
	srv.bg = bg
	srv.vm = vm
	return srv, nil
}

func (s *Service) getKey(_ interface{}, callframe *wasmedge.CallingFrame, params []interface{}) ([]interface{}, wasmedge.Result) {
	// As in: https://github.com/second-state/WasmEdge-go-examples/blob/master/go_HostFunc/hostfunc.go
	mem := callframe.GetMemoryByIndex(0)

	keyPtr := params[0].(int32)
	keySize := params[1].(int32)
	data, _ := mem.GetData(uint(keyPtr), uint(keySize))
	key := make([]byte, keySize)

	copy(key, data)

	val, found := s.kv[string(key)]
	if !found {
		return []interface{}{int32(0)}, wasmedge.Result_Success
	}

	valuePtr := s.allocate(int32(len(val)))
	data, _ = mem.GetData(uint(valuePtr), uint(len(val)))

	copy(data, val)

	outputPtr := params[2].(int32)
	data, _ = mem.GetData(uint(outputPtr), uint(8))
	binary.LittleEndian.PutUint32(data[0:4], uint32(valuePtr))
	binary.LittleEndian.PutUint32(data[4:], uint32(len(val)))

	return []interface{}{1}, wasmedge.Result_Success
}

func (s *Service) allocate(size int32) int32 {
	allocateResult, err := s.vm.Execute("allocate", size)
	if err != nil {
		panic(err)
	}
	pointerOfPointers := allocateResult[0].(int32)
	return pointerOfPointers
}

type Handler struct {
	exportName string
	service    *Service
}

func (s *Service) New(streamName string) *Handler {
	exportName := strings.Replace(streamName, ".", "_", -1)
	exportName = strings.Replace(exportName, "/", "_", -1)
	exportName = strings.ToLower(exportName)
	// TODO: do validation that there are only letters and digits left

	return &Handler{
		exportName: exportName,
		service:    s,
	}
}
func (h *Handler) handle(server interface{}, stream grpc.ServerStream) error {
	t0 := time.Now()
	defer func() {
		fmt.Println("Timing:", time.Since(t0))
	}()

	m := NewPassthroughBytes()
	if err := stream.RecvMsg(m); err != nil {
		return err
	}

	res, _, err := h.service.bg.Execute(h.exportName, m.Bytes)
	if err != nil {
		return fmt.Errorf("executing func %q: %w", h.exportName, err)
	}

	out := NewPassthroughBytes()
	out.Set(res[0].([]byte))

	if err := stream.SendMsg(out); err != nil {
		return fmt.Errorf("send msg: %w", err)
	}
	return nil
}

// Codec

type passthroughCodec struct{}

func (passthroughCodec) Marshal(v interface{}) ([]byte, error) {
	return v.(*passthroughBytes).Bytes, nil
}

func (passthroughCodec) Unmarshal(data []byte, v interface{}) error {
	el := v.(*passthroughBytes)
	el.Bytes = data
	return nil
}

func (passthroughCodec) Name() string { return "proto" }

// Passing bytes around

type passthroughBytes struct {
	Bytes []byte
}

func NewPassthroughBytes() *passthroughBytes {
	return &passthroughBytes{}
}
func (b *passthroughBytes) Set(in []byte) {
	b.Bytes = in
}
