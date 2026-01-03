package server

import (
	"context"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinternet/databaise/internal/logging"
)

var log = logging.New("server")

var server = mcp.NewServer(&mcp.Implementation{
	Name:    "databaise",
	Version: "2.0.0",
}, &mcp.ServerOptions{})

type Tool struct {
	Name        string
	Description string
}

type Handler[In, Out any] func(ctx context.Context, args In) (Out, error)

func AddTool[In, Out any](handler Handler[In, Out], tool Tool) {
	t := &mcp.Tool{
		Name:        tool.Name,
		Description: tool.Description,
	}

	mcp.AddTool(server, t, func(ctx context.Context, request *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Out, error) {
		res, err := handler(ctx, input)
		return nil, res, err
	})
}

func StartHTTP(address string) {
	log.Printf("Starting HTTP server on %s", address)
	handler := mcp.NewStreamableHTTPHandler(func(req *http.Request) *mcp.Server { return server }, nil)
	if err := http.ListenAndServe(address, handler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func StartSTDIO() {
	log.Printf("Starting STDIO server")
	logging.SetOutput(os.Stderr)
	t := &mcp.LoggingTransport{Transport: &mcp.StdioTransport{}, Writer: os.Stderr}
	if err := server.Run(context.Background(), t); err != nil {
		log.Printf("ERROR: Server failed: %v", err)
	}
}
