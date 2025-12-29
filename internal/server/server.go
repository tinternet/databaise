package server

import (
	"context"
	"net/http"
	"os"

	"github.com/google/jsonschema-go/jsonschema"
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
	applyInputSchema[In](t, false)

	mcp.AddTool(server, t, func(ctx context.Context, request *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Out, error) {
		res, err := handler(ctx, input)
		return nil, res, err
	})
}

// AddToolWithDatabaseName registers a tool that includes database_name in the schema.
// PayloadIn is the type used for schema generation (the inner payload type).
// In is the actual handler input type (typically Request[PayloadIn]).
func AddToolWithDatabaseName[PayloadIn, In, Out any](handler Handler[In, Out], tool Tool) {
	t := &mcp.Tool{
		Name:        tool.Name,
		Description: tool.Description,
	}
	applyInputSchema[PayloadIn](t, true)

	mcp.AddTool(server, t, func(ctx context.Context, request *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Out, error) {
		res, err := handler(ctx, input)
		return nil, res, err
	})
}

func applyInputSchema[T any](t *mcp.Tool, addDatabaseName bool) {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		log.Printf("WARNING: Failed to generate schema for tool %s: %v", t.Name, err)
		return
	}

	if addDatabaseName {
		if schema.Properties == nil {
			schema.Properties = make(map[string]*jsonschema.Schema)
		}
		schema.Properties["database_name"] = &jsonschema.Schema{
			Type:        "string",
			Description: "The name of the database to operate on.",
		}
		schema.Required = append([]string{"database_name"}, schema.Required...)
	}

	t.InputSchema = schema
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
