package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"

	processOrderWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processOrder"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Client failed %v", err)
	}
	defer c.Close()

	// Start a V1 workflow
	startWorkflow(c, 1)

	// Start a V2 workflow
	startWorkflow(c, 2)

	// Start a V3 workflow
	startWorkflow(c, 3)

	// Start the latest workflow
	startWorkflow(c, -1)

	// Start an unsupported version and cause a panic
	startWorkflow(c, 5)
}

func startWorkflow(c client.Client, v workflow.Version) {
	we, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{
			ID:        fmt.Sprintf("process-order-%s", workflowNonce()),
			TaskQueue: "process-order-queue", // must match the worker
		},
		processOrderWorkflow.ProcessOrderWorkflow,
		processOrderWorkflow.ProcessOrderInput{
			VERSION: v,
		},
	)
	if err != nil {
		log.Fatalf("start failed: %v", err)
	}
	log.Printf("started WorkflowID=%s RunID=%s", we.GetID(), we.GetRunID())
}

func workflowNonce() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	// Encode the bytes to Base64 (results in exactly 6 characters)
	return base64.RawURLEncoding.EncodeToString(b)[:6]
}
