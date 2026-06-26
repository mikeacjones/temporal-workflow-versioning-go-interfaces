package main

import (
	"log"

	"github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/activities"
	processOrderWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processOrder"
	processPaymentWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processPayment"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Client failed %v", err)
	}
	defer c.Close()

	w := worker.New(c, "process-order-queue", worker.Options{})

	w.RegisterWorkflow(processOrderWorkflow.ProcessOrderWorkflow)
	w.RegisterWorkflow(processPaymentWorkflow.ProcessPaymentWorkflow)
	w.RegisterActivity(&activities.ProcessOrderActivities{})

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker failed %v", err)
	}
}
