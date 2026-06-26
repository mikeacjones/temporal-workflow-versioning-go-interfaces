package processPaymentWorkflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

type ProcessPaymentInput struct {
	VERSION workflow.Version
}

type ProcessPaymentResult struct {
}

const flowChangeID = "workflow/processPayment"
const MIN_VERSION = 1

type processPayment interface {
	run(ctx workflow.Context, input ProcessPaymentInput) (ProcessPaymentResult, error)
}

func ProcessPaymentWorkflow(ctx workflow.Context, input ProcessPaymentInput) (ProcessPaymentResult, error) {
	return resolveFlowVersion(ctx, input.VERSION).run(ctx, input)
}

func resolveFlowVersion(ctx workflow.Context, v workflow.Version) processPayment {
	if v <= 0 {
		v = workflow.GetVersion(ctx, flowChangeID, MIN_VERSION, processPaymentVersionCurrent)
	} else {
		workflow.GetVersion(ctx, flowChangeID, v, v) //adds the version marker and search attribute into history
	}

	versions := map[workflow.Version]processPayment{
		processPaymentVersionCurrent: processPaymentWorkflow{},
	}

	version, ok := versions[v]
	if !ok {
		panic(fmt.Sprintf("unsupported %s version %d", flowChangeID, v))
	}
	return version
}
