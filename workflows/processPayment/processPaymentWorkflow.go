package processPaymentWorkflow

import (
	"go.temporal.io/sdk/workflow"
)

const processPaymentVersionCurrent = 1

type processPaymentWorkflow struct{}

func (processPaymentWorkflow) run(ctx workflow.Context, input ProcessPaymentInput) (ProcessPaymentResult, error) {
	// TODO: implement the current version of ProcessPaymentWorkflow here.
	return ProcessPaymentResult{}, nil
}
