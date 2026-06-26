package processOrderWorkflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func processOrderWorkflowV1(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error) {
	ao := workflow.ActivityOptions{
		// Must exceed the sum of all three WorkDuration constants.
		StartToCloseTimeout: 60 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var workResult string
	if err := workflow.ExecuteActivity(ctx, "ValidateOrder", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, "ShipOrder", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	return ProcessOrderResult{}, nil
}
