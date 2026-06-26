package processOrderWorkflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type processOrderWorkflowV3 struct{}

func (processOrderWorkflowV3) run(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error) {
	ao := workflow.ActivityOptions{
		// Must exceed the sum of all three WorkDuration constants.
		StartToCloseTimeout: 60 * time.Second,
		// Retries are unbounded (no MaximumAttempts) so a buggy step makes the
		// workflow get *stuck* rather than terminally fail — fix the activity,
		// redeploy, and the in-flight execution self-heals on its next retry.
		// Cap the backoff so that self-heal happens within a few seconds.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Second,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var workResult string
	if err := workflow.ExecuteActivity(ctx, "ValidateOrder", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, "GiftWrapOrder", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, "ShipOrder", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	if err := workflow.ExecuteActivity(ctx, "SendThankYou", input).Get(ctx, &workResult); err != nil {
		return ProcessOrderResult{}, err
	}

	return ProcessOrderResult{}, nil
}
