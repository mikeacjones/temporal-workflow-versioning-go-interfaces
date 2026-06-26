package processOrderWorkflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

const flowChangeID = "workflow/processOrder"
const MIN_VERSION = 1

// base pieces that are the same between all versions, eg registering a signal channel, can live here
// this example shows creating an approval channel and setting state for it automatically. whether a version of the
// workflow does anything with it or not is controlled by the workflow version
type ctxKey int

const stateKey ctxKey = 0

// Potentially confusing, but the naming convention right now is ProcessOrderWorkflow = base implementation that is then resolved into
// processOrderWorkflow (leading edge) or processOrderWorkflowVN
func ProcessOrderWorkflow(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error) {
	state := &ProcessOrderState{}
	ctx = workflow.WithValue(ctx, stateKey, state)

	cancelCh := workflow.GetSignalChannel(ctx, "approve")
	workflow.Go(ctx, func(ctx workflow.Context) {
		logger := workflow.GetLogger(ctx)
		for {
			var sig ApprovalSignal
			cancelCh.Receive(ctx, &sig)
			state.ApprovedBy = sig.ApprovedBy
			logger.Info("Signal received and set!")
		}
	})

	return resolveFlowVersion(ctx, input.VERSION).run(ctx, input)
}

func resolveFlowVersion(ctx workflow.Context, v workflow.Version) processOrder {
	if v <= 0 {
		v = workflow.GetVersion(ctx, flowChangeID, MIN_VERSION, processOrderVersionCurrent)
	} else {
		workflow.GetVersion(ctx, flowChangeID, v, v) //adds the version marker and search attribute into history
	}

	switch v {
	case processOrderVersionCurrent:
		return processOrderWorkflow{}
	case 1:
		return processOrderWorkflowV1{}
	case 2:
		return processOrderWorkflowV2{}
	case 3:
		return processOrderWorkflowV3{}
	default:
		panic(fmt.Sprintf("unsupported %s version %d", flowChangeID, v))
	}
}
