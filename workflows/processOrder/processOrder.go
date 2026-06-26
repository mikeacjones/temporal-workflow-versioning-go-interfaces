package processOrderWorkflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

type ProcessOrderInput struct {
	VERSION workflow.Version
}

type ProcessOrderResult struct {
}

const flowChangeID = "workflow/processOrder"
const MIN_VERSION = 1

type processOrder interface {
	run(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error)
}

func ProcessOrderWorkflow(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error) {
	return resolveFlowVersion(ctx, input.VERSION).run(ctx, input)
}

func resolveFlowVersion(ctx workflow.Context, v workflow.Version) processOrder {
	if v <= 0 {
		v = workflow.GetVersion(ctx, flowChangeID, MIN_VERSION, processOrderVersionCurrent)
	} else {
		workflow.GetVersion(ctx, flowChangeID, v, v) //adds the version marker and search attribute into history
	}

	versions := map[workflow.Version]processOrder{
		1:                          processOrderWorkflowV1{},
		2:                          processOrderWorkflowV2{},
		processOrderVersionCurrent: processOrderWorkflow{},
	}

	version, ok := versions[v]
	if !ok {
		panic(fmt.Sprintf("unsupported %s version %d", flowChangeID, v))
	}
	return version
}
