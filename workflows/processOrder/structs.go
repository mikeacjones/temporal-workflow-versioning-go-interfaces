package processOrderWorkflow

import "go.temporal.io/sdk/workflow"

type ProcessOrderInput struct {
	VERSION workflow.Version
}

type ProcessOrderState struct {
	ApprovedBy string
}

type ApprovalSignal struct {
	ApprovedBy string
}

type ProcessOrderResult struct {
}

// interface that all "versioned" workflows use
type processOrder func(ctx workflow.Context, input ProcessOrderInput) (ProcessOrderResult, error)
