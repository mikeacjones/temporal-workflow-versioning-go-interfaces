package activities

import (
	"context"
	"time"

	processOrderWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processOrder"
	"go.temporal.io/sdk/activity"
)

type ProcessOrderActivities struct{}

func (a *ProcessOrderActivities) ValidateOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(10 * time.Second):
		logger.Info("ValidateOrder completed")
		return "ValidateOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *ProcessOrderActivities) ShipOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(10 * time.Second):
		logger.Info("ShipOrder completed")
		return "ShipOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *ProcessOrderActivities) GiftWrapOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(10 * time.Second):
		logger.Info("GiftWrapOrder completed")
		return "GiftWrapOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *ProcessOrderActivities) SendThankYou(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(10 * time.Second):
		logger.Info("SendThankYou completed")
		return "SendThankYou complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
