package activities

import (
	"context"
	"errors"
	"os"
	"time"

	processOrderWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processOrder"
	"go.temporal.io/sdk/activity"
)

// workDuration is how long each "real work" activity sleeps before completing.
// Kept modest so orders are visibly in-flight in the demo without taking
// minutes. Override with WORK_DURATION (e.g. "500ms", "2s").
var workDuration = func() time.Duration {
	if v := os.Getenv("WORK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return time.Second
}()

type ProcessOrderActivities struct{}

func (a *ProcessOrderActivities) ValidateOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(workDuration):
		logger.Info("ValidateOrder completed")
		return "ValidateOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *ProcessOrderActivities) ShipOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(workDuration):
		logger.Info("ShipOrder completed")
		return "ShipOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *ProcessOrderActivities) GiftWrapOrder(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	select {
	case <-time.After(workDuration):
		logger.Info("GiftWrapOrder completed")
		return "GiftWrapOrder complete", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// SendThankYou is the step the LATEST workflow version introduced. It currently
// ships with a bug and always fails. Because activity retries are unbounded by
// default, latest (v3) orders get *stuck* retrying this step forever rather than
// terminally failing — so once the bug is fixed and the worker is redeployed,
// the in-flight v3 executions self-heal on their next retry and complete.
//
// Older pinned versions (v1, v2) never call SendThankYou, so they are immune.
func (a *ProcessOrderActivities) SendThankYou(ctx context.Context, input processOrderWorkflow.ProcessOrderInput) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Error("SendThankYou failed: simulated bug in the latest version's new step")
	return "", errors.New("SendThankYou: simulated bug in the latest version (fix me and redeploy)")
}
