// Command tui is an interactive demo that shows *why* you might let callers
// pin a workflow version when starting it, instead of always defaulting to
// "latest".
//
// It drives two scenarios against a running worker + Temporal server:
//
//   - Naive:       fire 200 orders, all at "latest". The latest version has a
//                  buggy new step (SendThankYou), so every order gets stuck.
//   - Progressive: roll latest out gradually (10% -> 20% -> 50% -> 80% -> 100%),
//                  pinning the rest to the known-good stable version (v2). As
//                  soon as the latest cohort looks unhealthy the rollout HALTS
//                  and all remaining traffic stays on the safe pinned version.
//
// Because activity retries are unbounded, the stuck v3 orders do not terminally
// fail — fix SendThankYou, redeploy the worker, and the in-flight v3 executions
// self-heal on their next retry. Then resume the rollout.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	processOrderWorkflow "github.com/mikeacjones/temporal-workflow-versioning-go-interfaces/workflows/processOrder"
)

const (
	taskQueue = "process-order-queue"

	// stableVersion is the known-good version we pin the bulk of traffic to
	// during a progressive rollout. latestVersion (<= 0) resolves to whatever
	// the worker currently considers "current" (v3 here).
	stableVersion workflow.Version = 2
	latestVersion workflow.Version = -1

	pollInterval = 800 * time.Millisecond

	// Live order stream: a tick fires every arrivalInterval and starts a
	// random 1..maxPerTick orders, giving "several orders a second".
	arrivalInterval = 250 * time.Millisecond
	maxPerTick      = 3

	// During a managed rollout, hold each stage for stageHold while watching
	// the v3 cohort, and auto-halt once haltStuckThreshold v3 orders are stuck.
	stageHold          = 7 * time.Second
	haltStuckThreshold = 3
)

// rolloutStages is the percentage of the live stream routed to "latest" (v3)
// at each step of a managed rollout. The remainder stays pinned to stable.
var rolloutStages = []int{10, 25, 50, 75, 100}

// ---- order state ------------------------------------------------------------

type orderState int

const (
	statePending orderState = iota
	stateRunning
	stateStuck
	stateCompleted
	stateFailed
)

type orderKind int

const (
	kindStable orderKind = iota
	kindLatest
)

func (k orderKind) String() string {
	if k == kindLatest {
		return "latest"
	}
	return "stable"
}

// ---- messages ---------------------------------------------------------------

type orderUpdateMsg struct {
	id    string
	kind  orderKind
	state orderState
}

type logMsg string

type streamStatusMsg struct{ running bool }

// rolloutStatusMsg reports the live rollout: pct of the stream going to v3 and
// a phase label (idle, ramping, complete, naive, halted).
type rolloutStatusMsg struct {
	pct   int
	phase string
}

type haltedMsg struct{ reason string }

type tickMsg time.Time

// ---- controller (talks to Temporal, sends tea.Msgs) -------------------------

type controller struct {
	c       client.Client
	program *tea.Program

	counter atomic.Int64
	runTag  string

	streaming     atomic.Bool  // the live order stream is running
	rolloutPct    atomic.Int32 // 0..100 of the stream currently routed to v3
	rolloutActive atomic.Bool  // a managed rollout goroutine is running
	halt          atomic.Bool  // manual/auto halt requested for the rollout

	// Health of the v3 cohort for the *current* rollout attempt.
	v3Started atomic.Int64
	v3Stuck   atomic.Int64
}

func (ct *controller) send(m tea.Msg) {
	if ct.program != nil {
		ct.program.Send(m)
	}
}

func (ct *controller) logf(format string, a ...any) {
	ct.send(logMsg(fmt.Sprintf(format, a...)))
}

// startOrder kicks off one workflow pinned to v and returns its handle.
func (ct *controller) startOrder(ctx context.Context, v workflow.Version, kind orderKind) (client.WorkflowRun, string, error) {
	id := fmt.Sprintf("order-%s-%s-%05d", ct.runTag, kind, ct.counter.Add(1))
	run, err := ct.c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: id, TaskQueue: taskQueue},
		processOrderWorkflow.ProcessOrderWorkflow,
		processOrderWorkflow.ProcessOrderInput{VERSION: v},
	)
	return run, id, err
}

// monitor polls a single execution and reports state changes to the UI. For
// latest (v3) orders it also bumps the controller's v3Stuck counter the first
// time the order is seen stuck, which is what the rollout watches to auto-halt.
// Polling continues after a stuck verdict so a later self-heal is reflected.
func (ct *controller) monitor(ctx context.Context, id string, kind orderKind) {
	ct.send(orderUpdateMsg{id, kind, statePending})
	stuckCounted := false
	interval := pollInterval

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		resp, err := ct.c.DescribeWorkflowExecution(ctx, id, "")
		if err != nil {
			continue
		}
		switch resp.GetWorkflowExecutionInfo().GetStatus() {
		case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			ct.send(orderUpdateMsg{id, kind, stateCompleted})
			return
		case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
			ct.send(orderUpdateMsg{id, kind, stateFailed})
			return
		default: // RUNNING
			stuck := false
			for _, pa := range resp.GetPendingActivities() {
				if pa.GetLastFailure() != nil {
					stuck = true
					break
				}
			}
			if stuck {
				ct.send(orderUpdateMsg{id, kind, stateStuck})
				if kind == kindLatest && !stuckCounted {
					ct.v3Stuck.Add(1)
					stuckCounted = true
				}
				interval = 3 * time.Second // back off; it's not going to change soon
			} else {
				ct.send(orderUpdateMsg{id, kind, stateRunning})
			}
		}
	}
}

// runStream is the steady-state production traffic: orders arrive continuously
// and are routed by the current rolloutPct — by default 0%, so everything is
// quietly pinned to the known-good stable version. Callers never see the
// version; "start" just means "process this order".
func (ct *controller) runStream(ctx context.Context) {
	if !ct.streaming.CompareAndSwap(false, true) {
		return
	}
	defer func() {
		ct.streaming.Store(false)
		ct.send(streamStatusMsg{false})
	}()

	ct.rolloutPct.Store(0)
	ct.rolloutActive.Store(false)
	ct.halt.Store(false)
	ct.v3Started.Store(0)
	ct.v3Stuck.Store(0)
	ct.send(streamStatusMsg{true})
	ct.send(rolloutStatusMsg{0, "idle"})
	ct.logf("live order stream started — all traffic pinned to stable v%d", stableVersion)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(arrivalInterval):
		}
		n := 1 + rand.Intn(maxPerTick)
		for i := 0; i < n; i++ {
			v, kind := stableVersion, kindStable
			if pct := ct.rolloutPct.Load(); pct > 0 && int32(rand.Intn(100)) < pct {
				v, kind = latestVersion, kindLatest
				ct.v3Started.Add(1)
			}
			if run, _, err := ct.startOrder(ctx, v, kind); err == nil {
				go ct.monitor(ctx, run.GetID(), kind)
			}
		}
	}
}

// startRollout performs a managed, health-gated rollout of v3 against the live
// stream: it ramps the fraction of incoming traffic sent to v3 through
// rolloutStages, holding at each step to watch the v3 cohort. The moment too
// many v3 orders are stuck it auto-halts and routes the whole stream back to
// stable — the bulk of traffic never even noticed.
func (ct *controller) startRollout(ctx context.Context) {
	if !ct.rolloutActive.CompareAndSwap(false, true) {
		ct.logf("a rollout is already in progress")
		return
	}
	defer ct.rolloutActive.Store(false)

	// Wait briefly for the stream to come up (it may have just been started).
	for i := 0; i < 40 && !ct.streaming.Load(); i++ {
		time.Sleep(50 * time.Millisecond)
	}
	ct.halt.Store(false)
	ct.v3Started.Store(0)
	ct.v3Stuck.Store(0)
	ct.logf("starting managed v3 rollout against the live stream")

	for _, pct := range rolloutStages {
		if ct.halt.Load() || ctx.Err() != nil {
			break
		}
		ct.rolloutPct.Store(int32(pct))
		ct.send(rolloutStatusMsg{pct, "ramping"})
		ct.logf("routing %d%% of live traffic to v3 (the rest stays on stable v%d)", pct, stableVersion)

		// Hold at this stage, watching v3 health.
		held := time.Duration(0)
		for held < stageHold {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				held += 500 * time.Millisecond
			}
			if ct.halt.Load() {
				break
			}
			if ct.v3Stuck.Load() >= haltStuckThreshold {
				reason := fmt.Sprintf("%d v3 orders stuck at %d%% rollout", ct.v3Stuck.Load(), pct)
				ct.halt.Store(true)
				ct.rolloutPct.Store(0) // route everything back to the safe version
				ct.send(rolloutStatusMsg{0, "halted"})
				ct.send(haltedMsg{reason})
				ct.logf("AUTO-HALT: %s — live stream reverted to 100%% stable v%d", reason, stableVersion)
				return
			}
		}
		if !ct.halt.Load() {
			ct.logf("v3 healthy at %d%% ✓", pct)
		}
	}

	if !ct.halt.Load() {
		ct.send(rolloutStatusMsg{100, "complete"})
		ct.logf("rollout complete — v3 is healthy; 100%% of live traffic now on latest")
	}
}

// naiveCutover is the "always start at latest" mistake expressed against the
// live stream: flip 100% of incoming traffic to v3 instantly, no gating.
func (ct *controller) naiveCutover() {
	ct.rolloutActive.Store(false)
	ct.halt.Store(false)
	ct.v3Started.Store(0)
	ct.v3Stuck.Store(0)
	ct.rolloutPct.Store(100)
	ct.send(rolloutStatusMsg{100, "naive"})
	ct.logf("NAIVE cutover — 100%% of live traffic flipped to v3 at once, no pinning, no gating")
}

// revertToStable routes the whole live stream back to the stable version.
func (ct *controller) revertToStable(reason string) {
	ct.halt.Store(true)
	ct.rolloutPct.Store(0)
	ct.send(rolloutStatusMsg{0, "halted"})
	ct.send(haltedMsg{reason})
	ct.logf("%s — live stream reverted to 100%% stable v%d", reason, stableVersion)
}

// ---- bubbletea model --------------------------------------------------------

type model struct {
	ct        *controller
	baseCtx   context.Context
	runCtx    context.Context
	runCancel context.CancelFunc

	orders map[string]orderKind  // id -> kind
	states map[string]orderState // id -> state

	streaming    bool
	rolloutPct   int
	rolloutPhase string // idle, ramping, complete, naive, halted
	halted       bool
	haltMsg      string
	logs         []string

	width, height int
}

func newModel(baseCtx context.Context, ct *controller) model {
	runCtx, cancel := context.WithCancel(baseCtx)
	return model{
		ct:           ct,
		baseCtx:      baseCtx,
		runCtx:       runCtx,
		runCancel:    cancel,
		orders:       map[string]orderKind{},
		states:       map[string]orderState{},
		rolloutPhase: "idle",
		logs:         []string{},
	}
}

func tick() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) Init() tea.Cmd { return tick() }

// resetRun cancels everything in flight (stream, monitors, rollout) and clears
// all state for a fresh demo.
func (m model) resetRun() model {
	m.runCancel()
	m.runCtx, m.runCancel = context.WithCancel(m.baseCtx)
	m.ct.streaming.Store(false)
	m.ct.rolloutActive.Store(false)
	m.ct.halt.Store(false)
	m.ct.rolloutPct.Store(0)
	m.ct.v3Started.Store(0)
	m.ct.v3Stuck.Store(0)
	m.orders = map[string]orderKind{}
	m.states = map[string]orderState{}
	m.streaming = false
	m.rolloutPct = 0
	m.rolloutPhase = "idle"
	m.halted = false
	m.haltMsg = ""
	return m
}

// ensureStream starts the live stream if it isn't already running.
func (m model) ensureStream() {
	if !m.ct.streaming.Load() {
		go m.ct.runStream(m.runCtx)
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.runCancel()
			return m, tea.Quit
		case "s":
			if m.ct.streaming.Load() {
				m.runCancel() // stop the stream (and its monitors)
				m.runCtx, m.runCancel = context.WithCancel(m.baseCtx)
				m.pushLog("stopping live stream")
			} else {
				go m.ct.runStream(m.runCtx)
			}
		case "v":
			m.halted = false
			m.ensureStream()
			go m.ct.startRollout(m.runCtx)
		case "n":
			m.halted = false
			m.ensureStream()
			// flip after a beat so the stream is up if it was just started
			go func() { m.ct.naiveCutover() }()
		case "h":
			if m.ct.rolloutPct.Load() > 0 || m.ct.rolloutActive.Load() {
				m.ct.revertToStable("manual halt")
			} else {
				m.pushLog("nothing to halt — stream is already on stable")
			}
		case "r":
			m = m.resetRun()
			m.pushLog("reset")
		}

	case orderUpdateMsg:
		m.orders[msg.id] = msg.kind
		m.states[msg.id] = msg.state

	case streamStatusMsg:
		m.streaming = msg.running

	case rolloutStatusMsg:
		m.rolloutPct = msg.pct
		m.rolloutPhase = msg.phase
		if msg.phase != "halted" {
			m.halted = false
		}

	case haltedMsg:
		m.halted = true
		m.haltMsg = msg.reason

	case logMsg:
		m.pushLog(string(msg))

	case tickMsg:
		return m, tick()
	}
	return m, nil
}

func (m *model) pushLog(s string) {
	ts := time.Now().Format("15:04:05")
	m.logs = append(m.logs, fmt.Sprintf("%s  %s", ts, s))
	if len(m.logs) > 8 {
		m.logs = m.logs[len(m.logs)-8:]
	}
}

// ---- view -------------------------------------------------------------------

var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("63"))
	dimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	goodStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	badStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	headerStyle = lipgloss.NewStyle().Bold(true)
)

type cohort struct {
	pending, running, stuck, completed, failed int
}

func (m model) tally() (latest, stable cohort) {
	for id, kind := range m.orders {
		c := &stable
		if kind == kindLatest {
			c = &latest
		}
		switch m.states[id] {
		case statePending:
			c.pending++
		case stateRunning:
			c.running++
		case stateStuck:
			c.stuck++
		case stateCompleted:
			c.completed++
		case stateFailed:
			c.failed++
		}
	}
	return
}

func bar(c cohort, width int) string {
	total := c.pending + c.running + c.stuck + c.completed + c.failed
	if total == 0 || width <= 0 {
		return strings.Repeat("·", width)
	}
	seg := func(n int) int { return n * width / total }
	out := goodStyle.Render(strings.Repeat("█", seg(c.completed)))
	out += badStyle.Render(strings.Repeat("█", seg(c.stuck+c.failed)))
	out += warnStyle.Render(strings.Repeat("█", seg(c.running)))
	out += dimStyle.Render(strings.Repeat("░", seg(c.pending)))
	// pad to width (rune-count is messy with styles; pad on plain length)
	plain := seg(c.completed) + seg(c.stuck+c.failed) + seg(c.running) + seg(c.pending)
	if plain < width {
		out += dimStyle.Render(strings.Repeat("░", width-plain))
	}
	return out
}

func cohortLine(label string, c cohort) string {
	bad := c.stuck + c.failed
	var verdict string
	switch {
	case bad > 0:
		verdict = badStyle.Render("✗ unhealthy")
	case c.completed > 0 || c.running > 0 || c.pending > 0:
		verdict = goodStyle.Render("✓ healthy")
	default:
		verdict = dimStyle.Render("—")
	}
	stats := fmt.Sprintf("done:%-4d running:%-4d stuck:%-4d failed:%-4d pending:%-4d",
		c.completed, c.running, c.stuck, c.failed, c.pending)
	return fmt.Sprintf("  %-14s %s  %s\n  %15s%s", headerStyle.Render(label), stats, verdict, "", bar(c, 50))
}

func (m model) View() string {
	latest, stable := m.tally()

	var b strings.Builder
	b.WriteString(titleStyle.Render("Temporal Workflow Versioning — Progressive Rollout Demo") + "\n")
	b.WriteString(dimStyle.Render(fmt.Sprintf(
		"queue=%s   stable=v%d (pinned, known-good)   latest=v3 (current, buggy new step)",
		taskQueue, stableVersion)) + "\n\n")

	// Stream status.
	streamState := dimStyle.Render("stopped — press [s] to start production traffic")
	if m.streaming {
		streamState = goodStyle.Render(fmt.Sprintf("LIVE — ~%d–%d orders/sec arriving", 1000/int(arrivalInterval/time.Millisecond), 1000*maxPerTick/int(arrivalInterval/time.Millisecond)))
	}
	b.WriteString(headerStyle.Render("Order stream: ") + streamState + "\n")

	// Rollout status.
	var roll string
	switch m.rolloutPhase {
	case "ramping":
		roll = warnStyle.Render(fmt.Sprintf("v3 rollout in progress — %d%% of live traffic on v3", m.rolloutPct))
	case "complete":
		roll = goodStyle.Render("v3 rollout COMPLETE — 100% of live traffic on v3")
	case "naive":
		roll = badStyle.Render("NAIVE cutover — 100% flipped to v3 at once (no gating)")
	case "halted":
		roll = badStyle.Render("ROLLOUT HALTED — live traffic reverted to 100% stable v2")
	default:
		roll = dimStyle.Render(fmt.Sprintf("idle — 100%% of traffic pinned to stable v%d  ([v] rollout v3 · [n] naive cutover)", stableVersion))
	}
	b.WriteString(headerStyle.Render("Rollout:      ") + roll + "\n")

	if m.halted {
		b.WriteString(badStyle.Render("              ⛔ "+m.haltMsg) + "\n")
		b.WriteString(warnStyle.Render(
			"              → fix SendThankYou, redeploy the worker; in-flight v3 self-heals, then press [v] to resume") + "\n")
	}
	b.WriteString("\n")

	b.WriteString(cohortLine("LATEST (v3)", latest) + "\n\n")
	b.WriteString(cohortLine("STABLE (v2)", stable) + "\n\n")

	b.WriteString(headerStyle.Render("Recent:") + "\n")
	if len(m.logs) == 0 {
		b.WriteString(dimStyle.Render("  (nothing yet)") + "\n")
	}
	for _, l := range m.logs {
		b.WriteString(dimStyle.Render("  • "+l) + "\n")
	}
	b.WriteString("\n")

	b.WriteString(dimStyle.Render(
		"[s] start/stop stream   [v] roll out v3   [n] naive cutover   [h] halt → stable   [r] reset   [q] quit"))
	return b.String()
}

// ---- main -------------------------------------------------------------------

func main() {
	hostPort := os.Getenv("TEMPORAL_ADDRESS")
	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Temporal (set TEMPORAL_ADDRESS if not localhost:7233): %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	ct := &controller{c: c, runTag: time.Now().Format("150405")}
	ctx := context.Background()

	m := newModel(ctx, ct)
	p := tea.NewProgram(m, tea.WithAltScreen())
	ct.program = p

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "tui error: %v\n", err)
		os.Exit(1)
	}
}
