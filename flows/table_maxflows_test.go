package flows

import (
	"fmt"
	"testing"
)

type testEvent struct {
	key  string
	when DateTimeNanoseconds
}

func (e testEvent) Timestamp() DateTimeNanoseconds { return e.when }
func (e testEvent) Key() string                    { return e.key }
func (e testEvent) LowToHigh() bool                { return true }
func (e testEvent) SetWindow(uint64)               {}
func (e testEvent) Window() uint64                 { return 0 }
func (e testEvent) EventNr() uint64                { return 0 }

// testFlow stays active until its idle timer fires. BaseFlow.Event would stop it at once, because
// a table with no records has nothing that keeps a flow alive.
type testFlow struct {
	BaseFlow
}

func (f *testFlow) Event(_ Event, context *EventContext) {
	context.initFlow(f)
	f.AddTimer(TimerIdle, f.idleEvent, context.when+f.table.IdleTimeout)
}

func newTestTable(maxFlows int) *FlowTable {
	newflow := func(_ Event, table *FlowTable, key string, forward bool, context *EventContext, id uint64) Flow {
		f := &testFlow{}
		f.Init(table, key, forward, context, id)
		return f
	}
	return NewFlowTable(RecordListMaker{}, newflow, FlowOptions{IdleTimeout: 10 * SecondsInNanoseconds, MaxFlows: maxFlows}, false, 0)
}

func feed(tab *FlowTable, n int, prefix string, when DateTimeNanoseconds) {
	for i := 0; i < n; i++ {
		tab.Event(testEvent{key: fmt.Sprintf("%s%d", prefix, i), when: when})
	}
}

func TestMaxFlowsRefusesNewFlowsWhenFull(t *testing.T) {
	tab := newTestTable(3)
	feed(tab, 5, "a", SecondsInNanoseconds)

	if got := len(tab.flows); got != 3 {
		t.Fatalf("flows in table = %d, want 3", got)
	}
	if got := tab.DroppedFlows(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
	// A packet for a flow the table already holds is never refused.
	tab.Event(testEvent{key: "a0", when: SecondsInNanoseconds})
	if got := tab.DroppedFlows(); got != 2 {
		t.Fatalf("dropped after packet for tracked flow = %d, want 2", got)
	}
}

func TestMaxFlowsReclaimsIdleFlowsBeforeRefusing(t *testing.T) {
	tab := newTestTable(3)
	feed(tab, 3, "a", SecondsInNanoseconds)

	// Past the idle timeout but before any periodic Expire: the full table must free the idle
	// flows itself instead of refusing the new ones.
	later := 20 * SecondsInNanoseconds
	feed(tab, 3, "b", later)

	if got := tab.DroppedFlows(); got != 0 {
		t.Fatalf("dropped = %d, want 0 (idle flows should have been reclaimed)", got)
	}
	for i := 0; i < 3; i++ {
		if _, ok := tab.flows[fmt.Sprintf("b%d", i)]; !ok {
			t.Fatalf("flow b%d not tracked", i)
		}
	}
}

func TestMaxFlowsZeroIsUnlimited(t *testing.T) {
	tab := newTestTable(0)
	feed(tab, 1000, "a", SecondsInNanoseconds)

	if got := len(tab.flows); got != 1000 {
		t.Fatalf("flows in table = %d, want 1000", got)
	}
	if got := tab.DroppedFlows(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
}
