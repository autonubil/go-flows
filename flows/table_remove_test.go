package flows

import (
	"fmt"
	"testing"
)

// slotOwners maps every slot to the keys that point at it.
func slotOwners(tab *FlowTable) map[int][]string {
	owners := map[int][]string{}
	for k, slot := range tab.flows {
		owners[slot] = append(owners[slot], k)
	}
	return owners
}

func assertNoSharedSlots(t *testing.T, tab *FlowTable) {
	t.Helper()
	for slot, keys := range slotOwners(tab) {
		if len(keys) > 1 {
			t.Fatalf("slot %d shared by %v", slot, keys)
		}
		if f := tab.flowlist[slot]; f == nil || f.Key() != keys[0] {
			t.Fatalf("key %s points at slot %d, which holds %v", keys[0], slot, f)
		}
	}
}

// A flow exported from within its own Event (a feature that sees the connection close) is stopped
// there and again by BaseFlow.Event. The second stop must not free a slot another flow holds.
func TestStopTwiceKeepsOtherFlowsSlot(t *testing.T) {
	tab := newTestTable(0)
	feed(tab, 2, "a", SecondsInNanoseconds) // a0 -> slot 0, a1 -> slot 1

	a1 := tab.flowlist[tab.flows["a1"]].(*testFlow)
	a1.Stop()
	a1.Stop()

	feed(tab, 2, "b", SecondsInNanoseconds)
	assertNoSharedSlots(t, tab)
	if got := tab.flowlist[tab.flows["a0"]].Key(); got != "a0" {
		t.Fatalf("a0 resolves to flow %s", got)
	}
}

// remove for a flow whose key is gone must not touch slot 0.
func TestRemoveUnknownFlowKeepsSlotZero(t *testing.T) {
	tab := newTestTable(0)
	feed(tab, 3, "a", SecondsInNanoseconds)

	a2 := tab.flowlist[tab.flows["a2"]]
	tab.remove(a2)
	tab.remove(a2)
	if len(tab.freelist) != 1 {
		t.Fatalf("freelist = %v, want one slot", tab.freelist)
	}

	feed(tab, 5, "b", SecondsInNanoseconds)
	assertNoSharedSlots(t, tab)
	for i := 0; i < 2; i++ {
		k := fmt.Sprintf("a%d", i)
		if got := tab.flowlist[tab.flows[k]].Key(); got != k {
			t.Fatalf("%s resolves to flow %s", k, got)
		}
	}
}
