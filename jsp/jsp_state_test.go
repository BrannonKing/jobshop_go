package jsp

import (
	"jobshop_go/dd"
	"testing"
)

func TestPermExact(t *testing.T) {
	instances := LoadInstances()
	context := NewJspPermutationContext[uint16, uint32](instances[0])
	cost, values := dd.SolveByFullExpansion[uint16, uint32](context, nil)
	if int(cost) != instances[0].Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instances[0].Optimum, values)
	}
}
