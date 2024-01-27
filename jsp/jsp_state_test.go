package jsp

import (
	"jobshop_go/dd"
	"log"
	"os"
	"testing"
)

func TestPermExact(t *testing.T) {
	instances := LoadInstances()
	logger := log.New(os.Stdout, "", 1)
	context := NewJspPermutationContext[uint16, uint32](instances[1])
	cost, values := dd.SolveByFullExpansion[uint16, uint32](context, logger)
	if int(cost) != instances[0].Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instances[1].Optimum, values)
	}
}
