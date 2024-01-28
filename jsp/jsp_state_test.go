package jsp

import (
	"jobshop_go/dd"
	"log"
	"maps"
	"math/rand"
	"os"
	"testing"
)

func TestPermExact(t *testing.T) {
	instances := LoadInstances()
	logger := log.New(os.Stdout, "", 1)
	context := NewJspPermutationContext[uint16, uint32](instances[0])
	cost, values := dd.SolveByFullExpansion[uint16, uint32](context, logger)
	if int(cost) != instances[0].Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instances[0].Optimum, values)
	}
}

type ITestState interface {
	Equals(state ITestState) bool
}

type TestState struct {
	mc map[int16]int32
}

func (j TestState) Equals(state ITestState) bool {
	j2 := state.(TestState)
	return maps.Equal(j.mc, j2.mc)
}

func TestAllocOnEquals(t *testing.T) {
	// generate some data:
	states := []*TestState{}
	for k := 0; k < 100; k++ {
		mc := map[int16]int32{}
		for i := int16(0); i < 100; i++ {
			for j := int16(0); j < 150; j++ {
				mc[int16(rand.Intn(30000))] = rand.Int31()
			}
			states = append(states, &TestState{mc})
			mc = maps.Clone(mc)
		}
		// memory should be stable for this check:
		for j := 0; j < 100; j++ {
			a := rand.Intn(len(states))
			b := rand.Intn(len(states))
			if states[a].Equals(*states[b]) {
				t.Logf("Don't care\n")
			}
		}
	}
}
