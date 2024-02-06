package jsp

import (
	"github.com/stretchr/testify/assert"
	"jobshop_go/dd"
	"log"
	"maps"
	"math/rand"
	"os"
	"testing"
)

func TestFull(t *testing.T) {
	instances := LoadInstances()
	instance := instances[1]
	logger := log.New(os.Stdout, "", 1)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveByFullExpansion[uint16, uint32](context, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestRestricted(t *testing.T) {
	instances := LoadInstances()
	instance := instances[2]
	logger := log.New(os.Stdout, "", 0)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveRestricted[uint16, uint32](context, 300, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestRelaxed(t *testing.T) {
	instances := LoadInstances()
	instance := instances[2]
	logger := log.New(os.Stdout, "", 0)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveRelaxed[uint16, uint32](context, nil, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestPermSepa(t *testing.T) {
	instances := LoadInstances()
	instance := instances[0]
	logger := log.New(os.Stdout, "", 0)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveBySeparation[uint16, uint32](context, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestMerge(t *testing.T) {
	a := JspState[uint16, int32]{}
	a.job_completions = map[uint16]int32{}
	a.job_completions[3] = 30
	a.job_completions[4] = 40
	a.cmax = 60
	a.job_maybes = map[uint16]int32{}
	a.job_maybes[1] = 10

	b := JspState[uint16, int32]{}
	b.job_completions = map[uint16]int32{}
	b.job_completions[3] = 20
	b.job_maybes = map[uint16]int32{}
	b.cmax = 70
	b.job_maybes[4] = 25
	b.job_maybes[5] = 50

	a.MergeFrom(nil, &b)
	assert.EqualValues(t, a.cmax, 70)
	assert.EqualValues(t, len(a.job_completions), 1)
	assert.EqualValues(t, a.job_completions[3], 30)
	assert.EqualValues(t, len(a.job_maybes), 3)
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
	var states []*TestState
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
