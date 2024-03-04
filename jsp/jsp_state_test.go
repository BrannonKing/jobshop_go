package jsp

import (
	"jobshop_go/dd"
	"log"
	"maps"
	"math"
	"math/rand"
	"os"
	"testing"
)

func TestFull(t *testing.T) {
	instances := LoadInstances()
	instance := instances[4]
	logger := log.New(os.Stdout, "", 1)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveByFullExpansion[uint16, uint32](context, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestRestricted(t *testing.T) {
	instances := LoadInstances()
	instance := instances[4]
	logger := log.New(os.Stdout, "", 0)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveRestricted[uint16, uint32](context, 300, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestRelaxed(t *testing.T) {
	instances := LoadInstances()
	instance := instances[5]
	logger := log.New(os.Stdout, "", 1)
	context := NewJspPermutationContext[uint16, uint32](instance, 0xffffffff)
	cost, values := dd.SolveRelaxed[uint16, uint32](context, JspCombineWorstStrategy[uint16, uint32]{110, 1}, logger)
	if int(cost) != instance.Optimum {
		t.Fatalf("Bad cost: %d != %d : %v\n", cost, instance.Optimum, values)
	}
}

func TestBnB(t *testing.T) {
	// what we need: a way to compare runs across multiple random instances, maybe same range of rand seed.
	// a way to swap out state models for test: one with no symmetry, vs midline, vs this one?
	// a way to throw away cutsets from infeasible/cutoff relaxations, even though these are rare?
	// do we at least have a way to measure how rare they are? Maybe we need a way to run the full relaxed tree.
	// instances := LoadInstances()
	// instance := instances[4] // 4 takes 1.5M iterations
	rand.Seed(42)
	instance := LoadRandom(5, 5)
	logger := log.New(os.Stdout, "", 0)
	context := NewJspPermutationContext[uint16, uint16](instance, math.MaxUint16)
	cost, values := dd.SolveBnb[uint16, uint16](context, context.GetVariables()+4, logger)
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

//func TestMerge(t *testing.T) {
//	a := JspState[uint16, int32]{}
//	a.job_all = map[uint16]int32{}
//	a.job_all[3] = 30
//	a.job_all[4] = 40
//	a.mach_completions = []int32{50, 60}
//	a.job_some = map[uint16]int32{}
//	a.job_some[1] = 10
//
//	b := JspState[uint16, int32]{}
//	b.job_all = map[uint16]int32{}
//	b.job_all[3] = 20
//	b.job_some = map[uint16]int32{}
//	b.mach_completions = []int32{5, 70}
//	b.job_some[4] = 25
//	b.job_some[5] = 50
//
//	a.MergeFrom(nil, &b)
//	assert.EqualValues(t, 2, len(a.mach_completions))
//	assert.EqualValues(t, 5, a.mach_completions[0])
//	assert.EqualValues(t, 60, a.mach_completions[1])
//	assert.EqualValues(t, 1, len(a.job_all))
//	assert.EqualValues(t, 30, a.job_all[3])
//	assert.EqualValues(t, 3, len(a.job_some))
//	assert.EqualValues(t, 40, a.job_some[4])
//}

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
