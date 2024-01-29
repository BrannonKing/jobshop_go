package jsp

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"jobshop_go/dd"
	"maps"
	"slices"
	"unsafe"
)

type jspDetails[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	machine      TValue
	prerequisite TValue
	delay        TCost
}

type JspContext[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	lookup   []jspDetails[TValue, TCost]
	values   []TValue
	maxCost  TCost
	instance *Instance
}

func NewJspPermutationContext[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float](instance *Instance) *JspContext[TValue, TCost] {
	lookup := make([]jspDetails[TValue, TCost], instance.Jobs*instance.Machines)
	i := 0
	maxCost := TCost(0)
	for _, row := range instance.Work {
		for o, data := range row {
			prerequisite := i
			if o == 0 {
				prerequisite = 0
			}
			lookup[i] = jspDetails[TValue, TCost]{TValue(data.Machine), TValue(prerequisite), TCost(data.Delay)}
			maxCost += TCost(data.Delay)
			i += 1
		}
	}

	values := make([]TValue, instance.Jobs*instance.Machines)
	for i := 0; i < instance.Jobs*instance.Machines; i++ {
		values[i] = TValue(i)
	}

	return &JspContext[TValue, TCost]{lookup, values, maxCost, instance}
}

func (j *JspContext[TValue, TCost]) GetStartingState() dd.State[TValue, TCost] {
	return &JspState[TValue, TCost]{make([]TCost, j.instance.Machines), map[TValue]TCost{}, false}
}

func (j *JspContext[TValue, TCost]) GetValues(variable int) []TValue {
	return j.values
}

func (j *JspContext[TValue, TCost]) GetVariables() int {
	return len(j.values)
}

func (j *JspContext[TValue, TCost]) Compare(a, b TCost) int {
	return cmp.Compare(a, b)
}

func (j *JspContext[TValue, TCost]) WorstCost() TCost {
	return j.maxCost
}

type JspState[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	machine_completions []TCost
	job_completions     map[TValue]TCost
	relaxed             bool
}

func (j *JspState[TValue, TCost]) TransitionTo(context dd.Context[TValue, TCost], value TValue) dd.State[TValue, TCost] {
	_, found := j.job_completions[value]
	if found { // don't do it again
		return nil
	}
	details := context.(*JspContext[TValue, TCost]).lookup[value]
	completion := j.machine_completions[details.machine] + details.delay
	if details.prerequisite > 0 {
		delay, found := j.job_completions[details.prerequisite-1]
		if !found {
			return nil // NOTE: an assumption that Delay
		}
		if delay+details.delay > completion {
			completion = delay + details.delay
		}
	}
	mc := slices.Clone(j.machine_completions)
	mc[details.machine] = completion
	jc := maps.Clone(j.job_completions)
	jc[value] = completion
	return &JspState[TValue, TCost]{machine_completions: mc, job_completions: jc, relaxed: false}
}

func (j *JspState[TValue, TCost]) CostTo(context dd.Context[TValue, TCost], child dd.State[TValue, TCost], value TValue) TCost {
	return slices.Max(child.(*JspState[TValue, TCost]).machine_completions) - slices.Max(j.machine_completions)
}

func (j *JspState[TValue, TCost]) MergeFrom(context dd.Context[TValue, TCost], state dd.State[TValue, TCost]) {
	incoming := state.(*JspState[TValue, TCost])
	if len(j.job_completions) <= 0 {
		// we're brand new; just copy in the other; and we never update those, so we can just point to the same data
		j.machine_completions = incoming.machine_completions
		j.job_completions = incoming.job_completions
		j.relaxed = false
	} else {
		for i, mc := range incoming.machine_completions {
			j.machine_completions[i] = max(j.machine_completions[i], mc)
		}
		for key, mc := range incoming.job_completions {
			current, found := j.job_completions[key]
			if found {
				j.job_completions[key] = max(current, mc)
			} else {
				j.job_completions[key] = mc
			}
		}
		j.relaxed = true // j.relaxed || incoming.relaxed || any_change
	}
}

func (j *JspState[TValue, TCost]) Unrelax(context dd.Context[TValue, TCost], removed dd.State[TValue, TCost]) {
	// the current state can stay the same, or it can be improved if we split off the worst contributor
}

func (j *JspState[TValue, TCost]) HashBytes() []byte { // we assume that the result of this will be consumed right away
	size := unsafe.Sizeof(j.machine_completions[0])
	return unsafe.Slice((*byte)(unsafe.Pointer(&j.machine_completions[0])), int(size)*len(j.machine_completions))
}

func (j *JspState[TValue, TCost]) Equals(state dd.State[TValue, TCost]) bool {
	j2 := state.(*JspState[TValue, TCost])
	return slices.Equal(j.machine_completions, j2.machine_completions) &&
		maps.Equal(j.job_completions, j2.job_completions)
}

func (j *JspState[TValue, TCost]) IsRelaxed() bool {
	return j.relaxed
}
