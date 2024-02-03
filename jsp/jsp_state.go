package jsp

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"jobshop_go/dd"
	"maps"
)

type jspDetails[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	machine      TValue
	prerequisite TValue
	delay        TCost
	totalDelay   TCost
	job          int
	operation    int
	totalPostOps int
}

func insertionSort[T any](arr []T, f func(a, b T) bool) {
	for i := 0; i < len(arr); i++ {
		for j := i; j > 0 && f(arr[j], arr[j-1]); j-- { //  arr[j] < arr[j-1]
			arr[j], arr[j-1] = arr[j-1], arr[j]
		}
	}
}

type JspContext[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	lookup            []jspDetails[TValue, TCost]
	values            []TValue
	maxCost           TCost
	instance          *Instance
	tasksByTotalDelay []TValue
	tasksByMachine    [][]TValue
}

func NewJspPermutationContext[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float](instance *Instance, maxTCost TCost) *JspContext[TValue, TCost] {
	lookup := make([]jspDetails[TValue, TCost], instance.Jobs*instance.Machines)
	i := TValue(0)
	maxCost := maxTCost
	machToTask := map[int][]TValue{}
	tasksByMachine := make([][]TValue, instance.Machines)
	for r, row := range instance.Work {
		for o, data := range row {
			machToTask[data.Machine] = append(machToTask[data.Machine], TValue(i))
			prerequisite := i // it's actually i-1, but we handle that where we use it
			if o == 0 {
				prerequisite = 0
			}
			totalDelay := TCost(0)
			for _, data2 := range row[o:] {
				totalDelay += TCost(data2.Delay)
			}
			lookup[i] = jspDetails[TValue, TCost]{TValue(data.Machine),
				TValue(prerequisite), TCost(data.Delay),
				totalDelay, r, o, len(row) - o + 1,
			}
			tasksByMachine[data.Machine] = append(tasksByMachine[data.Machine], i)
			i += 1
		}
	}

	values := make([]TValue, instance.Jobs*instance.Machines)
	tasksByTotalDelay := make([]TValue, instance.Jobs*instance.Machines)
	for i := 0; i < instance.Jobs*instance.Machines; i++ {
		values[i] = TValue(i)
		tasksByTotalDelay[i] = TValue(i)
	}
	insertionSort(tasksByTotalDelay, func(a, b TValue) bool { // need n^2 alg for dual sort criteria
		da := lookup[a]
		db := lookup[b]
		if da.job == db.job {
			return da.operation < db.operation
		}
		// This next line decides the whole heuristic. Here are some common ones:
		// LPT: favor the job with longest processing time (we don't have this data, could do it globally or dynamically)
		// MOR: favor the job with most operations remaining (we don't have this, but we could compute dynamically in the heuristic function)
		// TLD: job with the task with the longest delay (don't have this)
		// LSO: longest subsequent ops = da.totalDelay
		// MSO: most subsequent ops = da.TotalPostOps
		// APT: apparent tardiness = exp(- (d_j - p_j - t) / (K*avg(p))) / p_j  // p = avg delay of remaining tasks
		return da.totalPostOps > db.totalPostOps
	})

	return &JspContext[TValue, TCost]{lookup, values, maxCost, instance,
		tasksByTotalDelay, tasksByMachine}
}

func (j *JspContext[TValue, TCost]) GetStartingState() dd.State[TValue, TCost] {
	return &JspState[TValue, TCost]{nil, nil, 0, 0, 0}
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
	job_completions map[TValue]TCost
	job_maybes      map[TValue]TCost
	hash            uint64
	cmax            TCost // caches the latest completion
	heuristic       TCost // caches the score for the restriction if necessary
}

func (j *JspState[TValue, TCost]) TransitionTo(context dd.Context[TValue, TCost], value TValue) dd.State[TValue, TCost] {
	_, found := j.job_completions[value]
	if found { // don't do it again
		return nil
	}
	details := context.(*JspContext[TValue, TCost]).lookup[value]
	completion := TCost(0)
	if details.prerequisite > 0 {
		delay, found := j.job_completions[details.prerequisite-1]
		if !found {
			delay, found = j.job_maybes[details.prerequisite-1]
			if !found {
				return nil // NOTE: an assumption that Delay
			}
		}
		completion = delay + details.delay
	}
	for _, op := range context.(*JspContext[TValue, TCost]).tasksByMachine[details.machine] {
		completion = max(completion, j.job_completions[op]+details.delay)
	}
	var jc map[TValue]TCost
	if j.job_completions == nil {
		jc = map[TValue]TCost{value: completion}
	} else {
		jc = maps.Clone(j.job_completions)
		jc[value] = completion
	}
	jm := j.job_maybes
	return &JspState[TValue, TCost]{job_completions: jc, job_maybes: jm, hash: j.hash + (2654435761 * uint64(completion)),
		cmax: max(j.cmax, completion), heuristic: 0}
}

func (j *JspState[TValue, TCost]) CostTo(context dd.Context[TValue, TCost], child dd.State[TValue, TCost], value TValue) TCost {
	return child.(*JspState[TValue, TCost]).cmax - j.cmax
}

func (j *JspState[TValue, TCost]) MergeFrom(context dd.Context[TValue, TCost], state dd.State[TValue, TCost]) {
	incoming := state.(*JspState[TValue, TCost])
	if len(j.job_completions) <= 0 && len(j.job_maybes) <= 0 {
		// we're brand new; just copy in the other; and we never update those, so we can just point to the same data
		j.cmax = incoming.cmax
		j.job_completions = incoming.job_completions
		j.job_maybes = incoming.job_maybes
	} else {
		// merge is a relaxed node: it has to be a lower bound
		j.cmax = min(j.cmax, incoming.cmax)
		if j.job_maybes == nil {
			j.job_maybes = make(map[TValue]TCost)
		}
		// completions have to be in both.
		// otherwise we have to take the worst of the maybes
		for key, mc := range j.job_completions {
			_, found := incoming.job_completions[key]
			if !found {
				j.job_maybes[key] = mc
				delete(j.job_completions, key)
			}
		}
		for key, mc := range incoming.job_completions {
			current, found := j.job_completions[key]
			if found {
				j.job_completions[key] = max(current, mc)
			} else {
				current, found = j.job_maybes[key]
				if found {
					j.job_maybes[key] = max(current, mc)
				} else {
					j.job_maybes[key] = mc
				}
			}
		}
		for key, mc := range incoming.job_maybes {
			current, found := j.job_maybes[key]
			if found {
				j.job_maybes[key] = max(current, mc)
			} else {
				j.job_maybes[key] = mc
			}
		}
	}
}

func (j *JspState[TValue, TCost]) Unrelax(context dd.Context[TValue, TCost], removed dd.State[TValue, TCost]) {
	// the current state can stay the same, or it can be improved if we split off the worst contributor.
	// it also needs the value that made the new state.
	// it needs to remove that value only if all the parents of this node don't have it.
	// or we could shrink it back to the parents' completion time.
}

func (j *JspContext[TValue, TCost]) heuristic(state *JspState[TValue, TCost]) TCost {
	// lots of options here: can do it overall or do it per machine
	// can select item with most delay or item with most ops.
	// Easiest for starters: do the overall list, ordered by most delay to least.
	// Make a list of work to be done. Then sort it. Then add each item from that list.
	//if len(state.job_maybes) > 0 {
	//	panic("Not implemented")
	//}
	//
	//dones := map[TValue]TCost{}
	//for _, task := range j.tasksByTotalDelay {
	//	_, found := state.job_completions[task]
	//	if found {
	//		continue
	//	}
	//	details := j.lookup[task]
	//	completion := details.delay + mcs[details.machine]
	//	if details.prerequisite > 0 { // assuming we're sorted to support prereqs
	//		delay := max(state.job_completions[details.prerequisite-1], dones[details.prerequisite-1])
	//		if delay <= 0 {
	//			panic("Expected to have all prereqs!")
	//		}
	//		completion = max(completion, delay+details.delay)
	//	}
	//	mcs[details.machine] = completion
	//	dones[task] = completion
	//}
	//result := slices.Max(mcs)
	// ensuring they're bigger than optimum: fmt.Printf("HEUR: %v\n", result)
	return 0
}

func (j *JspState[TValue, TCost]) Heuristic(context dd.Context[TValue, TCost], runningCost TCost) TCost {
	return runningCost

	// return an estimate of the remaining time
	//if j.heuristic > 0 {
	//	return j.heuristic
	//}
	//// it's probably okay if this heuristic over-estimates (and that actually may be better in general).
	//// We can put them all in a list ahead of time, or we can recompute according to a rule here.
	//// We can use our current completion times (to be more accurate), or we can ignore those (to be faster).
	//// possibilities: most ops remaining (MOR), longest proc time (LPT), shortest proc time (SPT)
	//jcon := context.(*JspContext[TValue, TCost])
	//j.heuristic = jcon.heuristic(j)
	//return j.heuristic
}

func (j *JspState[TValue, TCost]) Hash() uint64 { // we assume that the result of this will be consumed right away
	return j.hash
}

func (j *JspState[TValue, TCost]) Equals(state dd.State[TValue, TCost]) bool {
	j2 := state.(*JspState[TValue, TCost])
	return j.cmax == j2.cmax &&
		maps.Equal(j.job_completions, j2.job_completions) && maps.Equal(j.job_maybes, j2.job_maybes)
}

func (j *JspState[TValue, TCost]) IsRelaxed() bool {
	return len(j.job_maybes) > 0
}
