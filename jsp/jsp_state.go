package jsp

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"hash/maphash"
	"jobshop_go/dd"
	"maps"
	"slices"
	"unsafe"
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
	hasher            maphash.Hash
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
		tasksByTotalDelay, tasksByMachine, maphash.Hash{}}
}

func (j *JspContext[TValue, TCost]) GetStartingState() dd.State[TValue, TCost] {
	return &JspState[TValue, TCost]{make([]TCost, j.instance.Machines), nil, nil, 0}
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
	mach_completions []TCost
	job_all          map[TValue]TCost
	job_some         map[TValue]TCost
	heuristic        TCost // caches the score for the restriction if necessary
}

func (j *JspState[TValue, TCost]) TransitionTo(context dd.Context[TValue, TCost], value TValue) dd.State[TValue, TCost] {
	_, found := j.job_all[value]
	if found { // don't do it again
		return nil
	}
	details := context.(*JspContext[TValue, TCost]).lookup[value]
	canStart := TCost(0)
	if details.prerequisite > 0 {
		canStart, found = j.job_all[details.prerequisite-1]
		if !found {
			canStart, found = j.job_some[details.prerequisite-1]
			if !found {
				return nil // NOTE: an assumption that Delay
			}
		}
	}
	canStart = max(j.mach_completions[details.machine], canStart) + details.delay
	jm := slices.Clone(j.mach_completions)
	jm[details.machine] = canStart
	ja := maps.Clone(j.job_all)
	if ja == nil {
		ja = map[TValue]TCost{}
	}
	ja[value] = canStart
	js := j.job_some
	if _, found = j.job_some[value]; found {
		js = maps.Clone(j.job_some)
		delete(js, value)
	}
	return &JspState[TValue, TCost]{jm, ja, js, 0}
}

func (j *JspState[TValue, TCost]) Cost(context dd.Context[TValue, TCost]) TCost {
	return slices.Max(j.mach_completions)
}

func (j *JspState[TValue, TCost]) Solution(context dd.Context[TValue, TCost]) []TValue {
	// copy the map into a slice and then sort it
	var values []TValue
	for k := range j.job_all {
		values = append(values, k)
	}
	slices.SortFunc(values, func(a, b TValue) int {
		return context.Compare(j.job_all[a], j.job_all[b])
	})
	return values
}

func (j *JspState[TValue, TCost]) MergeFrom(context dd.Context[TValue, TCost], state dd.State[TValue, TCost]) {
	// completions become intersection of two completions.
	// maybies become union of maybies plus any rejected completions.
	// cmax becomes biggest of any completions and the smallest maybe (since we know we've done at least one of those).
	incoming := state.(*JspState[TValue, TCost])
	j.heuristic = 0
	if len(j.job_all) <= 0 && len(j.job_some) <= 0 {
		// we're brand new; just copy in the other; and we never update those, so we can just point to the same data
		j.mach_completions = incoming.mach_completions
		j.job_all = incoming.job_all
		j.job_some = incoming.job_some
	} else {
		// merge is a relaxed node: it has to be a lower bound
		for m := 0; m < len(incoming.mach_completions); m++ {
			j.mach_completions[m] = min(j.mach_completions[m], incoming.mach_completions[m])
		}

		if j.job_some == nil {
			j.job_some = make(map[TValue]TCost)
		}
		// completions have to be in both.
		// otherwise we have to take the worst of the maybes.
		for key, mc := range j.job_all {
			mci, found := incoming.job_all[key]
			if !found {
				j.job_some[key] = mc
				delete(j.job_all, key)
			} else {
				// if it's in both, we need to keep the worst score for it.
				j.job_all[key] = min(mc, mci)
			}
		}
		for key, mci := range incoming.job_all {
			mc, found := j.job_all[key]
			if found {
				j.job_all[key] = min(mc, mci)
			} else {
				j.job_some[key] = min(j.job_some[key], mci)
			}
		}
		for key, mc := range incoming.job_some {
			current, found := j.job_some[key]
			if found {
				j.job_some[key] = min(current, mc)
			} else {
				j.job_some[key] = mc
			}
		}
	}
}

func (j *JspState[TValue, TCost]) Unrelax(context dd.Context[TValue, TCost], removed dd.State[TValue, TCost], value TValue) {
	// it needs to remove that value only if all the parents of this node don't have it?
	// jv := j.job_some[value]
	delete(j.job_some, value)
	if len(j.job_some) == 1 {
		for k, v := range j.job_some {
			j.job_all[k] = v
			delete(j.job_some, k)
		}
	}
	// or we could shrink it back to the parents' completion time.
}

func (j *JspContext[TValue, TCost]) heuristic(state *JspState[TValue, TCost]) TCost {
	// lots of options here: can do it overall or do it per machine
	// can select item with most delay or item with most ops.
	// Easiest for starters: do the overall list, ordered by most delay to least.
	// Make a list of work to be done. Then sort it. Then add each item from that list.
	//if len(state.job_some) > 0 {
	//	panic("Not implemented")
	//}
	//
	//dones := map[TValue]TCost{}
	//for _, task := range j.tasksByTotalDelay {
	//	_, found := state.job_all[task]
	//	if found {
	//		continue
	//	}
	//	details := j.lookup[task]
	//	completion := details.delay + mcs[details.machine]
	//	if details.prerequisite > 0 { // assuming we're sorted to support prereqs
	//		delay := max(state.job_all[details.prerequisite-1], dones[details.prerequisite-1])
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

func (j *JspState[TValue, TCost]) Heuristic(context dd.Context[TValue, TCost]) TCost {
	return j.Cost(context)

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

func (j *JspState[TValue, TCost]) HashBytes() []byte { // we assume that the result of this will be consumed right away
	size := unsafe.Sizeof(j.mach_completions[0])
	return unsafe.Slice((*byte)(unsafe.Pointer(&j.mach_completions[0])), int(size)*len(j.mach_completions))
}

func (j *JspState[TValue, TCost]) Equals(state dd.State[TValue, TCost]) bool {
	j2 := state.(*JspState[TValue, TCost])
	return slices.Equal(j.mach_completions, j2.mach_completions) &&
		maps.Equal(j.job_all, j2.job_all) && maps.Equal(j.job_some, j2.job_some)
}

func (j *JspState[TValue, TCost]) IsRelaxed() bool {
	return len(j.job_some) > 0
}

type JspPartitionStrategy[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	MaxWidth int
}

func (j JspPartitionStrategy[TValue, TCost]) Find(context dd.Context[TValue, TCost], states []*dd.State[TValue, TCost]) dd.Partition {
	//rand.Shuffle(len(states), func(i, j int) { // yikes! risky side-effects
	//	states[i], states[j] = states[j], states[i]
	//})
	// if we're 500 wide and want only 100, we want groups of 5
	result := dd.Partition{}
	lastStart := 0
	slotWidth := len(states) / j.MaxWidth
	if slotWidth < 2 {
		return result
	}
	for g := 0; g < slotWidth*j.MaxWidth; g += slotWidth {
		best := context.WorstCost()
		bestIdx := -1
		for i := g; i < g+slotWidth; i++ {
			cost := (*states[i]).Cost(context)
			if cost < best {
				best = cost
				bestIdx = i
			}
		}
		result.Keepers = append(result.Keepers, bestIdx)
		if lastStart < bestIdx {
			result.MergeGroups = append(result.MergeGroups, dd.PartitionPair{lastStart, bestIdx})
		}
		lastStart = bestIdx + 1
	}
	result.MergeGroups = append(result.MergeGroups, dd.PartitionPair{lastStart, len(states)})
	return result
}
