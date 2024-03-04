package jsp

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"jobshop_go/dd"
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
	offset            int
	varCount          int
	startingState     dd.State[TValue, TCost]
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
		// LPT: favor the job with the longest processing time (we don't have this data, could do it globally or dynamically)
		// MOR: favor the job with most operations remaining (we don't have this, but we could compute dynamically in the heuristic function)
		// TLD: job with the task with the longest delay (don't have this)
		// LSO: longest subsequent ops = da.totalDelay
		// MSO: most subsequent ops = da.TotalPostOps
		// APT: apparent tardiness = exp(- (d_j - p_j - t) / (K*avg(p))) / p_j  // p = avg delay of remaining tasks
		return da.totalPostOps > db.totalPostOps
	})

	starter := JspState[TValue, TCost]{make([]TCost, instance.Machines, instance.Machines), nil, nil, 0, ""}
	return &JspContext[TValue, TCost]{lookup, values, maxCost, instance,
		tasksByTotalDelay, tasksByMachine, 0, len(values), &starter}
}

func (j *JspContext[TValue, TCost]) GetStartingState() dd.State[TValue, TCost] {
	return j.startingState
}

func (j *JspContext[TValue, TCost]) GetValues() []TValue {
	return j.values
}

func (j *JspContext[TValue, TCost]) GetVariables() int {
	return j.varCount
}

func (j *JspContext[TValue, TCost]) Compare(a, b TCost) int {
	return cmp.Compare(a, b)
}

func (j *JspContext[TValue, TCost]) WorstCost() TCost {
	return j.maxCost
}

func (j *JspContext[TValue, TCost]) Child(state *dd.State[TValue, TCost], offset int) dd.Context[TValue, TCost] {
	starter := (*state).(*JspState[TValue, TCost])
	var values []TValue
	for _, value := range j.values {
		if slices.ContainsFunc(starter.job_all, func(v vcPair[TValue, TCost]) bool { return v.V == value }) {
			continue
		}
		values = append(values, value)
	}
	return &JspContext[TValue, TCost]{j.lookup, values, j.maxCost, j.instance,
		j.tasksByTotalDelay, j.tasksByMachine, offset, j.varCount, starter}
}

func (j *JspContext[TValue, TCost]) Offset() int {
	return j.offset
}

type vcPair[TValue, TCost any] struct {
	V TValue
	C TCost
}

type JspState[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	mach_completions []TCost
	job_all          []vcPair[TValue, TCost] // too slow: map[TValue]TCost
	job_some         []vcPair[TValue, TCost] // nope: map[TValue]TCost
	cmax             TCost                   // caches the score for the restriction if necessary
	id               string
}

func (j *JspState[TValue, TCost]) getCompletionTime(context dd.Context[TValue, TCost], value TValue) (TValue, TCost) {
	for _, pair := range j.job_all {
		if pair.V == value {
			return TValue(0), context.WorstCost()
		}
	}
	details := context.(*JspContext[TValue, TCost]).lookup[value]
	if details.prerequisite <= 0 {
		return details.machine, j.mach_completions[details.machine] + details.delay
	}
	startAt := TCost(0)
	found := false
	for _, pair := range j.job_all {
		if pair.V == details.prerequisite-1 {
			startAt = pair.C
			found = true
			break
		}
	}
	if !found {
		for _, pair := range j.job_some {
			if pair.V == details.prerequisite-1 {
				startAt = pair.C
				found = true
				break
			}
		}
	}
	if !found {
		return details.machine, context.WorstCost()
	}
	return details.machine, max(startAt, j.mach_completions[details.machine]) + details.delay
}

func (j *JspState[TValue, TCost]) TransitionTo(context dd.Context[TValue, TCost], value TValue) dd.State[TValue, TCost] {
	machine, complete := j.getCompletionTime(context, value)
	if complete == context.WorstCost() {
		return nil
	}
	// jm := slices.Clone(j.mach_completions)
	jm := make([]TCost, len(j.mach_completions))
	copy(jm, j.mach_completions) // clone call seems slower
	jm[machine] = complete
	ja := make([]vcPair[TValue, TCost], 0, len(j.job_all)+1)
	for _, pair := range j.job_all {
		if pair.V == value {
			panic("Don't do the same value twice!")
		} else {
			ja = append(ja, pair)
		}
	}
	ja = append(ja, vcPair[TValue, TCost]{value, complete})
	var js []vcPair[TValue, TCost]
	if j.job_some != nil {
		js = make([]vcPair[TValue, TCost], 0, len(j.job_some))
		for _, pair := range j.job_some {
			if pair.V != value {
				ja = append(ja, pair)
			}
		}
	}
	trailer := TCost(0)
	for _, value = range context.(*JspContext[TValue, TCost]).tasksByMachine[machine] {
		if slices.ContainsFunc(ja, func(v vcPair[TValue, TCost]) bool { return v.V == value }) {
			continue
		}
		if slices.ContainsFunc(js, func(v vcPair[TValue, TCost]) bool { return v.V == value }) {
			continue
		}
		details := context.(*JspContext[TValue, TCost]).lookup[value]
		trailer += details.delay
	}
	return &JspState[TValue, TCost]{jm, ja, js, max(j.cmax, complete+trailer), ""}
}

func (j *JspState[TValue, TCost]) Cost(context dd.Context[TValue, TCost]) TCost {
	return j.cmax
}

func (j *JspState[TValue, TCost]) Solution(context dd.Context[TValue, TCost]) []TValue {
	// copy the map into a slice and then sort it
	slices.SortFunc(j.job_all, func(a, b vcPair[TValue, TCost]) int {
		return context.Compare(a.C, b.C)
	})
	values := make([]TValue, 0, len(j.job_all))
	for _, pair := range j.job_all {
		values = append(values, pair.V)
	}
	return values
}

func (j *JspState[TValue, TCost]) MergeFrom(context dd.Context[TValue, TCost], state dd.State[TValue, TCost]) {
	// completions become intersection of two completions.
	// maybies become union of maybies plus any rejected completions.
	// cmax becomes biggest of any completions and the smallest maybe (since we know we've done at least one of those).
	incoming := state.(*JspState[TValue, TCost])
	if len(j.job_all) <= 0 && len(j.job_some) <= 0 {
		// we're brand new; just copy in the other; and we never update those, so we can just point to the same data
		j.mach_completions = incoming.mach_completions
		j.job_all = incoming.job_all
		j.job_some = incoming.job_some
		j.cmax = incoming.cmax
	} else {
		// merge is a relaxed node: it has to be a lower bound
		// Lukas's idea: merge sooner and make smaller merge groups.
		// My idea: we need to merge items that are very similar so that we don't lose our high C values
		for m := 0; m < len(incoming.mach_completions); m++ {
			j.mach_completions[m] = min(j.mach_completions[m], incoming.mach_completions[m])
		}
		j.cmax = min(j.cmax, incoming.cmax)
		panic("not done")

		// completions have to be in both.
		// otherwise we have to take the worst of the maybes.
		// we assume those in all are not in those in some.
		//	for key, mc := range j.job_all {
		//		mci, found := incoming.job_all[key]
		//		if !found {
		//			if mcs, found := j.job_some[key]; found {
		//				// this can happen if we merge multiple in a row
		//				j.job_some[key] = max(mcs, mc) // keep max until we see a relaxed sln > optimum
		//			} else {
		//				j.job_some[key] = mc
		//			}
		//			delete(j.job_all, key) // makes me nervous to delete from the thing we're iterating
		//		} else {
		//			// if it's in both, we need to keep the worst score for it. (not sure if that's min or max)
		//			j.job_all[key] = max(mc, mci)
		//
		//			// this should be implicit:
		//			// details := context.(*JspContext[TValue, TCost]).lookup[key]
		//			// j.mach_completions[details.machine] = max(j.mach_completions[details.machine], mc, mci)
		//		}
		//	}
		//	for key, mci := range incoming.job_all {
		//		_, found := j.job_all[key]
		//		if !found { // already took intersection above
		//			if mc, found := j.job_some[key]; found {
		//				j.job_some[key] = max(mc, mci)
		//			} else {
		//				j.job_some[key] = mci
		//			}
		//		}
		//	}
		//	for key, mc := range incoming.job_some {
		//		current, found := j.job_some[key]
		//		if found {
		//			j.job_some[key] = max(current, mc)
		//		} else {
		//			j.job_some[key] = mc
		//		}
		//	}
	}
}

func (j *JspState[TValue, TCost]) Unrelax(context dd.Context[TValue, TCost], removed dd.State[TValue, TCost], value TValue) {
	// it needs to remove that value only if all the parents of this node don't have it?
	// jv := j.job_some[value]
	//delete(j.job_some, value)
	//if len(j.job_some) == 1 {
	//	for k, v := range j.job_some {
	//		j.job_all[k] = v
	//		delete(j.job_some, k)
	//	}
	//}
	panic("Not implemented")
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

func (j *JspState[TValue, TCost]) ID(context dd.Context[TValue, TCost]) string { // we assume that the result of this will be consumed right away
	if len(j.id) == 0 {
		var pairs []vcPair[TValue, TCost] // ditch layer in GetValues and make cutset include the layer it came from
		for _, value := range context.(*JspContext[TValue, TCost]).GetValues() {
			_, done := j.getCompletionTime(context, value)
			if done != context.WorstCost() {
				// we'll assume integer cost for now
				pairs = append(pairs, vcPair[TValue, TCost]{value, done})
			}
		}
		pairs = append(pairs, vcPair[TValue, TCost]{TValue(0), j.cmax}) // break ambiguity in the done state
		// this assumes unsafe.Pointer increments the reference count
		j.id = unsafe.String((*byte)(unsafe.Pointer(&pairs[0])), len(pairs)*int(unsafe.Sizeof(pairs[0])))
	}
	return j.id
}

func (j *JspState[TValue, TCost]) IsRelaxed() bool {
	return len(j.job_some) > 0
}

type JspRandGrpPartitionStrategy[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	MaxWidth int
}

func (j JspRandGrpPartitionStrategy[TValue, TCost]) Find(context dd.Context[TValue, TCost], states []*dd.State[TValue, TCost]) dd.Partition {
	// sort possibilities:
	// 1. sort based on index of longest item first, then index of 2nd longest item, etc. Favors similar shapes.
	// 2. k-means++ . No. We need more clusters than this can handle.
	// 3.
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

type JspCombineWorstStrategy[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float] struct {
	MaxWidth int
	Groups   int
}

func (j JspCombineWorstStrategy[TValue, TCost]) Find(context dd.Context[TValue, TCost], states []*dd.State[TValue, TCost]) dd.Partition {
	// sort possibilities:
	// 1. sort based on index of longest item first, then index of 2nd longest item, etc. Favors similar shapes.
	// 2. k-means++ . No. We need more clusters than this can handle.
	// 3.
	//rand.Shuffle(len(states), func(i, j int) { // yikes! risky side-effects
	//	states[i], states[j] = states[j], states[i]
	//})
	// if we're 500 wide and want only 100, we want groups of 5
	result := dd.Partition{}
	if len(states) <= j.MaxWidth {
		result.Keepers = make([]int, len(states))
		for i := range states {
			result.Keepers[i] = i
		}
		return result
	}

	slices.SortFunc(states, func(a, b *dd.State[TValue, TCost]) int {
		return context.Compare((*a).Cost(context), (*b).Cost(context))
	})

	width := j.MaxWidth - j.Groups
	result.Keepers = make([]int, width)
	for i := 0; i < width; i++ {
		result.Keepers[i] = i
	}

	group_size := (len(states) - width) / (j.MaxWidth - width)
	for i := 0; i < j.MaxWidth-width; i++ {
		result.MergeGroups = append(result.MergeGroups, dd.PartitionPair{width + i*group_size, width + i*group_size + group_size})
	}
	result.MergeGroups[len(result.MergeGroups)-1].End = len(states)

	return result
}
