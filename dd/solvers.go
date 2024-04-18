package dd

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"log"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// step 1: generate the unitary relaxed graph
// step 2: solve for optimal path (along the full graph)
// step 3: replace the closest-to-front relaxed node with exact nodes
// step 4: optimizing again, we should go down one of our new paths only if it's competitive
// a node has a state for each and every constraint, it's also comparable,

type Context[TValue cmp.Ordered, TCost any] interface {
	GetStartingState() State[TValue, TCost]
	GetValues() []TValue
	GetVariables() int
	Compare(a, b TCost) int
	WorstCost() TCost
	Offset() int
	Child(state *State[TValue, TCost], offset int) Context[TValue, TCost]
}

type State[TValue cmp.Ordered, TCost any] interface {
	TransitionTo(context Context[TValue, TCost], value TValue) State[TValue, TCost]
	Cost(context Context[TValue, TCost]) TCost
	MergeFrom(context Context[TValue, TCost], state State[TValue, TCost])
	Unrelax(context Context[TValue, TCost], removed State[TValue, TCost], value TValue)
	Heuristic(context Context[TValue, TCost]) TCost
	ID(context Context[TValue, TCost]) string
	IsRelaxed() bool
	Solution(context Context[TValue, TCost]) []TValue
	AppendCutset([]State[TValue, TCost])
	Cutset() []State[TValue, TCost]
	ClearCutset()
}

type arcTo[TValue cmp.Ordered, TCost any] struct {
	state *State[TValue, TCost]
	cost  TCost
	value TValue
}

// solver mechanisms: 1. full expansion, 2. max width with merge/Unrelax, 3. by separation, 4. branch and bound

func SolveBySeparation[TValue cmp.Ordered, TCost constraints.Float | constraints.Integer](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {

	// since we don't know which arc is best in a relaxed situation, we have to allow multiple input arcs per child
	arcsFromKey := map[*State[TValue, TCost]][]arcTo[TValue, TCost]{} // holds all nodes going from key in this context

	// step 1: generate the relaxed tree
	variables := context.GetVariables()
	layers := make([][]*State[TValue, TCost], variables)
	starter := context.GetStartingState()
	parent := &starter

	for j := 0; j < variables; j++ {
		for _, value := range context.GetValues() {
			child := (*parent).TransitionTo(context, value)
			if child == nil {
				continue
			}
			layers[j] = append(layers[j], &child)
			cost := child.Cost(context) - (*parent).Cost(context)
			arcsFromKey[parent] = append(arcsFromKey[parent], arcTo[TValue, TCost]{&child, cost, value})
		}
		if len(layers[j]) == 0 { // infeasible
			return context.WorstCost(), nil
		}

		merger := layers[j][0]
		for i := 1; i < len(layers[j]); i++ {
			(*merger).MergeFrom(context, *layers[j][i])
			arcsFromKey[parent][i].state = merger
		}
		layers[j] = layers[j][:1]
		parent = merger
	}

	// step 2: while there is a relaxed node in the solution, Unrelax that one
	rounds := 0
	nodes := variables + 1
	for {
		rounds += 1
		bestCost, bestArcs := findSolution[TValue, TCost](context, &starter, layers, arcsFromKey)
		if bestArcs == nil {
			if logger != nil {
				logger.Printf("Infeasible.\n")
			}
			return bestCost, nil
		}
		if len(bestArcs) != variables {
			panic("bad arc list length")
		}
		parent = &starter
		splitAtLeastOne := false
		for j, arc := range bestArcs {
			relaxed := *arc.state
			if !relaxed.IsRelaxed() {
				continue
			}
			// TODO: remove this assertion:
			if !slices.Contains(layers[j], arc.state) {
				panic("Expected layer to contain state")
			}
			splitAtLeastOne = true
			// create the exact node:
			peer := relaxed.TransitionTo(context, arc.value)
			if peer == nil {
				panic("This should have never been created the first time")
			}
			peerPtr := &peer
			// TODO: check for duplicates here?
			// give the old node an option to update itself having had peer ripped from it:
			relaxed.Unrelax(context, peer, arc.value)
			// now duplicate the immediate descendants of the old endpoint
			for _, subarc := range arcsFromKey[arc.state] {
				cost := (*subarc.state).Cost(context) - (peer).Cost(context)
				// TODO: may need to detect infeasible arc here
				child := arcTo[TValue, TCost]{subarc.state, cost, subarc.value}
				arcsFromKey[peerPtr] = append(arcsFromKey[peerPtr], child)
			}
			layers[j] = append(layers[j], peerPtr)
			nodes += 1
			cost := peer.Cost(context) - (*parent).Cost(context)
			// recompute and update arc (which means we need to know its parent, which is easy from the chain)
			for si, subarc := range arcsFromKey[parent] {
				if subarc.value == arc.value { // we could use arc value as a key; it should only be there once
					subarc.cost = cost
					parent = subarc.state // same as relaxed
					subarc.state = peerPtr
					arcsFromKey[parent][si] = subarc
					break
				}
			}
			break // TODO: remove this
			// option: break right here to only update the first relaxed node
		}
		if !splitAtLeastOne {
			bestValues := make([]TValue, len(bestArcs))
			for i, arc := range bestArcs {
				bestValues[i] = arc.value
			}
			return bestCost, bestValues
		}
		if logger != nil && (rounds%1000) == 0 {
			logger.Printf("Round %d, %d nodes, %v bound\n", rounds, nodes, bestCost)
		}
	}

}

func findSolution[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], starter *State[TValue, TCost],
	layers [][]*State[TValue, TCost], arcsFromKey map[*State[TValue, TCost]][]arcTo[TValue, TCost]) (TCost, []arcTo[TValue, TCost]) {
	dist := map[*State[TValue, TCost]]arcTo[TValue, TCost]{} // TODO: don't reallocate this in every call
	variables := context.GetVariables()
	findBestDistance(context, []*State[TValue, TCost]{starter}, arcsFromKey, dist, false)
	if len(dist) <= 0 {
		panic("Failed to add any paths from starter")
	}
	for j := 0; j < variables-1; j++ {
		findBestDistance(context, layers[j], arcsFromKey, dist, true)
	}
	// find the best node in the last layer:
	var bestState *State[TValue, TCost] = nil
	bestCost := context.WorstCost()
	for _, state := range layers[variables-1] {
		dbs, found := dist[state]
		if found && context.Compare(dbs.cost, bestCost) < 0 {
			bestState = state
			bestCost = dbs.cost
		}
	}
	// if we don't have one, we're infeasible:
	if bestState == nil {
		return bestCost, nil
	}

	// walk up the tree to the starter:
	bestArcs := make([]arcTo[TValue, TCost], variables)
	for j := variables - 1; j >= 0; j-- {
		dbs, found := dist[bestState]
		if !found {
			panic("Unexpected missing state in dist")
		}
		bestArcs[j] = arcTo[TValue, TCost]{bestState, dbs.cost, dbs.value}
		bestState = dbs.state // this is the node that one came from
	}
	return bestCost, bestArcs
}

func findBestDistance[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	layer []*State[TValue, TCost], arcsFromKey map[*State[TValue, TCost]][]arcTo[TValue, TCost],
	dist map[*State[TValue, TCost]]arcTo[TValue, TCost], required bool) {
	for _, state := range layer {
		children, found := arcsFromKey[state]
		if !found {
			panic("Unable to find state in arcs")
		}
		dscost := TCost(0)
		ds, found := dist[state]
		if found {
			dscost = ds.cost
		} else {
			if required { // require the parent to be in the dist map
				continue
			}
		}
		for _, child := range children {
			cs, found := dist[child.state]
			if !found || context.Compare(dscost+child.cost, cs.cost) < 0 {
				dist[child.state] = arcTo[TValue, TCost]{state, dscost + child.cost, child.value}
				//if !slices.Contains(layers[j+1], child.state) {
				//	panic("Missing child state!")
				//}
			}
		}
	}
}

func solveByExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	reducer func([]*State[TValue, TCost]) []*State[TValue, TCost],
	logger *log.Logger, cutoff TCost, cutoffExact bool) []*State[TValue, TCost] {
	// we don't need to hold on to all states; only hold those that lead to the best cost for a given state.
	// when we get to the bottom, we take the best of those for our final solution.
	// but we have to walk up the chain to get the actual values.
	closed := map[string]*State[TValue, TCost]{}
	starter := context.GetStartingState()
	parents := []*State[TValue, TCost]{&starter}
	variables := context.GetVariables()
	for j := context.Offset(); j < variables; j++ {
		var children []*State[TValue, TCost]
		duplicates := 0
		for _, parent := range parents {
			for _, value := range context.GetValues() {
				child := (*parent).TransitionTo(context, value)
				if child == nil {
					continue
				}
				if cutoff != context.WorstCost() {
					cost := child.Cost(context)
					if cutoffExact && context.Compare(cost, cutoff) >= 0 {
						continue
					} else if !cutoffExact && context.Compare(cost, cutoff) > 0 {
						continue
					}
				}
				// here is an expensive check for existing nodes.
				// we're not sure if the size of the map is less than the size of the additional duplicate nodes
				key := child.ID(context)
				existing, found := closed[key]
				var childPtr *State[TValue, TCost]
				if found {
					childPtr = existing
					duplicates += 1
				}
				if childPtr == nil {
					childPtr = &child
					closed[key] = childPtr
					children = append(children, childPtr)
				}
			}
		}
		clear(closed)
		//if logger != nil {
		//	logger.Printf("Layer %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		//}
		parents = reducer(children)
		if len(parents) <= 0 {
			break
		}
	}
	return parents
}

func solveRestricted[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	maxWidth int, logger *log.Logger, cutoff TCost, cutoffExact bool) (*State[TValue, TCost], bool) {
	exact := true
	reducer := func(children []*State[TValue, TCost]) []*State[TValue, TCost] {
		if maxWidth > 0 && len(children) > maxWidth {
			exact = false
			// possible strategies: shuffle, sort, partial sort, sort with random chance of skip
			slices.SortFunc(children, func(a, b *State[TValue, TCost]) int {
				return context.Compare((*a).Cost(context), (*b).Cost(context))
			})
			return children[:maxWidth]
		} else {
			return children
		}
	}
	leafs := solveByExpansion[TValue, TCost](context, reducer, logger, cutoff, cutoffExact)
	if len(leafs) == 0 {
		return nil, exact
	} // exact and no way through means we don't need a cutset from them
	// not sure if the final sort ran or not:
	best := slices.MinFunc(leafs, func(a, b *State[TValue, TCost]) int {
		return context.Compare((*a).Cost(context), (*b).Cost(context))
	})
	return best, exact
}

func SolveRestricted[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	maxWidth int, logger *log.Logger) (TCost, []TValue) {
	best, _ := solveRestricted[TValue, TCost](context, maxWidth, logger, context.WorstCost(), false)
	if best == nil {
		return context.WorstCost(), nil
	}
	return (*best).Cost(context), (*best).Solution(context)
}

func SolveRelaxed[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	strategy PartitionStrategy[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	reducer := func(children []*State[TValue, TCost]) []*State[TValue, TCost] {
		// can put the method on the context object.
		// or we can do the clustering (or sorting) here

		// our current merge operation assumes that the items being merged have the same parent.
		// we don't necessarily need that restriction

		// it doesn't seem useful to have just a few merge nodes; those few will be too relaxed

		// partition options:
		// 1. cluster on cmax
		// 2. by machine (for m partitions)
		// 3. random groups; keep 1 best and merge the rest into another one
		// 4. cluster on similarity of done jobs (though in the final layers they will have done most everything)

		partition := strategy.Find(context, children)
		if len(partition.Keepers) == 0 && len(partition.MergeGroups) == 0 {
			return children
		}
		keepers := make([]*State[TValue, TCost], 0, len(partition.Keepers)+len(partition.MergeGroups))
		for _, i := range partition.Keepers {
			keepers = append(keepers, children[i])
		}
		for g := 0; g < len(partition.MergeGroups); g++ {
			if partition.MergeGroups[g].Start >= partition.MergeGroups[g].End {
				continue
			}
			target := children[partition.MergeGroups[g].Start]
			for i := partition.MergeGroups[g].Start + 1; i < partition.MergeGroups[g].End; i++ {
				(*target).MergeFrom(context, *children[i])
			}
			keepers = append(keepers, target)
		}
		return keepers
	}
	results := solveByExpansion[TValue, TCost](context, reducer, logger, context.WorstCost(), false)
	if len(results) == 0 {
		return context.WorstCost(), nil
	}
	// TODO: remove final reducer call for performance
	best := *slices.MinFunc(results, func(a, b *State[TValue, TCost]) int {
		return context.Compare((*a).Cost(context), (*b).Cost(context))
	})
	return best.Cost(context), best.Solution(context)
}

func SolveByFullExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	reducer := func(children []*State[TValue, TCost]) []*State[TValue, TCost] {
		return children
	}
	results := solveByExpansion[TValue, TCost](context, reducer, logger, context.WorstCost(), false)
	if len(results) == 0 {
		return context.WorstCost(), nil
	}
	best := *slices.MinFunc(results, func(a, b *State[TValue, TCost]) int {
		return context.Compare((*a).Cost(context), (*b).Cost(context))
	})
	return best.Cost(context), best.Solution(context)
}

type layerState[TValue cmp.Ordered, TCost cmp.Ordered] struct {
	state *State[TValue, TCost]
	layer int
}

type doubleSort[TValue cmp.Ordered, TCost cmp.Ordered] struct {
	children       []*State[TValue, TCost]
	childToParents [][]int
	context        Context[TValue, TCost]
}

func (d doubleSort[TValue, TCost]) Len() int {
	return len(d.children)
}

func (d doubleSort[TValue, TCost]) Swap(i, j int) {
	d.children[j], d.children[i] = d.children[i], d.children[j]
	d.childToParents[j], d.childToParents[i] = d.childToParents[i], d.childToParents[j]
}

func (d doubleSort[TValue, TCost]) Less(i, j int) bool {
	return d.context.Compare((*d.children[i]).Cost(d.context), (*d.children[j]).Cost(d.context)) < 0
}

func fastCutset[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	maxWidth int, logger *log.Logger, cutoff TCost, cutoffExact bool) map[string]layerState[TValue, TCost] {
	closed := map[string]int{}
	starter := context.GetStartingState()
	parents := []*State[TValue, TCost]{&starter}
	variables := context.GetVariables()
	cutset := map[string]layerState[TValue, TCost]{}
	for j := context.Offset(); j < variables; j++ {
		var children []*State[TValue, TCost]
		var childToParents [][]int
		duplicates := 0
		for p, parent := range parents {
			cv := context.GetValues()
			if len(cv) > maxWidth {
				panic("maxWidth is too small!")
			}
			for _, value := range cv {
				child := (*parent).TransitionTo(context, value)
				if child == nil {
					continue
				}
				if cutoff != context.WorstCost() {
					cost := child.Cost(context)
					if cutoffExact && context.Compare(cost, cutoff) >= 0 {
						continue
					} else if !cutoffExact && context.Compare(cost, cutoff) > 0 {
						continue
					}
				}
				// here is an expensive check for existing nodes.
				// we're not sure if the size of the map is less than the size of the additional duplicate nodes.
				// TODO: just skip this for merged nodes
				key := child.ID(context)
				existingIdx, found := closed[key]
				var childPtr *State[TValue, TCost]
				if found {
					childPtr = children[existingIdx]
					duplicates += 1
					childToParents[existingIdx] = append(childToParents[existingIdx], p)
				}
				if childPtr == nil {
					childPtr = &child
					closed[key] = len(children)
					children = append(children, childPtr)
					childToParents = append(childToParents, []int{p})
				}
			}
		}
		if logger != nil {
			logger.Printf("Layer %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		}
		clear(closed)
		if len(children) <= maxWidth {
			parents = children
			if len(parents) <= 0 {
				break
			}
			continue
		}
		ds := doubleSort[TValue, TCost]{children, childToParents, context}
		// sort our stuff. If selected for merge, put parent in cutset and yank their other kids
		sort.Sort(ds)
		// we're going to make a new merged node that has everything that doesn't fit in width.
		// the parents of those need to go into our cutset, as they will then have a merged child.
		for c, _ := range children[maxWidth:] {
			cp := childToParents[c]
			for _, parentIdx := range cp {
				parent := parents[parentIdx]
				cutset[(*parent).ID(context)] = layerState[TValue, TCost]{parent, j}
			}
		}

		// now we're going to keep children as long as we didn't just put their parent into the cutset:
		keepers := make([]*State[TValue, TCost], 0, maxWidth)
		for c, child := range children[:maxWidth] {
			cp := childToParents[c]
			unwanted := false
			for _, parentIdx := range cp {
				parent := parents[parentIdx]
				if _, found := cutset[(*parent).ID(context)]; found {
					unwanted = true
					break
				}
			}
			if !unwanted {
				keepers = append(keepers, child)
			}
		}
		parents = keepers
		if len(parents) <= 0 {
			break
		}
	}
	return cutset
}

func fastCutset2[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	maxWidth int, logger *log.Logger, cutoff TCost, cutoffExact bool) map[string]layerState[TValue, TCost] {
	//left off: maybe don't use strategy; just sort them. pass in parent map here and sort that too, like done in fast cutset
	//put cutset parent into each of their children. pass that down afterward
	//but a merged node can hold multiple cutset nodes; have to pass them all down (and remove them all at the end!)

	closed := map[string]int{}
	starter := context.GetStartingState()
	parents := []*State[TValue, TCost]{&starter}
	variables := context.GetVariables()
	for j := context.Offset(); j < variables; j++ {
		var children []*State[TValue, TCost]
		var childToParents [][]int
		duplicates := 0
		for p, parent := range parents {
			cv := context.GetValues()
			if len(cv) > maxWidth {
				panic("maxWidth is too small!")
			}
			for _, value := range cv {
				child := (*parent).TransitionTo(context, value)
				if child == nil {
					continue
				}
				if cutoff != context.WorstCost() {
					cost := child.Cost(context)
					if cutoffExact && context.Compare(cost, cutoff) >= 0 {
						continue
					} else if !cutoffExact && context.Compare(cost, cutoff) > 0 {
						continue
					}
				}
				// here is an expensive check for existing nodes.
				// we're not sure if the size of the map is less than the size of the additional duplicate nodes.
				// TODO: just skip this for merged nodes
				key := child.ID(context)
				existingIdx, found := closed[key]
				var childPtr *State[TValue, TCost]
				if found {
					childPtr = children[existingIdx]
					duplicates += 1
					childToParents[existingIdx] = append(childToParents[existingIdx], p)
				}
				if childPtr == nil {
					childPtr = &child
					closed[key] = len(children)
					children = append(children, childPtr)
					childToParents = append(childToParents, []int{p})
				}
				(*childPtr).AppendCutset((*parent).Cutset())
			}
			(*parent).ClearCutset()
		}
		//if logger != nil {
		//	logger.Printf("LayerFC %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		//}
		clear(closed)

		if j == variables-1 {
			// all nodes on the bottom layer outside of cutoff should have been dropped.
			// therefore, any cutset that makes it to the bottom is a keeper
			cutset := map[string]layerState[TValue, TCost]{}
			for _, child := range children {
				for _, cut := range (*child).Cutset() {
					cutset[cut.ID(context)] = layerState[TValue, TCost]{&cut, -1}
				}
			}
			return cutset
		}

		if len(children) <= maxWidth {
			parents = children
			if len(parents) <= 0 {
				break
			}
			continue
		}
		ds := doubleSort[TValue, TCost]{children, childToParents, context}
		// sort our stuff. If selected for merge, put parent in cutset and yank their other kids
		sort.Sort(ds)
		// we're going to make a new merged node that has everything that doesn't fit in width.
		// the parents of those need to go into our cutset, as they will then have a merged child.

		for c, child := range children[maxWidth:] {
			cp := childToParents[c]
			var added []State[TValue, TCost]
			for _, parentIdx := range cp {
				if !(*parents[parentIdx]).IsRelaxed() {
					added = append(added, *parents[parentIdx])
				}
			}
			(*child).AppendCutset(added)
		}

		merged := children[maxWidth]
		for _, child := range children[maxWidth+1:] {
			(*merged).MergeFrom(context, *child)
		}
		children[maxWidth] = merged
		parents = children[:maxWidth+1]
	}
	return map[string]layerState[TValue, TCost]{}
}

func SolveBnb[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost],
	maxWidth int, logger *log.Logger) (TCost, []TValue) {
	starter := context.GetStartingState()
	const workers = 6
	q := make([]layerState[TValue, TCost], 0, 100)
	q = append(q, layerState[TValue, TCost]{&starter, 0})
	cutoff := context.WorstCost()
	cutoffExact := false
	closed := map[string]bool{}
	var best *State[TValue, TCost]
	i := int64(0)
	readers := int32(0)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				atomic.AddInt64(&i, 1)
				atomic.AddInt32(&readers, 1)
			TryRead:
				var u layerState[TValue, TCost]
				wantsSleep := false
				mutex.Lock()
				if len(q) == 0 && readers >= workers {
					mutex.Unlock()
					break
				} else if len(q) == 0 {
					wantsSleep = true
				} else {
					u = q[len(q)-1]
					q[len(q)-1].state = nil
					q = q[:len(q)-1]
				}
				mutex.Unlock()
				if wantsSleep {
					time.Sleep(time.Millisecond)
					goto TryRead
				}
				atomic.AddInt32(&readers, -1)

				cu := context.Child(u.state, u.layer)
				restricted, exact := solveRestricted[TValue, TCost](cu, maxWidth, logger, cutoff, cutoffExact)
				if restricted != nil {
					cost := (*restricted).Cost(context)
					mutex.Lock()
					if context.Compare(cost, cutoff) < 0 ||
						(exact && context.Compare(cost, cutoff) <= 0) {
						logger.Printf("%d: Restricted %d, %v, %v\n", i, len(q), cost, exact)
						best = restricted
						cutoff = cost
						cutoffExact = exact
					}
					mutex.Unlock()
				}
				if !exact {
					mutex.Lock()
					lc := cutoff
					lce := cutoffExact
					mutex.Unlock()
					cutset := fastCutset2[TValue, TCost](cu, maxWidth, logger, lc, lce)
					mutex.Lock()
					if (i & 0x3f) == 0 {
						logger.Printf("%d: Relaxed %d, %d\n", i, len(q), len(cutset))
					}
					for id, lstate := range cutset {
						found := closed[id]
						if !found {
							closed[id] = true
							q = append(q, lstate)
						}
					}
					mutex.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	logger.Printf("%d: Done\n", i)
	if best == nil {
		return context.WorstCost(), nil
	}
	return (*best).Cost(context), (*best).Solution(context)
}

type PartitionPair struct {
	Start, End int
}

type Partition struct {
	Keepers     []int
	MergeGroups []PartitionPair
}

type PartitionStrategy[TValue cmp.Ordered, TCost cmp.Ordered] interface {
	Find(context Context[TValue, TCost], states []*State[TValue, TCost]) Partition
}
