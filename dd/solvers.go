package dd

import (
	"cmp"
	"golang.org/x/exp/constraints"
	"hash/maphash"
	"log"
	"slices"
)

// step 1: generate the unitary relaxed graph
// step 2: solve for optimal path (along the full graph)
// step 3: replace the closest-to-front relaxed node with exact nodes
// step 4: optimizing again, we should go down one of our new paths only if it's competitive
// a node has a state for each and every constraint, it's also comparable,

type Context[TValue cmp.Ordered, TCost any] interface {
	GetStartingState() State[TValue, TCost]
	GetValues(variable int) []TValue
	GetVariables() int
	Compare(a, b TCost) int
	WorstCost() TCost
}

type State[TValue cmp.Ordered, TCost any] interface {
	TransitionTo(context Context[TValue, TCost], value TValue) State[TValue, TCost]
	Cost(context Context[TValue, TCost]) TCost
	MergeFrom(context Context[TValue, TCost], state State[TValue, TCost])
	Unrelax(context Context[TValue, TCost], removed State[TValue, TCost], value TValue)
	Heuristic(context Context[TValue, TCost]) TCost
	HashBytes() []byte
	Equals(state State[TValue, TCost]) bool
	IsRelaxed() bool
	Solution(context Context[TValue, TCost]) []TValue
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
		for _, value := range context.GetValues(j) {
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
		if logger != nil && ((rounds%1000) == 0 || logger.Flags() == 1) {
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
	reducer func([]*State[TValue, TCost]) []*State[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	// we don't need to hold on to all states; only hold those that lead to the best cost for a given state.
	// when we get to the bottom, we take the best of those for our final solution.
	// but we have to walk up the chain to get the actual values.
	closed := map[uint64][]*State[TValue, TCost]{}
	starter := context.GetStartingState()
	parents := []*State[TValue, TCost]{&starter}
	variables := context.GetVariables()
	var bestSolution State[TValue, TCost]
	bestCost := context.WorstCost()
	hasher := maphash.Hash{}
	for j := 0; j < variables; j++ {
		var children []*State[TValue, TCost]
		duplicates := 0
		for _, parent := range parents {
			for _, value := range context.GetValues(j) {
				child := (*parent).TransitionTo(context, value)
				if child == nil {
					continue
				}
				// here is an expensive check for existing nodes.
				// we're not sure if the size of the map is less than the size of the additional duplicate nodes
				hasher.Reset()
				hasher.Write(child.HashBytes())
				hash := hasher.Sum64()
				others, found := closed[hash]
				var childPtr *State[TValue, TCost]
				if found {
					for _, other := range others {
						if child.Equals(*other) {
							childPtr = other
							duplicates += 1
							break
						}
					}
				}
				if childPtr == nil {
					childPtr = &child
					closed[hash] = append(closed[hash], childPtr)
					if j == variables-1 {
						childCost := (*childPtr).Cost(context)
						if context.Compare(childCost, bestCost) < 0 {
							bestSolution = *childPtr
							bestCost = childCost
						}
					} else {
						children = append(children, childPtr)
					}
				}
			}
		}
		clear(closed)
		if logger != nil && (logger.Flags()&1) == 1 {
			logger.Printf("Layer %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		}
		if j < variables-1 {
			parents = reducer(children)
			if len(parents) == 0 { // handle infeasibility
				return context.WorstCost(), nil
			}
		}
	}
	return bestSolution.Cost(context), bestSolution.Solution(context)
}

func SolveRestricted[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], maxWidth int, logger *log.Logger) (TCost, []TValue) {
	reducer := func(children []*State[TValue, TCost]) []*State[TValue, TCost] {
		if maxWidth > 0 && len(children) > maxWidth {
			// possible strategies: shuffle, sort, partial sort, sort with random chance of skip
			slices.SortFunc(children, func(a, b *State[TValue, TCost]) int {
				// return context.Compare(arcsTo[a].cost, arcsTo[b].cost)
				return context.Compare((*a).Heuristic(context), (*b).Heuristic(context))
			})
			return children[:maxWidth]
		} else {
			return children
		}
	}
	return solveByExpansion[TValue, TCost](context, reducer, logger)
}

func SolveByFullExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	reducer := func(children []*State[TValue, TCost]) []*State[TValue, TCost] {
		return children
	}
	return solveByExpansion[TValue, TCost](context, reducer, logger)
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
	return solveByExpansion[TValue, TCost](context, reducer, logger)
}
