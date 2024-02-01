package dd

import (
	"cmp"
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
	CostTo(context Context[TValue, TCost], child State[TValue, TCost], value TValue) TCost
	MergeFrom(context Context[TValue, TCost], state State[TValue, TCost])
	Unrelax(context Context[TValue, TCost], removed State[TValue, TCost])
	Heuristic(context Context[TValue, TCost], runningCost TCost) TCost
	HashBytes() []byte
	Equals(state State[TValue, TCost]) bool
	IsRelaxed() bool
}

type arcTo[TValue cmp.Ordered, TCost any] struct {
	state *State[TValue, TCost]
	cost  TCost
	value TValue
}

// solver mechanisms: 1. full expansion, 2. max width with merge/Unrelax, 3. by separation, 4. branch and bound

func SolveBySeparation[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {

	// since we don't know which arc is best in a relaxed situation, we have to allow multiple input arcs per child
	arcsFromKey := map[*State[TValue, TCost]][]arcTo[TValue, TCost]{} // holds all nodes going from key in this context

	// step 1: generate the relaxed tree
	variables := context.GetVariables()
	layers := make([][]*State[TValue, TCost], variables)
	starter := context.GetStartingState()
	parent := &starter
	for j := 0; j < variables; j++ {
		merged := context.GetStartingState()
		for _, value := range context.GetValues(j) {
			child := (*parent).TransitionTo(context, value)
			if child == nil {
				continue
			}
			merged.MergeFrom(context, child)
		}
		if merged.Equals(starter) { // infeasible
			return context.WorstCost(), nil
		}
		mergedPtr := &merged
		for _, value := range context.GetValues(j) {
			cost := (*parent).CostTo(context, merged, value)
			arcsFromKey[parent] = append(arcsFromKey[parent], arcTo[TValue, TCost]{mergedPtr, cost, value})
		}
		parent = mergedPtr
		layers[j] = []*State[TValue, TCost]{mergedPtr}
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
		if bestArcs[0].state != &starter {
			panic("Expected to start at starter")
		}
		parent = &starter
		splitAtLeastOne := false
		for j, arc := range bestArcs {
			relaxed := *arc.state
			if !relaxed.IsRelaxed() {
				continue
			}
			if !slices.Contains(layers[j-1], arc.state) {
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
			relaxed.Unrelax(context, peer)
			// now duplicate the immediate descendants of the old endpoint
			for _, subarc := range arcsFromKey[arc.state] {
				cost := peer.CostTo(context, *subarc.state, subarc.value)
				// TODO: may need to detect infeasible arc here
				child := arcTo[TValue, TCost]{subarc.state, cost, subarc.value}
				arcsFromKey[peerPtr] = append(arcsFromKey[peerPtr], child)
			}
			layers[j-1] = append(layers[j-1], peerPtr)
			nodes += 1
			cost := (*parent).CostTo(context, peer, arc.value)
			// recompute and update arc (which means we need to know its parent, which is easy from the chain)
			for si, subarc := range arcsFromKey[parent] {
				if subarc.value == arc.value { // we could use arc value as a key; it should only be there once
					subarc.cost = cost
					parent = subarc.state
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
	for j := 0; j < variables-1; j++ {
		findBestDistance(context, layers[j], arcsFromKey, dist, true)
	}
	var bestState *State[TValue, TCost] = nil
	bestCost := context.WorstCost()
	for _, state := range layers[variables-1] {
		dbs, founda := dist[state]
		if founda && context.Compare(dbs.cost, bestCost) < 0 {
			bestState = state
			bestCost = dbs.cost
		}
	}
	if bestState == nil {
		return bestCost, nil
	}
	bestArcs := make([]arcTo[TValue, TCost], variables)
	for j := variables - 1; j >= 0; j-- {
		dbs, found := dist[bestState]
		if !found {
			panic("Unexpected missing state in dist")
		}
		bestArcs[j] = dbs
		bestState = dbs.state
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
			if required {
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

func SolveByFullExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], maxWidth int, logger *log.Logger) (TCost, []TValue) {
	// we don't need to hold on to all states; only hold those that lead to the best cost for a given state.
	// when we get to the bottom, we take the best of those for our final solution.
	// but we have to walk up the chain to get the actual values.
	arcsTo := map[*State[TValue, TCost]]arcTo[TValue, TCost]{} // holds the best arc going to key
	closed := map[uint64][]*State[TValue, TCost]{}
	hasher := maphash.Hash{}
	starter := context.GetStartingState()
	parents := []*State[TValue, TCost]{&starter}
	variables := context.GetVariables()
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
				_, _ = hasher.Write(child.HashBytes())
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
					children = append(children, childPtr)
				}
				cost := (*parent).CostTo(context, child, value)
				arc, found := arcsTo[childPtr]
				if !found || context.Compare(cost+arcsTo[parent].cost, arc.cost) < 0 {
					arcsTo[childPtr] = arcTo[TValue, TCost]{parent, cost + arcsTo[parent].cost, value}
					// TODO: cleanup childless state nodes
				}
			}
		}
		clear(closed)
		if logger != nil && (logger.Flags()&1) == 1 {
			logger.Printf("Layer %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		}
		if maxWidth > 0 && len(children) > maxWidth {
			// possible strategies: shuffle, sort, partial sort, sort with random chance of skip
			slices.SortFunc(children, func(a, b *State[TValue, TCost]) int {
				// return context.Compare(arcsTo[a].cost, arcsTo[b].cost)
				return context.Compare((*a).Heuristic(context, arcsTo[a].cost), (*b).Heuristic(context, arcsTo[b].cost))
			})
			parents = children[:maxWidth]
			// parents = append(parents, children[len(children)-maxWidth/2:]...)
		} else {
			parents = children
		}
	}
	if len(parents) == 0 { // handle infeasibility
		return context.WorstCost(), nil
	}
	bestSolution := slices.MinFunc(parents, func(a, b *State[TValue, TCost]) int {
		return context.Compare(arcsTo[a].cost, arcsTo[b].cost)
	})
	bestValues := make([]TValue, variables)
	bestCost := arcsTo[bestSolution].cost
	for j := variables - 1; j >= 0; j-- {
		arc := arcsTo[bestSolution]
		bestValues[j] = arc.value
		bestSolution = arc.state
	}
	return bestCost, bestValues
}
