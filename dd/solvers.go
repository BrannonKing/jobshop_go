package dd

import (
	"cmp"
	"hash/maphash"
	"log"
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
	Compare(a, b TCost) bool
	WorstCost() TCost
}

type State[TValue cmp.Ordered, TCost any] interface {
	TransitionTo(context Context[TValue, TCost], value TValue) State[TValue, TCost]
	CostTo(context Context[TValue, TCost], child State[TValue, TCost], value TValue) TCost
	MergeFrom(context Context[TValue, TCost], state State[TValue, TCost])
	Unrelax(context Context[TValue, TCost], removed State[TValue, TCost])
	HashBytes() []byte
	Equals(state State[TValue, TCost]) bool
	IsRelaxed() bool
}

type arcTo[TValue cmp.Ordered, TCost any] struct {
	cost     TCost
	value    TValue
	endpoint *State[TValue, TCost]
}

// solver mechanisms: 1. full expansion, 2. max width with merge/Unrelax, 3. by separation, 4. branch and bound

func SolveBySeparation[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	arcsTo := map[*State[TValue, TCost]][]arcTo[TValue, TCost]{}

	// step 1: generate the relaxed tree
	variables := context.GetVariables()
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
		for _, value := range context.GetValues(j) {
			cost := (*parent).CostTo(context, merged, value)
			arcsTo[parent] = append(arcsTo[parent], arcTo[TValue, TCost]{cost, value, &merged})
		}
		parent = &merged
	}

	// step 2: while there is a relaxed node in the solution, Unrelax that one
	rounds := 0
	for {
		rounds += 1
		bestCost, bestArcs := findSolution[TValue, TCost](context, &starter, 0, arcsTo)
		parent = &starter
		splitAtLeastOne := false
		for _, arc := range bestArcs {
			if (*arc.endpoint).IsRelaxed() {
				splitAtLeastOne = true
				// create the exact node:
				child := (*arc.endpoint).TransitionTo(context, arc.value)
				if child == nil {
					panic("This should have never been created the first time")
				}
				// TODO: check for duplicates here?
				// give the old node an option to update itself having had child ripped from it:
				(*arc.endpoint).Unrelax(context, child)
				// now duplicate the immediate descendants of the old endpoint
				for _, subarc := range arcsTo[arc.endpoint] {
					cost := child.CostTo(context, *subarc.endpoint, arc.value)
					grandChild := arcTo[TValue, TCost]{cost, subarc.value, subarc.endpoint}
					arcsTo[&child] = append(arcsTo[&child], grandChild)
				}
				cost := (*parent).CostTo(context, child, arc.value)
				// recompute and update arc (which means we need to know its parent, which is easy from the chain)
				for _, subarc := range arcsTo[parent] {
					if subarc.value == arc.value { // we could use arc value as a key; it should only be there once
						subarc.cost = cost
						parent = subarc.endpoint
						subarc.endpoint = &child
						break
					}
				}
				// option: break right here to only update the first relaxed node
			}
		}
		if !splitAtLeastOne {
			bestValues := make([]TValue, len(bestArcs))
			for i, arc := range bestArcs {
				bestValues[i] = arc.value
			}
			return bestCost, bestValues
		}
		if logger != nil {
			logger.Printf("Round %d, %d nodes, ")
		}
	}

}

func SolveByFullExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	arcsTo := map[*State[TValue, TCost]][]arcTo[TValue, TCost]{}
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
				}
				cost := (*parent).CostTo(context, child, value)
				arcsTo[parent] = append(arcsTo[parent], arcTo[TValue, TCost]{cost, value, childPtr})
				children = append(children, childPtr)
			}
		}
		clear(closed)
		if logger != nil {
			logger.Printf("Layer %d, %d nodes, %d duplicates\n", j+1, len(children), duplicates)
		}
		parents = children
	}
	this is broken: need to use DAG shortest path instead:
	bestCost, bestArcs := findSolution[TValue, TCost](context, &starter, 0, arcsTo)
	bestValues := make([]TValue, len(bestArcs))
	for i, arc := range bestArcs {
		bestValues[i] = arc.value
	}
	return bestCost, bestValues
}

func findSolution[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], state *State[TValue, TCost],
	depth int, arcs map[*State[TValue, TCost]][]arcTo[TValue, TCost]) (TCost, []arcTo[TValue, TCost]) {
	bestCost := context.WorstCost()
	var bestChildArcs []arcTo[TValue, TCost]
	var bestArc arcTo[TValue, TCost]
	outputs := arcs[state]
	if outputs == nil {
		if depth == context.GetVariables() {
			return TCost(0), nil // made it to the end; no more cost
		}
		return context.WorstCost(), nil // made it to a clipped node; we don't want that
	}

	for _, arc := range outputs {
		costs, values := findSolution(context, arc.endpoint, depth+1, arcs)
		if context.Compare(costs+arc.cost, bestCost) {
			bestCost = costs + arc.cost // TODO: use a global best-cost, so we can exit sub-branches early
			bestChildArcs = values
			bestArc = arc
		}
	}

	bestValues := make([]arcTo[TValue, TCost], len(bestChildArcs)+1) // TODO: could allocate this all up front, eliminate copies
	copy(bestValues[1:], bestChildArcs)
	bestValues[0] = bestArc
	return bestCost, bestValues
}
