package dd

import (
	"cmp"
	"log"
)

// step 1: generate the unitary relaxed graph
// step 2: solve for optimal path (along the full graph)
// step 3: replace the closest-to-front relaxed node with exact nodes
// step 4: optimizing again, we should go down one of our new paths only if it's competitive
// a node has a state for each and every constraint, it's also comparable,

type Keyable interface {
	Hash() int64
	Equals(state Keyable) bool
}

type Context[TValue cmp.Ordered, TCost any] interface {
	GetStartingState() State[TValue, TCost]
	GetValues(variable int) []TValue
	GetVariables() int
	Compare(a, b TCost) bool
	WorstCost() TCost
}

type State[TValue cmp.Ordered, TCost any] interface {
	Keyable
	TransitionTo(context Context[TValue, TCost], value TValue) State[TValue, TCost]
	CostTo(context Context[TValue, TCost], child State[TValue, TCost], value TValue) TCost
	MergeFrom(context Context[TValue, TCost], state State[TValue, TCost])
	Split(context Context[TValue, TCost]) []State[TValue, TCost]
}

type arcTo[TValue cmp.Ordered, TCost any] struct {
	cost     TCost
	value    TValue
	endpoint State[TValue, TCost]
}

// solver mechanisms: 1. full expansion, 2. max width with merge/Split, 3. by separation, 4. branch and bound

//func SolveBySeparation[TValue cmp.Ordered, TCost cmp.Ordered, TState State[TValue, TCost]](initialState TState, goal Flags) (TCost, []TValue) {
//	arcsTo := map[*TState][]arcTo[TValue, TCost]{}
//	parents := []*TState{&initialState}
//	variable := 0
//	// step 1: generate the relaxed tree
//	// step 2: while there is a relaxed node in the solution, Split that one
//	for len(parents) > 0 {
//		var children []*TState
//		for _, parentPtr := range parents {
//			parent := *parentPtr
//			isLast, values := parent.values(variable)
//			var keeper TState
//			for _, value := range values {
//				child := parent.TransitionTo(value)
//				if child == nil {
//					continue
//				}
//				if keeper != nil {
//					keeper.MergeFrom(child)
//				}
//				else {
//					keeper = *child
//				}
//				cost := parent.CostTo(child)
//				arcsTo[&parent] = append(arcsTo[&parent], arcTo[TValue, TCost]{cost, child})
//				children = append(children, child)
//				if isLast && (goal &FirstFeasible) == FirstFeasible {
//					return firstFeasibleSolution(initialState, child, arcsTo)
//				}
//			}
//		}
//		variable += 1
//		parents = children
//	}
//	if (goal & Minimize) == Minimize {
//		return findMinimalSolution(initialState, arcsTo, variable)
//	} else if (goal & Maximize) == Maximize {
//		return findMaximalSolution(initialState, arcsTo, variable)
//	}
//}

func SolveByFullExpansion[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], logger *log.Logger) (TCost, []TValue) {
	arcsTo := map[*State[TValue, TCost]][]arcTo[TValue, TCost]{}
	closed := map[int64][]Keyable{}
	parents := []State[TValue, TCost]{context.GetStartingState()}
	variables := context.GetVariables()
	for j := 0; j < variables; j++ {
		var children []State[TValue, TCost]
		for _, parent := range parents {
			for _, value := range context.GetValues(j) {
				child := parent.TransitionTo(context, value)
				if child == nil {
					continue
				}
				if !addToClosed(closed, child) {
					continue
				}
				cost := parent.CostTo(context, child, value)
				arcsTo[&parent] = append(arcsTo[&parent], arcTo[TValue, TCost]{cost, value, child})
				children = append(children, child)
			}
		}
		if logger != nil {
			logger.Printf("Layer %d, %d nodes\n", j+1, len(children))
		}
		parents = children
	}
	return findSolution[TValue, TCost](context, context.GetStartingState(), 0, arcsTo)
}

func findSolution[TValue cmp.Ordered, TCost cmp.Ordered](context Context[TValue, TCost], state State[TValue, TCost],
	depth int, arcs map[*State[TValue, TCost]][]arcTo[TValue, TCost]) (TCost, []TValue) {
	bestCost := context.WorstCost()
	var bestOldValues []TValue
	var bestValue TValue
	outputs := arcs[&state]
	for _, arc := range outputs {
		costs, values := findSolution(context, arc.endpoint, depth+1, arcs)
		if context.Compare(bestCost, costs+arc.cost) {
			bestCost = costs + arc.cost
			bestOldValues = values
			bestValue = arc.value
		}
	}
	if bestOldValues == nil && depth == context.GetVariables() {
		return TCost(0), nil
	}

	bestValues := make([]TValue, len(bestOldValues)+1)
	copy(bestValues, bestOldValues)
	bestValues[len(bestOldValues)] = bestValue
	return bestCost, bestValues
}

func addToClosed(closed map[int64][]Keyable, child Keyable) bool {
	hash := child.Hash()
	others, found := closed[hash]
	if found {
		for _, other := range others {
			if child.Equals(other) {
				return false
			}
		}
	}
	closed[hash] = append(others, child)
	return true

}
