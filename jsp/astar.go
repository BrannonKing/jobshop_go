package jsp

import (
	"github.com/oleiade/lane/v2"
	"golang.org/x/exp/constraints"
	"sync"
)

func expandFour[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float](context JspContext[TValue, TCost],
	state JspState[TValue, TCost], upperBound TCost) []JspState[TValue, TCost] {
	parents := []JspState[TValue, TCost]{state}
	offset := context.Offset() + len(state.long_done) + len(state.just_done)
	variables := min(context.GetVariables(), offset+1)
	for j := offset; j < variables && len(parents) > 0; j++ {
		var children []JspState[TValue, TCost]
		for _, parent := range parents {
			for _, value := range context.GetValues() {
				child := parent.TransitionTo(&context, value)
				if child == nil {
					continue
				}
				cost := child.Cost(&context)
				if cost >= upperBound {
					continue
				}
				children = append(children, *child.(*JspState[TValue, TCost]))
			}
		}
		parents = children
	}
	return parents
}

func finishViaMOR[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float](context JspContext[TValue, TCost], state JspState[TValue, TCost]) TCost {
	return state.MostOpsRemaining(&context)
}

func aStarSearch[TValue constraints.Unsigned, TCost constraints.Integer | constraints.Float](context JspContext[TValue, TCost]) (TCost, []TValue) {
	opens := lane.NewMinPriorityQueue[JspState[TValue, TCost], int]()
	start := *context.startingState.(*JspState[TValue, TCost])
	upperBound := finishViaMOR(context, start)
	nodes := expandFour(context, start, upperBound)
	for _, node := range nodes {
		opens.Push(node, (int(node.cmax)<<8)+context.GetVariables()-len(node.just_done)-len(node.long_done))
	}
	var closed sync.Map
	var lowest JspState[TValue, TCost]
	notDone := true
	var wg sync.WaitGroup
	const workers = 1
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for notDone {
				lowest, _, _ = opens.Pop()
				if lowest.cmax >= upperBound {
					continue
				}
				if lowest.IsComplete(&context) {
					notDone = false
					break
				}
				if worker == 1 { // do this in just one thread
					// try to get a better upper bound
					newUpperBound := finishViaMOR(context, lowest)
					upperBound = min(upperBound, newUpperBound)
				}
				children := expandFour(context, lowest, upperBound)
				for _, child := range children {
					key := child.ID(&context)
					_, existed := closed.LoadOrStore(key, true)
					if !existed {
						opens.Push(child, (int(child.cmax)<<8)+context.GetVariables()-len(child.just_done)-len(child.long_done))
					}
				}
			}
		}()
	}
	wg.Wait()
	return lowest.Cost(&context), lowest.Solution(&context)
}
