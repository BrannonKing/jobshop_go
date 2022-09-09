package main

import "math/rand"

type ordered interface {
	LessThan(other ordered) bool
}

type PriorityTree[T ordered] struct {
	children [4]*PriorityTree[T]
	element  T
	parent   *PriorityTree[T]
}

func (this *PriorityTree[T]) breakConnectionToParent(replacement *PriorityTree[T]) {
	if this == nil {
		return
	}
	if this.parent == nil {
		return
	}
	for i, child := range this.parent.children {
		if child == this {
			this.parent.children[i] = replacement
			break
		}
	}
	this.parent = nil
}

func (q1 *PriorityTree[T]) Meld(q2 *PriorityTree[T]) *PriorityTree[T] {
	// we will return either q1 or q2 (and if either is null, return is obvious)
	if q1 == nil {
		q2.breakConnectionToParent(nil)
		return q2
	}
	if q2 == nil {
		q1.breakConnectionToParent(nil)
		return q1
	}
	if q1 == q2 {
		return q1
	}

	// q1 > q2, swap them so that q1 is the smallest
	if q2.element.LessThan(q1.element) {
		tmp := q1
		q1 = q2
		q2 = tmp
	}

	ret := q1
	ret.breakConnectionToParent(nil)

	for {
		// pick a random child branch
		childIdx := rand.Intn(len(q1.children))

		// at this point q2 is larger than or equal to q1
		if q1.children[childIdx] == nil {
			q2.parent = q1
			q1.children[childIdx] = q2
			break
		}

		// if the random child of q1 is less than or equal to q2 make that q1 the new head
		if !q2.element.LessThan(q1.children[childIdx].element) {
			q1 = q1.children[childIdx]
			continue
		}

		// our random child is larger than our q2 needing to be merged
		// things just got ugly: do the insert
		// we are going to disconnect the q1Child and replace it with q2
		// we are then going to continue with q2 in place of q1 and the child that needs to be merged as q2
		tmp := q1.children[childIdx]
		q1.children[childIdx] = q2
		q2.parent = q1
		q1 = q2  // q1 has to be the smaller
		q2 = tmp // tmp is larger than q2
	}

	return ret
}

func (this *PriorityTree[T]) MeldElement(element T) *PriorityTree[T] {
	tree := PriorityTree[T]{element: element}
	return this.Meld(&tree)
}

func (this *PriorityTree[T]) DeleteMin() *PriorityTree[T] {
	parent := this.parent
	newRoot := this.children[0].Meld(this.children[1])
	for i := 2; i < len(this.children); i++ {
		newRoot = newRoot.Meld(this.children[i])
	}

	if parent != nil {
		// really, this should never happen in standard A* usage
		this.breakConnectionToParent(newRoot)
		if newRoot != nil {
			newRoot.parent = parent
		}
	}
	return newRoot
}
