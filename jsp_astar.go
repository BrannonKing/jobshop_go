package main

import (
	"fmt"
	"jobshop_go/jsp"
	"sort"
)

type JobOpPair struct {
	Job       int
	Operation int
}

type Problem struct {
	JobCount       int
	JobMachines    [][]int
	JobDelays      [][]int
	JobDelayCumSum [][]int
	MachineToJobs  [][]JobOpPair
	// CumulativeDelays
	// MachineDelayCache
}

func instanceToProblem(instance *jsp.Instance) *Problem {
	problem := Problem{JobCount: instance.Jobs}
	problem.JobMachines = make([][]int, instance.Jobs)
	problem.JobDelays = make([][]int, instance.Jobs)
	problem.JobDelayCumSum = make([][]int, instance.Jobs)
	problem.MachineToJobs = make([][]JobOpPair, instance.Machines)
	for j, job := range instance.Work {
		problem.JobMachines[j] = make([]int, instance.Machines)
		problem.JobDelays[j] = make([]int, instance.Machines)
		problem.JobDelayCumSum[j] = make([]int, instance.Machines)
		for i, pair := range job {
			problem.JobMachines[j][i] = pair.Machine
			problem.JobDelays[j][i] = pair.Delay
			problem.MachineToJobs[pair.Machine] = append(problem.MachineToJobs[pair.Machine], JobOpPair{j, i})
		}
	}

	for j, job := range instance.Work {
		for k := len(job) - 1; k >= 0; k -= 1 {
			for l := k; l >= 0; l -= 1 {
				problem.JobDelayCumSum[j][l] += problem.JobDelays[j][k]
			}
		}
	}
	return &problem
}

func remainingMachineDelay(problem *Problem, machine int, mask int) int {
	delay := 0
	for _, pair := range problem.MachineToJobs[machine] {
		if (mask & (1 << pair.Job)) == 0 {
			delay += problem.JobDelays[pair.Job][pair.Operation]
		}
	}
	return delay
}

type JobScorePair struct {
	Job   int
	Score int
}

type Node struct {
	job            int
	operation      int
	machine        int
	machineJobMask int
	delay          int
	parent         *Node
	scores         []JobScorePair
}

func (node *Node) LessThan(other ordered) bool {
	node2 := other.(*Node)
	for i := 0; i < len(node.scores); i++ {
		if node.scores[i].Score != node2.scores[i].Score {
			if i > 0 {
				return node.scores[i].Score > node2.scores[i].Score
			}
			return node.scores[i].Score < node2.scores[i].Score
		}
	}
	return false // should never happen
}

func (node *Node) getJobInfoFor(job int, problem *Problem) (int, int, int, int, int) {
	prevJobDelay := 0
	jobPosition := 0
	for node != nil {
		if node.job == job {
			prevJobDelay = node.delay
			jobPosition = node.operation + 1
			break
		}
		node = node.parent
	}

	nextMachine := -1
	nextOpDelay := 0
	remainingJobDelay := 0
	if jobPosition < len(problem.MachineToJobs) { // assuming every job has each machine used once
		nextMachine = problem.JobMachines[job][jobPosition]
		nextOpDelay = problem.JobDelays[job][jobPosition]
		remainingJobDelay = problem.JobDelayCumSum[job][jobPosition]
	}
	return prevJobDelay, nextMachine, jobPosition, nextOpDelay, remainingJobDelay
}

func (node *Node) getMachineInfoFor(nextMachine int, problem *Problem) (int, int, int) {
	prevMachineDelay := 0
	mask := 0
	for node != nil {
		if node.machine == nextMachine {
			prevMachineDelay = node.delay
			mask = node.machineJobMask
			break
		}
		node = node.parent
	}
	remainder := remainingMachineDelay(problem, nextMachine, mask)
	return prevMachineDelay, remainder, mask
}

func (node *Node) findAndPushNeighbors(opens *PriorityTree[*Node], problem *Problem) (int, *PriorityTree[*Node]) {
	// the plan: if we're max(5, job count) from a table, we have to add a table to these children (or their parent?)
	// when checking scores, I would still need to build a table for the comparison.
	// a jump table is no better than a scores table; indeed, the latter would be smaller and not require doing the jump
	// if all nodes have a sorted table, the comparison function will be fast.
	// if every other node has a table, I could cut memory use by half,
	// but I need to find my node's position in the table (half the time)

	added := 0
	for j := 0; j < problem.JobCount; j++ {
		prevJobDelay, machine, opPosition, opDelay, remainingJobDelay := node.getJobInfoFor(j, problem)
		if machine < 0 {
			continue
		}
		prevMachineDelay, remainingMachDelay, machMask := node.getMachineInfoFor(machine, problem)
		g := opDelay
		if prevJobDelay > prevMachineDelay {
			g += prevJobDelay
		} else {
			g += prevMachineDelay
		}
		h := -opDelay
		if remainingJobDelay > remainingMachDelay {
			h += remainingJobDelay
		} else {
			h += remainingMachDelay
		}
		scores := make([]JobScorePair, problem.JobCount)
		if node != nil {
			copy(scores, node.scores)
		} else {
			for i, delays := range problem.JobDelayCumSum {
				scores[i] = JobScorePair{Job: i, Score: delays[0]}
			}
		}
		for i, pair := range scores {
			if pair.Job == j {
				scores[i] = JobScorePair{Job: j, Score: g + h}
				break
			}
		}
		sort.SliceStable(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })
		child := Node{scores: scores, parent: node, delay: g, job: j, operation: opPosition,
			machine: machine, machineJobMask: machMask | (1 << j)}
		opens = opens.MeldElement(&child)
		added++
	}
	return added, opens
}

func (problem *Problem) astar_search() *Node {
	var opens *PriorityTree[*Node]
	var lowest *Node
	added := 0
	count := 0
	i := 0
	for {
		added, opens = lowest.findAndPushNeighbors(opens, problem)
		count += added
		if added <= 0 {
			fmt.Printf("Done on iteration %d with %d nodes remaining.\n", i, count-i+problem.JobCount)
			return lowest
		}
		lowest = opens.element
		opens = opens.DeleteMin()
		i++
	}
}

func toyProblem1() *Problem {
	p := Problem{JobCount: 1, JobDelays: make([][]int, 1), JobDelayCumSum: make([][]int, 1),
		JobMachines: make([][]int, 1), MachineToJobs: make([][]JobOpPair, 3)}
	p.JobDelays[0] = []int{30, 40, 50}
	p.JobDelayCumSum[0] = []int{120, 90, 50}
	p.JobMachines[0] = []int{2, 0, 1}
	p.MachineToJobs[0] = []JobOpPair{{Job: 0, Operation: 1}}
	p.MachineToJobs[1] = []JobOpPair{{Job: 0, Operation: 2}}
	p.MachineToJobs[2] = []JobOpPair{{Job: 0, Operation: 0}}
	return &p
}

func toyProblem2() *Problem {
	p := Problem{JobCount: 2, JobDelays: make([][]int, 2), JobDelayCumSum: make([][]int, 2),
		JobMachines: make([][]int, 2), MachineToJobs: make([][]JobOpPair, 2)}
	a := 60
	d := 65
	p.JobDelays[0] = []int{a, 40}
	p.JobDelays[1] = []int{50, d}
	p.JobDelayCumSum[0] = []int{a + 40, 40}
	p.JobDelayCumSum[1] = []int{50 + d, d}
	p.JobMachines[0] = []int{1, 0}
	p.JobMachines[1] = []int{0, 1}
	p.MachineToJobs[0] = []JobOpPair{{Job: 0, Operation: 1}, {Job: 1, Operation: 0}}
	p.MachineToJobs[1] = []JobOpPair{{Job: 0, Operation: 0}, {Job: 1, Operation: 1}}
	return &p
}

func main() {
	instances := jsp.LoadInstances()
	instance := instances[1]
	problem := instanceToProblem(instance)
	//problem := toyProblem2()
	winner := problem.astar_search()
	fmt.Printf("Score %d", winner.scores)
	//fmt.Printf("Score %d should match %d.", winner.scores[0].Score, instance.Optimum)
}
