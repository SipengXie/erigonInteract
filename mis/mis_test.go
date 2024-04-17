package mis

import (
	conflictgraph "erigonInteract/conflictGraph"
	"testing"
)

func NewGraph() *conflictgraph.UndirectedGraph {
	G := conflictgraph.NewUndirectedGraph()
	for i := 0; i < 10; i++ {
		G.AddVertex(uint(i))
	}
	G.AddEdge(0, 1)
	G.AddEdge(0, 2)

	G.AddEdge(1, 2)
	G.AddEdge(1, 3)

	G.AddEdge(2, 3)

	G.AddEdge(3, 8)
	G.AddEdge(3, 4)

	G.AddEdge(4, 5)
	G.AddEdge(4, 7)

	G.AddEdge(5, 6)
	G.AddEdge(6, 7)

	G.AddEdge(8, 9)

	return G
}

func NewGraph2() *conflictgraph.UndirectedGraph {
	G := conflictgraph.NewUndirectedGraph()
	for i := 0; i < 6; i++ {
		G.AddVertex(uint(i))
	}
	G.AddEdge(0, 1)

	G.AddEdge(1, 2)
	G.AddEdge(1, 3)

	G.AddEdge(2, 4)
	G.AddEdge(2, 5)

	G.AddEdge(3, 4)
	G.AddEdge(3, 5)

	G.AddEdge(4, 5)

	return G
}

func TestSolveMIS(t *testing.T) {
	// graphBytes, _ := os.ReadFile("../graph.json")
	// var graph = &conflictgraph.UndirectedGraph{}
	// json.Unmarshal(graphBytes, graph)
	// a := uint256.NewInt(1)
	// b := a
	// a.Clear()
	// t.Log(b)

	graph := NewGraph2()
	// solution := NewSolution(graph)
	// solution.Solve()
	// vertices := solution.IndependentSet.ToSlice()
	// for _, v := range vertices {
	// 	t.Log(v.(uint))
	// }
	for {
		graphCpy := graph.Copy()

		MisSolution := NewSolution(graphCpy)
		MisSolution.Solve()
		ansSlice := MisSolution.IndependentSet.ToSlice()
		for _, v := range ansSlice {
			t.Log(v.(uint))
			graph.RemoveVertex(v.(uint))
		}
		t.Log("----")
		if len(graph.Vertices) == 0 {
			break
		}
	}
}
