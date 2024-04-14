package utils

import (
	"erigonInteract/accesslist"
	conflictgraph "erigonInteract/conflictGraph"
	"erigonInteract/mis"
	"sort"

	"github.com/ledgerwatch/erigon/core/types"
)

func GenerateCCGroups(vertexGroup [][]*conflictgraph.Vertex, txs types.Transactions, predictRWSets accesslist.RWSetList) ([]types.Transactions, []accesslist.RWSetList) {
	txsGroup := make([]types.Transactions, len(vertexGroup))
	RWSetsGroup := make([]accesslist.RWSetList, len(vertexGroup))
	for i := 0; i < len(vertexGroup); i++ {
		sort.Slice(vertexGroup[i], func(j, k int) bool {
			return vertexGroup[i][j].TxId < vertexGroup[i][k].TxId
		})

		for j := 0; j < len(vertexGroup[i]); j++ {
			txsGroup[i] = append(txsGroup[i], txs[vertexGroup[i][j].TxId])
			RWSetsGroup[i] = append(RWSetsGroup[i], predictRWSets[vertexGroup[i][j].TxId])
		}
	}
	return txsGroup, RWSetsGroup
}

// solveMISInTurn an approximation algorithm to solve MIS problem
func SolveMISInTurn(undiConfGraph *conflictgraph.UndirectedGraph) [][]uint {
	ans := make([][]uint, 0)
	for {
		MisSolution := mis.NewSolution(undiConfGraph)
		MisSolution.Solve()

		ansSlice := MisSolution.IndependentSet.ToSlice()
		ansSliceUint := make([]uint, len(ansSlice))
		for i, v := range ansSlice {
			ansSliceUint[i] = v.(uint)
		}
		ans = append(ans, ansSliceUint)
		for _, v := range undiConfGraph.Vertices {
			v.IsDeleted = false
			v.Degree = uint(len(undiConfGraph.AdjacencyMap[v.TxId]))
		}
		for _, v := range ansSlice {
			undiConfGraph.Vertices[v.(uint)].IsDeleted = true
		}
		undiConfGraph = undiConfGraph.CopyGraphWithDeletion()
		if len(undiConfGraph.Vertices) == 0 {
			break
		}
	}
	return ans
}

func GenerateUndiGraph(txs types.Transactions, predictRWSets []*accesslist.RWSet) *conflictgraph.UndirectedGraph {
	undiConfGraph := conflictgraph.NewUndirectedGraph()
	for i, tx := range txs {
		if predictRWSets[i] == nil {
			continue
		}
		undiConfGraph.AddVertex(tx.Hash(), uint(i))
	}
	for i := 0; i < txs.Len(); i++ {
		for j := i + 1; j < txs.Len(); j++ {
			if predictRWSets[i] == nil || predictRWSets[j] == nil {
				continue
			}
			if predictRWSets[i].HasConflict(*predictRWSets[j]) {
				undiConfGraph.AddEdge(uint(i), uint(j))
			}
		}
	}
	return undiConfGraph
}

func GenerateVertexGroups(txs types.Transactions, predictRWSets []*accesslist.RWSet) [][]*conflictgraph.Vertex {
	undiConfGraph := GenerateUndiGraph(txs, predictRWSets)
	groups := undiConfGraph.GetConnectedComponents()
	return groups
}

func GenerateMISGroups(txs types.Transactions, predictRWSets accesslist.RWSetList) [][]uint {
	undiGraph := GenerateUndiGraph(txs, predictRWSets)
	return SolveMISInTurn(undiGraph)
}

func GenerateDiGraph(txs types.Transactions, predictRWSets []*accesslist.RWSet) *conflictgraph.DirectedGraph {
	Graph := conflictgraph.NewDirectedGraph()
	for i, tx := range txs {
		if predictRWSets[i] == nil {
			continue
		}
		Graph.AddVertex(tx.Hash(), uint(i))
	}
	for i := 0; i < txs.Len(); i++ {
		for j := i + 1; j < txs.Len(); j++ {
			if predictRWSets[i] == nil || predictRWSets[j] == nil {
				continue
			}
			if predictRWSets[i].HasConflict(*predictRWSets[j]) {
				Graph.AddEdge(uint(i), uint(j))
			}
		}
	}
	return Graph
}

func GenerateDegreeZeroGroups(txs types.Transactions, predictRWSets []*accesslist.RWSet) [][]uint {
	graph := GenerateDiGraph(txs, predictRWSets)
	return graph.GetDegreeZero()
}
