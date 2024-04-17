package utils

import (
	"erigonInteract/accesslist"
	conflictgraph "erigonInteract/conflictGraph"
	"erigonInteract/mis"
	"sort"

	"github.com/ledgerwatch/erigon/core/types"
)

func GenerateCCGroups(vertexGroup [][]uint, txs types.Transactions, predictRWSets accesslist.RWSetList) ([]types.Transactions, []accesslist.RWSetList) {
	txsGroup := make([]types.Transactions, len(vertexGroup))
	RWSetsGroup := make([]accesslist.RWSetList, len(vertexGroup))
	for i := 0; i < len(vertexGroup); i++ {
		sort.Slice(vertexGroup[i], func(j, k int) bool {
			return vertexGroup[i][j] < vertexGroup[i][k]
		})

		for j := 0; j < len(vertexGroup[i]); j++ {
			txsGroup[i] = append(txsGroup[i], txs[vertexGroup[i][j]])
			RWSetsGroup[i] = append(RWSetsGroup[i], predictRWSets[vertexGroup[i][j]])
		}
	}
	return txsGroup, RWSetsGroup
}

// solveMISInTurn an approximation algorithm to solve MIS problem
func SolveMISInTurn(graph *conflictgraph.UndirectedGraph) [][]uint {
	ans := make([][]uint, 0)
	for {
		graphCpy := graph.Copy()
		MisSolution := mis.NewSolution(graphCpy)
		MisSolution.Solve()

		ansSlice := MisSolution.IndependentSet.ToSlice()
		ansSliceUint := make([]uint, len(ansSlice))
		for i, v := range ansSlice {
			ansSliceUint[i] = v.(uint)
			graph.RemoveVertex(v.(uint))
		}
		ans = append(ans, ansSliceUint)
		if len(graph.Vertices) == 0 {
			break
		}
	}
	return ans
}

func GenerateUndiGraph(vertexNum int, rwAccessedBy *accesslist.RwAccessedBy) *conflictgraph.UndirectedGraph {
	undiConfGraph := conflictgraph.NewUndirectedGraph()
	readBy := rwAccessedBy.ReadBy
	writeBy := rwAccessedBy.WriteBy

	// 先添加所有的点
	for i := 0; i < vertexNum; i++ {
		undiConfGraph.AddVertex(uint(i))
	}

	for addr, wAccess := range writeBy {
		for hash := range wAccess {
			wTxs := writeBy.TxIds(addr, hash)
			rTxs := readBy.TxIds(addr, hash)
			// 先添加所有写写冲突
			for i := 0; i < len(wTxs); i++ {
				for j := i + 1; j < len(wTxs); j++ {
					undiConfGraph.AddEdge(wTxs[i], wTxs[j])
				}
			}
			// 再添加所有读写冲突
			for _, rTx := range rTxs {
				for _, wTx := range wTxs {
					if rTx == wTx {
						continue
					}
					undiConfGraph.AddEdge(rTx, wTx)
				}
			}
		}
	}

	return undiConfGraph
}

func GenerateVertexIdGroups(txs types.Transactions, rwAccessedBy *accesslist.RwAccessedBy) [][]uint {
	undiConfGraph := GenerateUndiGraph(len(txs), rwAccessedBy)
	groups := undiConfGraph.GetConnectedComponents()
	return groups
}

func GenerateMISGroups(txs types.Transactions, rwAccessedBy *accesslist.RwAccessedBy) [][]uint {
	undiGraph := GenerateUndiGraph(len(txs), rwAccessedBy)
	return SolveMISInTurn(undiGraph)
}

func GenerateDiGraph(vertexNum int, rwAccessedBy *accesslist.RwAccessedBy) *conflictgraph.DirectedGraph {
	Graph := conflictgraph.NewDirectedGraph()
	readBy := rwAccessedBy.ReadBy
	writeBy := rwAccessedBy.WriteBy

	// 先添加所有的点
	for i := 0; i < vertexNum; i++ {
		Graph.AddVertex(uint(i))
	}

	for addr, wAccess := range writeBy {
		for hash := range wAccess {
			wTxs := writeBy.TxIds(addr, hash)
			rTxs := readBy.TxIds(addr, hash)
			// 先添加所有写写冲突，返回的wTxs和rTxs是有序的
			for i := 0; i < len(wTxs); i++ {
				for j := i + 1; j < len(wTxs); j++ {
					Graph.AddEdge(wTxs[i], wTxs[j])
				}
			}
			// 再添加所有读写冲突，不过有方向
			for _, rTx := range rTxs {
				for _, wTx := range wTxs {
					if rTx == wTx {
						continue
					}
					Graph.AddEdge(min(rTx, wTx), max(rTx, wTx))
				}
			}
		}

	}
	return Graph
}

func GenerateTopoGroups(txs types.Transactions, rwAccessedBy *accesslist.RwAccessedBy) [][]uint {
	graph := GenerateDiGraph(len(txs), rwAccessedBy)
	return graph.GetTopo()
}
