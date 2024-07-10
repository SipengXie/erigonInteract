package oldmis

import (
	"erigonInteract/accesslist"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// Vertex 表示图中的顶点
type OldVertex struct {
	TxId      uint        `json:"txId"`   // 顶点的 TxId
	TxHash    common.Hash `json:"txHash"` // 顶点的 TxHash
	IsDeleted bool        `json:"isDeleted"`
	Degree    uint        `json:"degree"` // 顶点的度, 在有向图里之代表入度
}

// UndirectedGraph 表示无向图
type OldUndirectedGraph struct {
	Vertices     map[uint]*OldVertex `json:"vertices"`     // 顶点集合
	AdjacencyMap map[uint][]uint     `json:"adjacencyMap"` // 邻接边表
}

// NewUndirectedGraph 创建一个新的无向图
func OldNewUndirectedGraph() *OldUndirectedGraph {
	return &OldUndirectedGraph{
		Vertices:     make(map[uint]*OldVertex),
		AdjacencyMap: make(map[uint][]uint),
	}
}

func (g *OldUndirectedGraph) CopyGraphWithDeletion() *OldUndirectedGraph {
	NewG := OldNewUndirectedGraph()
	for id, v := range g.Vertices {
		if !v.IsDeleted {
			NewG.OldAddVertex(v.TxHash, id)
		}
	}
	for id := range NewG.Vertices {
		for _, neighborId := range g.AdjacencyMap[id] {
			neighbor := g.Vertices[neighborId]
			if !neighbor.IsDeleted && !NewG.OldHasEdge(id, neighborId) {
				NewG.OldAddEdge(id, neighbor.TxId)
			}
		}
	}
	return NewG
}

// AddVertex 向图中添加一个顶点
func (g *OldUndirectedGraph) OldAddVertex(tx common.Hash, id uint) {
	_, exist := g.Vertices[id]
	if exist {
		return
	}
	v := &OldVertex{
		TxId:      id,
		TxHash:    tx,
		IsDeleted: false,
		Degree:    0,
	}
	g.Vertices[id] = v
	g.AdjacencyMap[id] = make([]uint, 0)
}

// AddEdge 向图中添加一条边
func (g *OldUndirectedGraph) OldAddEdge(source, destination uint) {
	if g.OldHasEdge(source, destination) {
		return
	}
	g.AdjacencyMap[source] = append(g.AdjacencyMap[source], destination)
	g.AdjacencyMap[destination] = append(g.AdjacencyMap[destination], source)
	g.Vertices[source].Degree++
	g.Vertices[destination].Degree++
}

func (g *OldUndirectedGraph) OldHasEdge(tx1, tx2 uint) bool {
	v1 := g.Vertices[tx1]
	v2 := g.Vertices[tx2]
	if v1.IsDeleted || v2.IsDeleted {
		return false
	}
	for _, tx := range g.AdjacencyMap[tx1] {
		if tx == tx2 {
			return true
		}
	}
	return false
}

func (g *OldUndirectedGraph) OldRemoveVertex(tx uint) {
	v := g.Vertices[tx]
	v.IsDeleted = true
	for _, neighborTx := range g.AdjacencyMap[tx] {
		neighbor := g.Vertices[neighborTx]
		if !neighbor.IsDeleted {
			neighbor.Degree--
		}
	}
}

// GetConnectedComponents 获取图中的连通分量（使用深度优先搜索）
func (g *OldUndirectedGraph) OldGetConnectedComponents() [][]*OldVertex {
	visited := make(map[*OldVertex]bool)
	components := [][]*OldVertex{}

	for _, v := range g.Vertices {
		if !visited[v] && !v.IsDeleted {
			component := []*OldVertex{}
			g.Olddfs(v, visited, &component)
			components = append(components, component)
		}
	}

	return components
}

// dfs 深度优先搜索函数
func (g *OldUndirectedGraph) Olddfs(v *OldVertex, visited map[*OldVertex]bool, component *[]*OldVertex) {
	visited[v] = true
	*component = append(*component, v)

	for _, neighborId := range g.AdjacencyMap[v.TxId] {
		neighbor := g.Vertices[neighborId]
		if !visited[neighbor] && !neighbor.IsDeleted {
			g.Olddfs(neighbor, visited, component)
		}
	}
}

func OldGenerateUndiGraph(txs types.Transactions, predictRWSets []*accesslist.RWSet) *OldUndirectedGraph {
	undiConfGraph := OldNewUndirectedGraph()
	for i, tx := range txs {
		if predictRWSets[i] == nil {
			continue
		}
		undiConfGraph.OldAddVertex(tx.Hash(), uint(i))
	}
	for i := 0; i < txs.Len(); i++ {
		for j := i + 1; j < txs.Len(); j++ {
			if predictRWSets[i] == nil || predictRWSets[j] == nil {
				continue
			}
			if predictRWSets[i].HasConflict(*predictRWSets[j]) {
				undiConfGraph.OldAddEdge(uint(i), uint(j))
			}
		}
	}
	return undiConfGraph
}

func GenerateVertexGroups(txs types.Transactions, predictRWSets []*accesslist.RWSet) [][]*OldVertex {
	undiConfGraph := OldGenerateUndiGraph(txs, predictRWSets)
	groups := undiConfGraph.OldGetConnectedComponents()
	return groups
}

func OldSolveMISInTurn(undiConfGraph *OldUndirectedGraph) [][]uint {
	ans := make([][]uint, 0)
	for {
		MisSolution := NewSolution(undiConfGraph)
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
