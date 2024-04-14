package conflictgraph

// Vertex 表示图中的顶点
type Vertex struct {
	TxId uint `json:"txId"` // 顶点的 TxId
	// TxHash common.Hash `json:"txHash"` // 顶点的 TxHash
	// IsDeleted bool        `json:"isDeleted"`
	Degree uint `json:"degree"` // 顶点的度, 在有向图里之代表入度
}

// UndirectedGraph 表示无向图
type UndirectedGraph struct {
	Vertices map[uint]*Vertex `json:"vertices"` // 顶点集合
	// AdjacencyMap map[uint][]uint  `json:"adjacencyMap"` // 邻接边表
	AdjacencyMap map[uint]map[uint]struct{} `json:"adjacencyMap"` // 邻接边表
}

// NewUndirectedGraph 创建一个新的无向图
func NewUndirectedGraph() *UndirectedGraph {
	return &UndirectedGraph{
		Vertices:     make(map[uint]*Vertex),
		AdjacencyMap: make(map[uint]map[uint]struct{}),
	}
}

func (h *UndirectedGraph) Copy() *UndirectedGraph {
	NewG := NewUndirectedGraph()

	for id := range h.Vertices {
		NewG.AddVertex(id)
	}
	for id := range NewG.Vertices {
		for neighborId := range h.AdjacencyMap[id] {
			NewG.AddEdge(id, neighborId)
		}
	}
	return NewG

}

// func (g *UndirectedGraph) CopyGraphWithDeletion() *UndirectedGraph {
// 	NewG := NewUndirectedGraph()

// 	for id, v := range g.Vertices {
// 		if !v.IsDeleted {
// 			NewG.AddVertex(v.TxHash, id)
// 		}
// 	}
// 	for id := range NewG.Vertices {
// 		for _, neighborId := range g.AdjacencyMap[id] {
// 			neighbor := g.Vertices[neighborId]
// 			if !neighbor.IsDeleted && !NewG.HasEdge(id, neighborId) {
// 				NewG.AddEdge(id, neighbor.TxId)
// 			}
// 		}
// 	}
// 	return NewG
// }

// AddVertex 向图中添加一个顶点
func (g *UndirectedGraph) AddVertex(id uint) {
	_, exist := g.Vertices[id]
	if exist {
		return
	}
	v := &Vertex{
		TxId: id,
		// TxHash: tx,
		//		IsDeleted: false,
		Degree: 0,
	}
	g.Vertices[id] = v
	g.AdjacencyMap[id] = make(map[uint]struct{})
}

// AddEdge 向图中添加一条边
func (g *UndirectedGraph) AddEdge(source, destination uint) {
	if g.HasEdge(source, destination) {
		// 为了防止重复计算Degree
		return
	}
	g.AdjacencyMap[source][destination] = struct{}{}
	g.AdjacencyMap[destination][source] = struct{}{}
	g.Vertices[source].Degree++
	g.Vertices[destination].Degree++
}

func (g *UndirectedGraph) HasEdge(tx1, tx2 uint) bool {
	_, ok := g.Vertices[tx1]
	if !ok {
		return false
	}

	_, ok = g.Vertices[tx2]
	if !ok {
		return false
	}

	_, ok = g.AdjacencyMap[tx1][tx2]
	return ok
}

func (g *UndirectedGraph) RemoveVertex(tx uint) {
	for neighborTx := range g.AdjacencyMap[tx] {
		neighbor := g.Vertices[neighborTx]
		neighbor.Degree--
		delete(g.AdjacencyMap[neighborTx], tx)
	}

	delete(g.AdjacencyMap, tx)
	delete(g.Vertices, tx)
}

// GetConnectedComponents 获取图中的连通分量（使用深度优先搜索）
func (g *UndirectedGraph) GetConnectedComponents() [][]uint {
	visited := make(map[uint]bool)
	components := make([][]uint, 0)

	for key := range g.Vertices {
		if !visited[key] {
			component := make([]uint, 0)
			g.dfs(key, visited, &component)
			components = append(components, component)
		}
	}

	return components
}

// dfs 深度优先搜索函数
func (g *UndirectedGraph) dfs(key uint, visited map[uint]bool, component *[]uint) {
	visited[key] = true
	*component = append(*component, key)

	for neighborTx := range g.AdjacencyMap[key] {
		if !visited[neighborTx] {
			g.dfs(neighborTx, visited, component)
		}
	}
}
