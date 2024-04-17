package mis

import (
	conflictgraph "erigonInteract/conflictGraph"

	set "github.com/deckarep/golang-set"
)

// 一个比较尴尬的事情是这个算法好像不一定是确定性的
// 不确定性来自于我们Set存底层是Map，可能造成不一致的Pop；
// 实际上，我们可以通过修改数据结构以及存储的数据来保证一致性
const MAX_UINT = uint(2147483647)

type VertexStack []uint

func (s *VertexStack) Push(v uint) {
	*s = append(*s, v)
}

func (s *VertexStack) Pop() uint {
	old := *s
	n := len(old)
	if n == 0 {
		return MAX_UINT
	}
	v := old[n-1]
	*s = old[0 : n-1]
	return v
}

type LinearTime struct {
	Graph *conflictgraph.UndirectedGraph

	VerticesOne, VerticesTwo, VerticesGreaterThanThree, IndependentSet set.Set // 存txID

	Stack VertexStack
}

func NewSolution(graph *conflictgraph.UndirectedGraph) *LinearTime {
	VerticesOne := set.NewSet()
	VerticesTwo := set.NewSet()
	VerticesGreaterThanThree := set.NewSet()
	IndependentSet := set.NewSet()
	Stack := make([]uint, 0)

	for k, v := range graph.Vertices {
		switch v.Degree {
		case 0:
			IndependentSet.Add(k)
		case 1:
			VerticesOne.Add(k)
		case 2:
			VerticesTwo.Add(k)
		default:
			VerticesGreaterThanThree.Add(k)
		}
	}

	return &LinearTime{
		Graph:                    graph,
		VerticesOne:              VerticesOne,
		VerticesTwo:              VerticesTwo,
		VerticesGreaterThanThree: VerticesGreaterThanThree,
		IndependentSet:           IndependentSet,
		Stack:                    Stack,
	}
}

func (s *LinearTime) Solve() {
	for s.VerticesOne.Cardinality() > 0 || s.VerticesTwo.Cardinality() > 0 || s.VerticesGreaterThanThree.Cardinality() > 0 {
		if s.VerticesOne.Cardinality() > 0 {
			s.degreeOneReduction()
		} else if s.VerticesTwo.Cardinality() > 0 {
			s.degreeTwoPathReduction()
		} else {
			s.inexactReduction()
		}
	}
	canAdd := true
	for id := s.Stack.Pop(); id != MAX_UINT; id = s.Stack.Pop() {
		if canAdd {
			s.IndependentSet.Add(id)
			canAdd = false
		} else {
			canAdd = true
		}
	}
}

// 更新Set的状态
func (s *LinearTime) updateSet(neighbor *conflictgraph.Vertex) {
	neighborId := neighbor.TxId
	switch neighbor.Degree {
	case 0:
		s.IndependentSet.Add(neighborId)
		s.VerticesOne.Remove(neighborId)
	case 1:
		s.VerticesOne.Add(neighborId)
		s.VerticesTwo.Remove(neighborId)
	case 2:
		s.VerticesTwo.Add(neighborId)
		s.VerticesGreaterThanThree.Remove(neighborId)
	}

}

func (s *LinearTime) deleteVertex(id uint) {
	v, ok := s.Graph.Vertices[id]
	if !ok {
		panic("怎么会找不到呢")
		// 在现在的实现下是走不到这里了，运行起来没错的话，这句话可以删掉
	}
	// for each neighbor w of v in G
	for neighborId := range s.Graph.AdjacencyMap[id] {
		neighbor, ok := s.Graph.Vertices[neighborId]
		if !ok {
			panic("Unexpected neighborId")
		}
		neighbor.Degree--
		delete(s.Graph.AdjacencyMap[neighborId], id)
		s.updateSet(neighbor)
	}

	// remove v from G, v1, v2, v3
	switch v.Degree {
	case 0:
		break
	case 1:
		s.VerticesOne.Remove(id)
	case 2:
		s.VerticesTwo.Remove(id)
	default:
		s.VerticesGreaterThanThree.Remove(id)
	}
	// 这下是彻底删掉了
	delete(s.Graph.Vertices, id)
	delete(s.Graph.AdjacencyMap, id)
}

func (s *LinearTime) degreeOneReduction() {
	txId := s.VerticesOne.Pop().(uint)
	s.VerticesOne.Add(txId)
	// for出来的都是存在图中的
	for neighborId := range s.Graph.AdjacencyMap[txId] {
		s.deleteVertex(neighborId)
	}
}

func (s *LinearTime) inexactReduction() {
	slices := s.VerticesGreaterThanThree.ToSlice()
	var maxDegree = s.Graph.Vertices[slices[0].(uint)].Degree
	var maxDegreeId = slices[0].(uint)

	for _, txId := range slices {
		vertex := s.Graph.Vertices[txId.(uint)]
		if vertex.Degree > maxDegree {
			maxDegree = vertex.Degree
			maxDegreeId = txId.(uint)
		}
	}
	s.deleteVertex(maxDegreeId)
}

// 为Degree 2的端点找到不在path中的邻居
func (s *LinearTime) getOutsideNeighbor(u uint) uint {
	for neighBorId := range s.Graph.AdjacencyMap[u] {
		neighbor := s.Graph.Vertices[neighBorId]
		if neighbor.Degree != 2 {
			return neighBorId
		}
	}
	return MAX_UINT
}

func (s *LinearTime) degreeTwoPathReduction() {
	uId := s.VerticesTwo.Pop().(uint)
	s.VerticesTwo.Add(uId)
	path, isCycle := s.findLongestDegreeTwoPath(uId)
	if isCycle {
		s.deleteVertex(uId)
	} else {
		path = s.pathReOrg(path)
		// v, w不属于path，是path两端连接的，不属于path的点
		// 注意 path[i] 的边表可能不止一个元素 需要for一次才能找到他的两个存活的邻居
		var v, w uint = MAX_UINT, MAX_UINT
		if len(path) == 1 {
			// 如果path只有一个元素,v和w是他的两个不同的邻居；
			// 下面else的逻辑不能完成这个判断
			for neighborId := range s.Graph.AdjacencyMap[path[0]] {
				if v == MAX_UINT {
					v = neighborId
				} else {
					w = neighborId
				}
			}
		} else {
			v = s.getOutsideNeighbor(path[0])
			w = s.getOutsideNeighbor(path[len(path)-1])
		}
		if v == MAX_UINT || w == MAX_UINT {
			panic("v or w is MAX_UINT")
		}
		if v == w {
			s.deleteVertex(v)
		} else if len(path)%2 == 1 {
			if s.Graph.HasEdge(v, w) {
				s.deleteVertex(v)
				s.deleteVertex(w)
			} else {
				// 因为所有被删除的点都在Path上，我们可以轻松的把他们拿下，而不用触发deleteVertex
				// remove all vertices except v1(path[0]) from G
				// remove all vertices of path(including path[0],  ?? really?) from V2

				s.VerticesTwo.Remove(path[0])
				for i := 1; i < len(path); i++ {
					s.Graph.RemoveVertex(path[i])
					s.VerticesTwo.Remove(path[i])
				}
				// and add edge bwteen v1(path[0]) and w （v1的度不还是2吗）
				s.Graph.AddEdge(path[0], w)
				// push vl(path[-1]),...,v2(path[1]) into S
				for i := len(path) - 1; i > 0; i-- {
					s.Stack.Push(path[i])
				}
				// 在这个情况下w,v以及v1(path[0])的度都没有改变
			}
		} else {
			// 因为所有被删除的点都在Path上，我们可以轻松的把他们拿下，而不用触发deleteVertex
			// remove all vertices of path from G and V2
			for _, point := range path {
				s.Graph.RemoveVertex(point)
				s.VerticesTwo.Remove(point)
			}
			// and add an edge, if not exists, between v and w
			if !s.Graph.HasEdge(v, w) {
				// 这个情况下v,w的度没有变
				s.Graph.AddEdge(v, w)
			} else {
				// 这个情况下v,w的度都减一了，要更新一下Set状态
				s.updateSet(s.Graph.Vertices[v])
				s.updateSet(s.Graph.Vertices[w])
			}
			// push vl,...,v1 into S
			for i := len(path) - 1; i >= 0; i-- {
				s.Stack.Push(path[i])
			}
		}
	}
}

// 从一个点出发，找到他属于的最长的degree为2的路径，注意这个路径并没有被排序，即还不知道谁是两头的点
func (s *LinearTime) findLongestDegreeTwoPath(vId uint) ([]uint, bool) {
	visited := make(map[uint]bool)
	longestPath := make([]uint, 0)
	isCycle := true

	s.dfsToFindDegreeTwoPath(vId, visited, &longestPath)

	// 判断是不是degree 2环，若是，那么每个点的两个邻居都会被访问过，即不存在没被访问过的邻居
	for _, id := range longestPath {
		for neighborId := range s.Graph.AdjacencyMap[id] {
			if !visited[neighborId] {
				isCycle = false
				break
			}
		}
		if !isCycle {
			break
		}
	}

	return longestPath, isCycle
}

// 只是一个DFS
func (s *LinearTime) dfsToFindDegreeTwoPath(vId uint, visited map[uint]bool, path *[]uint) {
	visited[vId] = true
	*path = append(*path, vId)

	for neighborId := range s.Graph.AdjacencyMap[vId] {
		neighbor := s.Graph.Vertices[neighborId]
		if !visited[neighborId] && neighbor.Degree == 2 {
			s.dfsToFindDegreeTwoPath(neighborId, visited, path)
		}
	}
}

// 找出路径的一个端点，就能找到另一个
func (s *LinearTime) pathReOrg(initPath []uint) []uint {
	inPath := make(map[uint]bool)
	visited := make(map[uint]bool)
	var st = MAX_UINT // 还没找到
	for _, v := range initPath {
		inPath[v] = true
		if st == MAX_UINT {
			// 看一下当前这个v是不是一个端点
			for neighborId := range s.Graph.AdjacencyMap[v] {
				neighbor := s.Graph.Vertices[neighborId]
				if neighbor.Degree != 2 {
					// 是端点
					st = v
					break
				}
			}
		} else {
			// 走完所有的initPath生成inPath
			continue
		}
	}

	path := make([]uint, 0)
	// 从st开始DFS
	s.dfsToReOrgPath(st, visited, inPath, &path)
	return path
}

func (s *LinearTime) dfsToReOrgPath(v uint, visited map[uint]bool, inPath map[uint]bool, path *[]uint) {
	visited[v] = true
	*path = append(*path, v)
	for neighborId := range s.Graph.AdjacencyMap[v] {
		if !visited[neighborId] && inPath[neighborId] {
			s.dfsToReOrgPath(neighborId, visited, inPath, path)
		}
	}
}
