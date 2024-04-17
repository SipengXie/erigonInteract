package gria

import (
	"container/heap"
	"fmt"

	"github.com/ledgerwatch/erigon/core/types"
)

type TxWithIndex struct {
	Tx  types.Transaction
	Tid int
}

// 正常比大小，Gas一样比Tid
func my_cmp(a, b *TxWithIndex) int {
	if a.Tx.GetGas() < b.Tx.GetGas() {
		return -1
	}
	if a.Tx.GetGas() > b.Tx.GetGas() {
		return 1
	}
	if a.Tid < b.Tid {
		return -1
	}
	if a.Tid > b.Tid {
		return 1
	}
	return 0
}

// Tid 从小到大 排序
type SortingTxs []TxWithIndex

func (s SortingTxs) Len() int {
	return len(s)
}

func (s SortingTxs) Less(i, j int) bool {
	return s[i].Tid < s[j].Tid
}

func (s SortingTxs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type sumData struct {
	sum uint64
	id  int
}

// 做一个小根堆，用来做贪心的fallback
type sumArray []sumData

func (s sumArray) Len() int {
	return len(s)
}

func (s sumArray) Less(i, j int) bool {
	return s[i].sum < s[j].sum
}

func (s sumArray) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *sumArray) Push(x interface{}) {
	*s = append(*s, x.(sumData))
}

func (s *sumArray) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[:n-1]
	return x
}

func GreedyGrouping(txs types.Transactions, k int) []SortingTxs {
	avlTree := &AVLTree{}
	var sum uint64
	for i, tx := range txs {
		avlTree.Add(&TxWithIndex{Tx: tx, Tid: i})
		sum += tx.GetGas()
	}
	average := sum / uint64(k)

	// k 组结果
	result := make([]SortingTxs, k)
	gasSums := make(sumArray, k)

	for i := 0; i < k; i++ {
		// 先把最大的放进去
		largest, err := avlTree.Largest()
		if err != nil {
			fmt.Println(err)
			break
		}
		cur_group := SortingTxs{TxWithIndex{Tx: largest.Tx, Tid: largest.Tid}}
		cur_sum := largest.Tx.GetGas()

		avlTree.Remove(largest)

		for {
			// 寻找一个最大的tx，使得加入后cur_sum不超过average
			find := avlTree.Search(average - cur_sum)
			if find.Tx == nil {
				break
			}

			cur_sum += find.Tx.GetGas()
			cur_group = append(cur_group, find)
			avlTree.Remove(&find)
		}
		result[i] = cur_group
		gasSums[i] = sumData{cur_sum, i}
	}

	// 可能余下了一些没有分配完的Txs
	heap.Init(&gasSums)
	nodes := avlTree.Flatten()
	// nodes是按Gas从小到大排序的，我们要反过来遍历
	for i := len(nodes) - 1; i >= 0; i-- {
		tx := nodes[i].payload
		// 找一个最小的GasSum组丢进去,还需要获得gasSums对应的groupId
		min := heap.Pop(&gasSums).(sumData)
		result[min.id] = append(result[min.id], *tx)
		min.sum += tx.Tx.GetGas()
		heap.Push(&gasSums, min)
	}
	// fmt.Println("Average:", average)
	// fmt.Println(gasSums)
	// for i, group := range result {
	// 	sort.Sort(group)
	// 	fmt.Println("Group", i, ":", group)
	// }

	return result
}
