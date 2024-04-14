package gria

import (
	"errors"
)

var (
	ErrEmptyTree = errors.New("empty tree")
)

// AVLTree structure. Public methods are Add, Remove, Update, Search, Flatten.
type AVLTree struct {
	root *AVLNode
}

func (t *AVLTree) Add(p *TxWithIndex) {
	t.root = t.root.add(p)
}

func (t *AVLTree) Remove(p *TxWithIndex) {
	t.root = t.root.remove(p)
}

func (t *AVLTree) Search(targetGas uint64) TxWithIndex {
	ans := new(TxWithIndex)
	t.root.search(targetGas, ans)
	return *ans
}

func (t *AVLTree) Largest() (*TxWithIndex, error) {
	if t.root == nil {
		return nil, ErrEmptyTree
	}
	return t.root.findLargest().payload, nil // might get error if root is nil
}

func (t *AVLTree) Flatten() []*AVLNode {
	nodes := make([]*AVLNode, 0)
	if t.root == nil {
		return nodes
	}
	t.root.displayNodesInOrder(&nodes)
	return nodes
}

// AVLNode structure
type AVLNode struct {
	payload *TxWithIndex

	// height counts nodes (not edges)
	height int
	left   *AVLNode
	right  *AVLNode
}

// Adds a new node
func (n *AVLNode) add(p *TxWithIndex) *AVLNode {
	if n == nil {
		return &AVLNode{p, 1, nil, nil}
	}
	res := my_cmp(p, n.payload)
	if res < 0 {
		n.left = n.left.add(p)
	} else {
		n.right = n.right.add(p)
		// add中不会有res = 0的情况
	}
	return n.rebalanceTree()
}

// Removes a node
func (n *AVLNode) remove(p *TxWithIndex) *AVLNode {
	if n == nil {
		return nil
	}
	res := my_cmp(p, n.payload)
	if res < 0 {
		n.left = n.left.remove(p)
	} else if res > 0 {
		n.right = n.right.remove(p)
	} else {
		if n.left != nil && n.right != nil {
			// node to delete found with both children;
			// replace values with smallest node of the right sub-tree
			rightMinNode := n.right.findSmallest()
			n.payload = rightMinNode.payload
			// delete smallest node that we replaced
			n.right = n.right.remove(rightMinNode.payload)
		} else if n.left != nil {
			// node only has left child
			n = n.left
		} else if n.right != nil {
			// node only has right child
			n = n.right
		} else {
			// node has no children
			n = nil
			return n
		}
	}
	return n.rebalanceTree()
}

// 寻找小于等于targetGas的最大的节点
func (n *AVLNode) search(targetGas uint64, ans *TxWithIndex) {
	if n == nil {
		return
	}
	if n.payload.Tx.GetGas() == targetGas {
		*ans = *n.payload
		return
	}
	// 当前小于Target，向右走
	if n.payload.Tx.GetGas() < targetGas {
		*ans = *n.payload // 当前可能是答案
		n.right.search(targetGas, ans)
	} else {
		// 当前大于Target，向左走
		n.left.search(targetGas, ans)
	}
}

func (n *AVLNode) displayNodesInOrder(nodes *[]*AVLNode) {
	if n.left != nil {
		n.left.displayNodesInOrder(nodes)
	}
	(*nodes) = append((*nodes), n)
	if n.right != nil {
		n.right.displayNodesInOrder(nodes)
	}
}

func (n *AVLNode) getHeight() int {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *AVLNode) recalculateHeight() {
	n.height = 1 + max(n.left.getHeight(), n.right.getHeight())
}

// Checks if node is balanced and rebalance
func (n *AVLNode) rebalanceTree() *AVLNode {
	if n == nil {
		return n
	}
	n.recalculateHeight()

	// check balance factor and rotateLeft if right-heavy and rotateRight if left-heavy
	balanceFactor := n.left.getHeight() - n.right.getHeight()
	if balanceFactor == -2 {
		// check if child is left-heavy and rotateRight first
		if n.right.left.getHeight() > n.right.right.getHeight() {
			n.right = n.right.rotateRight()
		}
		return n.rotateLeft()
	} else if balanceFactor == 2 {
		// check if child is right-heavy and rotateLeft first
		if n.left.right.getHeight() > n.left.left.getHeight() {
			n.left = n.left.rotateLeft()
		}
		return n.rotateRight()
	}
	return n
}

// Rotate nodes left to balance node
func (n *AVLNode) rotateLeft() *AVLNode {
	newRoot := n.right
	n.right = newRoot.left
	newRoot.left = n
	n.recalculateHeight()
	newRoot.recalculateHeight()
	return newRoot
}

// Rotate nodes right to balance node
func (n *AVLNode) rotateRight() *AVLNode {
	newRoot := n.left
	n.left = newRoot.right
	newRoot.right = n
	n.recalculateHeight()
	newRoot.recalculateHeight()
	return newRoot
}

// Finds the smallest child (based on the key) for the current node
func (n *AVLNode) findSmallest() *AVLNode {
	if n.left != nil {
		return n.left.findSmallest()
	} else {
		return n
	}
}

// Finds the largest child (based on the key) for the current node
func (n *AVLNode) findLargest() *AVLNode {
	if n.right != nil {
		return n.right.findLargest()
	} else {
		return n
	}
}
