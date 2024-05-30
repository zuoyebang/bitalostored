// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"bytes"
	"fmt"
)

type Node struct {
	Parent   *Node
	Children []*Node
	Value    interface{}
	Kind     Kind
}

func NewNode(k Kind, v interface{}, ch ...*Node) *Node {
	n := &Node{
		Kind:  k,
		Value: v,
	}
	for _, c := range ch {
		Insert(n, c)
	}
	return n
}

func (a *Node) Equal(b *Node) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.Value != b.Value {
		return false
	}
	if len(a.Children) != len(b.Children) {
		return false
	}
	for i, c := range a.Children {
		if !c.Equal(b.Children[i]) {
			return false
		}
	}
	return true
}

func (a *Node) String() string {
	var buf bytes.Buffer
	buf.WriteString(a.Kind.String())
	if a.Value != nil {
		buf.WriteString(" =")
		buf.WriteString(fmt.Sprintf("%v", a.Value))
	}
	if len(a.Children) > 0 {
		buf.WriteString(" [")
		for i, c := range a.Children {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.String())
		}
		buf.WriteString("]")
	}
	return buf.String()
}

func Insert(parent *Node, children ...*Node) {
	parent.Children = append(parent.Children, children...)
	for _, ch := range children {
		ch.Parent = parent
	}
}

type List struct {
	Not   bool
	Chars string
}

type Range struct {
	Not    bool
	Lo, Hi rune
}

type Text struct {
	Text string
}

type Kind int

const (
	KindNothing Kind = iota
	KindPattern
	KindList
	KindRange
	KindText
	KindAny
	KindSuper
	KindSingle
	KindAnyOf
)

func (k Kind) String() string {
	switch k {
	case KindNothing:
		return "Nothing"
	case KindPattern:
		return "Pattern"
	case KindList:
		return "List"
	case KindRange:
		return "Range"
	case KindText:
		return "Text"
	case KindAny:
		return "Any"
	case KindSuper:
		return "Super"
	case KindSingle:
		return "Single"
	case KindAnyOf:
		return "AnyOf"
	default:
		return ""
	}
}
