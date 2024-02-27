// Copyright 2019 The Bitalostored author and other contributors.
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

package models

import "sort"

type GroupSlice []*Group

func (s GroupSlice) Len() int {
	return len(s)
}

func (s GroupSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s GroupSlice) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func SortGroup(group map[int]*Group) []*Group {
	slice := make([]*Group, 0, len(group))
	for _, g := range group {
		slice = append(slice, g)
	}
	sort.Sort(GroupSlice(slice))
	return slice
}

type ProxySlice []*Proxy

func (s ProxySlice) Len() int {
	return len(s)
}

func (s ProxySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ProxySlice) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func SortProxy(proxy map[string]*Proxy) []*Proxy {
	slice := make([]*Proxy, 0, len(proxy))
	for _, p := range proxy {
		slice = append(slice, p)
	}
	sort.Sort(ProxySlice(slice))
	return slice
}

type MigrateSlice []*Migrate

func (s MigrateSlice) Len() int {
	return len(s)
}

func (s MigrateSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s MigrateSlice) Less(i, j int) bool {
	return s[i].CreateTime < s[j].CreateTime
}

func SortMigrate(group map[int]*Migrate) []*Migrate {
	slice := make([]*Migrate, 0, len(group))
	for _, g := range group {
		slice = append(slice, g)
	}
	sort.Sort(MigrateSlice(slice))
	return slice
}
