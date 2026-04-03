// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "log"

func PageLimit(offset, limit int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if limit > 100 {
		limit = 100
	}
	if limit <= 0 {
		limit = 10
	}
	return offset, limit
}
func Argument(d map[string]interface{}, name string) (string, bool) {
	if d[name] != nil {
		if s, ok := d[name].(string); ok {
			if s != "" {
				return s, true
			}
			log.Panicf("option %s requires an argument", name)
		} else {
			log.Panicf("option %s isn't a valid string", name)
		}
	}
	return "", false
}

type Page[T any] struct {
	data        []T
	endPageData []T
	pageSize    int
	totalPage   int
	currentPage int
}

func NewPage[T any](data []T, pageSize int) *Page[T] {
	p := &Page[T]{
		data:        data,
		pageSize:    pageSize,
		currentPage: 1,
	}

	total := len(data) / pageSize
	if len(data)%pageSize > 0 {
		total++
	}
	p.totalPage = total

	if total > 0 {
		startIndex := (total - 1) * pageSize
		p.endPageData = data[startIndex:]
	} else {
		p.endPageData = data
	}

	return p
}

func (p *Page[T]) GetPageData(page int) []T {
	if page < 1 {
		page = 1
	}
	if page > p.totalPage {
		return p.endPageData
	}

	start := (page - 1) * p.pageSize
	end := start + p.pageSize
	if end > len(p.data) {
		end = len(p.data)
	}

	p.currentPage = page
	return p.data[start:end]
}

func (p *Page[T]) NextPage() []T {
	if p.currentPage >= p.totalPage {
		return p.endPageData
	}
	p.currentPage++
	return p.GetPageData(p.currentPage)
}

func (p *Page[T]) PrevPage() []T {
	if p.currentPage <= 1 {
		return p.GetPageData(1)
	}
	p.currentPage--
	return p.GetPageData(p.currentPage)
}

func (p *Page[T]) TotalPages() int {
	return p.totalPage
}

func (p *Page[T]) CurrentPage() int {
	return p.currentPage
}
