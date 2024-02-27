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

package match

import (
	"fmt"
	"strings"
)

type PrefixSuffix struct {
	Prefix, Suffix string
}

func NewPrefixSuffix(p, s string) PrefixSuffix {
	return PrefixSuffix{p, s}
}

func (self PrefixSuffix) Index(s string) (int, []int) {
	prefixIdx := strings.Index(s, self.Prefix)
	if prefixIdx == -1 {
		return -1, nil
	}

	suffixLen := len(self.Suffix)
	if suffixLen <= 0 {
		return prefixIdx, []int{len(s) - prefixIdx}
	}

	if (len(s) - prefixIdx) <= 0 {
		return -1, nil
	}

	segments := acquireSegments(len(s) - prefixIdx)
	for sub := s[prefixIdx:]; ; {
		suffixIdx := strings.LastIndex(sub, self.Suffix)
		if suffixIdx == -1 {
			break
		}

		segments = append(segments, suffixIdx+suffixLen)
		sub = sub[:suffixIdx]
	}

	if len(segments) == 0 {
		releaseSegments(segments)
		return -1, nil
	}

	reverseSegments(segments)

	return prefixIdx, segments
}

func (self PrefixSuffix) Len() int {
	return lenNo
}

func (self PrefixSuffix) Match(s string) bool {
	return strings.HasPrefix(s, self.Prefix) && strings.HasSuffix(s, self.Suffix)
}

func (self PrefixSuffix) String() string {
	return fmt.Sprintf("<prefix_suffix:[%s,%s]>", self.Prefix, self.Suffix)
}
