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

package vectormap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinTop(t *testing.T) {
	{
		ctrl := []metadata{{-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128}}
		counter := []counter{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}
		minTop, max := BuildMinTopCounter[uint8](ctrl, counter, 5)

		for i := 0; i < len(minTop); i++ {
			assert.Less(t, minTop[i].value, uint8(4), "g: %d, s: %d, v: %d", minTop[i].g, minTop[i].s, minTop[i].value)
			fmt.Printf("%v ", *minTop[i])
		}
		fmt.Println("max:", max)
	}

	{
		ctrl := []metadata{{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 10, 12, 14, 1, -128}, {3, 1, 4, -2, 5, 9, 2, 6, 5, 3, 5, 9, 2, 6, 5, 3}}
		counter := []counter{{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 10, 12, 14, 0, 0}, {3, 1, 4, 0, 5, 9, 2, 6, 5, 3, 5, 9, 2, 6, 5, 3}}
		minTop, max := BuildMinTopCounter[uint8](ctrl, counter, 5)

		for i := 0; i < len(minTop); i++ {
			assert.Less(t, minTop[i].value, uint8(4), "g: %d, s: %d, v: %d", minTop[i].g, minTop[i].s, minTop[i].value)
			fmt.Printf("%v ", *minTop[i])
		}
		fmt.Println("max:", max)
	}

	{
		ctrl := []metadata{{3, 1, 4, -2, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128}}
		arr := []counter{{3, 1, 2}}
		minTop, max := BuildMinTopCounter[uint8](ctrl, arr, 5)

		for i := 0; i < len(minTop); i++ {
			assert.Less(t, minTop[i].value, uint8(4), "g: %d, s: %d, v: %d", minTop[i].g, minTop[i].s, minTop[i].value)
			fmt.Printf("%v ", *minTop[i])
		}
		fmt.Println("max:", max)
	}
}
