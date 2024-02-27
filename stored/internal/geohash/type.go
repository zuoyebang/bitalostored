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

package geohash

type Range struct {
	Max float64
	Min float64
}

type Point struct {
	Longitude float64
	Latitude  float64
}

type HashBits struct {
	Bits uint64
	Step uint8
}

func (hash HashBits) IsZero() bool {
	return hash.Bits == 0 && hash.Step == 0
}

func (hash *HashBits) Clean() {
	hash.Bits = 0
	hash.Step = 0
}

type Neighbors struct {
	North     HashBits
	East      HashBits
	West      HashBits
	South     HashBits
	NorthEast HashBits
	SouthEast HashBits
	NorthWest HashBits
	SouthWest HashBits
}

type Area struct {
	Hash      HashBits
	Longitude Range
	Latitude  Range
}

type Radius struct {
	Area
	Hash HashBits
	*Neighbors
}
