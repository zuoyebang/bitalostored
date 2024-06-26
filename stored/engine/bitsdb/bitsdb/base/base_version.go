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

package base

const (
	KeyKindDefault uint8 = iota
	KeyKindFieldCompress
)

const keyVersionDecoder uint64 = 1<<56 - 1

func EncodeKeyVersion(keyVersion uint64, kind uint8) uint64 {
	return (uint64(kind) << 56) | keyVersion
}

func DecodeKeyVersionKind(vk uint64) uint8 {
	return uint8(vk >> 56)
}

func DecodeKeyVersion(vk uint64) (uint64, uint8) {
	return vk & keyVersionDecoder, uint8(vk >> 56)
}
