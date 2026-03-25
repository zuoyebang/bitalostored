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

import (
	"bytes"
	"crypto/aes"
)

const SECRETKEY = "bitalos-v4dashboardpaasbitriezyb"

func AESEncrypt(key []byte, data []byte) ([]byte, error) {
	data = pkcs7Padding(data, aes.BlockSize)
	c, _ := aes.NewCipher(key)
	out := make([]byte, len(data))
	c.Encrypt(out, data)
	return out, nil
}

func AESDecrypt(key []byte, data []byte) ([]byte, error) {
	c, _ := aes.NewCipher(key)
	out := make([]byte, len(data))
	c.Decrypt(out, data)
	out = pkcs7Unpadding(out)
	return out, nil
}

func pkcs7Padding(src []byte, blockSize int) []byte {
	padNum := blockSize - len(src)%blockSize
	pad := bytes.Repeat([]byte{byte(padNum)}, padNum)
	return append(src, pad...)
}

func pkcs7Unpadding(src []byte) []byte {
	n := len(src)
	unPadNum := int(src[n-1])
	return src[:n-unPadNum]
}
