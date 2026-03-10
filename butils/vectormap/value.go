// Copyright 2019-2022 The Zuoyebang-Stored and Zuoyebang-Bitalosdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.

package vectormap

import (
	"unsafe"

	"github.com/zuoyebang/bitalostored/butils/vectormap/manual"
)

const bufferSize = int(unsafe.Sizeof(Buffer{}))

type Buffer struct {
	buf []byte
	ref refcnt
}

func (b *Buffer) acquire() {
	b.ref.acquire()
}

func (b *Buffer) release() {
	if b.ref.release() {
		b.free()
	}
}

func (b *Buffer) free() {
	n := bufferSize + cap(b.buf)
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(b))[:n:n]
	b.buf = nil
	manual.Free(buf)
}
