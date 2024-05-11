// Copyright 2019-2022 The Zuoyebang-Stored and Zuoyebang-Bitalosdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.

//go:build 386 || amd64p32 || arm || armbe || mips || mipsle || mips64p32 || mips64p32le || ppc || sparc
// +build 386 amd64p32 arm armbe mips mipsle mips64p32 mips64p32le ppc sparc

package manual

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<31 - 1
)
