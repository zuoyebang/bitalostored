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

package utils

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func TestLookupIP(t *testing.T) {
	LookupIP("localhost")
}

func TestLookupIPTimeout(t *testing.T) {
	start := time.Now()
	LookupIPTimeout("testtesttest", time.Millisecond)
	since := time.Since(start)
	assert.True(t, since < time.Millisecond*10)
}

func TestResolveTCPAddr(t *testing.T) {
	tcpAddr := ResolveTCPAddr("127.0.0.1:1000")
	assert.True(t, tcpAddr != nil)
	assert.True(t, tcpAddr.IP.Equal(net.ParseIP("127.0.0.1")))
	assert.True(t, tcpAddr.Port == 1000)
}

func TestResolveTCPAddrTimeout(t *testing.T) {
	start := time.Now()
	ResolveTCPAddrTimeout("testtesttest", time.Millisecond)
	since := time.Since(start)
	assert.True(t, since < time.Millisecond*10)
}

func TestReplaceUnspecifiedIP(t *testing.T) {
	Hostname = "guest"
	HostIPs, InterfaceIPs = nil, nil

	_, err1 := ReplaceUnspecifiedIP("tcp", "0.0.0.0:1000", "")
	assert.True(t, err1 != nil)
	_, err2 := ReplaceUnspecifiedIP("tcp", "1.1.1.1:0", "")
	assert.True(t, err2 != nil)

	addr3, err3 := ReplaceUnspecifiedIP("tcp", "0.0.0.0:1000", "127.0.0.1:2000")
	assert.Error(t, err3)
	assert.True(t, addr3 == "127.0.0.1:2000")

	InterfaceIPs = []string{"ip0"}
	addr4, err4 := ReplaceUnspecifiedIP("tcp", "0.0.0.0:1000", "")
	assert.Error(t, err4)
	assert.True(t, addr4 == "ip0:1000")

	HostIPs = []string{"ip1"}
	addr5, err5 := ReplaceUnspecifiedIP("tcp", "0.0.0.0:1000", "")
	assert.Error(t, err5)
	assert.True(t, addr5 == Hostname+":1000")
}
