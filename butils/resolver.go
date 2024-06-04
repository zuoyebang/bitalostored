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

package butils

import (
	"net"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/errors"
)

var (
	Hostname, _ = os.Hostname()

	HostIPs, InterfaceIPs []string
)

func init() {
	if ipAddrs := LookupIPTimeout(Hostname, 30*time.Millisecond); len(ipAddrs) != 0 {
		for _, ip := range ipAddrs {
			if ip.IsGlobalUnicast() {
				HostIPs = append(HostIPs, ip.String())
			}
		}
	}
	if ifAddrs, _ := net.InterfaceAddrs(); len(ifAddrs) != 0 {
		for i := range ifAddrs {
			var ip net.IP
			switch in := ifAddrs[i].(type) {
			case *net.IPNet:
				ip = in.IP
			case *net.IPAddr:
				ip = in.IP
			}
			if ip.IsGlobalUnicast() {
				InterfaceIPs = append(InterfaceIPs, ip.String())
			}
		}
	}
}

func LookupIPTimeout(host string, timeout time.Duration) []net.IP {
	cntx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var ch = make(chan []net.IP, 1)
	go func() {
		ips, _ := net.LookupIP(host)
		ch <- ips
	}()
	select {
	case ipAddrs := <-ch:
		return ipAddrs
	case <-cntx.Done():
		return nil
	}
}

func ReplaceUnspecifiedIP(network string, listenAddr, globalAddr string) (string, error) {
	if globalAddr == "" {
		return replaceUnspecifiedIP(network, listenAddr, true)
	} else {
		return replaceUnspecifiedIP(network, globalAddr, false)
	}
}

func replaceUnspecifiedIP(network string, address string, replace bool) (string, error) {
	switch network {
	default:
		return "", errors.New(net.UnknownNetworkError(network).Error())
	case "unix", "unixpacket":
		return address, nil
	case "tcp", "tcp4", "tcp6":
		tcpAddr, err := net.ResolveTCPAddr(network, address)
		if err != nil {
			return "", err
		}
		if tcpAddr.Port != 0 {
			if !tcpAddr.IP.IsUnspecified() {
				return address, nil
			}
			if replace {
				if len(HostIPs) != 0 {
					return net.JoinHostPort(Hostname, strconv.Itoa(tcpAddr.Port)), nil
				}
				if len(InterfaceIPs) != 0 {
					return net.JoinHostPort(InterfaceIPs[0], strconv.Itoa(tcpAddr.Port)), nil
				}
			}
		}
		return "", errors.Errorf("resolve address %s to %s", address, tcpAddr.String())
	}
}

func GetLocalIp() string {
	addrs, _ := net.InterfaceAddrs()
	var ip string
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ip = ipNet.IP.String()
				if ip != "127.0.0.1" {
					return ip
				}
			}
		}
	}
	return "127.0.0.1"
}

func GetPortByHostPort(hostport string) string {
	if _, port, err := net.SplitHostPort(hostport); err == nil {
		return port
	}
	return ""
}

func ResolveTCPAddrTimeout(addr string, timeout time.Duration) *net.TCPAddr {
	cntx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var ch = make(chan *net.TCPAddr, 1)
	go func() {
		tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
		ch <- tcpAddr
	}()
	select {
	case tcpAddr := <-ch:
		return tcpAddr
	case <-cntx.Done():
		return nil
	}
}
