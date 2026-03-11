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

package cmd_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

type Node struct {
	Cmd       *exec.Cmd
	CloseFunc func()
	Address   string
	DBPath    string
}

func ensureBitalosServerBinary() error {
	binaryPath := "../../bin/bitalostored"

	if _, err := os.Stat(binaryPath); err == nil {
		// Binary exists, return
		return nil
	}

	fmt.Println("Bitalos server binary not found, building it with make...")

	binDir := filepath.Dir(binaryPath)
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return fmt.Errorf("failed to create bin directory: %v", err)
	}

	makeCmd := exec.Command("make", "bitalos-server")
	makeCmd.Dir = "../.."

	output, err := makeCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to build bitalos-server binary with make: %v, output: %s", err, string(output))
	}

	fmt.Println("Bitalos server binary built successfully with make")
	return nil
}

func startSingleNode(configFile, address, dbPath string) (*Node, func(), error) {
	if err := ensureBitalosServerBinary(); err != nil {
		return nil, nil, fmt.Errorf("failed to ensure bitalos-server binary: %v", err)
	}

	os.RemoveAll(dbPath)

	cmd := exec.Command("../../bin/bitalostored", "--conf.file", configFile)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	closeFunc := func() {
		if cmd.Process != nil {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

			done := make(chan error, 1)
			go func() {
				done <- cmd.Wait()
			}()

			select {
			case <-time.After(2 * time.Second):
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			case <-done:
				// Process exited gracefully
			}
		}

		os.RemoveAll(dbPath)
	}

	time.Sleep(500 * time.Millisecond)

	if err := waitForServer(address, 5*time.Second); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("server did not start properly: %v", err)
	}

	node := &Node{
		Cmd:       cmd,
		CloseFunc: closeFunc,
		Address:   address,
		DBPath:    dbPath,
	}

	return node, closeFunc, nil
}

func waitForServer(address string, timeout time.Duration) error {
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("timeout waiting for server on %s", address)
		case <-ticker.C:
			conn, err := net.Dial("tcp", address)
			if err == nil {
				conn.Close()
				return nil
			}
		}
	}
}

func startCluster() ([]*Node, func(), error) {
	nodes := make([]*Node, 3)
	cleanupFuncs := make([]func(), 3)

	configFiles := []string{
		"../../conf/bitalostored-node1.toml",
		"../../conf/bitalostored-node2.toml",
		"../../conf/bitalostored-node3.toml",
	}

	dbPath := []string{
		"./testdb/node1",
		"./testdb/node2",
		"./testdb/node3",
	}

	addresses := []string{
		":6371",
		":6372",
		":6373",
	}

	for i := 0; i < 3; i++ {
		node, closeFunc, err := startSingleNode(configFiles[i], addresses[i], dbPath[i])
		if err != nil {
			for j := 0; j < i; j++ {
				if cleanupFuncs[j] != nil {
					cleanupFuncs[j]()
				}
			}
			return nil, nil, err
		}
		nodes[i] = node
		cleanupFuncs[i] = closeFunc
	}

	clusterCloseFunc := func() {
		for i := 0; i < 3; i++ {
			if cleanupFuncs[i] != nil {
				cleanupFuncs[i]()
			}
		}

		os.RemoveAll(testDBDir)
	}

	time.Sleep(10 * time.Second)

	return nodes, clusterCloseFunc, nil
}

func TestClusterReplication(t *testing.T) {
	nodes, closeCluster, err := startCluster()
	require.NoError(t, err)
	defer closeCluster()

	require.Equal(t, 3, len(nodes))

	conn1 := getTestConnWithAddr("127.0.0.1" + nodes[0].Address)
	conn2 := getTestConnWithAddr("127.0.0.1" + nodes[1].Address)
	conn3 := getTestConnWithAddr("127.0.0.1" + nodes[2].Address)

	defer conn1.Close()
	defer conn2.Close()
	defer conn3.Close()

	_, err = conn1.Do("ping")
	require.NoError(t, err)
	_, err = conn2.Do("ping")
	require.NoError(t, err)
	_, err = conn3.Do("ping")
	require.NoError(t, err)

	testKey := "replication_test_key"
	testValue := "replication_test_value"

	_, err = conn1.Do("set", testKey, testValue)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	result2, err := redis.String(conn2.Do("get", testKey))
	require.NoError(t, err)
	require.Equal(t, testValue, result2)

	result3, err := redis.String(conn3.Do("get", testKey))
	require.NoError(t, err)
	require.Equal(t, testValue, result3)

	result1, err := redis.String(conn1.Do("get", testKey))
	require.NoError(t, err)
	require.Equal(t, testValue, result1)
}
