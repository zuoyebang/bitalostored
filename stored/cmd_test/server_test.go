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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/raft"
	"github.com/zuoyebang/bitalostored/stored/server"
)

const (
	testDBDir  = "./testdb"
	testDBConf = "../../conf/bitalostored.toml"
	testDBPort = ":6379"
)

func testRandBytes(len int) []byte {
	val := make([]byte, len)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		val[i] = byte(b)
	}
	return val
}

func init() {
	cacheEable := false
	if cacheEable {
		readNum = 2
	}
	skipTx = true
}

var skipTx bool
var readNum int = 1

func getTestConnWithAddr(addr string) redis.Conn {
	conn, err := redis.Dial("tcp", addr,
		redis.DialPassword(""),
		redis.DialDatabase(0),
		redis.DialConnectTimeout(60*time.Second),
		redis.DialReadTimeout(60*time.Second),
		redis.DialWriteTimeout(60*time.Second),
	)
	if err != nil {
		panic(err)
	}
	return conn
}

func startServer(configFile, serverAddr string) (func(), error) {
	os.RemoveAll(testDBDir)

	if err := config.GlobalConfig.LoadFromFile(configFile, serverAddr, 0, 1); err != nil {
		return nil, err
	}

	log.NewLogger(&log.Options{
		IsDebug:      config.GlobalConfig.Log.IsDebug,
		RotationTime: config.GlobalConfig.Log.RotationTime,
		LogPath:      config.GetBitalosLogPath(),
	})

	s, err := server.NewServer()
	if err != nil {
		return nil, err
	}

	if err = raft.InitRaftInstance(config.GlobalConfig, s); err != nil {
		log.Warnf("Failed to initialize raft: %v", err)
	}

	go s.ListenAndServe()

	time.Sleep(100 * time.Millisecond)

	closeFunc := func() {
		s.Close()

		time.Sleep(100 * time.Millisecond)

		os.RemoveAll(testDBDir)
	}

	return closeFunc, nil
}

// TestServerStartup uses the generic startServer method to test server startup
func TestServerStartup(t *testing.T) {
	// Start the server using the generic method
	closeServer, err := startServer(testDBConf, testDBPort)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	t.Log("Server started successfully")

	// Close the server using the returned callback
	closeServer()

	t.Log("Server shutdown completed successfully")
}

func TestServerCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	res, err := redis.String(c.Do("info"))
	require.NoError(t, err)
	t.Logf("%+v", res)
}
