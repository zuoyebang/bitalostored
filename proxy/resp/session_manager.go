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

package resp

import (
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
)

var globalSessionManager *SessionManager

func init() {
	globalSessionManager = NewSessionManager()
	go globalSessionManager.run()
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions sync.Map
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		mu:       sync.RWMutex{},
		sessions: sync.Map{},
	}
}

func (m *SessionManager) AddSession(s *Session) {
	m.sessions.Store(s, struct{}{})
}

func (m *SessionManager) run() {
	for {
		time.Sleep(time.Second * 2)

		closeNum := 0

		m.sessions.Range(func(key, _ any) bool {
			s, ok := key.(*Session)
			if !ok || s.activeQuit {
				m.sessions.Delete(key)
			} else {
				s.Stats.FlushOpStats(dostats.CmdServer)
				closed := s.closeSpareConn()
				if closed {
					closeNum++
				}
			}

			return true
		})

		if closeNum >= 2 {
			log.Infof("spare sessions closed num:%d", closeNum)
		}
	}
}
