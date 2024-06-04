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

package proxy

import (
	"net/http"

	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
)

func serveProxy(p *Proxy, config *config.Config) {
	if p.IsClosed() {
		return
	}
	defer p.Close()

	log.Infof("proxy start service on %s", p.ProxyAddress())

	go func() {
		for {
			conn, err := p.lproxy.Accept()
			if err != nil {
				log.Errorf("proxy accept on error %s", err.Error())
				continue
			}
			newClientRESP(conn, p, config)
		}
	}()

	select {
	case <-p.exitC:
		if rc := newGlobalRequestClient(); rc != nil {
			rc.closeAllRespClients()
			rc.proxyConnWait.Wait()
		}
		log.Info("proxy shutdown")
	}
}

func serveAdmin(p *Proxy) {
	if p.IsClosed() {
		return
	}
	defer p.Close()

	log.Infof("admin start service on %s", p.AdminAddress())

	eh := make(chan error, 1)
	go func() {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(p))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(p.ladmin)
	}()

	select {
	case <-p.exitC:
		log.Info("admin shutdown")
	case err := <-eh:
		log.Errorf("admin exit on error:%s", err.Error())
	}
}
