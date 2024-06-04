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

package dashcore

import (
	"bytes"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

func (s *DashCore) InitDefaultPconfig() {
	if pconfigList, err := s.GetPConfigList(); err == nil {
		for name, pconfig := range models.DefaultPconfigKeyList {
			if _, ok := pconfigList[name]; !ok {
				if err := s.CreatePConfig(pconfig); err != nil {
					log.Errorf("InitDefaultPconfig err : %s", err.Error())
				}
			}
		}
	} else {
		log.Warnf("InitDefaultPconfig : %v, err : %s", models.DefaultPconfigKeyList, err.Error())
	}
}

func (s *DashCore) CreatePConfig(pconfig *models.Pconfig) error {
	if len(pconfig.Name) <= 0 {
		return errors.Errorf("invalid params name = %s, content = %+v", pconfig.Name, pconfig.Content)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.pconfig[pconfig.Name] != nil {
		return errors.Errorf("pconfig-[%s] already exists", pconfig.Name)
	}

	defer s.dirtyPconfigCache(pconfig.Name)
	updateBlackWhiteList(pconfig)
	pconfig.OutOfSync = true

	log.Warnf("CreatePConfig data : %v", pconfig)
	return s.storeCreatePconfig(pconfig)
}

func updateBlackWhiteList(pconfig *models.Pconfig) {
	if len(pconfig.Content.Black) > 0 {
		pconfig.Content.BlackMap = make(map[string]bool)
		for _, name := range pconfig.Content.Black {
			pconfig.Content.BlackMap[name] = true
		}
	}
	if len(pconfig.Content.White) > 0 {
		pconfig.Content.WhiteMap = make(map[string]bool)
		for _, name := range pconfig.Content.White {
			pconfig.Content.WhiteMap[name] = true
		}
	}
	pconfig.BuildTrie()
}

func (s *DashCore) UpdatePConfig(pconfig *models.Pconfig) error {
	if len(pconfig.Name) <= 0 {
		return errors.Errorf("invalid params name = %s, content = %+v", pconfig.Name, pconfig.Content)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.pconfig[pconfig.Name] == nil {
		return errors.Errorf("pconfig-[%s] not exists", pconfig.Name)
	}

	defer s.dirtyPconfigCache(pconfig.Name)
	updateBlackWhiteList(pconfig)
	pconfig.OutOfSync = true

	log.Warnf("UpdatePConfig data : %v", pconfig)
	return s.storeUpdatePconfig(pconfig)
}

func (s *DashCore) RemovePConfig(name string) error {
	if len(name) <= 0 {
		return errors.Errorf("invalid params name = %s", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.pconfig[name] == nil {
		return errors.Errorf("pconfig-[%s] not exists", name)
	}

	defer s.dirtyPconfigCache(name)
	pconfig := &models.Pconfig{
		Name: name,
	}
	return s.storeRemovePconfig(pconfig)
}

func (s *DashCore) GetPConfig(name string) (*models.Pconfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	if ctx.pconfig[name] == nil {
		return nil, errors.Errorf("pconfig-[%s] not exists", name)
	}
	return ctx.pconfig[name], nil
}

func (s *DashCore) GetPConfigList() (map[string]*models.Pconfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	return ctx.pconfig, nil
}

func (s *DashCore) ResyncAllPconfig() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, pconfig := range ctx.pconfig {
		if err := s.resyncProxyPconfig(ctx, pconfig); err != nil {
			log.Warnf("pconfig-[%s] resync pconfig failed", pconfig.Name)
			return err
		}

		defer s.dirtyPconfigCache(pconfig.Name)

		pconfig.OutOfSync = false
		if err := s.storeUpdatePconfig(pconfig); err != nil {
			return err
		}
	}
	return nil
}

func (s *DashCore) ResyncOnePconfig(name string) error {
	s.mu.Lock()
	a := bytes.Buffer{}
	a.String()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	pconfig := ctx.pconfig[name]

	if pconfig == nil {
		return errors.Errorf("pconfig-[%s] not exists", name)
	}

	if err := s.resyncProxyPconfig(ctx, pconfig); err != nil {
		log.Warnf("pconfig-[%s] resync pconfig failed", pconfig.Name)
		return err
	}

	defer s.dirtyPconfigCache(pconfig.Name)

	pconfig.OutOfSync = false

	if err := s.storeUpdatePconfig(pconfig); err != nil {
		return err
	}

	return nil
}

func (s *DashCore) resyncProxyPconfig(ctx *context, pconfig *models.Pconfig) error {
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).FillPconfigs([]*models.Pconfig{pconfig})
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync pconfig failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync pconfig failed", t)
			}
		}
	}
	return nil
}
