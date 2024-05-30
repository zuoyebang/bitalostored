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
	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

func (s *DashCore) dirtySlotsCache(sid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.slots != nil {
			s.cache.slots[sid] = nil
		}
	})
}

func (s *DashCore) dirtyGroupCache(gid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.group != nil {
			s.cache.group[gid] = nil
		}
	})
}

func (s *DashCore) dirtyMigrateCache(gid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.migrate != nil {
			s.cache.migrate[gid] = nil
		}
	})
}

func (s *DashCore) dirtyPconfigCache(name string) {
	s.cache.hooks.PushBack(func() {
		if s.cache.pconfig != nil {
			s.cache.pconfig[name] = nil
		}
	})
}

func (s *DashCore) dirtyProxyCache(token string) {
	s.cache.hooks.PushBack(func() {
		if s.cache.proxy != nil {
			s.cache.proxy[token] = nil
		}
	})
}

func (s *DashCore) dirtyCacheAll() {
	s.cache.hooks.PushBack(func() {
		s.cache.slots = nil
		s.cache.group = nil
		s.cache.proxy = nil
		s.cache.migrate = nil
	})
}

func (s *DashCore) refillCache() error {
	for i := s.cache.hooks.Len(); i != 0; i-- {
		e := s.cache.hooks.Front()
		s.cache.hooks.Remove(e).(func())()
	}

	if slots, err := s.refillCacheSlots(s.cache.slots); err != nil {
		log.ErrorErrorf(err, "store: load slots failed")
		return errors.Errorf("store: load slots failed")
	} else {
		s.cache.slots = slots
	}
	if group, err := s.refillCacheGroup(s.cache.group); err != nil {
		log.ErrorErrorf(err, "store: load group failed")
		return errors.Errorf("store: load group failed")
	} else {
		s.cache.group = group
	}
	if proxy, err := s.refillCacheProxy(s.cache.proxy); err != nil {
		log.ErrorErrorf(err, "store: load proxy failed")
		return errors.Errorf("store: load proxy failed")
	} else {
		s.cache.proxy = proxy
	}
	if migrate, err := s.refillCacheMigrate(s.cache.migrate); err != nil {
		log.ErrorErrorf(err, "store: load migrate failed")
		return errors.Errorf("store: load migrate failed")
	} else {
		s.cache.migrate = migrate
	}
	if pconfig, err := s.refillCachePconfig(s.cache.pconfig); err != nil {
		log.ErrorErrorf(err, "store: load pconfig failed")
		return errors.Errorf("store: load pconfig failed")
	} else {
		s.cache.pconfig = pconfig
	}
	return nil
}

func (s *DashCore) refillCacheSlots(slots []*models.SlotMapping) ([]*models.SlotMapping, error) {
	if slots == nil {
		return s.store.SlotMappings()
	}
	for i, _ := range slots {
		if slots[i] != nil {
			continue
		}
		m, err := s.store.LoadSlotMapping(i)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &models.SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *DashCore) refillCacheGroup(group map[int]*models.Group) (map[int]*models.Group, error) {
	if group == nil {
		return s.store.ListGroup()
	}
	for i, _ := range group {
		if group[i] != nil {
			continue
		}
		g, err := s.store.LoadGroup(i)
		if err != nil {
			return nil, err
		}
		if g != nil {
			group[i] = g
		} else {
			delete(group, i)
		}
	}
	return group, nil
}

func (s *DashCore) refillCacheMigrate(migrate map[int]*models.Migrate) (map[int]*models.Migrate, error) {
	if migrate == nil {
		return s.store.ListMigrate()
	}
	for i, _ := range migrate {
		if migrate[i] != nil {
			continue
		}
		m, err := s.store.LoadMigrate(i)
		if err != nil {
			return nil, err
		}
		if m != nil {
			migrate[i] = m
		} else {
			delete(migrate, i)
		}
	}
	return migrate, nil
}

func (s *DashCore) refillCacheProxy(proxy map[string]*models.Proxy) (map[string]*models.Proxy, error) {
	if proxy == nil {
		return s.store.ListProxy()
	}
	for t, _ := range proxy {
		if proxy[t] != nil {
			continue
		}
		p, err := s.store.LoadProxy(t)
		if err != nil {
			return nil, err
		}
		if p != nil {
			proxy[t] = p
		} else {
			delete(proxy, t)
		}
	}
	return proxy, nil
}

func (s *DashCore) refillCachePconfig(pconfig map[string]*models.Pconfig) (map[string]*models.Pconfig, error) {
	if pconfig == nil {
		return s.store.ListPconfig()
	}
	for t, _ := range pconfig {
		if pconfig[t] != nil {
			continue
		}
		p, err := s.store.LoadPconfig(t)
		if err != nil {
			return nil, err
		}
		if p != nil {
			pconfig[t] = p
		} else {
			delete(pconfig, t)
		}
	}
	return pconfig, nil
}

func (s *DashCore) storeUpdateSlotMapping(m *models.SlotMapping) error {
	if err := s.store.UpdateSlotMapping(m); err != nil {
		log.ErrorErrorf(err, "store: update slot-[%d] failed", m.Id)
		return errors.Errorf("store: update slot-[%d] failed", m.Id)
	}
	return nil
}

func (s *DashCore) storeCreateGroup(g *models.Group) error {
	log.Warnf("create group-[%d]: %s", g.Id, g.Encode())
	if err := s.store.UpdateGroup(g); err != nil {
		log.ErrorErrorf(err, "store: create group-[%d] failed", g.Id)
		return errors.Errorf("store: create group-[%d] failed", g.Id)
	}
	return nil
}

func (s *DashCore) storeUpdateGroup(g *models.Group) error {
	if err := s.store.UpdateGroup(g); err != nil {
		log.ErrorErrorf(err, "store: update group-[%d] failed", g.Id)
		return errors.Errorf("store: update group-[%d] failed", g.Id)
	}
	return nil
}

func (s *DashCore) storeRemoveGroup(g *models.Group) error {
	log.Warnf("remove group-[%d]: %s", g.Id, g.Encode())
	if err := s.store.DeleteGroup(g.Id); err != nil {
		log.ErrorErrorf(err, "store: remove group-[%d] failed", g.Id)
		return errors.Errorf("store: remove group-[%d] failed", g.Id)
	}
	return nil
}

func (s *DashCore) storeDeRaftGroup(g *models.Group) error {
	log.Warnf("deraft group-[%d]: %s", g.Id, g.Encode())
	if err := s.store.DeleteGroup(g.Id); err != nil {
		log.ErrorErrorf(err, "store: remove group-[%d] failed", g.Id)
		return errors.Errorf("store: remove group-[%d] failed", g.Id)
	}
	return nil
}

func (s *DashCore) storeUpdateMigrate(m *models.Migrate) error {
	log.Warnf("update migrate-[%04d]: %s", m.SID, m.Encode())
	if err := s.store.UpdateMigrate(m); err != nil {
		log.ErrorErrorf(err, "store: update migrate-[%04d] failed", m.SID)
		return errors.Errorf("store: update migrate-[%04d] failed", m.SID)
	}
	return nil
}

func (s *DashCore) storeCreateProxy(p *models.Proxy) error {
	log.Warnf("create proxy-[%s]: %s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(p); err != nil {
		log.ErrorErrorf(err, "store: create proxy-[%s] failed", p.Token)
		return errors.Errorf("store: create proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *DashCore) storeUpdateProxy(p *models.Proxy) error {
	log.Warnf("update proxy-[%s]: %s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(p); err != nil {
		log.ErrorErrorf(err, "store: update proxy-[%s] failed", p.Token)
		return errors.Errorf("store: update proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *DashCore) storeRemoveProxy(p *models.Proxy) error {
	log.Warnf("remove proxy-[%s]: %s", p.Token, p.Encode())
	if err := s.store.DeleteProxy(p.Token); err != nil {
		log.ErrorErrorf(err, "store: remove proxy-[%s] failed", p.Token)
		return errors.Errorf("store: remove proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *DashCore) storeCreatePconfig(pconfig *models.Pconfig) error {
	log.Warnf("create pconfig-[%s]: %s", pconfig.Name, string(pconfig.Encode()))
	if err := s.store.UpdatePconfig(pconfig); err != nil {
		log.ErrorErrorf(err, "store: create pconfig-[%s] failed", pconfig.Name)
		return errors.Errorf("store: create pconfig-[%s] failed", pconfig.Name)
	}
	return nil
}

func (s *DashCore) storeUpdatePconfig(pconfig *models.Pconfig) error {
	log.Warnf("update pconfig-[%s]: %s", pconfig.Name, pconfig.Encode())
	if err := s.store.UpdatePconfig(pconfig); err != nil {
		log.ErrorErrorf(err, "store: update pconfig-[%s] failed", pconfig.Name)
		return errors.Errorf("store: update pconfig-[%s] failed", pconfig.Name)
	}
	return nil
}

func (s *DashCore) storeRemovePconfig(pconfig *models.Pconfig) error {
	log.Warnf("remove pconfig-[%s]: %s", pconfig.Name, pconfig.Encode())

	if err := s.store.DeletePconfig(pconfig.Name); err != nil {
		log.ErrorErrorf(err, "store: remove pconfig-[%s] failed", pconfig.Name)
		return errors.Errorf("store: remove pconfig-[%s] failed", pconfig.Name)
	}
	return nil
}

func (s *DashCore) storeGetAdmin(username string) (*models.Admin, error) {
	if data, err := s.store.LoadAdmin(username); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (s *DashCore) storeCreateAdmin(admin *models.Admin) error {
	log.Warnf("create admin-[%s]: %s", admin.Username, admin.Encode())
	if err := s.store.UpdateAdmin(admin); err != nil {
		log.ErrorErrorf(err, "store: create pconfig-[%s] failed", admin.Username)
		return errors.Errorf("store: create pconfig-[%s] failed", admin.Username)
	}
	return nil
}

func (s *DashCore) storeUpdateAdmin(admin *models.Admin) error {
	log.Warnf("update admin-[%s]: %s", admin.Username, admin.Encode())
	if err := s.store.UpdateAdmin(admin); err != nil {
		log.ErrorErrorf(err, "store: update pconfig-[%s] failed", admin.Username)
		return errors.Errorf("store: update pconfig-[%s] failed", admin.Username)
	}
	return nil
}

func (s *DashCore) storeRemoveAdmin(admin *models.Admin) error {
	log.Warnf("remove admin-[%s]: %s", admin.Username, admin.Encode())

	if err := s.store.DeleteAdmin(admin.Username); err != nil {
		log.ErrorErrorf(err, "store: remove pconfig-[%s] failed", admin.Username)
		return errors.Errorf("store: remove pconfig-[%s] failed", admin.Username)
	}
	return nil
}

func (s *DashCore) storeGetAdminList() (map[string]*models.Admin, error) {
	if data, err := s.store.ListAdmin(); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}
