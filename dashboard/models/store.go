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

package models

import (
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
)

func init() {
	if filepath.Separator != '/' {
		log.Panicf("bad Separator = '%c', must be '/'", filepath.Separator)
	}
}

const StoredDir = "/stored"

func ProductDir(product string) string {
	return filepath.Join(StoredDir, product)
}

func LockPath(product string) string {
	return filepath.Join(StoredDir, product, "topom")
}

func DashCoreBackupPath(product string) string {
	return filepath.Join(StoredDir, product, "topom-backup")
}

func DepartmentPath(product string) string {
	return filepath.Join(StoredDir, product, "department")
}

func SlotPath(product string, sid int) string {
	return filepath.Join(StoredDir, product, "slots", fmt.Sprintf("slot-%04d", sid))
}

func GroupDir(product string) string {
	return filepath.Join(StoredDir, product, "group")
}

func MigrateDir(product string) string {
	return filepath.Join(StoredDir, product, "migrate")
}

func ProxyDir(product string) string {
	return filepath.Join(StoredDir, product, "proxy")
}

func PconfigDir(product string) string {
	return filepath.Join(StoredDir, product, "pconfig")
}

func AdminDir() string {
	return filepath.Join(StoredDir, "admin")
}

func CloudPath(product string) string {
	return filepath.Join(StoredDir, product, "cloud")
}

func GroupPath(product string, gid int) string {
	return filepath.Join(StoredDir, product, "group", fmt.Sprintf("group-%04d", gid))
}

func MigratePath(product string, sid int) string {
	return filepath.Join(StoredDir, product, "migrate", fmt.Sprintf("migrate-%04d", sid))
}

func ProxyPath(product string, token string) string {
	return filepath.Join(StoredDir, product, "proxy", fmt.Sprintf("proxy-%s", token))
}

func PconfigPath(product string, name string) string {
	return filepath.Join(StoredDir, product, "pconfig", fmt.Sprintf("%s", name))
}

func AdminPath(name string) string {
	return filepath.Join(StoredDir, "admin", fmt.Sprintf("%s", name))
}

func LoadDashCore(client Client, product string) (*DashCore, error) {
	b, err := client.Read(LockPath(product))
	if err != nil || b == nil {
		return nil, err
	}
	t := &DashCore{}
	if err := JsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

func LoadBackUpDashCore(client Client, product string) (*DashCore, error) {
	b, err := client.Read(DashCoreBackupPath(product))
	if err != nil || b == nil {
		return nil, err
	}
	t := &DashCore{}
	if err := JsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

func LoadDepartment(client Client, product string) (*Department, error) {
	b, err := client.Read(DepartmentPath(product))
	if err != nil || b == nil {
		return nil, err
	}
	d := &Department{}
	if err := JsonDecode(d, b); err != nil {
		return nil, err
	}
	return d, nil
}

type Store struct {
	client  Client
	product string
}

func NewStore(client Client, product string) *Store {
	return &Store{client, product}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() Client {
	return s.client
}

func (s *Store) LockPath() string {
	return LockPath(s.product)
}

func (s *Store) BackUpPath() string {
	return DashCoreBackupPath(s.product)
}

func (s *Store) DepartmentPath() string {
	return DepartmentPath(s.product)
}

func (s *Store) SlotPath(sid int) string {
	return SlotPath(s.product, sid)
}

func (s *Store) GroupDir() string {
	return GroupDir(s.product)
}

func (s *Store) MigrateDir() string {
	return MigrateDir(s.product)
}

func (s *Store) ProxyDir() string {
	return ProxyDir(s.product)
}

func (s *Store) PconfigDir() string {
	return PconfigDir(s.product)
}

func (s *Store) AdminDir() string {
	return AdminDir()
}

func (s *Store) GroupPath(gid int) string {
	return GroupPath(s.product, gid)
}

func (s *Store) MigratePath(sid int) string {
	return MigratePath(s.product, sid)
}

func (s *Store) ProxyPath(token string) string {
	return ProxyPath(s.product, token)
}

func (s *Store) PconfigPath(name string) string {
	return PconfigPath(s.product, name)
}

func (s *Store) AdminPath(username string) string {
	return AdminPath(username)
}

func (s *Store) Acquire(dashCore *DashCore) error {
	return s.client.Create(s.LockPath(), dashCore.Encode())
}

func (s *Store) BackUp(dashCore *DashCore) error {
	return s.client.Create(s.BackUpPath(), dashCore.Encode())
}

func (s *Store) Release() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) ReleaseBackUp() error {
	return s.client.Delete(s.BackUpPath())
}

func (s *Store) LoadDashCore() (*DashCore, error) {
	return LoadDashCore(s.client, s.product)
}

func (s *Store) LoadBackUpDashCore() (*DashCore, error) {
	return LoadBackUpDashCore(s.client, s.product)
}

func (s *Store) LoadDepartment() (*Department, error) {
	return LoadDepartment(s.client, s.product)
}

func (s *Store) SlotMappings() ([]*SlotMapping, error) {
	slots := make([]*SlotMapping, MaxSlotNum)
	for i := range slots {
		m, err := s.LoadSlotMapping(i)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *Store) UpdateDashCore(t *DashCore) error {
	return s.client.Update(s.LockPath(), t.Encode())
}

func (s *Store) UpdateDepartment(departmentName, productName string) error {
	return s.client.Update(s.DepartmentPath(), jsonEncode(Department{Name: departmentName}))
}

func (s *Store) LoadSlotMapping(sid int) (*SlotMapping, error) {
	b, err := s.client.Read(s.SlotPath(sid))
	if err != nil || b == nil {
		return nil, err
	}
	m := &SlotMapping{}
	if err := JsonDecode(m, b); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Store) UpdateSlotMapping(m *SlotMapping) error {
	return s.client.Update(s.SlotPath(m.Id), m.Encode())
}

func (s *Store) ListGroup() (map[int]*Group, error) {
	paths, err := s.client.List(s.GroupDir())
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, path := range paths {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		g := &Group{}
		if err := JsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(gid int) (*Group, error) {
	b, err := s.client.Read(s.GroupPath(gid))
	if err != nil || b == nil {
		return nil, err
	}
	g := &Group{}
	if err := JsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) UpdateGroup(g *Group) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) DeRaftGroup(gid int, addr, token string) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) ListMigrate() (map[int]*Migrate, error) {
	paths, err := s.client.List(s.MigrateDir())
	if err != nil {
		return nil, err
	}
	migrates := make(map[int]*Migrate)
	for _, path := range paths {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		m := &Migrate{}
		if err := JsonDecode(m, b); err != nil {
			return nil, err
		}
		migrates[m.SID] = m
	}
	return migrates, nil
}

func (s *Store) LoadMigrate(sid int) (*Migrate, error) {
	b, err := s.client.Read(s.MigratePath(sid))
	if err != nil || b == nil {
		return nil, err
	}
	m := &Migrate{}
	if err := JsonDecode(m, b); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Store) UpdateMigrate(g *Migrate) error {
	return s.client.Update(s.MigratePath(g.SID), g.Encode())
}

func (s *Store) DeleteMigrate(sid int) error {
	return s.client.Delete(s.MigratePath(sid))
}

func (s *Store) ListPconfig() (map[string]*Pconfig, error) {
	paths, err := s.client.List(s.PconfigDir())
	if err != nil {
		return nil, err
	}
	pconfig := make(map[string]*Pconfig)
	for _, path := range paths {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		p := &Pconfig{}
		if err := JsonDecode(p, b); err != nil {
			return nil, err
		}
		pconfig[p.Name] = p
	}
	return pconfig, nil
}

func (s *Store) ListAdmin() (map[string]*Admin, error) {
	paths, err := s.client.List(s.AdminDir())
	if err != nil {
		return nil, err
	}
	admins := make(map[string]*Admin)
	for _, path := range paths {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		p := &Admin{}
		if err := JsonDecode(p, b); err != nil {
			return nil, err
		}
		admins[p.Username] = p
	}
	return admins, nil
}

func (s *Store) ListProxy() (map[string]*Proxy, error) {
	paths, err := s.client.List(s.ProxyDir())
	if err != nil {
		return nil, err
	}
	proxy := make(map[string]*Proxy)
	for _, path := range paths {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		p := &Proxy{}
		if err := JsonDecode(p, b); err != nil {
			return nil, err
		}
		proxy[p.Token] = p
	}
	return proxy, nil
}

func (s *Store) ListStored() ([]string, error) {
	if paths, err := s.client.List(StoredDir); err != nil {
		return nil, err
	} else {
		return paths, nil
	}
}

func (s *Store) LoadPconfig(name string) (*Pconfig, error) {
	b, err := s.client.Read(s.PconfigPath(name))
	if err != nil || b == nil {
		return nil, err
	}
	p := &Pconfig{}
	if err := JsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdatePconfig(p *Pconfig) error {
	return s.client.Update(s.PconfigPath(p.Name), p.Encode())
}

func (s *Store) DeletePconfig(name string) error {
	return s.client.Delete(s.PconfigPath(name))
}

func (s *Store) LoadAdmin(name string) (*Admin, error) {
	b, err := s.client.Read(s.AdminPath(name))
	if err != nil || b == nil {
		return nil, err
	}
	a := &Admin{}
	if err := JsonDecode(a, b); err != nil {
		return nil, err
	}
	return a, nil
}

func (s *Store) UpdateAdmin(a *Admin) error {
	return s.client.Update(s.AdminPath(a.Username), a.Encode())
}

func (s *Store) DeleteAdmin(username string) error {
	return s.client.Delete(s.AdminPath(username))
}

func (s *Store) LoadProxy(token string) (*Proxy, error) {
	b, err := s.client.Read(s.ProxyPath(token))
	if err != nil || b == nil {
		return nil, err
	}
	p := &Proxy{}
	if err := JsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateProxy(p *Proxy) error {
	return s.client.Update(s.ProxyPath(p.Token), p.Encode())
}

func (s *Store) DeleteProxy(token string) error {
	return s.client.Delete(s.ProxyPath(token))
}

func ValidateProduct(name string) error {
	if regexp.MustCompile(`^\w[\w\.\-]*$`).MatchString(name) {
		return nil
	}
	return errors.Errorf("bad product name = %s", name)
}
