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
	"github.com/martini-contrib/sessions"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

var AdminKey = "user_token"
var NeedLoginErr = errors.New("need login first")

func (s *DashCore) GetLoginAdmin(session sessions.Session) (*models.Admin, error) {
	if res := session.Get(AdminKey); res != nil {
		adminByte := res.([]byte)
		admin := models.Admin{}

		if err := models.JsonDecode(&admin, adminByte); err != nil {
			return nil, NeedLoginErr
		}

		if len(admin.Username) <= 0 || len(admin.Password) <= 0 {
			return nil, errors.Errorf("invalid params username = %s, password = %s", admin.Username, admin.Password)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if storeAdmin, _ := s.storeGetAdmin(admin.Username); storeAdmin == nil {
			return nil, errors.Errorf("admin-[%s] not exists", admin.Username)
		} else {
			if storeAdmin.Username == admin.Username && storeAdmin.Password == admin.Password {
				return storeAdmin.Snapshot(), nil
			}
		}
		return nil, NeedLoginErr
	}
	return nil, NeedLoginErr
}

func (s *DashCore) AdminLogin(session sessions.Session, admin *models.Admin) (*models.Admin, error) {
	if len(admin.Username) <= 0 || len(admin.Password) <= 0 {
		return nil, errors.Errorf("invalid params username = %s, password = %s", admin.Username, admin.Password)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if storeAdmin, _ := s.storeGetAdmin(admin.Username); storeAdmin == nil {
		return nil, errors.Errorf("admin-[%s] not exists", admin.Username)
	} else {
		if storeAdmin.Username == admin.Username && storeAdmin.Password == admin.Password {
			session.Set(AdminKey, admin.Encode())
			return storeAdmin.Snapshot(), nil
		}

		return nil, errors.New("username or password error")
	}

}

func (s *DashCore) CreateAdmin(admin *models.Admin) error {
	if len(admin.Username) <= 0 {
		return errors.Errorf("invalid params username = %s, password = %s, role = %v", admin.Username, admin.Password, admin.Role)
	}
	if admin.Role != models.SUPERADMIN && admin.Role != models.OPADMIN && admin.Role != models.READADMIN {
		return errors.Errorf("invalid params username = %s, password = %s, role = %v", admin.Username, admin.Password, admin.Role)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if admin, _ := s.storeGetAdmin(admin.Username); admin != nil {
		return errors.Errorf("admin-[%s] already exists", admin.Username)
	}

	if len(admin.Password) <= 0 {
		admin.Password = admin.Username
	}
	log.Warnf("CreateAdmin data : %v", admin)
	return s.storeCreateAdmin(admin)
}

func (s *DashCore) UpdateAdmin(admin *models.Admin) error {
	if len(admin.Username) <= 0 {
		return errors.Errorf("invalid params username = %s, password = %s", admin.Username, admin.Password)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if admin, _ := s.storeGetAdmin(admin.Username); admin == nil {
		return errors.Errorf("admin-[%s] not exists", admin.Username)
	}

	return s.storeUpdateAdmin(admin)
}

func (s *DashCore) RemoveAdmin(username string) error {
	if len(username) <= 0 {
		return errors.Errorf("invalid params name = %s", username)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if storeAdmin, _ := s.storeGetAdmin(username); storeAdmin == nil {
		return errors.Errorf("admin-[%s] not exists", username)
	}

	admin := &models.Admin{
		Username: username,
	}

	return s.storeRemoveAdmin(admin)
}

func (s *DashCore) GetAdmin(name string) (*models.Admin, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storeGetAdmin(name)
}

func (s *DashCore) GetAdminList() (map[string]*models.Admin, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.storeGetAdminList()
}
