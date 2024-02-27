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

package models

const (
	SUPERADMIN AdminRole = 1
	OPADMIN    AdminRole = 2
	READADMIN  AdminRole = 3
)

type AdminRole int

type Admin struct {
	Username string    `form:"username" binding:"required" json:"username"`
	Password string    `form:"password" binding:"required" json:"password"`
	Role     AdminRole `form:"role" json:"role"`
}

func (a *Admin) Encode() []byte {
	return jsonEncode(a)
}

func (a *Admin) CheckAddRolePower() bool {
	if a.Role == SUPERADMIN {
		return true
	}
	return false
}

func (a *Admin) CheckOPRolePower() bool {
	if a.Role <= OPADMIN {
		return true
	}
	return false
}

func (a *Admin) CheckReadRolePower() bool {
	if a.Role == READADMIN {
		return true
	}
	return false
}

func (a *Admin) Snapshot() *Admin {
	return &Admin{
		Username: a.Username,
		Password: "",
		Role:     a.Role,
	}
}
