// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package srv_user

import (
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strings"
)

type User struct {
	Up string `json:"up"`
	jwt.RegisteredClaims
}

var _ servicer.Servicer = new(User)

func (input *User) CheckParams(ctx *gin.Context) error {
	if input.Up == "" {
		return errors.New("invalid auth")
	}
	return nil
}

func (input *User) BuildOutput(ctx *gin.Context) (interface{}, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(input.Up)
	if err != nil {
		return "", errors.New("login failed.Please check your username and password")
	}
	strUp := string(decodeBytes)
	sp := strings.Split(strUp, "|")
	if len(sp) != 2 {
		return "", errors.New("login failed.Please check your username and password")
	}
	uname := sp[0]
	password := sp[1]
	ud, err := tbl_dashboard.GetUserAccount(uname)
	if err != nil {
		return "", err
	}
	if ud == nil {
		return "", errors.New("login failed.Please check your username and password")
	}
	if len(ud.Auth) > 0 {
		password = math2.GetMd5(password + ud.Auth)
	}
	if ud.Username != uname || ud.Password != password {
		return "", errors.New("login failed.Please check your username and password")
	}
	var token string
	switch ud.Role {
	case def.AccountRoleNormal:
		input.RegisteredClaims = jwt.RegisteredClaims{
			Issuer: piss,
		}
		token, err = getToken(input)
		if err != nil {
			return nil, err
		}
		cookieDomain := ctx.Request.Host
		ctx.SetCookie(def.CookiePToken, token, 60*60*24*30, "/", cookieDomain, false, true)
	}
	return token, nil
}

var (
	secret = []byte("bitalos-paas")
	piss   = "bitalospaas"
)

func getToken(claims *User) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(secret)
	if err != nil {
		return "", errors.New("failed to get token")
	}
	return signedToken, nil
}

func VerifyPToken(strToken string) bool {
	token, err := jwt.ParseWithClaims(strToken, &User{}, func(token *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		log.Warnf("verify token failed.token:%s.err:%+v", strToken, err)
		return false
	}
	_, ok := token.Claims.(*User)
	if !ok {
		log.Warn("illegal token.")
		return false
	}
	iss, err := token.Claims.GetIssuer()
	if err != nil || iss != piss {
		log.Warn("invalid ptoken.")
		return false
	}
	return true
}
