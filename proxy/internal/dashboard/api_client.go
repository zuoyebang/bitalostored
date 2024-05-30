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

package dashboard

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"

	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/internal/rpc"
)

type ApiClient struct {
	addr     string
	xauth    string
	proto    string
	username string
	password string
}

func NewApiClient(addr string, cfg *config.Config) *ApiClient {
	return &ApiClient{
		addr:     addr,
		proto:    cfg.DashboardProtoType,
		username: cfg.DashboardUsername,
		password: cfg.DashboardPassword,
	}
}

func (c *ApiClient) SetXAuth(name string) {
	c.xauth = rpc.NewXAuth(name)
}

func (c *ApiClient) encodeURL(format string, args ...interface{}) string {
	return rpc.EncodeURL(c.addr, format, args...)
}

type responseDashboardModel struct {
	Status int                   `json:"status"`
	Data   models.DashboardModel `json:"data"`
}

func (c *ApiClient) ModelFE(clusterName string) (*models.DashboardModel, error) {
	url := fmt.Sprintf("%s://%s/api/topom/model?forward=%s", c.proto, c.addr, clusterName)
	res, err := http.Get(url)
	if err != nil || (res != nil && res.StatusCode != 200) {
		return nil, err
	}
	model := &responseDashboardModel{}
	if err = json.NewDecoder(res.Body).Decode(model); err != nil {
		return nil, err
	}
	if model.Status != 200 {
		return nil, errors.New("get dashboard model failed")
	}
	return &model.Data, nil
}

func (c *ApiClient) Model() (*models.DashboardModel, error) {
	url := c.encodeURL("/api/topom/model")
	model := &models.DashboardModel{}
	if err := rpc.ApiGetJson(url, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (c *ApiClient) OnlineProxyFE(addr, clusterName string) error {
	client := &http.Client{}
	onlineUrl := fmt.Sprintf("%s://%s/api/topom/proxy/online/%s/%s?forward=%s", c.proto, c.addr, c.xauth, addr, clusterName)
	request, err := http.NewRequest(http.MethodPut, onlineUrl, nil)
	if err != nil {
		return err
	}
	loginUrl := fmt.Sprintf("%s://%s/login?forward=%s", c.proto, c.addr, clusterName)
	resp, err := http.PostForm(loginUrl, url.Values{"username": {c.username}, "password": {c.password}})
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("online proxy failed status code is not 200")
	}
	defer resp.Body.Close()
	request.Header.Add("Cookie", resp.Header.Get("Set-Cookie"))
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return errors.New("online proxy failed status code is not 200")
	}
	return nil
}

func (c *ApiClient) OnlineProxy(addr string) error {
	url := c.encodeURL("/api/topom/proxy/online/%s/%s", c.xauth, addr)
	return rpc.ApiPutJson(url, nil, nil)
}
