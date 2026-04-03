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

package dashboard

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/rpc"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	jsoniter "github.com/json-iterator/go"
)

type DashboardTopomResp struct {
	ErrMsg interface{} `json:"errmsg"`
	Status int         `json:"status"`
	Data   TopomStats  `json:"data"`
}

type DashboardTopomBaseResp struct {
	ErrMsg interface{} `json:"errmsg"`
	Status int         `json:"status"`
}

type TopomStats struct {
	Stats DashboardTopom `json:"stats"`
}
type DashboardTopom struct {
	ReadCrossCloud bool `json:"read_cross_cloud"`
	Group          struct {
		Models []*Group              `json:"models"`
		Stats  map[string]*StatsData `json:"stats"`
	} `json:"group"`
}

type StatsData struct {
	Stats map[string]string `json:"stats"`
}

type DashboardGroup struct {
	GroupModels []GroupModel `json:"models"`
}

type GroupModel struct {
	Servers []ModelServer `json:"servers"`
}

type Group struct {
	Servers []*GroupServer `json:"servers"`
	Id      uint           `json:"id"`
}

type GroupServer struct {
	Addr         string `json:"server"`
	ReplicaGroup bool   `json:"replica_group"`
	Role         string `json:"server_role"`
}

type ModelServer struct {
	Server  string `json:"server"`
	Replica bool   `json:"replica_group"`
}

type ProxySlots struct {
	ErrMsg interface{}  `json:"errmsg"`
	Status int          `json:"status"`
	Data   []*SlotsData `json:"data"`
}

type SlotsData struct {
	MasterGroupId uint              `json:"master_addr_group_id"`
	MasterAddr    string            `json:"master_addr"`
	GroupServers  map[string]string `json:"group_servers_cloudmap"`
}

var serverCookie map[string]string

func init() {
	serverCookie = make(map[string]string)
}

func SetDashboardCookie(ctx *gin.Context) error {
	if c, ok := serverCookie[config.GetConf().Domains.DashboardDomain]; ok {
		log.Info("local cookie")
		var cookies *http.Cookie
		_ = jsoniter.UnmarshalFromString(c, &cookies)
		if cookies.Expires.After(time.Now()) {
			return nil
		}
		log.Info("cookie expired")
	}
	username := config.GetConf().PaasServer.DhUsername
	paas := config.GetConf().PaasServer.DhPassword
	loginURL := config.GetConf().Domains.DashboardDomain + "/login"
	resp, err := http.PostForm(loginURL, url.Values{"username": {username}, "password": {paas}})
	if err != nil {
		log.Error("err:", err)
		return err
	}
	defer resp.Body.Close()
	cookie, err := http.ParseSetCookie(resp.Header.Get("Set-Cookie"))
	if err != nil {
		log.Errorf("decode cookie failed err:%v", err.Error())
		dErr := rpc.SendDingding(rpc.OpErrTitle, "decode cookie failed")
		if dErr != nil {
			log.Errorf("send dingding failed, err:%v", dErr)
		}
		return err
	}
	cookieStr, _ := jsoniter.MarshalToString(cookie)
	serverCookie[config.GetConf().Domains.DashboardDomain] = cookieStr
	log.Infof("set dashboard cookie domain=%s cookie-string=%s", config.GetConf().Domains.DashboardDomain, cookieStr)
	return nil
}

func GetSlots(proxyAddress string) (*ProxySlots, error) {
	res, err := http.Get(fmt.Sprintf("http://%s/proxy/slots", proxyAddress))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resp := ProxySlots{}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}
	// log.Infof("proxyslots:%+v", resp)
	return &resp, nil
}

func GetTopom(clusterName string) (*DashboardTopomResp, error) {
	res, err := http.Get(fmt.Sprintf("%s/topom?forward=%s", config.GetConf().Domains.DashboardDomain, clusterName))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resp := DashboardTopomResp{}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func GetDashboardNodeTopom(addr string) (*DashboardTopomBaseResp, error) {
	res, err := http.Get(fmt.Sprintf("http://%s/topom/model", addr))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resp := DashboardTopomBaseResp{}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func IsDashboardNodeAlive(addr string) bool {
	_, err := GetDashboardNodeTopom(addr)
	if err != nil {
		return false
	}
	return true
}

func IsDashboardAlive(clusterName string) bool {
	_, err := GetTopom(clusterName)
	if err != nil {
		return false
	}
	return true
}

func formatClusterName(clusterName string) string {
	if clusterName == "our-search-page" {
		clusterName = "ocr-search-page"
	}
	if clusterName == "our-search-inv" {
		clusterName = "ocr-search-inv"
	}
	return clusterName
}

func ReplicaNode(ctx *gin.Context, replicaServer, clusterName string, replica, groupId uint) error {
	clusterName = formatClusterName(clusterName)
	replicaUrl := fmt.Sprintf("api/topom/group/replica-groups/%s/%d/%s/%d", rpc.NewXAuth(clusterName), groupId, replicaServer, replica)
	if err := putDashboard(ctx, replicaUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func ChangeServerRole(ctx *gin.Context, clusterName, serverRole, addr string, groupId uint) error {
	clusterName = formatClusterName(clusterName)
	changeUrl := fmt.Sprintf("api/topom/group/changerole/%s/%d/%s/%s", rpc.NewXAuth(clusterName), groupId, addr, serverRole)
	if err := putDashboard(ctx, changeUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func Promote(ctx *gin.Context, clusterName, address string, groupId uint) error {
	clusterName = formatClusterName(clusterName)
	changeUrl := fmt.Sprintf("api/topom/group/promote/%s/%d/%s", rpc.NewXAuth(clusterName), groupId, address)
	if err := putDashboard(ctx, changeUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func putDashboard(ctx *gin.Context, uri, clusterName string) error {
	client := &http.Client{}
	url := fmt.Sprintf("%s/%s?forward=%s", config.GetConf().Domains.DashboardDomain, uri, clusterName)
	request, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		log.Errorf("%s failed.err:%+v.url:%s", uri, err, url)
		return err
	}
	cookieStr := serverCookie[config.GetConf().Domains.DashboardDomain]
	var dhCookie *http.Cookie
	_ = jsoniter.UnmarshalFromString(cookieStr, &dhCookie)
	request.AddCookie(dhCookie)
	log.Infof("%s putDashboard dhcookie=%s", url, serverCookie[config.GetConf().Domains.DashboardDomain])
	response, err := client.Do(request)
	if err != nil {
		log.Errorf("%s failed.err:%+v.url:%s", uri, err, url)
		return err
	}
	if response.StatusCode != 200 {
		log.Errorf("%s failed.http code is  not 200.code:%d.uri:%s", uri, response.StatusCode, url)
		return errors.New("dashboard operation failed.http code is not 200")
	}
	return nil
}

func RemoveProxyNode(ctx *gin.Context, clusterName string, hostname string, nodeAddr string) error {
	proxyToken := rpc.NewToken(
		clusterName,
		hostname,
		nodeAddr,
	)
	putUrl := fmt.Sprintf("api/topom/proxy/remove/%s/%s/1",
		rpc.NewXAuth(clusterName), // xauth
		proxyToken,
	)
	if err := putDashboard(ctx, putUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func RemoveServerNode(ctx *gin.Context, clusterName string, groupId int, nodeAddr, raftAddr string, nodeId int) error {
	clusterName = formatClusterName(clusterName)
	putUrl := fmt.Sprintf("api/topom/group/mount/%s/%d/%s/%s/%d/%d",
		rpc.NewXAuth(clusterName), // xauth
		groupId,                   // gid
		nodeAddr,                  // addr
		raftAddr,                  // raftaddr
		nodeId,                    // nodeid
		def.ModelRemoveNode,       // model
	)
	if err := putDashboard(ctx, putUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func DelServerNode(ctx *gin.Context, clusterName string, groupId uint, nodeAddr string, nodeId int) error {
	clusterName = formatClusterName(clusterName)
	putUrl := fmt.Sprintf("api/topom/group/del/%s/%d/%s/%d", rpc.NewXAuth(clusterName), groupId, nodeAddr, nodeId)
	if err := putDashboard(ctx, putUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func SyncGroup(ctx *gin.Context, clusterName string, groupId uint) error {
	clusterName = formatClusterName(clusterName)
	syncUrl := fmt.Sprintf("api/topom/group/resync/%s/%d", rpc.NewXAuth(clusterName), groupId)
	if err := putDashboard(ctx, syncUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func SyncAllGroup(ctx *gin.Context, clusterName string) error {
	clusterName = formatClusterName(clusterName)
	syncUrl := fmt.Sprintf("api/topom/group/resync-all/%s", rpc.NewXAuth(clusterName))
	if err := putDashboard(ctx, syncUrl, clusterName); err != nil {
		return err
	}
	return nil
}

func CreateNewGroup(ctx *gin.Context, clusterName string, groupId int) error {
	clusterName = formatClusterName(clusterName)
	hash := rpc.NewXAuth(clusterName)
	client := &http.Client{}
	url := fmt.Sprintf("%s/api/topom/group/create/%s/%s?forward=%s", config.GetConf().Domains.DashboardDomain, hash, strconv.Itoa(groupId), clusterName)
	request, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		log.Errorf("create dashboard group id failed.err:%+v.url:%s", err, url)
		return err
	}
	if serverCookie[config.GetConf().Domains.DashboardDomain] == "" {
		log.Errorf("create dashboard group id failed. cookie is empty. url:%s", url)
		return errors.New("fe cookie is empty")
	}
	cookieStr := serverCookie[config.GetConf().Domains.DashboardDomain]
	var dhCookie *http.Cookie
	_ = jsoniter.UnmarshalFromString(cookieStr, &dhCookie)
	request.AddCookie(dhCookie)
	response, err := client.Do(request)
	if err != nil {
		log.Errorf("create dashboard group id failed.err:%+v.url:%s", err, url)
		return err
	}
	if response.StatusCode != 200 {
		log.Errorf("create dashboard group id failed.http code is  not 200.code:%d.uri:%s", response.StatusCode, url)
		return errors.New("create dashboard group id failed.http code is not 200")
	}
	return nil
}

func AddNodesToGroup(ctx *gin.Context, machineInfos []MachineInfo, clusterName string, groupId int) error {
	clusterName = formatClusterName(clusterName)
	hash := rpc.NewXAuth(clusterName)
	log.Info("add dashboard info:", machineInfos)
	// time.Sleep(time.Second * 10)
	if SetDashboardCookie(ctx) != nil {
		log.Error("set dashboard request cookie failed.")
		return errors.New("set dashboard request cookie failed")
	}
	cookieStr := serverCookie[config.GetConf().Domains.DashboardDomain]
	var dhCookie *http.Cookie
	_ = jsoniter.UnmarshalFromString(cookieStr, &dhCookie)

	for _, mInfo := range machineInfos {
		client := &http.Client{}
		addNodeURL := fmt.Sprintf("%s/api/topom/group/add/%s/%s/%s/%s/%s?forward=%s",
			config.GetConf().Domains.DashboardDomain, hash, strconv.Itoa(groupId), mInfo.IpPort, mInfo.IDC, "master_slave_node", clusterName)
		request, err := http.NewRequest(http.MethodPut, addNodeURL, nil)
		log.Infof("add server to dashboard. request url:%s", addNodeURL)
		if err != nil {
			log.Errorf("add dashboard group failed.err:%+v.", err)
			return err
		}
		request.AddCookie(dhCookie)
		response, err := client.Do(request)
		if err != nil {
			log.Errorf("add dashboard group failed.err:%v url:%s", err, addNodeURL)
			return err
		}
		if response.StatusCode != 200 {
			log.Errorf("add dashboard group failed.http code is not 200.response:%+v", response)
			return errors.New("add dashboard group failed.http code is not 200")
		}
	}
	return nil
}

func AddNodeToGroup(ctx *gin.Context, mInfo MachineInfo, clusterName string, groupId int, nodeType string) error {
	clusterName = formatClusterName(clusterName)
	hash := rpc.NewXAuth(clusterName)
	client := &http.Client{}
	addNodeURL := fmt.Sprintf("%s/api/topom/group/add/%s/%s/%s/%s/%s?forward=%s",
		config.GetConf().Domains.DashboardDomain, hash, strconv.Itoa(groupId), mInfo.IpPort, mInfo.IDC, nodeType, clusterName)
	request, err := http.NewRequest(http.MethodPut, addNodeURL, nil)
	log.Infof("add server to dasnboard. request url:%s", addNodeURL)
	if err != nil {
		log.Errorf("add dashboard group failed.err:%+v.", err)
		return err
	}
	cookieStr := serverCookie[config.GetConf().Domains.DashboardDomain]
	var dhCookie *http.Cookie
	_ = jsoniter.UnmarshalFromString(cookieStr, &dhCookie)
	request.AddCookie(dhCookie)
	response, err := client.Do(request)
	if err != nil {
		log.Errorf("add dashboard group failed.err:%v, url:%s", err, addNodeURL)
		return err
	}
	if response.StatusCode != 200 {
		log.Errorf("add dashboard group failed.http code is not 200.response:%+v", response)
		return errors.New("add dashboard group failed.http code is not 200")
	}
	return nil
}

type MachineInfo struct {
	IpPort string
	IDC    string
}
