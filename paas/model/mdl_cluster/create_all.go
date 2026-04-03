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

package mdl_cluster

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"math/rand"
	"time"
)

const DefaultClusterName = "default"

type ClusterEnv struct {
	RegionInfo   *tbl_region.Region
	StoredId     uint
	ClusterName  string
	Department   string
	DashboardEnv *DashboardCreateEnv
	ProxyEnv     *ProxyCreateEnv
	ServerEnv    *ServerCreateEnv
}

type DashboardCreateEnv struct {
	Port         uint16
	MachineInfo  *tbl_machine.Machine
	CosFileInfo  *tbl_cosfile.CosFile
	ServiceId    uint
	ConfigPackId uint
	RegionInfo   *tbl_region.Region
	ClusterName  string
	Department   string
	ClusterInfo  *tbl_cluster.Cluster
}

type ProxyCreateEnv struct {
	ServicePort          uint16
	ClusterPort          uint16
	Ips                  []string
	CosFileInfo          *tbl_cosfile.CosFile
	ServiceId            uint
	ConfigPackId         uint
	RegionInfo           *tbl_region.Region
	ClusterName          string
	Department           string
	DashboardClusterInfo *tbl_cluster.Cluster
}

type ServerCreateEnv struct {
	Ips                  []string
	CosFileInfo          *tbl_cosfile.CosFile
	ServiceId            uint
	ConfigPackId         uint
	RegionInfo           *tbl_region.Region
	ClusterName          string
	Department           string
	DashboardClusterInfo *tbl_cluster.Cluster
	NodePerGroup         uint
}

func PrepareClusterEnv(regionId uint, clusterName string, serverCosId uint, department string, nodePerGroup uint) error {
	regionInfo, err := tbl_region.GetInfo(regionId)
	if err != nil {
		return err
	}
	serverCos, err := tbl_cosfile.GetCosFile(serverCosId)
	if err != nil {
		return err
	}

	clusterEnv := &ClusterEnv{
		RegionInfo:  regionInfo,
		ClusterName: clusterName,
		Department:  department,
		DashboardEnv: &DashboardCreateEnv{
			ServiceId:   def.SERVICE_ID_DASHBOARD,
			ClusterName: clusterName,
			Department:  department,
			RegionInfo:  regionInfo,
		},
		ProxyEnv: &ProxyCreateEnv{
			ServiceId:   def.SERVICE_ID_PROXY,
			ClusterName: clusterName,
			Department:  department,
		},
		ServerEnv: &ServerCreateEnv{
			RegionInfo:   regionInfo,
			ClusterName:  clusterName,
			ServiceId:    def.SERVICE_ID_BITALOS,
			CosFileInfo:  serverCos,
			Department:   department,
			NodePerGroup: nodePerGroup,
		},
	}

	err = clusterEnv.DashboardEnv.init(clusterName, regionInfo)
	if err != nil {
		return err
	}

	ports, err := find3Ports()
	if err != nil {
		return err
	}
	if len(ports) < 3 {
		return nil
	}

	dashboardPort := ports[0]
	proxyServicePort := ports[1]
	proxyClusterPort := ports[2]
	clusterEnv.DashboardEnv.Port = uint16(dashboardPort)
	clusterEnv.ProxyEnv.ServicePort = uint16(proxyServicePort)
	clusterEnv.ProxyEnv.ClusterPort = uint16(proxyClusterPort)

	if err := clusterEnv.DashboardEnv.create(); err != nil {
		log.Errorf("dashboard create fail. cluster:%s err:%s", clusterName, err)
		return err
	}
	log.Infof("dashboard create succ. cluster:%s", clusterName)

	clusterEnv.gocreate()

	return nil
}

func (ce *ClusterEnv) gocreate() {
	go func() {
		// Wait until the dashboard node is alive
		dhAlive := false
		addr := fmt.Sprintf("%s:%d", ce.DashboardEnv.MachineInfo.IP, ce.DashboardEnv.Port)
		for i := 0; i < 24; i++ {
			if dhAlive = dashboard.IsDashboardNodeAlive(addr); dhAlive {
				break
			}
			time.Sleep(5 * time.Second)
		}
		if !dhAlive {
			log.Errorf("dashboard not alive. create dashboard fail. cluster:%s node:%s", ce.ClusterName, addr)
			return
		}
		log.Infof("create dashboard succ. cluster:%s node:%s", ce.ClusterName, addr)

		ce.ProxyEnv.DashboardClusterInfo = ce.DashboardEnv.ClusterInfo
		ce.ProxyEnv.RegionInfo = ce.DashboardEnv.RegionInfo
		if err := ce.ProxyEnv.init(); err != nil {
			log.Errorf("init proxy info. cluster:%s err:%s", ce.ClusterName, err)
			return
		}
		log.Infof("init proxy succ. cluster:%s", ce.ClusterName)

		if err := ce.ProxyEnv.create(); err != nil {
			log.Errorf("create proxy cluster. cluster:%s err:%s", ce.ClusterName, err)
			return
		}
		log.Infof("create proxy succ. cluster:%s", ce.ClusterName)

		ce.ServerEnv.DashboardClusterInfo = ce.DashboardEnv.ClusterInfo
		if err := ce.ServerEnv.init(); err != nil {
			log.Errorf("init server info. err:%s", err)
			return
		}
		log.Infof("init server succ. cluster:%s", ce.ClusterName)

		if err := ce.ServerEnv.create(); err != nil {
			log.Errorf("create server cluster. cluster:%s err:%s", ce.ClusterName, err)
			return
		}
		log.Infof("create server succ. cluster:%s", ce.ClusterName)
	}()
}

func (dhEnv *DashboardCreateEnv) create() error {
	input := CreateDashboardModelInput{
		RegionId:     dhEnv.RegionInfo.ID,
		ServiceId:    dhEnv.ServiceId,
		CosFileId:    dhEnv.CosFileInfo.ID,
		ConfigPackId: dhEnv.ConfigPackId,
		ClusterName:  dhEnv.ClusterName,
		AssignedPort: uint(dhEnv.Port),
		MachineId:    dhEnv.MachineInfo.ID,
		Operation:    def.OPERATION_START,
		Department:   dhEnv.Department,
	}
	clusterInfo, err := CreateClusterForDashboard(&input)
	if err != nil {
		return err
	}
	dhEnv.ClusterInfo = clusterInfo
	return nil
}

func (dhEnv *DashboardCreateEnv) init(clusterName string, regionInfo *tbl_region.Region) error {
	dhPackId, err := createNewConfig(clusterName, def.SERVICE_ID_DASHBOARD)
	if err != nil {
		return err
	}
	dhEnv.ConfigPackId = dhPackId

	dhMachines, err := FindRegionIp(regionInfo, 1)
	if err != nil {
		return err
	}
	dhEnv.MachineInfo = dhMachines[0]

	cosFileInfo, err := tbl_cosfile.GetMaxVersion(def.SERVICE_ID_DASHBOARD)
	if err != nil {
		return err
	}
	dhEnv.CosFileInfo = cosFileInfo
	return nil
}

func (pce *ProxyCreateEnv) init() error {
	clusterName := pce.ClusterName
	serviceId := uint(def.SERVICE_ID_PROXY)
	packId, err := createNewConfig(clusterName, serviceId)
	if err != nil {
		return err
	}
	pce.ConfigPackId = packId

	machineInfos, err := FindRegionIp(pce.RegionInfo, 1)
	if err != nil {
		return err
	}
	pce.Ips = []string{machineInfos[0].IP}

	cosFileInfo, err := tbl_cosfile.GetMaxVersion(serviceId)
	if err != nil {
		return err
	}
	pce.CosFileInfo = cosFileInfo
	return nil
}

func (pce *ProxyCreateEnv) create() error {
	input := CreateProxyModelInput{
		RegionId:          pce.RegionInfo.ID,
		ServiceId:         pce.ServiceId,
		ConfigPackId:      pce.ConfigPackId,
		CosFileId:         pce.CosFileInfo.ID,
		ClusterName:       pce.ClusterName,
		StoredId:          pce.DashboardClusterInfo.Id,
		AssignedPort:      uint(pce.ServicePort),
		AssignedAdminPort: uint(pce.ClusterPort),
		Operation:         def.OPERATION_SUPERVISOR_START,
		Ips:               pce.Ips,
		Department:        pce.Department,
	}
	_, err := CreateProxyCluster(&input)
	if err != nil {
		return err
	}
	return nil
}

func (sce *ServerCreateEnv) init() error {
	clusterName := sce.ClusterName
	serviceId := uint(def.SERVICE_ID_BITALOS)
	packId, err := createNewConfig(clusterName, serviceId)
	if err != nil {
		return err
	}
	sce.ConfigPackId = packId

	machineList, err := FindRegionIp(sce.RegionInfo, int(sce.NodePerGroup))
	if err != nil {
		return err
	}

	sce.Ips = make([]string, 0, 3)
	for _, m := range machineList {
		sce.Ips = append(sce.Ips, m.IP)
	}
	return nil
}

func (sce *ServerCreateEnv) create() error {
	input := CreateServerModelInput{
		RegionId:     sce.RegionInfo.ID,
		ServiceId:    sce.ServiceId,
		ConfigPackId: sce.ConfigPackId,
		CosFileId:    sce.CosFileInfo.ID,
		ClusterName:  sce.ClusterName,
		StoredId:     sce.DashboardClusterInfo.Id,
		Operation:    def.OPERATION_SUPERVISOR_START,
		Ips:          sce.Ips,
		Department:   sce.Department,
	}
	_, err := CreateServerCluster(&input)
	if err != nil {
		return err
	}
	return nil
}

func FindRegionIp(regionInfo *tbl_region.Region, num int) ([]*tbl_machine.Machine, error) {
	rid := regionInfo.ID
	if regionInfo.NewId > 0 {
		rid = regionInfo.NewId
	}
	mids, err := tbl_regionmachine.GetMachinesByRegion(rid)
	if err != nil {
		return nil, err
	}
	if len(mids) <= 0 {
		return nil, errors.New("no enough machines")
	}
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	machineList := make([]*tbl_machine.Machine, 0, num)
	for i := 0; i < num*2; i++ {
		idx := r.Intn(len(mids))
		m, _ := tbl_machine.GetInfo(mids[idx])
		if m != nil {
			if m.Status == def.MACHINE_STATUS_OFFLINE {
				continue
			}
			machineList = append(machineList, m)
		}
		if len(machineList) >= num {
			return machineList, nil
		}
	}
	return nil, errors.New("no enough machines")
}

func createNewConfig(clusterName string, serviceId uint) (uint, error) {
	if clusterName == "" {
		return 0, errors.New("empty cluster")
	}

	firstConfig, err := tbl_config.GetFirstConfigPack(serviceId, DefaultClusterName)
	if err != nil {
		return 0, err
	}

	configs, err := tbl_config.GetListByPack(firstConfig.ConfigPackId, serviceId)
	if err != nil {
		return 0, err
	}
	for _, c := range configs {
		c.ConfigPackName = clusterName
	}
	return tbl_config.CreateConfigs(serviceId, configs)
}

func find3Ports() ([]uint, error) {
	portMin := 8200
	portMax := 8900
	fetchNum := 3
	_, idlePorts, err := tbl_hostport.GetIdlePortList(portMin, portMax, fetchNum)
	if err != nil {
		log.Errorf("get port fail. err:%s", err)
		return nil, err
	}
	if len(idlePorts) < 3 {
		log.Errorf("no enough ports. ports:%+v", idlePorts)
		return nil, errors.New("no enough ports")
	}
	return idlePorts, nil
}
