import { Observable, from } from 'rxjs'
import { AxiosPromise, AxiosResponse } from 'axios'
import { ajaxGetJSON, ajaxPost } from 'rxjs/internal-compatibility'
import { CommonResponse } from '../interfaces/commons'
import {
  AddServerToGroupParams,
  HomeDataResponse,
  MigrateRange,
  MigrateSome,
  PcConfigItem,
  ProxyModels,
  Enable
} from '../interfaces/home'
import { getBitalosproxyName, getBitalosproxyXName, getXAuth } from '../commons'
import { ajax, ajaxGet, ajaxPut } from './index'

// main
export const getStoreList$ = (): Observable<AxiosResponse<any>> => from(ajax('/clusters'))
export const login$ = ({ username, password }) => ajaxPost('/login', { username, password })
export const getInfo = () => ajax('/info')
export const getHomepageData = () => ajax('/homepage')
export const handleException = (params) => ajax.post('/handleexception', { ...params })

// home
export const getHomeData$ = () => ajaxGetJSON<CommonResponse<HomeDataResponse>>('/topom?forward=' + getBitalosproxyName())
// export const getHomeConfig$ = () => ajaxGetJSON('api/topom/pconfig/list/5f2606499efec355243d9a0aa9d95ac7?forward=' + id)
export const getHomeStats$ = () => ajaxGetJSON(`/api/topom/stats/${getBitalosproxyXName()}?forward=` + getBitalosproxyName())
export const getCloudType = (): AxiosPromise<any> => ajax('/constants');
export const bindDepartment = (value) => ajaxPut(`/api/topom/department/${getXAuth(getBitalosproxyName())}/${value}?forward=${getBitalosproxyName()}`)

// proxy
export const addProxy = (ip: string, type: string) => ajaxPut(`/api/topom/proxy/create/${getBitalosproxyXName()}/${ip}/${type}?forward=${getBitalosproxyName()}`)
export const deleteProxy = (proxy: ProxyModels) => ajaxPut(`/api/topom/proxy/remove/${getBitalosproxyXName()}/${proxy.token}/0?forward=${getBitalosproxyName()}`)
export const syncProxy = (proxy: ProxyModels) => ajaxPut(`/api/topom/proxy/reinit/${getBitalosproxyXName()}/${proxy.token}?forward=${getBitalosproxyName()}`)
export const forceDel = (proxy: ProxyModels) => ajaxPut(`/api/topom/proxy/remove/${getBitalosproxyXName()}/${proxy.token}/1?forward=${getBitalosproxyName()}`)
export const hotkeys = () => ajaxGet<CommonResponse<any>>(`/api/topom/proxy/stat/${getXAuth(getBitalosproxyName())}/hotkeys?forward=${getBitalosproxyName()}`)
export const slowkeys = () => ajaxGet<CommonResponse<any>>(`/api/topom/proxy/stat/${getXAuth(getBitalosproxyName())}/slowkeys?forward=${getBitalosproxyName()}`)
export const crossCloud = (value: number) => ajaxPut(`/api/topom/proxy/readcrosscloud/${getXAuth(getBitalosproxyName())}/${value}?forward=${getBitalosproxyName()}`)
export const getLog = (address, ip, query, queryTime) => ajax.post(`http://${ip}:8080/storedagent/logquery`, { address, query, queryTime })

// slots
export const createRange = ({ from, to, group, migrate }: MigrateRange) => ajaxPut(`/api/topom/slots/action/create-range/${getBitalosproxyXName()}/${from}/${to}/${group}/${migrate}?forward=${getBitalosproxyName()}`)
export const createSome = ({ from, to, slots }: MigrateSome) => ajaxPut(`/api/topom/slots/action/create-some/${getBitalosproxyXName()}/${from}/${to}/${slots}?forward=${getBitalosproxyName()}`)
export const initSlots = () => ajaxPut(`/api/topom/slots/action/create/init/${getXAuth(getBitalosproxyName())}?forward=${getBitalosproxyName()}`)
export const searchSlot = key => ajax(`/api/topom/tools/whichgroupkey/${key}?forward=${getBitalosproxyName()}`)

// group
export const replicaGroupServer = (groupId: number | string, server: string, isReplica: boolean) =>
  ajaxPut(`/api/topom/group/replica-groups/${getBitalosproxyXName()}/${groupId}/${server}/${Number(isReplica)}?forward=${getBitalosproxyName()}`)
export const degradeGroup = (groupId: number | string, isopen: number) =>
  ajaxPut(`/api/topom/group/degradegroup/${getBitalosproxyXName()}/${groupId}/${isopen}?forward=${getBitalosproxyName()}`)
export const enableGroup = ({ groupId, server, value }: Enable) =>
  ajaxPut(`/api/topom/group/lrucache/${getBitalosproxyXName()}/${groupId}/${server}/${value}?forward=${getBitalosproxyName()}`)
// --editor
export const pendingGroupServer = (server: string, isPending: boolean) =>
  isPending ?
    ajaxPut(`/api/topom/group/action/remove/${getBitalosproxyXName()}/${server}?forward=${getBitalosproxyName()}`) :
    ajaxPut(`/api/topom/group/action/create/${getBitalosproxyXName()}/${server}?forward=${getBitalosproxyName()}`)
export const compact = (server: string, dbtype: string) => ajaxPut(`/api/topom/group/compact/${getBitalosproxyXName()}/${server}/${dbtype}?forward=${getBitalosproxyName()}`)
export const logCompact = (groupId: number | string) =>
  ajaxPut(`/api/topom/group/logcompact/${getBitalosproxyXName()}/${groupId}?forward=${getBitalosproxyName()}`)
export const syncAll = () => ajaxPut(`/api/topom/group/resync-all/${getBitalosproxyXName()}?forward=${getBitalosproxyName()}`)
export const enableAll = () => ajaxPut(`/api/topom/group/replica-groups-all/${getBitalosproxyXName()}/1?forward=${getBitalosproxyName()}`)
export const disableAll = () => ajaxPut(`/api/topom/group/replica-groups-all/${getBitalosproxyXName()}/0?forward=${getBitalosproxyName()}`)

export const addGroup = (groupId: number | string) => ajaxPut(`/api/topom/group/create/${getBitalosproxyXName()}/${groupId}?forward=${getBitalosproxyName()}`)
export const deleteGroup = (groupId: number | string) => ajaxPut(`/api/topom/group/remove/${getBitalosproxyXName()}/${groupId}?forward=${getBitalosproxyName()}`)
export const addServerToGroup = ({ groupId, server, cloudType, server_role }: AddServerToGroupParams) =>
  ajaxPut(`/api/topom/group/add/${getBitalosproxyXName()}/${groupId}/${server}/${cloudType}/${server_role}?forward=${getBitalosproxyName()}`)

export const deleteServerToGroup = ({ groupId, server, nodeid }: AddServerToGroupParams) =>
  ajaxPut(`/api/topom/group/del/${getBitalosproxyXName()}/${groupId}/${server}/${nodeid}?forward=${getBitalosproxyName()}`)

export const showMembership = ({ groupId, server }) =>
  ajaxGet<CommonResponse<any>>(`/api/topom/group/getclustermembership/${getBitalosproxyXName()}/${groupId}/${server}?forward=${getBitalosproxyName()}`)

export const handleNode = ({ groupId, server, raftaddr, nodeid, model }) =>
  ajaxPut(`/api/topom/group/mount/${getBitalosproxyXName()}/${groupId}/${server}/${raftaddr}/${nodeid}/${model}?forward=${getBitalosproxyName()}`)

// export const addObserverNode = ({groupId,raftaddr,nodeid}) =>
//   ajaxPut(`/api/topom/group/mount/${getBitalosproxyXName()}/${groupId}/${raftaddr}/${nodeid}/${2}/?forward=${getBitalosproxyName()}`)

// group table col-1
export const syncGroup = (groupId: string) => ajaxPut(`/api/topom/group/resync/${getBitalosproxyXName()}/${groupId}?forward=${getBitalosproxyName()}`)
export const promoteGroup = (groupId: number | string, server: string) => ajaxPut(`/api/topom/group/promote/${getBitalosproxyXName()}/${groupId}/${server}?forward=${getBitalosproxyName()}`)
export const replicaGroups = (groupId: number | string, server: string, status: number) => ajaxPut(`/api/topom/group/replica-groups/${getBitalosproxyXName()}/${groupId}/${server}/${status}?forward=${getBitalosproxyName()}`)

// pc config
export const getPcConfigList = () => ajax.get<CommonResponse<any>>(`/api/topom/pconfig/list/${getBitalosproxyXName()}?forward=${getBitalosproxyName()}`)
export const updatePcConfig = (pcConfig: PcConfigItem) => ajax.put(`/api/topom/pconfig/update/${getBitalosproxyXName()}?forward=${getBitalosproxyName()}`, pcConfig)
export const syncPcConfig = () => ajax.put(`/api/topom/pconfig/resync-all/${getBitalosproxyXName()}?forward=${getBitalosproxyName()}`)

// migrate table
export const getMigrateTable$ = () => ajaxGetJSON(`/api/topom/migratelist/${getBitalosproxyXName()}?forward=${getBitalosproxyName()}`)
export const migrate = (value) => ajaxPut(`/api/topom/slots/action/disabled/${getXAuth(getBitalosproxyName())}/${value}?forward=${getBitalosproxyName()}`)
