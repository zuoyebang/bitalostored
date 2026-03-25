import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * 
 * @param {*} params 
 */
export function getServiceList (params) {
  return io.get(baseUrl + '/service/list', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params 
 */
export function createCluster (params) {
  return io.post(baseUrl + '/clustercreate/storedproxy', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params 
 */
export function getProxyInfo (params) {
  return io.get(baseUrl + '/clusterinfo/storedproxy', params)
    .then(data => {
      return data.data
    })
}

/**
 * alignproxy
 * @param {*} params 
 */
export function alignproxy (params) {
  return io.post(baseUrl + '/cluster/alignproxy', params)
    .then(data => {
      return data.data
    })
}

/**
 * switchdashboard
 * @param {*} params 
 */
export function switchdashboard (params) {
  return io.post(baseUrl + '/cluster/switchdashboard', params)
    .then(data => {
      return data.data
    })
}

/**
 * node config
 * @param {*} params 
 */
 export function nodeConfig (params) {
  return io.get(baseUrl + '/node/config', params)
    .then(data => {
      return data.data
    })
}

/**
 * check port
 * @param {*} params 
 */
 export function checkPort (params) {
  return io.get(baseUrl + '/machine/checkport', params)
    .then(data => {
      return data.data
    })
}

/**
 * add odin
 * @param {*} params 
 */
 export function addOdin (params) {
  return io.post(baseUrl + '/cluster/addodin', params)
    .then(data => {
      return data.data
    })
}

/**
 * add odin
 * @param {*} params 
 */
export function addOdinLink (params) {
  return io.post(baseUrl + '/cluster/addodinLink', params)
    .then(data => {
      return data.data
    })
}

/**
 * multi upgrade
 * @param {*} params
 */
 export function multiUpgrade(params) {
  return io.post(baseUrl + "/node/nomalmultiupgrade", params).then((data) => {
    return data.data;
  });
}