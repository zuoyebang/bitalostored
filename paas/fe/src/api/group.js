import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * group list
 * @param {*} params 
 */
export function getGroupList (params) {
  return io.get(baseUrl + '/clusterinfo/storedmatrix', params)
    .then(data => {
      return data.data
    })
}

/**
 * package detail
 * @param {*} params 
 */
export function getPackageDetail (params) {
  return io.get(baseUrl + '/package/detail', params)
    .then(data => {
      return data.data
    })
}

/**
 * group create
 * @param {*} params 
 */
export function groupCreate (params) {
  return io.post(baseUrl + '/group/create', params)
    .then(data => {
      return data.data
    })
}

/**
 * node create
 * @param {*} params 
 */
export function nodeCreate (params) {
  return io.post(baseUrl + '/node/add', params)
    .then(data => {
      return data.data
    })
}

/**
 * group replica
 * @param {*} params 
 */
 export function groupReplica (params) {
  return io.post(baseUrl + '/dashboard/replica', params)
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
 * node migrate
 * @param {*} params 
 */
 export function nodeMigrate (params) {
  return io.post(baseUrl + '/cluster/nodemigrate', params)
    .then(data => {
      return data.data
    })
}

/**
 * expansion
 * @param {*} params 
 */
 export function expansion (params) {
  return io.post(baseUrl + '/cluster/expansion', params)
    .then(data => {
      return data.data
    })
}

/**
 * @param {*} params 
 */
export function clusterAddWitnessApi (params) {
  return io.post(baseUrl + '/node/addclusterwitness', params)
    .then(data => {
      return data.data
    })
}

/**
 * @param {*} params 
 */
export function clusterRemoveWitnessApi (params) {
  return io.post(baseUrl + '/node/removeclusterwitness', params)
    .then(data => {
      return data.data
    })
}

/**
 * single
 * @param {*} params 
 */
 export function single (params) {
  return io.post(baseUrl + '/group/reraft', params)
    .then(data => {
      return data.data
    })
}

export function copy (params) {
  return io.post(baseUrl + '/group/copy', params)
    .then(data => {
      return data.data
    })
}

export function groupMarkOffline (params) {
  return io.post(baseUrl + '/group/markoffline', params)
    .then(data => {
      return data.data
    })
}

export function nodeOffline (params) {
  return io.post(baseUrl + '/node/offline', params)
    .then(data => {
      return data.data
    })
}


