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
export function getTableInfo (params) {
  return io.get(baseUrl + '/controlfe/formfields', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
export function getSelectInfo (params) {
  return io.get(baseUrl + '/controlfe/constantlist', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
export function getStoredList (params) {
  return io.get(baseUrl + '/cluster/storedlist', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
export function getPackageDetail (params) {
  return io.get(baseUrl + '/resource/list', params)
    .then(data => {
      return data.data
    })
}

/**
 * action
 */
export function updateAction (params) {
  return io.post(baseUrl + '/node/upgrade', params)
    .then(data => {
      return data.data
    })
}


/**
 * grade
 */
export function updateGrade (params) {
  return io.post(baseUrl + '/node/operate', params)
    .then(data => {
      return data.data
    })
}

/**
 * package update
 */
export function packageUpdate (params) {
  return io.post(baseUrl + '/package/update', params)
    .then(data => {
      return data.data
    })
}

/**
 * get operation
 */
export function getOperationList (params) {
  return io.get(baseUrl + '/service/operations', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params
 */
export function login (params) {
  return io.post(baseUrl + '/login', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params
 */
export function getTemplate (params) {
  return io.get(baseUrl + '/package/templates', params)
    .then(data => {
      return data.data
    })
}

/**
 * cluster list
 * @param {*} params
 */
export function getCluster (params) {
  return io.get(baseUrl + '/cluster/list', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params
 */
export function markOffline (params) {
  return io.get(baseUrl + '/cluster/markoffline', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 * @param {*} params
 */
export function deleteOffline (params) {
  return io.get(baseUrl + '/cluster/deleteoffline', params)
    .then(data => {
      return data.data
    })
}

/**
 * config list
 * @param {*} params
 */
 export function getConfigList (params) {
  return io.get(baseUrl + '/config/list', params)
    .then(data => {
      return data.data
    })
}

/**
 * bind department
 * @param {*} params
 */
 export function bindDepartment (params) {
 return io.post(baseUrl + '/cluster/binddepartment', params)
   .then(data => {
     return data.data
   })
}

/**
 * department list
 * @param {*} params
 */
 export function departmentList (params) {
  return io.get(baseUrl + '/cluster/departmentlist', params)
    .then(data => {
      return data.data
    })
}

/**
 * offline
 * @param {*} params
 */
 export function offline (params) {
  return io.post(baseUrl + '/cluster/offline', params)
    .then(data => {
      return data.data
    })
 }

/**
 * config replace
 * @param {*} params
 */
 export function configReplace (params) {
  return io.post(baseUrl + '/config/replace', params)
    .then(data => {
      return data.data
    })
}

/**
 * config update
 * @param {*} params
 */
 export function configUpdate (params) {
  return io.post(baseUrl + '/config/update', params)
    .then(data => {
      return data.data
    })
}

/**
 * remove file
 * @param {*} params
 */
 export function removeFile (params) {
  return io.post(baseUrl + '/file/remove', params)
    .then(data => {
      return data.data
    })
}

/**
 * packlist
 * @param {*} params
 */
 export function getPackList (params) {
  return io.get(baseUrl + '/config/packlist', params)
    .then(data => {
      return data.data
    })
}

/**
 * remove config
 * @param {*} params
 */
 export function removeConfig (params) {
  return io.post(baseUrl + '/config/remove', params)
    .then(data => {
      return data.data
    })
}

/**
 * bind config
 * @param {*} params
 */
 export function bindConfig (params) {
  return io.post(baseUrl + '/config/bind', params)
    .then(data => {
      return data.data
    })
}

