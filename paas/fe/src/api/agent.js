import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * 
 */
export function getMachineList (params) {
  return io.get(baseUrl + '/machineinfo/list', params)
    .then(data => {
      return data.data
    })
}


/**
 * 
 */
export function createMachine (params) {
  return io.post(baseUrl + '/machine/addmulti', params)
    .then(data => {
      return data.data
    })
}

/**
 * mark offline machine(multi)
 */
export function apiMarkOfflineMachine (params) {
  return io.post(baseUrl + '/machine/markoffline', params)
    .then(data => {
      return data.data
    })
}

/**
 * multi delete machine
 */
export function apiMultiDeleteMachine (params) {
  return io.post(baseUrl + '/machine/multiremove', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
export function agentUpgrade (params) {
  return io.post(baseUrl + '/agent/upgrade', params)
    .then(data => {
      return data.data
    })
}

/**
 * build package
 * @param {*} params 
 */
export function packageCreate (params) {
  return io.post(baseUrl + '/file/build', params)
    .then(data => {
      return data.data
    })
}

/**
 * machine update
 * @param {*} params 
 */
export function machineUpdate (params) {
  return io.post(baseUrl + '/machine/update', params)
    .then(data => {
      return data.data
    })
}

/**
 * bind
 * @param {*} params 
 */
export function machinesBind (params) {
  return io.post(baseUrl + '/region/bindmachines', params)
    .then(data => {
      return data.data
    })
}

/**
 * unbind
 * @param {*} params 
 */
export function machinesUnBind (params) {
  return io.post(baseUrl + '/region/unbindmachines', params)
    .then(data => {
      return data.data
    })
}

/**
 * remove
 * @param {*} params 
 */
 export function remove (params) {
  return io.post(baseUrl + '/region/remove', params)
    .then(data => {
      return data.data
    })
}

/**
 * offline
 * @param {*} params 
 */
 export function offline (params) {
  return io.post(baseUrl + '/machine/offline', params)
    .then(data => {
      return data.data
    })
}


/**
 * removeMachine
 * @param {*} params 
 */
 export function removeMachine (params) {
  return io.post(baseUrl + '/machine/remove', params)
    .then(data => {
      return data.data
    })
}

/**
 * budgetlist
 * @param {*} params 
 */
 export function getBudgetList (params) {
  return io.get(baseUrl + '/machine/budgetlist', params)
    .then(data => {
      return data.data
    })
}

/**
 * regionRemoveMachines
 * @param {*} params 
 */
 export function regionRemoveMachines (params) {
  return io.post(baseUrl + '/region/removemachines', params)
    .then(data => {
      return data.data
    })
}