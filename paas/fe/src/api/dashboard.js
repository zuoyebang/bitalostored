import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * 
 */
export function createFe (params) {
  return io.post(baseUrl + '/clustercreate/storedfe', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
export function createDashboard (params) {
  return io.post(baseUrl + '/clustercreate/storeddashboard', params)
    .then(data => {
      return data.data
    })
}

/**
 * 
 */
 export function replaceDashboard (params) {
  return io.post(baseUrl + '/cluster/replacedashboard', params)
    .then(data => {
      return data.data
    })
}

/**
 * copy dashboard
 */
export function clusterCopy (params) {
  return io.post(baseUrl + '/cluster/copy', params)
    .then(data => {
      return data.data
    })
}

/**
 * operation
 */
 export function getOperation (params) {
  return io.get(baseUrl + '/service/operations', params)
    .then(data => {
      return data.data
    })
}
