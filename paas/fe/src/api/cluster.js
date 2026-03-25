import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * deployoverview
 * @param {string, string} params
 */
export function deployOverview (params) {
  return io.post(baseUrl + '/cluster/deployoverview', params)
    .then(data => {
      return data.data
    })
}

/**
 * deployinfo
 * @param {number} params
 */
export function deployInfo (params) {
  return io.post(baseUrl + '/cluster/deployinfo', params)
    .then(data => {
      return data.data
    })
}

/**
 * serverdetail
 * @param {number} params
 * 
 */
export function deployDetail (params) {
  return io.post(baseUrl + '/cluster/serverdetail', params)
    .then(data => {
      return data.data
    })
}

/**
 * clusterinfo
 * @param {string, string} params
 */
export function machineInfo (params) {
  return io.post(baseUrl + '/machine/clusterinfo', params)
    .then(data => {
      return data.data
    })
}
