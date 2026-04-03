import io from '@/utils/io'
const baseUrl = '/bitalospaas';

/**
 * task list
 * @param {*} params 
 */
export function getTaskList (params) {
  return io.get(baseUrl + '/task/recent', params)
    .then(data => {
      return data.data
    })
}

/**
 * unpublish
 * @param {*} params 
 */
export function getUnpublishTaskList (params) {
  return io.get(baseUrl + '/task/unreleased', params)
    .then(data => {
      return data.data
    })
}

/**
 * unpublishList
 * @param {*} params 
 */
export function publishStatus (params) {
  return io.post(baseUrl + '/task/status', params)
    .then(data => {
      return data.data
    })
}

/**
 * server history
 * @param {*} params 
 */
export function getServerHistory (params) {
  return io.get(baseUrl + '/task/serverhistory', params)
    .then(data => {
      return data.data
    })
}