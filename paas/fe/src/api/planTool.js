import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getScheduleList(params) {
  return io.get(baseUrl + '/schedule/list',params).then((data) => {
    return data.data;
  });
}

export function getClusterList () {
  return io.get(baseUrl + '/schedule/clusterlist ',{}).then((data) => {
    return data.data;
  });
}

export function createSchedule (params) {
  return io.post(baseUrl + '/schedule/create', params)
    .then(data => {
      return data.data
    })
 }

 export function getExecprocess (params) {
  return io.get(baseUrl + '/schedule/execprocess', params)
    .then(data => {
      return data.data
    })
 }
 export function syncSchedule (params) {
  return io.post(baseUrl + '/schedule/sync', params)
    .then(data => {
      return data.data
    })
 }

 export function scheduleLoadConfig (params) {
  return io.post(baseUrl + '/schedule/loadconfig', params)
    .then(data => {
      return data.data
    })
 }
 export function getExeclog (params) {
  return io.get(baseUrl + '/schedule/execlog', params)
    .then(data => {
      return data.data
    })
 }