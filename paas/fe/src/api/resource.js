import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getClusterNames(params) {
  return io.get(baseUrl + '/cluster/clusternames', params).then((data) => {
    return data.data;
  });
}

export function getResourceData(params) {
  return io.post(baseUrl + '/resource/list', params)
  .then(data => {
    return data.data
  });
}


export function apply(params) {
  return io.post(baseUrl + '/resource/apply', params)
  .then(data => {
    return data.data
  });
}

export function addResourcePoolApi(params) {
  return io.post(baseUrl + '/resource/addrecord', params)
  .then(data => {
    return data.data
  });
}

export function editValue(params) {
  return io.post(baseUrl + '/resource/editvalue', params)
  .then(data => {
    return data.data
  });
}

export function controlCost(params) {
  return io.post(baseUrl + '/resource/controlcost', params)
  .then(data => {
    return data.data
  });
}

export function setCpu(params) {
  return io.post(baseUrl + '/resource/cpusetpermession', params)
  .then((data) => {
    return data.data;
  });
}
