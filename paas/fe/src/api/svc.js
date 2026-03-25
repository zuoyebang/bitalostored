import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getSvcList() {
  return io.get(baseUrl + "/svc/list").then((data) => {
    return data.data;
  });
}


export function getSvcData(params) {
  return io.post(baseUrl + '/svc/query', params)
  .then(data => {
    return data.data
  });
}

export function removeCluster(params) {
  return io.post(baseUrl + '/svc/delcluster', params)
  .then(data => {
    return data.data
  });
}

export function addCluster(params) {
  return io.post(baseUrl + '/svc/addcluster', params)
  .then(data => {
    return data.data
  });
}

export function addSvc(params) {
  return io.post(baseUrl + '/svc/add', params)
  .then(data => {
    return data.data
  });
}

export function getCloud() {
  return io.get(baseUrl + "/svc/cloud").then((data) => {
    return data.data;
  });
}
