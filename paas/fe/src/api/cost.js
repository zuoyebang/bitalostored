import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getClusterNames(params) {
  return io.get(baseUrl + '/cost/clusternames', params).then((data) => {
    return data.data;
  });
}

export function getBudgetList(params) {
  return io.get(baseUrl + '/cost/budgetlist', params).then((data) => {
    return data.data;
  });
}

export function getCostData(params) {
  return io.post(baseUrl + '/cost/list', params)
  .then(data => {
    return data.data
  });
}


export function editValue(params) {
  return io.post(baseUrl + '/cost/editcpu', params)
  .then(data => {
    return data.data
  });
}

export function delValue(params) {
  return io.post(baseUrl + '/cost/delcost', params)
  .then(data => {
    return data.data
  });
}

export function addCost(params) {
  return io.post(baseUrl + '/cost/addcost', params)
  .then(data => {
    return data.data
  });
}