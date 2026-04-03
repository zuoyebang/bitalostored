import io from "@/utils/io";
const baseUrl = '/bitalospaas';

/**
 * region list
 * @param {*} params
 */
export function getRegionList(params) {
  return io.get(baseUrl + "/region/list", params).then((data) => {
    return data.data;
  });
}

/**
 * create region
 * @param {*} params
 */
export function createRegion(params) {
  return io.post(baseUrl + "/region/create", params).then((data) => {
    return data.data;
  });
}

/**
 * cluster list
 * @param {*} params
 */
export function getclusterList(params) {
  return io.get(baseUrl + "/cluster/list", params).then((data) => {
    return data.data;
  });
}

/**
 * file list
 * @param {*} params
 */
export function getFileList(params) {
  return io.get(baseUrl + "/file/list", params).then((data) => {
    return data.data;
  });
}
export function getPackageList(params) {
  return io.get(baseUrl + "/file/list", params).then((data) => {
    return data.data;
  });
}

/**
 * machine list
 * @param {*} params
 */
export function getMachineList(params) {
  return io.get(baseUrl + "/machine/infos", params).then((data) => {
    return data.data;
  });
}

/**
 * machine list
 * @param {*} params
 */
export function getMachineAll(params) {
  return io.get(baseUrl + "/machine/all", params).then((data) => {
    return data.data;
  });
}

/**
 * create package
 * @param {*} params
 */
export function packageCreate(params) {
  return io.post(baseUrl + "/package/create", params).then((data) => {
    return data.data;
  });
}

/**
 * create cluster
 * @param {*} params
 */
export function clustercreate(params) {
  return io
    .post(baseUrl + "/clustercreate/storedmatrix", params)
    .then((data) => {
      return data.data;
    });
}

/**
 * create cluster group
 * @param {*} params
 */
export function clustercreategroup(params) {
  return io
    .post(baseUrl + "/cluster/createall", params)
    .then((data) => {
      return data.data;
    });
}

/**
 * node
 * @param {*} params
 */
export function nodeBalance(params) {
  return io.post(baseUrl + "/machine/rebalanced", params).then((data) => {
    return data.data;
  });
}

/**
 * exportInfo
 * @param {*} params
 */
export function exportInfo(params) {
  return io.get(baseUrl + "/cluster/exportinfo", params).then((data) => {
    return data.data;
  });
}

/**
 * syncAll
 */
export function syncAll() {
  return io.get(baseUrl + "/cluster/syncall").then((data) => {
    return data.data;
  });
}

/**
 * node config
 * @param {*} params
 */
export function nodeConfig(params) {
  return io.get(baseUrl + "/node/config", params).then((data) => {
    return data.data;
  });
}

/**
 * update grafana
 * @param {*} params
 */
export function updateGrafana(params) {
  return io.post(baseUrl + "/cluster/updategrafana", params).then((data) => {
    return data.data;
  });
}

/**
 * multi upgrade
 * @param {*} params
 */
export function multiUpgrade(params) {
  return io.post(baseUrl + "/node/multiupgrade", params).then((data) => {
    return data.data;
  });
}

export function replicateMachine(params) {
  return io.post(baseUrl + "/machine/replicate", params).then((data) => {
    return data.data;
  });
}

export function machineMigrate(params) {
  return io.post(baseUrl + "/machine/migrate", params).then((data) => {
    return data.data;
  });
}

export function machineRemoveProxyApi(params) {
  return io.post(baseUrl + "/machine/removeproxy", params).then((data) => {
    return data.data;
  });
}

/**
 * add local file
 * @param {*} params
 */
export function addLocalFile(params) {
  return io.post(baseUrl + "/file/addlocal", params).then((data) => {
    return data.data;
  });
}