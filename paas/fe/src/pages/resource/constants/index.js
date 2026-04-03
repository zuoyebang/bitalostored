export const serviceIdList = [
  {
    label: 'All',
    value: 0
  },
  {
    label: 'proxy',
    value: 2
  },
  {
    label: 'bitalos',
    value: 6
  }
];

export const idcList = [
  {
    label: 'All',
    value: 'all'
  },
  {
    label: 'ali',
    value: 'ali'
  },
  {
    label: 'txcloud',
    value: 'txcloud'
  },
  {
    label: 'baidu',
    value: 'baidu'
  },
  {
    label: 'tencent',
    value: 'tencent'
  },
  {
    label: 'txsh',
    value: 'txsh'
  },
  {
    label: 'txgz',
    value: 'txgz'
  }
];

export const  manualList = [
  {
    label: 'All',
    value: 0
  },
  {
    label: 'Suggestion same as system limit',
    value: 1
  },
  {
    label: 'Suggestion different from system limit',
    value: 2
  },
  {
    label: 'Manual preset value not empty',
    value: 3
  }
];

export const cpuSetTypeList = [
  {
    label: 'All',
    value: -1
  },
  {
    label: 'Not bound',
    value: 0
  },
  {
    label: 'Exclusive',
    value: 1
  },
  {
    label: 'Shared',
    value: 2
  }
];

export const tableHeader = [
  {
    label: 'Cluster Name',
    value: 'clusterName'
  },
  {
    label: 'Service Type',
    value: 'serviceName'
  },
  {
    label: 'Service Port',
    value: 'port'
  },
  {
    label: 'Cloud',
    value: 'idc'
  },
  {
    label: 'Metric Type',
    value: 'metricName'
  },
  {
    label: 'Current Limit',
    value: 'cgroupLimit'
  },
  {
    label: 'System Suggestion',
    value: 'suggestValue'
  },
  {
    label: 'Manual Preset',
    value: 'manualValue'
  },
  {
    label: 'CPU Cost',
    value: 'costValue'
  },
  /*
  {
    label: 'Min CPU Number',
    value: 'minCpu'
  },
  {
    label: 'Max CPU Number',
    value: 'maxCpu',
    slot: 'maxCpu'
  },
  */
  {
    label: 'Exclusive CPU',
    value: 'cpuSetType',
    slot: 'exclusiveCpu'
  },
  {
    label: 'Shared CPU',
    value: 'cpuSetType',
    slot: 'shareCpu'
  },
  {
    label: 'Sync Time',
    value: 'syncTime'
  },
  {
    label: 'Apply Time',
    value: 'applyTime'
  },
  {
    label: 'Update Time',
    value: 'updateTime'
  }
];
