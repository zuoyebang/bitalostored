
const routesTable = [
  {
    path: '/',
    name: 'stored-bitalos',
    menuName: 'stored',
    component: () => import('@/pages/matrix/matrix.vue'),
    meta: {
      isMenu: true,
      serviceId: 6,
    },
  },
  {
    path: '/task',
    name: 'stored-task',
    component: () => import('@/pages/matrix/components/task.vue'),
  },
  {
    path: '/machine',
    name: 'stored-machine',
    component: () => import('@/pages/matrix/components/machine.vue'),
  },
  {
    path: '/group-info',
    name: 'group-info',
    component: () => import('@/pages/matrix/components/group-info.vue'),
  },
  {
    path: '/config',
    name: 'config',
    component: () => import('@/components/config.vue'),
  },
  {
    path: '/task-info',
    name: 'stored-taskInfo',
    component: () => import('@/pages/proxy/components/task-info.vue'),
  },
  {
    path: '/stored-proxy',
    name: 'stored-proxy',
    menuName: 'proxy',
    component: () => import('@/pages/proxy/proxy.vue'),
    meta: {
      isMenu: true,
      serviceId: 2,
    },
  },
  {
    path: '/stored-dashboard',
    name: 'stored-dashboard',
    menuName: 'dashboard',
    component: () => import('@/pages/dashboard/dashboard.vue'),
    meta: {
      isMenu: true,
      serviceId: 3,
    },
  },
  {
    path: '/stored-fe',
    name: 'stored-fe',
    menuName: 'fe',
    component: () => import('@/pages/fe/fe.vue'),
    meta: {
      isMenu: true,
      serviceId: 4,
    },
  },
  {
    path: '/stored-agent',
    name: 'stored-agent',
    menuName: 'controller',
    component: () => import('@/pages/agent/agent.vue'),
    meta: {
      isMenu: true,
    },
  },
  {
    path: '/login',
    name: 'login',
    component: () => import('@/pages/login/login.vue'),
    meta: {
      isMenu: false,
    },
  },
  {
    path: '/deploy-overview',
    name: 'cluster',
    menuName: 'cluster',
    component: () => import('@/pages/deploy/deployOverview.vue'),
    meta: {
      isMenu: true,
    },
  },
  {
    path: '/deploy-info',
    name: 'Cluster Info - Home',
    component: () => import('@/pages/deploy/components/deployInfo.vue'),
  },
  {
    path: '/deploy-detail',
    name: 'Cluster Info - View',
    component: () => import('@/pages/deploy/components/deployDetail.vue'),
  },
  {
    path: '/machineStat',
    name: 'machine',
    menuName: 'machine',
    component: () => import('@/pages/machine/machine.vue'),
    meta: {
      isMenu: true,
    },
  },
  {
    path: '/resource',
    name: 'resource pool',
    menuName: 'resource pool',
    component: () => import('@/pages/resource/resource.vue'),
    meta: {
      isMenu: true,
    },
  },
  {
    path: '/operate-history',
    name: 'operation record',
    menuName: 'operation record',
    component: () => import('@/pages/operateHistory/index.vue'),
    meta: {
      isMenu: true,
    },
  },
  {
    path: '*',
    redirect: '/404',
  },
]

export default routesTable
