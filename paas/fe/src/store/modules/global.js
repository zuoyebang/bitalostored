import routesTable from '@/router/baseRoutes'

const global = {
  state: {
    accessRoutes: routesTable,
    tableInfo: [],
    selectInfo: [],
    userInfo: null
  },
  mutations: {
    SET_ACCESS_ROUTES: (state, routes) => {
      state.accessRoutes = routes
    },
    SET_TABLE_INFO: (state, info) => {
      state.tableInfo = info
    },
    SET_SELECT_INFO: (state, info) => {
      state.selectInfo = info
    },
    SET_USERINFO: (state, info) => {
      state.userInfo = info
    }
  },
  actions: {
    setAccessRoutes({ commit }, routes) {
      commit('SET_ACCESS_ROUTES', routes)
    },
    setTableInfo({ commit }, info) {
      commit('SET_TABLE_INFO', info)
    },
    setSelectInfo({ commit }, info) {
      commit('SET_SELECT_INFO', info)
    },
  },
  getters: {
    accessRoutes: (state) => state.accessRoutes,
    tableInfo: (state) => state.tableInfo,
    selectInfo: (state) => state.selectInfo,
  }
}

export default global
