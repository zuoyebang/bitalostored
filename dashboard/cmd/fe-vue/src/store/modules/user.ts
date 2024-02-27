import {SET_ROLE} from '@/store/types'

const state = {
  role: undefined
}

const getters = {
  role: (s) => s.role,
}

const actions = {
  [SET_ROLE]({commit}, val: number) {
    commit(SET_ROLE, val)
  }
}

const mutations = {
  [SET_ROLE]: (s, val: number) => s.role = val,
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations
}
