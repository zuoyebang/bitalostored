import {SET_BITALOSPROXY} from '@/store/types'
import {getXAuth} from '@/commons'

const state = {
  name: '',
}

const getters = {
  name: (s) => s.name,
  xname: (s) => getXAuth(s.name),
}

const actions = {
  [SET_BITALOSPROXY]({commit}, val: string) {
    console.log(val)
    commit(SET_BITALOSPROXY, val)
  },
}

const mutations = {
  [SET_BITALOSPROXY]: (s, val: string) => s.name = val,
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
}
