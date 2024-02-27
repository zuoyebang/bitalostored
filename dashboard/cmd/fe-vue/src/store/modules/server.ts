import {ADD_ITEM, SET_CLOSE, SET_OPEN} from '@/store/types'
import {ServerItem} from '@/interfaces/commons'

const state = {
  list: [] as ServerItem[],
  isOpen: false,
}

const getters = {
  list: (s) => s.list,
  isOpen: (s) => s.isOpen,
}

const actions = {
  [ADD_ITEM]: ({commit}, item: ServerItem) => commit(ADD_ITEM, item),
  [SET_CLOSE]: ({commit}) => commit(SET_CLOSE),
  [SET_OPEN]: ({commit}) => commit(SET_OPEN),
}

const mutations = {
  [ADD_ITEM]: (s, item) => s.list.unshift(item),
  [SET_CLOSE]: (s) => s.isOpen = false,
  [SET_OPEN]: (s) => s.isOpen = true,
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
}
