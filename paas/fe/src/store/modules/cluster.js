const cluster = {
  state: {
    budgets: '',
    idcs: ''
  },
  mutations: {
    SET_BUDGETS: (state, budgets) => {
      state.budgets = budgets
    },
    SET_IDCS: (state, idcs) => {
      state.idcs = idcs
    },
  },
  actions: {
    setBudgets({ commit }, budgets) {
      commit('SET_BUDGETS', budgets)
    },
    setIdcs({ commit }, idcs) {
      commit('SET_IDCS', idcs)
    },
  },
  getters: {
    budgets: (state) => state.budgets,
    idcs: (state) => state.idcs
  }
}

export default cluster