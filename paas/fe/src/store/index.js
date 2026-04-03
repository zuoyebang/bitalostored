import Vue from 'vue'
import Vuex from 'vuex'
import global from './modules/global'
import cluster from './modules/cluster'

Vue.use(Vuex)

const store = new Vuex.Store({
  modules: {
    global,
    cluster
  }
})

export default store
