import Vue from 'vue'
import Vuex from 'vuex'
import user from './modules/user'
import {namespace} from 'vuex-class'
import bitalosproxy from '@/store/modules/bitalosproxy'
import server from '@/store/modules/server'

Vue.use(Vuex)

export default new Vuex.Store({
  modules: {user, bitalosproxy, server},
})

export const UserNamespace = namespace('user')
export const BitalosproxyNamespace = namespace('bitalosproxy')
export const ServerNamespace = namespace('server')
