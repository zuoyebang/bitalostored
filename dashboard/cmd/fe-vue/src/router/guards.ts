import store from '../store'
import {SET_BITALOSPROXY} from '@/store/types'
import {getUToken} from '@/commons'

export const loginGuard = (to, from, next) => {
  if (to.name === 'Login') {
    next()
    return
  }
  if (store.getters['user/role'] || getUToken()) {
    next()
  } else {
    next('/login')
  }
}

export const bitalosproxyGuard = (to, from, next) => {
  if (to.name === 'Home' && to.params && to.params.id) {
    store.dispatch('bitalosproxy/' + SET_BITALOSPROXY, to.params.id)
  }
  next()
}
