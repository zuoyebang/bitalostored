import sha256 from 'sha256'
import store from '@/store'
import Cookies from 'js-cookie'

export function getXAuth(bitalosproxy: string) {
  return sha256('Stored-XAuth-[' + bitalosproxy + ']').substring(0, 32)
}

export function getBitalosproxyXName() {
  return store.getters['bitalosproxy/xname']
}

export function getBitalosproxyName() {
  return store.getters['bitalosproxy/name']
}

export function humanSize(size) {
  if (size < 1024) {
    return size + ' B'
  }
  size /= 1024
  if (size < 1024) {
    return size.toFixed(2) + ' KB'
  }
  size /= 1024
  if (size < 1024) {
    return size.toFixed(2) + ' MB'
  }
  size /= 1024
  if (size < 1024) {
    return size.toFixed(2) + ' GB'
  }
  size /= 1024
  return size.toFixed(2) + ' TB'
}

export function getUToken() {
  return Cookies.get('utoken')
}
