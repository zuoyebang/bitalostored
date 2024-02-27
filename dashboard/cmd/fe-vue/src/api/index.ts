import {ServerItem} from '@/interfaces/commons'
import {getUToken} from '@/commons'
import axios from 'axios'
import store from '@/store'
import {ADD_ITEM, SET_OPEN} from '@/store/types'

export * from './home'

export const ajax = axios.create({
  // transformResponse: [(data, headers) => {
  //   console.log(data, headers)
  //   return data
  // }],
  // transformRequest: [(data, headers) => {
  //   console.log(data, headers)
  //   return {data, headers}
  // }],
})
ajax.interceptors.request.use((config) => {
  if (!getUToken()) {
    location.assign('/#/login')
    return
  }
  return config
})
ajax.interceptors.response.use(
  (resp) => resp,
  (err) => {
    store.dispatch('server/' + ADD_ITEM, {
      data: err.response ? err.response.data : '403',
      url: err.config.url,
      type: 'response',
      msg: err.response.data.errmsg ? err.response.data.errmsg.cause : err.response.data,
      time: new Date(),
    } as ServerItem)
    store.dispatch('server/' + SET_OPEN)
  },
)

export const ajaxPut = ajax.put
export const ajaxGet = ajax.get
