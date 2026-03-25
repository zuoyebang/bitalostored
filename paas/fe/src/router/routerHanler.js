
import baseRoutes from './baseRoutes'
import store from '@/store'

const getAccessRoutes = (menuInfo) => {
  const casRoutesPath = menuInfo.map(item => item.serviceName)
  const resRoutes = []
  baseRoutes.forEach(item => {
    if (casRoutesPath.includes(item.name) || item.path === '*') {
      menuInfo.forEach(content => {
        if (item.name === content.serviceName) {
          item.meta.serviceId = content.serviceId || ''
          item.meta.serviceName = content.serviceName || ''
        }
      }) 
      resRoutes.push(item)
    }
  })
  return resRoutes
}

const routerHandler = (router, menuInfo) => {
  let accessRoutes = getAccessRoutes(menuInfo)
  store.dispatch('setAccessRoutes', accessRoutes)
  router.addRoutes(accessRoutes)
}

export default routerHandler