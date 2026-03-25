import Vue from 'vue'
import Router from 'vue-router'
import baseRoutes from './baseRoutes'

const originalPush = Router.prototype.push
Router.prototype.push = function push(location) {
  return originalPush.call(this, location).catch((err) => err)
}

Vue.use(Router)

export default new Router({
  // mode: 'history',
  base: process.env.BASE_URL,
  routes: baseRoutes,
})
