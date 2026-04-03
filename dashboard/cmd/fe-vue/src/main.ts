import Vue from 'vue'
import App from './app'
import router from './router'
import store from './store'
import VueRx from 'vue-rx'
import vuetify from './plugins/vuetify'
import ElementUI from 'element-ui'
import elementEnLocale from 'element-ui/lib/locale/lang/en'
import 'element-ui/lib/theme-chalk/index.css' // Element UI styles
import '@/assets/app.sass'
import VueClipboard from 'vue-clipboard2'
import '@mdi/font/css/materialdesignicons.min.css'

VueClipboard.config.autoSetContainer = true

Vue.use(VueClipboard)
Vue.use(VueRx)
Vue.use(ElementUI, { size: 'mini', locale: elementEnLocale })
Vue.config.productionTip = false

new Vue({
  router,
  store,
  render: h => h(App),
  vuetify,
}).$mount('#app')
