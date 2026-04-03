<template>
  <el-container class="is-vertical">
    <zyb-header />
    <el-container class="body-container">
      <zyb-menu />
      <el-main class="main">
        <div class="content-wrap">
          <router-view v-if="isReloadAlive&&hasRouter"></router-view>
        </div>
      </el-main>
    </el-container>
  </el-container>
</template>
<script>
import zybHeader from './header'
import zybMenu from './menu'
import router from '../router'
import routerHander from '@/router/routerHanler'
import { getServiceList, getTableInfo, getSelectInfo } from '@/api/'

export default {
  components: {
    zybHeader,
    zybMenu,
  },
  data() {
    return {
      isReloadAlive : true,
      hasRouter: false
    }
  },
  provide() {
    return {
      reload: this.reload
    }
  },
  created() {
    const params = {
      serviceType: 'home'
    }
    Promise.all([
      getServiceList(params),
      getTableInfo(),
      getSelectInfo()
    ]).then((res) => {
      routerHander(router, res[0].rows)
      this.hasRouter = true;
      this.$store.dispatch('setTableInfo', res[1])
      this.$store.dispatch('setSelectInfo', res[2])
    })
  },
  methods: {
    reload() {
			this.isReloadAlive = false
			this.$nextTick(function(){
				this.isReloadAlive = true
			})
		}
  }
}
</script>
<style lang="scss">
@import '@/styles/variable.scss';
.el-container {
  height: 100%;
  &.body-container {
    height: calc(100% - 50px);
  }
}

.main {
  overflow-y: scroll;
  // padding: 0 !important;
}
.height {
  height: 100%;
  // border: 1px solid red;
}
.waterMarkBox {
  position: absolute;
  left: 0px;
  top: 0px;
  width: 100%;
  height: 100%;
  display: block;
  z-index: 9999;
  pointer-events: none;
  overflow: hidden;
  opacity: 0.05;
}
</style>
