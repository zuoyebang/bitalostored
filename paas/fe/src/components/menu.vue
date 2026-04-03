<template>
  <el-aside class="aside">
    <el-menu :default-active="active" class="zyb-menu" :router="true">
      <template v-for="(route, idx) in accessRoutes">
        <el-submenu :index="route.path" :key="idx" v-if="route.children && route.children.length > 0">
          <template slot="title">{{ route.menuName }}</template>
          <el-menu-item v-for="(item, index) in route.children.filter(item=>!item.hide)" @click="menuClick()" :index="item.path" :key="index+'-'+item.path">{{ item.menuName }}</el-menu-item>
        </el-submenu>
        <template v-else>
          
        <el-menu-item
          @click="menuClick()"
          class="menu-text"
          v-if="route.meta && route.meta.isMenu"
          :index="route.path"
          :key="idx"
          >{{ route.menuName }}</el-menu-item
        >
        </template>
      </template>
    </el-menu>
  </el-aside>
</template>
<script>
import { mapGetters } from 'vuex'

export default {
  data() {
    return {
      active: '',
    }
  },
  methods: {
    handleActive(val) {
      this.active = val.path
    },
    menuClick() {
      this.reload()
    },
  },
  inject: ['reload'],
  created() {},
  computed: {
    ...mapGetters(['accessRoutes']),
  },
  watch: {
    $route(val) {
      this.handleActive(val)
    },
  },
}
</script>
<style lang="scss">
@import '@/styles/variable.scss';

.aside {
  background-color: white;
  transition: 0.3s;
  width: 160px !important;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.12), 0 0 3px 0 rgba(0, 0, 0, 0.04);
  .el-menu.zyb-menu {
    border-right: none;
    width: 160px !important;
  }
  &.collapsed {
    width: 50px !important;
  }
  .menu-text {
    font-size: 14px;
  }
  .el-submenu__icon-arrow{
    margin-top:-8px;
  }
}
</style>
