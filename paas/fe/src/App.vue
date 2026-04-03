<template>
  <div id="app">
    <login v-if="!userInfo" />
    <stored v-else />
  </div>
</template>

<script>
import stored from './components/templete'
import login from './pages/login/login.vue'
import { mapState } from 'vuex'
export default {
  name: 'App',
  components: {
    stored,
    login,
  },
  computed: {
    ...mapState({
      userInfo: state => state.global.userInfo
    })
  },
  methods: {
    getUserInfo(){
      const str = localStorage.getItem('userInfo');
      if(str){
        this.$store.commit('SET_USERINFO', JSON.parse(str))
      }
    }
  },
  created(){
    this.getUserInfo()
  }
}
</script>

<style>
#app {
  color: #000;
  font-family: 'Microsoft YaHei';
  height: 100%;
}
</style>
