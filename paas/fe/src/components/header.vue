<template>
  <el-header class="header">
    <el-button type="primary" class="dropdown-pass">
      BITALOSPAAS
    </el-button>
    <span class="name" @click="logout">{{ userInfo.username }}</span>
  </el-header>
</template>
<script>
import { mapState } from 'vuex';
export default {
  data() {
    return {};
  },
  computed: {
    ...mapState({
      userInfo: (state) => state.global.userInfo
    })
  },
  methods: {
    goHome() {
      this.$router.push({ path: '/' });
    },
    logout() {
      this.$confirm('Logout?', 'Tip', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          window.localStorage.setItem('userInfo', '');
          this.$store.commit('SET_USERINFO', null);
          this.$message({
            type: 'success',
            message: 'Logout Success!'
          });
        })
        .catch(() => {
          this.$message({
            type: 'info',
            message: 'Cancelled'
          });
        });
    }
  }
};
</script>
<style lang="scss">
@import '@/styles/variable.scss';

.header {
  background-color: white;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.12), 0 0 3px 0 rgba(0, 0, 0, 0.04);
  position: relative;
  .name {
    float: right;
    line-height: 50px;
    margin-right: 20px;
    cursor: pointer;
  }
  .env {
    float: right;
    line-height: 50px;
    background: rgb(249, 2, 2);
    margin-right: 40px;
    cursor: pointer;
  }
  .overseas {
    float: left;
    line-height: 50px;
    margin-left: 150px;
    background: rgb(0, 136, 255);
    position: absolute;
    color: $color-white;
    cursor: pointer;
  }
  .dev {
    float: left;
    line-height: 50px;
    margin-left: 280px;
    background: rgba(89, 110, 112, 0.933);
    position: absolute;
    color: $color-white;
    cursor: pointer;
  }
  .test {
    float: left;
    line-height: 50px;
    margin-left: 350px;
    background: rgb(120, 109, 164);
    position: absolute;
    color: $color-white;
    cursor: pointer;
  }
  .dropdown-pass{
    margin-left: -20px;
    height: 50px;
    width: 160px;
  }
}
</style>
