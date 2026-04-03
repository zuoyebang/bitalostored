<template>
  <div class="login">
    <el-form class="form">
      <el-form-item label="Username">
        <el-input v-model="form.username"></el-input>
      </el-form-item>
      <el-form-item label="Password">
        <el-input v-model="form.password"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button class="btn" @click="login" type="primary">Login</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import {login} from '../../api/index'
export default {
  name: "login",
  data(){
    return {
      form: {
        username: '',
        password: ''
      }
    }
  },
  methods: {
    login(){
      const params = this.form.username + '|' + this.form.password
      // 将账号密码进行base64 编码
      login({up: window.btoa(params)}).then(res=>{
        const data = {
          username: this.form.username,
          token: res
        }
        localStorage.setItem('userInfo', JSON.stringify(data))
        this.$store.commit('SET_USERINFO', data)
      })
    }
  }
}
</script>

<style scoped lang="scss">
  .login {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    .form {
      width: 300px;
      margin: auto;
      .btn {
        width: 300px;
      }
    }
  }
</style>
