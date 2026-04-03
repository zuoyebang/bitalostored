<template>
  <div class="update">
    <el-dialog title="package update" :before-close="close" size="small" :visible.sync="dialogUpdate">
      <el-form class="update-package" label-width="150px">
        <el-form-item label="packageTag:" prop="packageTag">
          <el-input v-model="packageObj.packageTag" autocomplete="off"></el-input>
        </el-form-item>
        <el-form-item label="arg:" prop="arg">
          <el-input v-model="packageObj.arg" autocomplete="off"></el-input>
        </el-form-item>
        <el-form-item label="env:" prop="env">
          <el-input v-model="packageObj.env" autocomplete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="close()">Cancel</el-button>
        <el-button type="primary" @click="updatePackage()">Confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { packageUpdate } from '@/api/'

export default {
  data() {
    return {
      packageObj: {}
    }
  },
  props: {
    packageInfo: {
      type: Object,
      required: true,
      default: () => ({}),
    },
    dialogUpdate: {
      type: Boolean,
      required: true,
      default: false,
    }
  },
  methods: {
    close() {
      this.$emit('changeUpdateDialog', false)
    },
    updatePackage() {
      packageUpdate(this.packageObj).then( res => {
        console.log(res)
        this.$message.success('success')
        this.$emit('changeUpdateDialog', false)
        this.$emit('changeUpdate', true)
      }).catch(err => {
        console.log(err)
      })
    }
  },
  watch: {
    dialogUpdate(val) {
      if (val) {
        this.packageObj = JSON.parse(JSON.stringify(this.packageInfo))
      }
    }
  }
}
</script>

<style scoped>

</style>