<template>
  <div class="new-package">
    <el-dialog title="build" :before-close="close" size="small" :visible.sync="dialogPackage">
      <el-form class="create-machine" :model="packageForm" ref="createPackage" :rules="packageRules" label-width="150px">
        <el-form-item label="gitBranch:" prop="gitBranch" style="margin-top: 12%">
          <el-input v-model="packageForm.gitBranch" autocomplete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="close()">Cancel</el-button>
        <el-button type="primary" @click="createPackage('createPackage')">Confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { packageCreate } from '@/api/agent'
import { getCluster } from '@/api/index'

export default {
  data() {
    return {
      packageForm: {
        gitBranch: '',
      },
      clusterList: [],
      templates: [],
      options: [
        {text: 'Yes', value: true},
        {text: 'No', value: false},
      ],
      packageRules: {
        gitBranch: [
          { required: true, message: 'gitBranch is require', trigger: 'blur' }
        ],
        server: [
          { required: false }
        ],
      },
    }
  },
  props: {
    dialogPackage: {
      type: Boolean,
      required: true,
      default: false,
    },
    serviceId: {
      type: Number,
      required: true,
      default: 0,
    },
  },
  watch: {
    dialogPackage(val){
      if (val) {
        this.getCluster()
      }
    }
  },
  methods: {
    createPackage(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const params = { serviceId: this.serviceId, ...{
              ...this.packageForm,
              // clusterId: this.packageForm.clusterId || 0
            }};
          const load = this.$loading();
          packageCreate(params).then((res) => {
            this.$emit('changeDialog', false)
            this.$emit('getPackage', false)
            this.$message.success('success')

            this.$alert(`<div style="white-space: pre-wrap;width: 340px;max-height: 600px;overflow-y: scroll">${res}</div>`, '', {
              confirmButtonText: 'Confirm',
              dangerouslyUseHTMLString: true
            });
          }).catch((err) => {
            this.$emit('changeDialog', false)
            console.log(err)
          }).finally(()=>load.close())
        } else {
          this.$message.error('Please select or enter required items!')
          return false;
        }
      })
    },
    close() {
      this.$emit('changeDialog', false)
    },
    getCluster(){
      getCluster({serviceId: this.serviceId}).then(res=>{
        if(res&&res.rows)this.clusterList = res.rows;
      })
    }
  }
}
</script>

<style scoped lang="scss">
  .new-package {
    .el-input {
      width: 260px;
    }
    .el-select {
      width: 260px;
    }
  }
</style>
