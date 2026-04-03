<template>
  <div class="action">
    <el-dialog title="action" size="small" :before-close="close" :visible.sync="dialogGrade">
      <el-form class="create-machine" :model="actionObj" ref="actionForm" :rules="actionRules" label-width="150px">
        <el-form-item label="operation:" prop="operation" style="margin-top: 15%">
          <el-select v-model="actionObj.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item v-if="actionObj.operation === 'supervisor-stop' && (actionObj.serviceId == 1 || actionObj.serviceId == 6)" label="offline:" prop="remove">
          <el-select v-model.number="actionObj.remove">
            <el-option
                v-for="(item, index) in removeList"
                :key="index"
                :label="item"
                :value="index"
            ></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="close()">Cancel</el-button>
        <el-button type="primary" @click="updateGrade('actionForm')">Confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { updateGrade, getOperationList } from '@/api/'

export default {
  data() {
    return {
      actionObj: {},
      operationList: [],
      removeList: ['No', 'Yes'],
      actionRules: {
        operation: [
          { required: true, message: 'please select operation', trigger: 'change' }
        ], 
      }
    }
  },
  props: {
    gradeForm: {
      type: Object,
      required: true,
      default: () => ({}),
    },
    dialogGrade: {
      type: Boolean,
      required: true,
      default: false,
    }
  },
  methods: {
    close() {
      this.$emit('changGrade', false)
    },
    updateGrade(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          updateGrade(this.actionObj).then(() => {
            this.$emit('changGrade', false)
            this.$emit('getActionList', false)
            this.$message.success('success')
          }).catch((err) => {
            this.$emit('changGrade', false)
            console.log(err)
          })
        } else {
          this.$message.error('Please select or enter required items!')
          return false;
        }
      })
    },
    getOperationList() {
      const params = {
        serviceId: this.gradeForm.serviceId
      }
      getOperationList(params).then(res => {
        this.operationList = res.operations || []
      }).catch(err => {
        console.log(err)
      })
    }
  },
  watch: {
    dialogGrade(val) {
      if (val) {
        this.getOperationList()
        this.actionObj = JSON.parse(JSON.stringify(this.gradeForm))
      }
    }
  }
}
</script>

<style scoped lang="scss">

</style>