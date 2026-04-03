<template>
  <div class="machine-container">
    <el-card class="machine">
      <div class="machine-box">
       <div class="machine-header">
        <span class="title">Machine Stat </span>
        <el-button class="button" type="primary" @click="nodeNigrate">node migrate</el-button>
        <el-table stripe :data="machineList" :header-cell-style="{fontWeight:600,color:'#606266'}">
          <template v-for="(item, index) in tableHeader">
            <el-table-column align="center" :label="item.text" :key="index">
              <template slot-scope="{ row }">
                <span>{{ row[item.text] }}</span>
              </template>
            </el-table-column>
          </template>
        </el-table>
       </div>
     </div>
    </el-card>
    <el-dialog title="node migrate" size="small" :visible.sync="dialogNode">
      <el-form class="create-machine" label-width="100px">
        <el-form-item label="instancesFromMachines:">
          <el-checkbox-group v-model="nodeForm.instancesFromMachines" class="checkbox-group">
            <el-checkbox v-for="(item, index) in machineIdList" :key="index" :label="item.machineId">
              {{ item.ip }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item label="instancesToMachines:">
          <el-checkbox-group v-model="nodeForm.instancesToMachines" class="checkbox-group">
            <el-checkbox v-for="(item, index) in machineIdList" :key="index" :label="item.machineId">
              {{ item.ip }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogNode = false">Cancel</el-button>
        <el-button type="primary" @click="nodeNigradeSubmit()">Confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import { getMachineList, getMachineAll, nodeBalance } from '@/api/matrix'
export default {
  data() {
    return {
      tableHeader: [],
      dialogNode: false,
      machineList: [],
      nodeParams: {},
      machineIdList: [],
      nodeForm: {
        instancesFromMachines: [],
        instancesToMachines: [],
      }
    }
  },
  mounted() {
    this.tableHeader = this.tableInfo.machineStatInfos
    const params = this.$route.query
    this.nodeParams = params
    this.getMachineList(params)
  },
  methods: {
    nodeNigrate() {
      this.dialogNode = true
      this.getMachineall()
    },
    getMachineList(params) {
      getMachineList(params).then((res) => {
        this.machineList = res.machineInfos || []
      }).catch((err) => {
        console.log(err)
      })
    },
    getMachineall() {
      const params = {
        regionId: this.nodeParams.regionId
      }
      getMachineAll(params).then((res) => {
        this.machineIdList = res.rows || []
      }).catch((err) => {
        console.log(err)
      })
    },
    nodeNigradeSubmit() {
      const params = {...this.nodeParams, ...this.nodeForm}
      params.regionId = params.regionId - 0
      params.serviceId = params.serviceId - 0
      nodeBalance(params).then(res => {
        this.dialogNode = false
        console.log(res)
      }).catch(err => {
        console.log(err)
        this.dialogNode = false
      })
    }
  },
  computed: {
    ...mapGetters(['tableInfo']),
  },
}
</script>

<style scoped lang="scss">
  @import '@/styles/variable.scss';
  .machine-container {
    .button {
      float: right;
      margin-bottom: 20px;
    }
    .checkbox-group {
      display: block;
      margin: 40px 0 40px -100px;
      height: 40px;
      .el-checkbox {
        margin-right: 20px;
        margin-left: 0;
        width: 100px;
      }
    }
  }
</style>>
