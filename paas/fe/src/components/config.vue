<template>
  <div class="config">
    <div class="title">
      <span v-for="(value, key) in configDetail[activeName]" :key="key"> {{ key }} : {{ value }}</span>
    </div>
    <el-card class="box-card">
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane  v-for="(item, index) in configList" :key="index" :name="item.index" :label="item.buttonName">
          <el-row>
            <el-col :span="16" class="col">
              Config:<br/>
              <div style="margin: 20px;"></div>
              <table width="95%" height="100%">
                <tr> 
                  <td><el-input type="textarea" :rows="30" placeholder="please enter content" v-model="configList[activeName].content"></el-input></td>
                </tr>
                </table>
            </el-col>
            <el-col :span="8" class="col">
              Comment:<br/>
              <div style="margin: 20px;"></div>
              <table width="80%" height="100%">
                <tr> 
                  <td><el-input type="textarea" :rows="4" style="height:500px"  placeholder="please enter content" v-model="form.comment"></el-input></td>
                </tr>
                </table>
            </el-col>
          </el-row>
        </el-tab-pane>
      </el-tabs>
      <div style="float:right;padding-bottom:15px;margin-top:10px">
        <el-button type="success" @click="submit(configQuery.type)">{{ configQuery.type }}</el-button>
      </div>
    </el-card>

    <el-dialog title="Tip" :visible.sync="dialogVisible" size="small">
      <el-alert
        title="Confirm add new cluster config?"
        type="warning"
        center
        show-icon>
      </el-alert>
      <el-form ref="form" :model="form" label-width="150px" style="margin-top: 20px">
        <el-form-item label="configPackName">
          <el-input v-model="form.configPackName"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">Cancel</el-button>
        <el-button type="primary" @click="createConfig">Confirm</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { getConfigList, configUpdate } from '@/api'
export default {
  data() {
    return {
      name: '',
      selectVal: '',
      activeName: 0,
      configQuery: {
        type: ''
      },
      configList: [],
      form: {configPackName: ''},
      options: [{
          value: false,
          label: 'No'
        }, {
          value: true,
          label: 'Yes'
        },
      ],
      idcOptions: [{
          value: 'false',
          label: 'No'
        }, {
          value: 'true',
          label: 'Yes'
        },
      ],
      dialogVisible: false,
      action: '',
      configs: [],
      configDetail: [],
      configServiceId: 0,
    }
  },
  props: {
  },
  components: {

  },
  computed: {
  },
  mounted() {
    this.configQuery = this.$route.query
    this.getConfigList()
  },
  methods: {
    handleClick() {
      this.form = this.configList[this.activeName]
    },
    getConfigList() {
      const params = {
        clusterId: this.configQuery.clusterId,
        configPackId: this.configQuery.configPackId,
        serviceId: this.configQuery.serviceId,
        clusterName: this.configQuery.clusterName
      }
      getConfigList(params).then((res) => {
        var tempObj = {}
        const temp = []
        for(var i=0;i<res.length;i++){
          for(var obj in res[i]){
            if(obj==='content' || obj==='comment' || obj==='buttonName'){
              console.log()
            } else {
              tempObj[obj] = res[i][obj]
            }
            if (obj === 'serviceId') {
              this.configServiceId = res[i][obj]
            }
          }
          temp.push(tempObj)
          tempObj = {}
        }
        this.configDetail = temp
        this.configList = res
        this.configs = res
        this.form = res[0]
      }).catch((err) => {
        console.log(err)
      })
    },
    submit(type) {
      if(type === 'copy Config') {
        this.dialogVisible = true
        this.action = 'copy'
      } else if (type === 'new Config') {
        this.dialogVisible = true
        this.action = 'new'
      } else {
        this.$confirm('Confirm update cluster config?', 'Tip', {
          confirmButtonText: 'Confirm',
          cancelButtonText: 'Cancel',
          type: 'warning'
        }).then(() => {
          const parmas = {
            action: 'update',
            configs: this.configs
          }
          configUpdate(parmas).then((res) => {
            console.log(res)
            this.dialogVisible = false
          }).catch((err) => {
            console.log(err)
          })
        }).catch(() => {
          this.$message({
            type: 'info',
            message: 'Cancelled'
          });
        });
      }
    },
    createConfig() {
      this.dialogVisible = false
      for(var i=0;i<this.configs.length;i++){
        this.configs[i].configPackName = this.form.configPackName
      }
      const parmas = {
        action: this.action,
        configs: this.configs
      }
      configUpdate(parmas).then((res) => {
        const params = {
          clusterId: this.configQuery.clusterId,
          configPackId: res.configPackId,
          serviceId: this.configQuery.serviceId
        }
        this.getConfigTwo(params)
        this.configQuery.type = 'update Config'
        this.dialogVisible = false
      }).catch((err) => {
        console.log(err)
      })
    },
    getConfigTwo(params) {
      getConfigList(params).then((res) => {
        this.configList = res
        this.configs = res
        this.form = res[0]
      }).catch((err) => {
        console.log(err)
      })
    }
  }
}
</script>

<style scoped lang="scss">
.config {
  margin-top: 10px;
}
.title {
    width: 100%;
    margin: 0 auto;
}
.box-card {
    margin-top: 20px;
    height: 100%;
}

</style>>
