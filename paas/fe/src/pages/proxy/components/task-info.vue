<template>
  <div class="task-info">
    <el-card class="box-card">
      <div class="proxy-container">
        <div class="proxy-header">
          <el-form class="demo-form-inline">
            <el-form-item label="Proxy Info">
              <el-checkbox v-model="autoStatus">
                auto refresh
              </el-checkbox>
              <el-button class="button" type="primary" @click="alignProxy()"
                >align proxy</el-button
              >
              <el-button
                class="button"
                :style="{ 'margin-right': '20px' }"
                type="primary"
                @click="markOffline"
                >Mark offline nodes</el-button
              >
              <el-button
                class="button"
                :style="{ 'margin-right': '20px' }"
                type="primary"
                @click="removeOffline"
                >Remove offline shards</el-button
              >
              <el-button
                class="button"
                :style="{ 'margin-right': '20px' }"
                type="primary"
                @click="multiUpgrade"
                >multi upgrade</el-button
              >
            </el-form-item>
            <span style="margin-right: 10px">txcloud: {{this.txcloudNums}}</span>
            <span style="margin-right: 10px">tencent: {{this.tencentNums}}</span>
            <span style="margin-right: 10px">ali: {{this.aliNums}}</span>
            <span style="margin-right: 10px">baidu: {{this.bdNums}}</span>
          </el-form>
          <el-table
            stripe
            :data="tableList"
            :header-cell-style="{ fontWeight: 600, color: '#606266' }"
            v-loading="loading"
          >
            <template v-for="(item, index) in tableHeader">
              <el-table-column
                align="center"
                :prop="item.text"
                :label="item.text"
                :key="index"
              >
              </el-table-column>
            </template>
            <el-table-column align="center" label="operate" width="270">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="upgrade(row)"
                  >upgrade</el-button
                >
                <el-button size="mini" type="primary" @click="action(row)"
                  >action</el-button
                >
                <el-button size="mini" type="primary" @click="config(row)"
                  >config</el-button
                >
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-card>
    <update-action
      :actionForm="actionForm"
      :dialogAction="dialogAction"
      @changAction="changAction"
      @getActionList="getActionList"
    ></update-action>
    <update-grade
      :gradeForm="gradeForm"
      :dialogGrade="dialogGrade"
      @changGrade="changGrade"
      @getActionList="getActionList"
    ></update-grade>
    <task class="task"></task>
    <el-dialog title="align proxy" size="small" :visible.sync="dialogProxy">
      <el-form
        :model="proxyForm"
        :rules="proxyRules"
        class="create-cluster"
        ref="proxyRules"
        label-position="right"
        label-width="160px"
      >
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="proxyForm.packageId">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="proxyForm.operation">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="ipList:" prop="ipList">
          <el-input
            type="textarea"
            v-model="proxyForm.ipList"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogProxy = false">Cancel</el-button>
        <el-button type="primary" @click="alignProxySubmit('proxyRules')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog title="config" :visible.sync="dialogConfig">
      <p style="white-space:pre-wrap">{{ configInfo }}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogConfig = false">Cancel</el-button>
        <el-button type="primary" @click="dialogConfig = false"
          >Confirm</el-button
        >
      </span>
    </el-dialog>
    <el-dialog
      title="multi upgrade"
      size="small"
      :visible.sync="dialogMuitiUpgrade"
    >
      <el-form
        :model="multiUpgradeForm"
        :rules="multiUpgradeRules"
        ref="nodeForm"
        class="multi-upgrade"
        label-position="right"
        label-width="150px"
      >
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="multiUpgradeForm.operation">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model.number="multiUpgradeForm.packageId">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="updateConfig:" prop="updateConfig">
          <el-select v-model="multiUpgradeForm.updateConfig">
            <el-option
              v-for="(item, index) in updateConfigOptions"
              :key="index"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="groupNodes:" prop="groupNodes">
          <el-cascader
            :options="groupNodesList"
            :props="{ multiple: true }"
            v-model="multiUpgradeForm.groupNodes"
          ></el-cascader>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogMuitiUpgrade = false">Cancel</el-button>
        <el-button type="primary" @click="multiUpgradeAction()"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
  </div>
</template>

<script>
import task from "@/components/task.vue";
import { mapGetters } from "vuex";
import { getProxyInfo, alignproxy, nodeConfig, multiUpgrade } from "@/api/proxy";
import { getFileList } from "@/api/matrix";
import { getStoredList } from "@/api/";
import { markOffline, deleteOffline } from "@/api/index";
import updateAction from "@/components/update-action.vue";
import updateGrade from "@/components/update-grade.vue";

export default {
  data() {
    return {
      tableHeader: [],
      autoStatus: false,
      loading: true,
      tableList: [],
      actionForm: {
        packageId: -1,
        operation: "",
      },
      timer: "",
      dialogAction: false,
      dialogGrade: false,
      dialogDashboard: false,
      dialogConfig: false,
      gradeForm: {},
      dialogProxy: false,
      operationList: [],
      idcList: [],
      serviceId: "",
      packageList: [],
      storedList: [],
      fileList: [],
      clusterId: "",
      proxyRules: {
        packageId: [
          {
            required: true,
            message: "please select packageId",
            trigger: "change",
          },
        ],
        operation: [
          {
            required: true,
            message: "please select operation",
            trigger: "change",
          },
        ],
        regionId: [
          {
            required: true,
            message: "please select regionId",
            trigger: "change",
          },
        ],
        ipList: [
          {
            required: true,
            message: "please input ip",
            trigger: "blur",
          },
        ],
      },
      dashboardRules: {
        storedId: [
          {
            required: true,
            message: "please select storedId",
            trigger: "change",
          },
        ],
      },
      dashboardForm: {
        storedId: "",
      },
      proxyForm: {
        packageId: "",
        nodeNum: "",
        storedAuth: "",
        operation: "",
        idc: "",
        ipList: "",
      },
      configInfo: "",
      nodeNumOptions: [
        {
          value: 1,
          label: "1",
        },
        {
          value: 2,
          label: "2",
        },
        {
          value: 3,
          label: "3",
        },
        {
          value: 4,
          label: "4",
        },
        {
          value: 5,
          label: "5",
        },
      ],
      multiUpgradeForm: {
        operation: "",
        packageId: "",
        updateConfig: "false",
        groupNodes: [],
      },
      multiUpgradeRules: {
        operation: [
          {
            required: true,
            message: "please select operation",
            trigger: "change",
          },
        ],
      },
      dialogMuitiUpgrade: false,
      updateConfigOptions: [
        {
          value: "true",
          label: "Yes",
        },
        {
          value: "false",
          label: "No",
        },
      ],
      groupNodesList: [],
      groupList: [],
      txcloudNums: 0,
      txshNums: 0,
      txgzNums: 0,
      tencentNums: 0,
      aliNums: 0,
      bdNums: 0,
    };
  },
  computed: {
    ...mapGetters(["tableInfo", "selectInfo"]),
  },
  components: {
    task: task,
    updateGrade: updateGrade,
    updateAction: updateAction,
  },
  beforeRouteLeave(to, from, next) {
    if (this.timer) clearInterval(this.timer);
    next();
  },
  mounted() {
    this.tableHeader = this.tableInfo.proxyInfos || [];
    this.operationList = this.selectInfo.proxyOperations || [];
    this.idcList = this.selectInfo.idcOptions || [];
    this.serviceId = this.$route.query.serviceId - 0 || "";
    this.clusterId = this.$route.query.clusterId - 0 || "";
    this.getProxyInfo();
  },
  methods: {
    getProxyInfo() {
      this.loading = true;
      const params = {
        clusterId: this.clusterId,
      };
      getProxyInfo(params)
        .then((res) => {
          this.groupNodesList = [];
          this.tableList = res.rows;
          this.loading = false;
          this.groupList = res.rows;
          const tx6Children = []
          const tx5Children = []
          const aliChildren = []
          const bdChildren = []
          const txshChildren = []
          const txgzChildren = []
          this.txcloudNums = 0
          this.txshNums = 0
          this.txgzNums = 0
          this.tencentNums = 0
          this.aliNums = 0
          this.bdNums = 0
          this.groupList.map((element) => {
            if (element.nodeStatus == 'online') {
              if (element.idc == "txcloud") {
                this.txcloudNums++
                tx6Children.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
              if (element.idc == "tencent") {
                this.tencentNums++
                tx5Children.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
              if (element.idc == "ali") {
                this.aliNums++
                aliChildren.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
              if (element.idc == "baidu") {
                this.bdNums++
                bdChildren.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
              if (element.idc == "txsh") {
                this.txshNums++
                txshChildren.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
              if (element.idc == "txgz") {
                this.txgzNums++
                txgzChildren.push({ value: element.nodeId, label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}` });
              }
            }
          });
          if (tx6Children.length > 0) {
            this.groupNodesList.push({value: 1, label:`txcloud`, children:tx6Children})
          }
          if (txshChildren.length > 0) {
            this.groupNodesList.push({value: 1, label:`txsh`, children:txshChildren})
          }
          if (txgzChildren.length > 0) {
            this.groupNodesList.push({value: 1, label:`txgz`, children:txgzChildren})
          }
          if (tx5Children.length > 0) {
            this.groupNodesList.push({value: 2, label:`tencent`, children:tx5Children})
          }
          if (aliChildren.length > 0) {
            this.groupNodesList.push({value: 3, label:`ali`, children:aliChildren})
          }
          if (bdChildren.length > 0) {
            this.groupNodesList.push({value: 4, label:`baidu`, children:bdChildren})
          }
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
    },
    autoFrfresh() {
      if (this.timer) clearInterval(this.timer);
      this.timer = setInterval(() => {
        if (this.autoStatus) this.getProxyInfo();
      }, 5000);
    },
    getFileList() {
      const params = {
        serviceId: this.serviceId,
      };
      getFileList(params).then((res) => {
        this.fileList = res || [];
      });
    },
    getStoredList() {
      const params = {
        isDashboard: true,
      };
      getStoredList(params)
        .then((res) => {
          this.storedList = res.rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    // dashboardSubmit(formName) {
    //   this.$refs[formName].validate((valid) => {
    //     if (valid) {
    //       const params = {
    //         clusterId: this.clusterId,
    //         ...this.dashboardForm
    //       }
    //       switchdashboard(params).then(() => {
    //         this.dialogDashboard = false
    //         this.getProxyInfo()
    //         this.$message.success('success')
    //       }).catch(() => {
    //         this.dialogDashboard = false
    //       })
    //     } else {
    //       this.$message.error('请输入或选择必填项！')
    //       return false;
    //     }
    //   })
    // },
    alignProxy() {
      this.dialogProxy = true;
      this.proxyForm = {
        packageId: "",
        nodeNum: "",
        storedAuth: "",
        operation: "",
        idc: "",
        ipList: "",
      };
      this.getFileList();
    },
    alignProxySubmit(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          if (!this.proxyForm.nodeNum) {
            this.proxyForm.nodeNum = 0;
          }
          const params = {
            clusterId: this.clusterId,
            serviceId: this.serviceId,
            ...this.proxyForm,
          };
          alignproxy(params)
            .then(() => {
              this.dialogProxy = false;
              this.getProxyInfo();
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogProxy = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    upgrade(row) {
      this.actionForm = Object.assign(this.actionForm, row);
      this.dialogAction = true;
    },
    changAction(val) {
      this.dialogAction = val;
    },
    action(row) {
      this.gradeForm = row;
      this.dialogGrade = true;
    },
    changGrade(val) {
      this.dialogGrade = val;
    },
    getActionList() {
      this.getProxyInfo();
    },
    config(row) {
      this.dialogConfig = true;
      const params = {
        clusterId: row.clusterId,
        groupId: row.groupId,
        nodeId: row.nodeId,
      };
      nodeConfig(params)
        .then((res) => {
          this.configInfo = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    markOffline() {
      this.$confirm("Do you want to detect offline nodes in the current cluster?", "Notice", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "warning",
      })
        .then(() => {
          markOffline({ clusterId: this.$route.query.clusterId }).then(() => {
            this.$message.success("Operation successful");
            this.getProxyInfo();
          });
        })
        .catch(() => {});
    },
    removeOffline() {
      this.$confirm("Do you want to delete offline nodes in the current cluster?", "Notice", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "warning",
      })
        .then(() => {
          deleteOffline({ clusterId: this.$route.query.clusterId }).then(() => {
            this.$message.success("Operation successful");
            this.getProxyInfo();
          });
        })
        .catch(() => {});
    },
    multiUpgrade() {
      this.dialogMuitiUpgrade = true;
      if (this.operationList.length > 0) {
        if (!this.operationList.includes(this.multiUpgradeForm.operation)) {
          this.multiUpgradeForm.operation = this.operationList[0];
        }
      }
      this.multiUpgradeForm.groupNodes = [];
      this.getFileList();
    },
    multiUpgradeAction() {
      this.dialogMuitiUpgrade = false;
      const nodesMap = this.multiUpgradeForm.groupNodes.reduce((prev, cur) => {
        if (prev.has(`${cur[0]}`)) {
          prev.set(`${cur[0]}`, `${prev.get("" + cur[0])},${cur[1]}`);
        } else {
          prev.set(`${cur[0]}`, `${cur[1]}`);
        }
        return prev;
      }, new Map());
      let nodes = Array.from(nodesMap).map((item) => {
        return { key: Number(item[0]), value: item[1] };
      });
      const params = {
        clusterId: Number(this.$route.query.clusterId),
        packageId: this.multiUpgradeForm.packageId,
        operation: this.multiUpgradeForm.operation,
        updateConfig: this.multiUpgradeForm.updateConfig,
        groupNodes: nodes,
      };
      multiUpgrade(params)
        .then(() => {
          this.getProxyInfo();
        })
        .catch((err) => {
          console.log(err);
        });
    },
  },
  watch: {
    autoStatus(val) {
      if (val) this.autoFrfresh();
    },
  },
};
</script>

<style scoped lang="scss">
.task-info {
  .button {
    float: right;
    margin-left: 20px;
  }
  .el-input {
    width: 260px;
  }
  .el-select {
    width: 260px;
  }
  .task {
    margin-top: 10px;
  }
  .checkbox-group {
    display: block;
    margin: 40px 0 40px -100px;
    height: 40px;
    .el-radio {
      margin-right: 20px;
      margin-left: 0;
      width: 100px;
    }
    .el-checkbox {
      margin-right: 20px;
      margin-left: 0;
      width: 100px;
    }
  }
}
</style>
