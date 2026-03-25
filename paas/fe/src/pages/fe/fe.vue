<template>
  <div class="stored-matrix" v-loading="loading">
    <el-card class="box-card">
      <div class="matrix-container">
        <div class="matrix-header">
          <el-form class="demo-form-inline">
            <el-form-item label="Clusterlist">
              <span>count: {{ feList.count }}</span>
              <el-button class="button" type="primary" @click="newFE"
                >new cluster</el-button
              >
              <el-button
                class="button"
                style="margin-right: 20px"
                type="primary"
                @click="newConf"
                >new config</el-button
              >
            </el-form-item>
          </el-form>
          <el-table
            stripe
            :data="feList.rows"
            class="table"
            :header-cell-style="{ fontWeight: 600, color: '#606266' }"
          >
            <template v-for="(item, index) in dashboardHeader">
              <el-table-column
                align="center"
                :prop="item.text"
                :label="item.text"
                :key="index"
              >
              </el-table-column>
            </template>
            <el-table-column align="center" label="operate" width="430">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="upgrade(row)"
                  >upgrade</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="action(row)"
                  >action</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px; visibility: hidden"
                  type="primary"
                  @click="bindDepartment(row)"
                  >department</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px; visibility: hidden"
                  type="primary"
                  @click="bindDepartment(row)"
                  >department</el-button
                >
                <el-button
                  size="mini"
                  class="firstbutton"
                  type="primary"
                  @click="config(row, 'copy')"
                  >copy config</el-button
                >
                <el-button
                  size="mini"
                  class="opbutton"
                  style="margin-left: 5px"
                  type="primary"
                  @click="config(row, 'update')"
                  >view config</el-button
                >
                <el-button
                  size="mini"
                  class="opbutton"
                  style="margin-left: 5px"
                  type="primary"
                  @click="bindConfig(row)"
                  >bind config</el-button
                >
                <el-button
                  size="mini"
                  class="opbutton"
                  style="margin-left: 5px"
                  type="primary"
                  @click="removeConfig(row)"
                  >remove config</el-button
                >
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            layout="total, prev, pager, next, jumper"
            :total="pagination.total"
            :page-size="pagination.num"
            @current-change="handleTableChange"
            :current-page.sync="pagination.curPage"
          >
          </el-pagination>
        </div>
      </div>
    </el-card>
    <el-card class="package">
      <div class="matrix-container">
        <div class="matrix-header" style="display: flex; justify-content: space-between; align-items: center;">
          <span class="title">Filelist</span>
          <el-dropdown>
            <el-button class="button" type="primary">
              Actions ▼
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="newPackage">build</el-dropdown-item>
              <el-dropdown-item @click.native="openLocalFileDialog">add local file</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </div>
        <package-list
          :fileList="fileList"
          @changePackageList="changePackageList"
        ></package-list>
      </div>
    </el-card>
    <el-dialog title="new FE" size="small" :visible.sync="dialogFE">
      <el-form
        class="create-region"
        :model="feForm"
        :rules="feRules"
        label-width="120px"
        label-position="right"
        ref="feRules"
      >
        <el-form-item label="clusterName:" prop="clusterName">
          <el-input
            placeholder="clusterName"
            v-model="feForm.clusterName"
          ></el-input>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="feForm.packageId" placeholder="please select">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="config:" prop="configPackId">
          <el-select v-model="feForm.configPackId" placeholder="please select">
            <el-option
              v-for="(item, index) in packList"
              :key="index"
              :label="item.configPackName"
              :value="item.configPackId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="regionId:" prop="regionId">
          <el-select v-model="feForm.regionId" @change="selectRegion" placeholder="please select">
            <el-option
              v-for="(item, index) in regionList"
              :key="index"
              :label="item.regionName"
              :value="item.regionId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="assignedPort:" prop="assignedPort">
          <el-input
            placeholder="assignedPort"
            v-model.number="feForm.assignedPort"
          ></el-input>
        </el-form-item>
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="feForm.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="machineId:" prop="machineId">
          <el-radio-group v-model="feForm.machineId" class="checkbox-group">
            <el-radio
              v-for="(item, index) in machineIdList"
              :key="index"
              :label="item.machineId"
            >
              {{ item.ip }}
            </el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFE = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="createFe('feRules')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>

    <el-dialog
      title="bind config"
      size="small"
      :visible.sync="dialogBindSingle"
    >
      <el-form
        :model="bindConfigForm"
        :rules="singleRules"
        ref="nodeForm"
        class="create-group"
        label-position="right"
        label-width="135px"
      >
        <el-form-item
          label="configPackId:"
          prop="configPackId"
          style="margin-top: 12%"
        >
          <el-select v-model="bindConfigForm.configPackId" placeholder="please select">
            <el-option
              v-for="(item, index) in packList"
              :key="index"
              :label="item.configPackName"
              :value="item.configPackId"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogBindSingle = false">Cancel</el-button>
        <el-button
          type="primary"
          style="margin-left: 10px"
          @click="submitSingle()"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <new-package
      :dialogPackage="dialogPackage"
      :serviceId="serviceId"
      @changeDialog="changeDialog"
      @getFile="getFile"
    ></new-package>
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
    <el-dialog title="Add Local File" size="small" :visible.sync="dialogLocalFile">
      <el-form
        class="create-region"
        :model="localFileForm"
        :rules="localFileRules"
        label-width="120px"
        label-position="right"
        ref="localFileRules"
      >
        <el-form-item label="File Name:" prop="fileName">
          <el-input
            placeholder="File Name"
            v-model="localFileForm.fileName"
          ></el-input>
        </el-form-item>
        <el-form-item label="Version:" prop="version">
          <el-input
            placeholder="Version"
            v-model="localFileForm.version"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogLocalFile = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="addLocalFile('localFileRules')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
  </div>
</template>

<script>
import * as apis from "@/api/matrix";
import { getPackList, removeConfig, bindConfig } from "@/api/";
import { mapGetters } from "vuex";
import { createFe } from "@/api/dashboard";
import packageList from "@/components/package-list.vue";
import newPackage from "@/components/new-package.vue";
import updateGrade from "@/components/update-grade.vue";
import updateAction from "@/components/update-action.vue";

export default {
  data() {
    return {
      regionId: "",
      loading: true,
      serviceName: "",
      dashboardHeader: [],
      regionList: [],
      machineIds: [],
      tableList: [],
      storedlist: [],
      idcList: [],
      machineIdList: [],
      strategyList: [],
      operationList: [],
      feList: [],
      packList: [],
      offset: 0,
      page: 0,
      pageSize: 20,
      total: 0,
      fileList: [],
      regionName: "",
      serviceId: 0,
      dialogregion: false,
      dialogPackage: false,
      dialogInfo: false,
      dialogFE: false,
      dialogGrade: false,
      dialogAction: false,
      dialogBindSingle: false,
      dialogLocalFile: false,
      packageHeader: [],
      pagination: {
        total: 0,
        curPage: 0,
        num: 10,
      },
      localFileForm: {
        fileName: '',
        version: '',
      },
      clusterForm: {
        clusterName: "",
        groupSum: "",
        nodeSum: "",
        strategy: "",
        packageId: "",
        operation: "",
        priorityIDC: "",
        machineIdList: [],
        storedId: "",
        storedAuth: "",
        regionId: "",
      },
      feForm: {
        clusterName: "",
        serviceId: "",
        packageId: "",
        machineId: [],
        assignedPort: "",
        operation: "",
        configPackId: "",
      },
      gradeForm: {},
      actionForm: {},
      bindConfigForm: {
        configPackId: "",
        clusterId: "",
      },
      clusterRules: {
        clusterName: [
          {
            required: true,
            message: "clusterName is require",
            trigger: "blur",
          },
        ],
        nodeSum: [
          {
            required: true,
            type: "number",
            message: "nodeSum is not a number",
            trigger: "blur",
          },
        ],
        groupSum: [
          {
            required: true,
            type: "number",
            message: "groupSum is not a number",
            trigger: "blur",
          },
        ],
        strategy: [
          {
            required: true,
            message: "please select strategy",
            trigger: "change",
          },
        ],
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
        priorityIDC: [
          {
            required: true,
            message: "please select priorityIDC",
            trigger: "change",
          },
        ],
        storedId: [
          {
            required: true,
            message: "please select storedId",
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
      },
      feRules: {
        clusterName: [
          {
            required: true,
            message: "clusterName is require",
            trigger: "blur",
          },
        ],
        regionId: [
          {
            required: true,
            message: "please select regionId",
            trigger: "change",
          },
        ],
        packageId: [
          {
            required: true,
            message: "please select packageId",
            trigger: "change",
          },
        ],
        configPackId: [
          {
            required: true,
            message: "please select configPackId",
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
        machineId: [
          {
            required: true,
            message: "please select machineId",
            trigger: "change",
          },
        ],
        assignedPort: [
          {
            required: true,
            type: "number",
            message: "assignedPort is number",
            trigger: "change",
          },
        ],
      },
      singleRules: {
        configPackId: [
          {
            required: true,
            message: "please select configPackId",
            trigger: "change",
          },
        ],
      },
      localFileRules: {
        fileName: [
          {
            required: true,
            message: "File Name is required",
            trigger: "blur",
          },
        ],
        version: [
          {
            required: true,
            message: "Version is required",
            trigger: "blur",
          },
        ],
      },
    };
  },
  inject: ["reload"],
  components: {
    packageList: packageList,
    newPackage: newPackage,
    updateGrade: updateGrade,
    updateAction: updateAction,
  },
  mounted() {
    this.packageHeader = this.tableInfo.packageInfos || [];
    this.strategyList = this.selectInfo.strategyList || [];
    this.operationList = this.selectInfo.dashboardFEOperations || [];
    this.idcList = this.selectInfo.idcOptions || [];
    this.machineIdList = this.selectInfo.machineStatus || [];
    this.dashboardHeader = this.tableInfo.storedInfos || [];
    this.init();
  },
  computed: {
    ...mapGetters(["tableInfo", "selectInfo"]),
    tableHeader() {
      return this.tableInfo.clusterInfos || [];
    },
  },
  methods: {
    init() {
      this.serviceId = this.$route.meta.serviceId || "";
      this.serviceName = this.$route.meta.serviceName || "";
      this.getRegionList();
      this.getFileList();
      this.getFeList();
    },
    handleTableChange(val) {
      this.pagination.curPage = val;
      this.getFeList();
    },
    /**
     * 获取select
     */
    getRegionList() {
      const serviceId = this.$route.meta.serviceId || "";
      const params = {
        serviceId: serviceId,
      };
      getPackList(params).then((res) => {
        this.packList = res.configPackList;
      });
      apis
        .getRegionList(params)
        .then((res) => {
          this.loading = false;
          this.regionList = res.rows || [];
          this.regionId = this.regionList[0] && this.regionList[0].regionId;
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
    },
    /**
     * 获取clusterList
     */
    selectRegion() {
      this.getMachineAll(true);
    },
    newFE() {
      this.dialogFE = true;
      this.machineIdList = [];
    },
    getFeList() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
        page: this.pagination.curPage,
        num: this.pagination.num,
      };
      apis
        .getclusterList(params)
        .then((res) => {
          this.loading = false;
          this.feList = res || [];
          this.feServiceId = res.serviceId || "";
          this.pagination.total = res.count || 0;
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
    },
    createFe(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { serviceId } = this.$route.meta || "";
          this.feForm.serviceId = Number(serviceId);
          createFe(this.feForm)
            .then(() => {
              this.$message.success("success");
              this.dialogFE = false;
              this.pagination.curPage = 0;
              this.getFeList();
            })
            .then(() => {
              this.dialogFE = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    /**
     * create region
     */
    createCluster(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.clusterForm.serviceId = this.serviceId;
          apis
            .clustercreate(this.clusterForm)
            .then(() => {
              this.$message.success("success");
              this.dialogregion = false;
              this.getclusterList();
            })
            .then(() => {
              this.dialogregion = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    newPackage() {
      this.dialogPackage = true;
    },
    changeDialog(val) {
      this.dialogPackage = val;
      this.getFileList();
    },
    getFile() {
      this.getFileList();
    },
    /**
     * 获取fileList
     */
    getFileList() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
      };
      apis
        .getFileList(params)
        .then((res) => {
          this.fileList = res || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    changePackageList() {
      this.getFileList();
    },
    /**
     * go machine
     */
    regionStart(row) {
      const params = {
        regionId: this.regionId,
        clusterId: row.clusterId,
        serviceId: this.serviceId,
        serviceName: this.serviceName,
      };
      this.$router.push({ path: "/machine", query: params });
    },
    regionTask(row) {
      const params = {
        clusterId: row.clusterId,
      };
      this.$router.push({ path: "/task", query: params });
    },
    regionGroup(row) {
      const params = {
        clusterId: row.clusterId,
        serviceId: this.serviceId,
        regionId: row.regionId,
      };
      this.$router.push({ path: "/group-info", query: params });
    },
    selectCluterregion() {
      this.getMachineAll(true);
    },
    /**
     * 获取下拉
     */
    getMachineAll(status) {
      const data = {
        regionId: status ? this.feForm.regionId : this.regionId,
      };
      // const params = {
      //   isDashboard: true
      // }
      Promise.all([apis.getMachineAll(data)])
        .then((res) => {
          this.machineIdList = res[0].rows || [];
          this.storedlist = res[1].rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newConf() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        clusterId: 0,
        configPackId: 0,
        serviceId: serviceId,
        type: "new Config",
      };
      this.$router.push({ path: "/config", query: params });
    },
    config(row, type) {
      const { serviceId } = this.$route.meta || "";
      const params = {
        clusterId: row.clusterId,
        configPackId: row.configPackId,
        serviceId: serviceId,
        type: type + " Config",
      };
      this.$router.push({ path: "/config", query: params });
    },
    // exportInfo(row) {
    //   this.dialogInfo = true
    //   apis.exportInfo(row.clusterId).then((res) => {
    //     console.log(res)
    //   }).catch((err) => {
    //     console.log(err)
    //   })
    // },
    upgrade(row) {
      this.actionForm = row;
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
    getActionList() {},
    removeConfig(row) {
      this.$confirm("Are you sure to remove?", "Confirm", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            clusterId: row.clusterId,
            configPackId: row.configPackId,
          };
          removeConfig(params)
            .then(() => {
              this.getFeList();
              this.$message({
                type: "success",
                message: "Success!",
              });
            })
            .catch((err) => {
              console.log(err);
            });
        })
        .catch(() => {
          this.$message({
            type: "info",
            message: "Cancelled",
          });
        });
    },
    bindConfig(row) {
      this.dialogBindSingle = true;
      this.bindConfigForm.clusterId = row.clusterId;
    },
    submitSingle() {
      const params = {
        clusterId: this.bindConfigForm.clusterId,
        configPackId: this.bindConfigForm.configPackId,
      };
      bindConfig(params)
        .then(() => {
          this.getFeList();
          this.dialogBindSingle = false;
          this.bindConfigForm.configPackId = "";
          this.$message({
            type: "success",
            message: "Success!",
          });
        })
        .catch((err) => {
          console.log(err);
        });
    },
    openLocalFileDialog() {
      this.dialogLocalFile = true;
      this.localFileForm = {
        fileName: '',
        version: '',
      };
    },
    addLocalFile(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { serviceId } = this.$route.meta || "";
          const params = {
            serviceId: Number(serviceId),
            fileName: this.localFileForm.fileName,
            version: this.localFileForm.version,
          };
          apis.addLocalFile(params)
            .then(() => {
              this.$message.success("success");
              this.dialogLocalFile = false;
              this.getFileList();
            })
            .catch((err) => {
              console.log(err);
              this.$message.error("Failed to add local file");
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
  },
};
</script>

<style scoped lang="scss">
@import "@/styles/variable.scss";
.stored-matrix {
  .box-card {
    width: 100%;
    min-height: 200px;
    .pagination-con {
      width: 100%;
      text-align: center;
      .pagination {
        display: inline-block;
        margin-top: 30px;
      }
    }
  }
  .button {
    float: right;
  }
  .package {
    margin-top: 10px;
    .button {
      float: right;
      margin-bottom: 20px;
    }
  }
  .create-region {
    .el-input {
      width: 260px;
    }
    .el-select {
      width: 265px;
    }
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
  .title {
    display: inline-block;
    margin-top: 5px;
  }
  .el-button + .el-button {
    margin-left: 0;
  }
  .opbutton {
    margin-top: 5px;
  }
  .firstbutton {
    margin-top: 5px;
  }
}
</style>
