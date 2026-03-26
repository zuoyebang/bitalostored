<template>
  <div class="stored-proxy" v-loading="loading">
    <el-card class="box-card">
      <div class="proxy-container">
        <div class="proxy-header">
          <el-form class="demo-form-inline">
            <el-form-item label="Clusterlist" class="form-item">
              <span style="margin-right: 10px"
                >count: {{ tableList.count }}</span
              >
              <el-select
                class="select"
                v-model="selectVal"
                placeholder="please select"
                @change="selectRegion"
              >
                <el-option
                  v-for="(item, index) in departmentList"
                  :key="index"
                  :label="item"
                  :value="item"
                ></el-option>
              </el-select>
              <span style="margin-left: 10px;margin-right: 10px"
                >cluster name</span
              >
              <el-select v-model="searchParams.name" placeholder="Please select" @change="search" filterable>
                <el-option
                  v-for="(item, index) in clusterNames"
                  :key="index"
                  :label="item"
                  :value="item"
                ></el-option>
              </el-select>
              <el-button class="button" type="primary" @click="newCluster"
                >new cluster</el-button
              >
              <el-button
                class="button"
                style="margin-right: 20px"
                type="primary"
                @click="replaceConf"
                >replace config</el-button
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
            :data="tableList.rows"
            :header-cell-style="{ fontWeight: 600, color: '#606266' }"
          >
            <template v-for="(item, index) in tableHeader">
              <el-table-column
                align="center"
                :prop="item.text"
                :label="item.text"
                :key="index"
                v-if="item.text !== 'dashboardAddr'"
              >
              </el-table-column>
              <el-table-column
                align="center"
                :label="item.text"
                :key="index"
                :show-overflow-tooltip="true"
                width="200"
                v-else
              >
                <template slot-scope="{ row }">
                  <a :href="row.dashboardAddr" target="_blank">{{
                    row.dashboardAddr
                  }}</a>
                </template>
              </el-table-column>
            </template>

            <el-table-column align="center" label="operate" width="500">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="taskinfo(row)"
                  >proxy</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="offline(row)"
                  >offline</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="exportInfo(row)"
                  >export</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="bindDepartment(row)"
                  >bind department</el-button
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
      <div class="proxy-container">
        <div class="proxy-header" style="display: flex; justify-content: space-between; align-items: center;">
          <span class="title">Filelist</span>
          <el-dropdown>
            <el-button class="button" type="primary">
              Action ▼
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="newPackage">git build</el-dropdown-item>
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
    <el-dialog title="new proxy" size="small" :visible.sync="dialogCluster">
      <el-form
        :model="clusterForm"
        :rules="clusterRules"
        class="create-cluster"
        ref="clusterForm"
        label-position="right"
        label-width="140px"
      >
        <el-form-item label="clusterName" prop="clusterName">
          <el-input v-model="clusterForm.clusterName"></el-input>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="clusterForm.packageId">
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
          <el-select v-model="clusterForm.configPackId" placeholder="please select">
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
          <el-select v-model="clusterForm.regionId">
            <el-option
              v-for="(item, index) in regionList"
              :key="index"
              :label="item.regionName"
              :value="item.regionId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="storedId:" prop="storedId">
          <el-select v-model="clusterForm.storedId">
            <el-option
              v-for="(item, index) in storedList"
              :key="index"
              :label="item.clusterName"
              :value="item.storedId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="clusterForm.operation">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <!---
        <el-form-item label="Node Count:" prop="nodeNum">
          <el-select v-model="clusterForm.nodeNum" placeholder="Please select">
            <el-option
              v-for="item in nodeNumOptions"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        -->
        <el-form-item label="IDC:" prop="idc">
          <el-select v-model="clusterForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="assignedPort:" prop="assignedPort">
          <el-input
            v-model.number="clusterForm.assignedPort"
            @change="checkPort(clusterForm.assignedPort,'assigned')"
          ></el-input>
        </el-form-item>
        <el-form-item label="assignedAdminPort:" prop="assignedAdminPort">
          <el-input
          v-model.number="clusterForm.assignedAdminPort"
          @change="checkPort(clusterForm.assignedAdminPort,'assignedAdmin')"
          ></el-input>
        </el-form-item>
        <el-form-item label="ipList:" prop="ipList">
          <el-input
            type="textarea"
            v-model="clusterForm.ipList"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogCluster = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="createCluster('clusterForm')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog title="export info" :visible.sync="dialogInfo" size="small">
      <p style="white-space: pre-wrap">{{ grafanaInfo }}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogInfo = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="dialogInfo = false"
          >Confirm</el-button
        >
      </span>
    </el-dialog>
    <el-dialog title="bind department" :visible.sync="dialogBind" size="small">
      <el-form :model="bindForm">
        <el-form-item label="department:">
          <el-input v-model="bindForm.department"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogBind = false">Cancel</el-button>
        <el-button style="margin-left: 10px" type="primary" @click="bind"
          >Confirm</el-button
        >
      </span>
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
          @click="bindSingleOk"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <new-package
      :dialogPackage="dialogPackage"
      :serviceId="2"
      @changeDialog="changeDialog"
      @getFile="getFile"
    ></new-package>
    <replace-config
      :dialogRepalce="dialogRepalce"
      @changeDialogRepalce="changeDialogRepalce"
    ></replace-config>
    <el-dialog title="Add Local File" size="small" :visible.sync="dialogLocalFile">
      <el-form
        :model="localFileForm"
        :rules="localFileRules"
        class="create-cluster"
        ref="localFileRules"
        label-position="right"
        label-width="140px"
      >
        <el-form-item label="File Name" prop="fileName">
          <el-input v-model="localFileForm.fileName"></el-input>
        </el-form-item>
        <el-form-item label="Version" prop="version">
          <el-input v-model="localFileForm.version"></el-input>
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
import {
  getclusterList,
  getFileList,
  exportInfo,
  getRegionList,
  addLocalFile as postAddLocalFile,
} from "@/api/matrix";
import {
  getServiceList,
  getStoredList,
  bindDepartment,
  departmentList,
  offline,
  getPackList,
  removeConfig,
  bindConfig,
} from "@/api/";
import { getClusterNames } from "@/api/resource";
import { createCluster as postCreateCluster, checkPort } from "@/api/proxy";
import { mapGetters } from "vuex";
import packageList from "@/components/package-list.vue";
import newPackage from "@/components/new-package.vue";
import replaceConfig from "../../components/replaceConfig.vue";
import openWindow from "@/utils/openWindow";

export default {
  data() {
    return {
      regionId: "",
      regionList: [],
      searchParams:{
        name: ''
      },
      clusterNames: [],
      tableHeader: [],
      tableList: [],
      serviceList: [],
      operationList: [],
      storedList: [],

      departmentList: [],
      packList: [],
      loading: true,
      serviceId: 2,
      offset: 0,
      page: 0,
      pageSize: 20,
      total: 100,
      fileList: [],
      idcList: [],
      dialogCluster: false,
      dialogPackage: false,
      dialogInfo: false,
      dialogBind: false,
      dialogRepalce: false,

      dialogBindSingle: false,
      pagination: {
        total: 0,
        curPage: 0,
        num: 10,
      },
      clusterForm: {
        clusterName: "",
        packageId: "",
        storedId: "",
        assignedPort: "",
        assignedAdminPort: "",
        operation: "",
        storedAuth: "",
        regionId: "",
        isStored: "false",
        configPackId: "",
        nodeNum: "",
        idc: "",
        ipList: "",
      },
      bindForm: {
        department: "",
      },

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
        assignedPort: [
          {
            required: true,
            type: "number",
            message: "assignedPort is not a number",
            trigger: "change",
          },
        ],
        assignedAdminPort: [
          {
            required: true,
            type: "number",
            message: "assignedAdminPort is not a number",
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
        nodeNum: [
          {
            required: false,
            message: "please select nodeNum",
            trigger: "change",
          },
        ],
        ipList: [
          {
            required: true,
            message: "please input ip list",
            trigger: "blur",
          },
        ],
        idc: [
          {
            required: true,
            message: "please select IDC",
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
      option: [
        {
          value: "false",
          label: "No",
        },
        {
          value: "true",
          label: "Yes",
        },
      ],
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
      grafanaInfo: "",
      oldConfig: "",
      newConfig: "",
      selectVal: "",
      dialogLocalFile: false,
      localFileForm: {
        fileName: '',
        version: '',
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
  mounted() {
    this.tableHeader = this.tableInfo.clusterInfos || [];
    this.operationList = this.selectInfo.proxyOperations || [];
    this.idcList = this.selectInfo.idcOptions || [];
    this.init();
  },
  components: {
    packageList: packageList,
    newPackage: newPackage,
    replaceConfig: replaceConfig,
  },
  computed: {
    ...mapGetters(["tableInfo", "selectInfo"]),
  },
  methods: {
    init() {
      this.serviceId = this.$route.meta.serviceId || "";
      this.getRegionList();
      this.getServiceList();
      const params = {
        service_id: this.serviceId,
      }
      getClusterNames(params)
      .then(data => {
        this.clusterNames = data;
      });
    },
    search() {
      this.selectRegion()
    },
    /**
     * 获取select
     */
    getRegionList() {
      const serviceId = this.$route.meta.serviceId || "";
      const params = {
        serviceId: serviceId,
      };
      this.serviceId = serviceId;
      departmentList(params)
        .then((res) => {
          this.departmentList = res || [];
        })
        .catch((err) => {
          console.log(err);
          this.loading = false;
        });
      getPackList(params).then((res) => {
        this.packList = res.configPackList;
      });
      getRegionList(params)
        .then((res) => {
          this.loading = false;
          this.regionList = res.rows || [];
          this.regionId = this.regionList[0] && this.regionList[0].regionId;
          this.pagination.curPage = 0;
          this.selectRegion();
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
    },
    /**
     * 处理分页
     */
    handleTableChange(val) {
      this.pagination.curPage = val;
      this.selectRegion();
    },
    /**
     * 获取clusterList
     */
    selectRegion() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
        department: this.selectVal,
        page: this.pagination.curPage,
        num: this.pagination.num,
        clusterName: this.searchParams.name,
      };
      getclusterList(params)
        .then((res) => {
          this.tableList = res || [];
          this.total = res.count || 0;
          this.pagination.total = res.count || 0;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    /**
     * 获取下拉常量
     */

    newCluster() {
      this.dialogCluster = true;
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
    /**
     * create region
     */
    createCluster(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { serviceId } = this.$route.meta || "";
          this.clusterForm.serviceId = serviceId;
          postCreateCluster(this.clusterForm)
            .then(() => {
              this.dialogCluster = false;
              this.getRegionList();
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogCluster = false;
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
    changePackageList() {
      this.getFileList();
    },
    taskinfo(row) {
      const params = {
        clusterId: row.clusterId,
        serviceId: 2,
        regionId: row.regionId,
      };
      openWindow({ path: "/task-info", query: params });
    },
    /**
     * getServiceList
     */
    async getServiceList() {
      const data = {
        serviceType: "proxy",
      };
      try {
        const service = await getServiceList(data);
        this.serviceList = service.rows || [];
        //this.serviceId = service.rows[0].serviceId || "";
        this.getFileList();
      } catch (err) {
        console.log(err);
      }
    },
    getFileList() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
      };
      getFileList(params).then((res) => {
        this.fileList = res || [];
      });
    },
    selectService() {
      this.getFileList();
    },
    /**
     * go machine
     */
    regionStart(row) {
      const params = {
        regionId: this.regionId,
        clusterId: row.clusterId,
      };
      this.$router.push({ path: "/machine", query: params });
    },

    exportInfo(row) {
      this.dialogInfo = true;
      const params = {
        clusterId: row.clusterId,
        clusterName: row.clusterName,
      };
      exportInfo(params)
        .then((res) => {
          this.grafanaInfo = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    config(row, type) {
      const { serviceId } = this.$route.meta || "";
      const params = {
        clusterId: row.clusterId,
        configPackId: row.configPackId,
        serviceId: serviceId,
        type: type + " Config",
      };
      openWindow({ path: "/config", query: params });
    },
    newConf() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        clusterId: 0,
        configPackId: 0,
        serviceId: serviceId,
        type: "new Config",
      };
      openWindow({ path: "/config", query: params });
    },
    checkPort(port,type) {
      const params = {
        regionId: Number(this.clusterForm.regionId),
        port: port,
      };
      checkPort(params)
        .then((res) => {
          if (!res.isLegal) {
            this.$alert(type+" port is occupied", "Alert", {
              confirmButtonText: "OK",
              type: "warning",
            });
          }
        })
        .catch((err) => {
          console.log(err);
        });
    },
    bindDepartment(row) {
      this.dialogBind = true;
      this.bindForm.clusterId = row.clusterId;
    },
    bind() {
      bindDepartment(this.bindForm)
        .then(() => {
          this.$message({
            type: "success",
            message: "Binding successful",
          });
          this.init();
        })
        .catch((err) => {
          console.log(err);
        });
      this.dialogBind = false;
    },
    offline(row) {
      this.$confirm("Are you sure to offline this cluster configuration?", "Confirmation", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            clusterId: row.clusterId,
          };
          offline(params)
            .then(() => {
              this.$message({
                type: "success",
                message: "Offline successful!",
              });
              this.init();
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
    replaceConf() {
      this.dialogRepalce = true;
    },
    changeDialogRepalce(val) {
      this.dialogRepalce = val;
    },


    removeConfig(row) {
      this.$confirm("Are you sure to remove?", "Confirmation", {
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
              this.selectRegion();
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
          this.selectRegion();
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
          const params = {
            serviceId: this.serviceId,
            fileName: this.localFileForm.fileName,
            version: this.localFileForm.version,
          };
          postAddLocalFile(params)
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
.stored-proxy {
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
  .create-cluster {
    .el-input {
      width: 260px;
    }
    .el-select {
      width: 265px;
    }
  }
  .package-table {
    margin-top: 10px;
  }
  .button {
    float: right;
  }
  .package-title {
    margin-top: -20px;
  }
  .package {
    margin-top: 15px;
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
