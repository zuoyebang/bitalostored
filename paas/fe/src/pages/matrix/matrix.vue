<template>
  <div class="stored-matrix" v-loading="loading">
    <el-card class="box-card">
      <div class="matrix-container">
        <div class="matrix-header">
          <el-form class="demo-form-inline">
            <el-form-item label="Clusterlist">
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
              <el-select v-model="searchParams.name" placeholder="choose" @change="search" filterable>
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
              <el-button class="button" style="margin-right: 20px" type="primary" @click="openCreateClusterGroupDialog">
                quick deploy
              </el-button>
              <el-button
                class="button"
                style="margin-right: 20px"
                type="primary"
                @click="showDialogSyncAll"
                >sync all</el-button
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
            <el-table-column align="center" label="monitor" width="100">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="grafana(row)"
                  >grafana</el-button
                >
              </template>
            </el-table-column>
            <el-table-column align="center" label="operate" width="470">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="regionGroup(row)"
                  >group</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="regionTask(row)"
                  >task</el-button
                >
                <el-button
                  size="mini"
                  style="margin-left: 5px"
                  type="primary"
                  @click="regionStart(row)"
                  >balance</el-button
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
    <el-dialog title="new bitalos" size="small" :visible.sync="dialogregion">
      <el-form
        :model="clusterForm"
        :rules="clusterRules"
        ref="clusterForm"
        class="create-region"
        label-position="right"
        label-width="150px"
      >
        <el-form-item label="clusterName:" prop="clusterName">
          <el-input
            placeholder="clusterName"
            v-model="clusterForm.clusterName"
          ></el-input>
        </el-form-item>
        <el-form-item label="groupSum:(max 24)" prop="groupSum">
          <el-input
            placeholder="groupSum"
            v-model.number="clusterForm.groupSum"
          ></el-input>
        </el-form-item>
        <el-form-item label="nodeSum:(max 7, each shard node sum)" prop="nodeSum">
          <el-input
            placeholder="nodeSum"
            v-model.number="clusterForm.nodeSum"
          ></el-input>
        </el-form-item>
        <el-form-item label="strategy:" prop="strategy">
          <el-select v-model="clusterForm.strategy" placeholder="please select">
            <el-option
              v-for="(item, index) in strategyList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="server:" prop="server">
          <el-radio-group v-model="clusterForm.server">
            <!-- <el-radio label="matrix" value="matrix"></el-radio> -->
            <el-radio label="bitalos" value="bitalos"></el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="clusterForm.packageId" placeholder="please select">
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
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="clusterForm.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="regionId:" prop="regionId">
          <el-select
            v-model="clusterForm.regionId"
            @change="selectCluterregion"
            filterable
            placeholder="please select"
          >
            <el-option
              v-for="(item, index) in regionList"
              :key="index"
              :label="item.regionName"
              :value="item.regionId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="priorityIDC:" prop="priorityIDC">
          <el-select v-model="clusterForm.priorityIDC" placeholder="please select">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="storedId:" prop="storedId">
          <el-select v-model="clusterForm.storedId" placeholder="please select">
            <el-option
              v-for="(item, index) in storedlist"
              :key="index"
              :label="item.clusterName"
              :value="item.storedId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <!--
         <el-form-item label="storedAuth:" prop="storedAuth">
          <el-input placeholder="storedAuth" v-model="clusterForm.storedAuth"></el-input>
        </el-form-item>
        -->
        <!--
        <el-form-item label="machineIdList:">
          <el-checkbox-group
            v-model="clusterForm.machineIdList"
            class="checkbox-group"
          >
            <el-checkbox
              v-for="(item, index) in machineIdList"
              :key="index"
              :label="item.machineId"
            >
              {{ item.ip }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        -->
        <el-form-item label="ipList:" prop="ipList">
          <el-input
            type="textarea"
            v-model="clusterForm.ipList"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogregion = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="createCluster('clusterForm')"
          >Confirm</el-button
        >
      </span>
    </el-dialog>
    <el-dialog title="export info" :visible.sync="dialogInfo" size="small">
      <p style="white-space: pre-wrap">{{ grafanaInfo }}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogInfo = false">Cancel</el-button>
        <el-button
          type="primary"
          style="margin-left: 10px"
          @click="dialogInfo = false"
          >Confirm</el-button
        >
      </span>
    </el-dialog>
    <el-dialog title="quick deploy" :visible.sync="dialogcreateclustergroup">
      <el-form
        :model="clusterGroupForm"
        :rules="clusterRules"
        ref="clusterGroupForm"
        class="create-region"
        label-position="right"
        label-width="160px"
      >
        <el-form-item label="clusterName:" prop="clusterName">
          <el-input
            placeholder="clusterName"
            v-model="clusterGroupForm.clusterName"
          ></el-input>
        </el-form-item>

        <el-form-item label="budgetUnit:" prop="budgetUnit">
          <el-input
            placeholder="budgetUnit"
            v-model="clusterGroupForm.budgetUnit"
          ></el-input>
        </el-form-item>
        
        <el-form-item label="nodePerGroup:" prop="nodePerGroup">
          <el-input
            placeholder="3"
            v-model.number="clusterGroupForm.nodePerGroup"
          ></el-input>
        </el-form-item>

        <el-form-item label="stored-version:" prop="serverCosFileId">
          <el-select v-model="clusterGroupForm.serverCosFileId" placeholder="please select">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>

        
        <el-form-item label="regionId:" prop="regionId">
          <el-select
            v-model="clusterGroupForm.regionId"
            filterable
            placeholder="please select"
          >
            <el-option
              v-for="(item, index) in regionList"
              :key="index"
              :label="item.regionName"
              :value="item.regionId"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogcreateclustergroup = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="createClusterGroupSubmit('clusterGroupForm')"
          >Confirm</el-button
        >
      </span>
    </el-dialog>

    <el-dialog title="sync all clusters" :visible.sync="dialogSyncAllInfo" size="small">
      <p style="white-space: pre-wrap">{{ syncAllResp }}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogSyncAllInfo = false">Cancel</el-button>
        <el-button
          type="primary"
          style="margin-left: 10px"
          @click="confirmSyncAll"
          >Confirm</el-button
        >
      </span>
    </el-dialog>

    <new-package
      :dialogPackage="dialogPackage"
      :serviceId="serviceId"
      @changeDialog="changeDialog"
      @getPackage="getPackage"
    ></new-package>
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
          <el-select v-model="bindConfigForm.configPackId">
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
    <replace-config
      :dialogRepalce="dialogRepalce"
      @changeDialogRepalce="changeDialogRepalce"
    ></replace-config>
    <el-dialog title="Add Local File" size="small" :visible.sync="dialogLocalFile">
      <el-form
        :model="localFileForm"
        :rules="localFileRules"
        ref="localFileRules"
        class="create-region"
        label-position="right"
        label-width="100px"
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
import {
  getStoredList,
  bindDepartment,
  departmentList,
  offline,
  getPackList,
  removeConfig,
  bindConfig,
} from "@/api/";
import { mapGetters } from "vuex";
import { getClusterNames } from "@/api/resource";
import packageList from "@/components/package-list.vue";
import newPackage from "@/components/new-package.vue";
import replaceConfig from "../../components/replaceConfig.vue";
import openWindow from "@/utils/openWindow";

export default {
  data() {
    return {
      regionId: "",
      loading: true,
      serviceName: "",
      clusterNames: [],
      searchParams:{
        name: ''
      },
      regionList: [],
      machineIds: [],
      tableList: [],
      storedlist: [],
      idcList: [],
      machineIdList: [],
      strategyList: [],
      operationList: [],
      departmentList: [],
      packList: [],
      offset: 0,
      fileList: [],
      regionName: "",
      serviceId: 0,
      dialogregion: false,
      dialogPackage: false,
      dialogInfo: false,
      dialogSyncAllInfo: false,
      dialogBind: false,
      dialogRepalce: false,
      dialogBindSingle: false,
      dialogcreateclustergroup: false,
      packageHeader: [],
      clusterForm: {
        clusterName: "",
        groupSum: "",
        nodeSum: "",
        strategy: "",
        server: "",
        packageId: "",
        operation: "",
        priorityIDC: "",
        machineIdList: [],
        storedId: "",
        storedAuth: "",
        regionId: "",
        configPackId: "",
        ipList: "",
      },
      clusterGroupForm: {
        clusterName: "",
        regionId: "",
        serverCosFileId:"",
        budgetUnit: "",
        nodePerGroup:"",
      },
      bindForm: {
        department: "",
      },
      bindConfigForm: {
        configPackId: "",
        clusterId: "",
      },
      pagination: {
        total: 0,
        curPage: 0,
        num: 10,
      },
      newConfig: "",
      oldConfig: "",
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
        server: [
          {
            required: true,
            message: "please select server",
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
        ipList: [
          {
            required: true,
            message: "please input ip list",
            trigger: "blur",
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
      grafanaInfo: "",
      syncAllResp: "",
      selectVal: "",
      updateGraId: 0,
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
  inject: ["reload"],
  components: {
    packageList: packageList,
    newPackage: newPackage,
    replaceConfig: replaceConfig,
  },
  mounted() {
    this.packageHeader = this.tableInfo.packageInfos || [];
    this.strategyList = this.selectInfo.strategyList || [];
    this.operationList = this.selectInfo.matrixOperations || [];
    this.idcList = this.selectInfo.idcOptions || [];
    this.machineIdList = this.selectInfo.machineStatus || [];
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
      this.loading = true;
      const params = {
        serviceId: this.serviceId,
      };
      departmentList(params)
        .then((res) => {
          this.departmentList = res || [];
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
      getPackList(params).then((res) => {
        this.packList = res.configPackList;
      });
      this.pagination.curPage = 0;
      this.selectRegion();
      apis
        .getRegionList(params)
        .then((res) => {
          this.regionList = res.rows || [];
          this.regionId = this.regionList[0] && this.regionList[0].regionId;
        })
        .catch((err) => {
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
      const params = {
        department: this.selectVal,
        serviceId: this.serviceId,
        page: this.pagination.curPage,
        num: this.pagination.num,
        clusterName: this.searchParams.name,
      };
      apis
        .getclusterList(params)
        .then((res) => {
          this.loading = false;
          this.tableList = res || [];
          this.pagination.total = res.count || 0;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newCluster() {
      this.dialogregion = true;
      this.getMachineAll();
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
          this.$message.error("Please enter or select the required fields!");
          return false;
        }
      });
    },
    createClusterGroupSubmit(formName) {
      //console.log(this.clusterGroupForm);
      this.$refs[formName].validate((valid) => {
        if (valid) {
            apis.clustercreategroup(this.clusterGroupForm)
            .then(() => {
              this.$message.success("success");
              this.dialogcreateclustergroup = false;
            })
            .then(() => {
              this.dialogcreateclustergroup = false;
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
      const params = {
        serviceId: this.serviceId,
        clusterId: -1,
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
      openWindow({ path: "/group-info", query: params });
    },
    selectCluterregion() {
      this.getMachineAll(true);
    },
    /**
     * 获取下拉
     */
    getMachineAll(status) {
      const data = {
        regionId: status ? this.clusterForm.regionId : this.regionId,
      };
      const params = {
        isDashboard: true,
      };
      Promise.all([apis.getMachineAll(data), getStoredList(params)])
        .then((res) => {
          this.machineIdList = res[0].rows || [];
          this.storedlist = res[1].rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newConf() {
      const params = {
        clusterId: 0,
        configPackId: 0,
        serviceId: this.serviceId,
        type: "new Config",
      };
      openWindow({ path: "/config", query: params });
    },
    openCreateClusterGroupDialog() {
      this.dialogcreateclustergroup = true;
    },

    config(row, type) {
      let params = {};
      if (row.clusterId) {
        params = {
          clusterId: row.clusterId,
          configPackId: row.configPackId,
          serviceId: this.serviceId,
          type: type + " Config",
        };
      } else {
        params = {
          clusterId: 0,
          configPackId: row.configPackId,
          serviceId: this.serviceId,
          type: type + " Config",
        };
      }
      openWindow({ path: "/config", query: params });
    },
    getPackage() {
      this.getFileList();
    },
    exportInfo(row) {
      this.dialogInfo = true;
      const params = {
        clusterId: row.clusterId,
        clusterName: row.clusterName,
      };
      apis
        .exportInfo(params)
        .then((res) => {
          this.grafanaInfo = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    showDialogSyncAll() {
      this.syncAllResp = "";
      this.dialogSyncAllInfo = true;
    },
    confirmSyncAll() {
      apis
        .syncAll()
        .then((res) => {
          this.syncAllResp = res;
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
    grafana(row) {
      this.updateGraId = row.clusterId;
      this.$confirm("update cluster grafana?", "提示", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            clusterId: row.clusterId,
          };
          apis
            .updateGrafana(params)
            .then((res) => {
              console.log(res);
              this.dialogupdateGra = false;
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
          this.dialogupdateGra = false;
        });
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
    .el-form-item__label {
      white-space: nowrap !important;
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
