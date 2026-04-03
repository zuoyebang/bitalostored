<template>
  <div class="dashboard-container" v-loading="loading">
    <el-card class="dashboard-container">
      <el-form class="demo-form-inline">
        <el-form-item label="Clusterlist" class="form-item">
          <span style="margin-right: 10px"
            >count: {{ dashboardList.count }}</span
          >
          <el-select
            class="select"
            v-model="selectVal"
            placeholder="please select"
            @change="getDashboardList"
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
              <el-select v-model="searchParams.name" placeholder="please select" @change="search" filterable>
                <el-option
                  v-for="(item, index) in clusterNames"
                  :key="index"
                  :label="item"
                  :value="item"
                ></el-option>
              </el-select>
          <el-button class="button" type="primary" @click="newDashboard"
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
          <el-button
            class="button"
            style="margin-right: 20px"
            type="primary"
            @click="multiUpgrade"
            >multi upgrade</el-button
          >
        </el-form-item>
      </el-form>
      <el-table
        stripe
        :data="dashboardList.rows"
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
        <el-table-column align="center" label="operate" width="500">
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
              style="margin-left: 5px"
              type="primary"
              @click="offline(row)"
              >offline</el-button
            >
            <el-button
              size="mini"
              style="margin-left: 5px"
              type="primary"
              @click="replace(row)"
              >replace</el-button
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
    </el-card>
    <el-card class="dashboard-container">
      <div class="dashboard-container" style="display: flex; justify-content: space-between; align-items: center;">
        <p class="title">Filelist</p>
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
    </el-card>
    <el-dialog
      title="new dashboard"
      size="small"
      :visible.sync="dialogdashboard"
    >
      <el-form
        class="create-machine"
        :model="dashBoardForm"
        :rules="dashboardRules"
        label-width="120px"
        ref="dashboardRules"
      >
        <el-form-item label="clusterName:" prop="clusterName">
          <el-input
            placeholder="clusterName"
            v-model="dashBoardForm.clusterName"
          ></el-input>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="dashBoardForm.packageId" placeholder="please select">
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
          <el-select v-model="dashBoardForm.configPackId" placeholder="please select">
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
          <el-select v-model="dashBoardForm.regionId" @change="selectRegion" placeholder="please select">
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
            v-model.number="dashBoardForm.assignedPort"
          ></el-input>
        </el-form-item>
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="dashBoardForm.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="role:" prop="role">
          <el-radio-group v-model="dashBoardForm.role">
            <el-radio :label="'master'">master</el-radio>
            <el-radio :label="'backup'">backup</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="storedAuth:" prop="storedAuth">
          <el-input v-model="dashBoardForm.storedAuth"></el-input>
        </el-form-item>
        <el-form-item label="machineId:" prop="machineId">
          <el-radio-group
            v-model="dashBoardForm.machineId"
            class="checkbox-group"
          >
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
        <el-button @click="dialogdashboard = false">Cancel</el-button>
        <el-button
          style="margin-left: 10px"
          type="primary"
          @click="createdashboard('dashboardRules')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
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
    <el-dialog
      title="replace Dashboard"
      size="small"
      :visible.sync="dialogreplace"
    >
      <el-form
        class="create-machine"
        :model="replaceForm"
        label-width="120px"
        :rules="replaceRule"
      >
        <el-form-item label="targetMachine:" prop="machineId">
          <el-radio-group v-model="replaceForm.machineId">
            <el-radio
              v-for="(item, index) in replaceIdList"
              :key="index"
              :label="item.machineId"
              >{{ item.ip }}</el-radio
            >
          </el-radio-group>
        </el-form-item>
        <el-form-item label="version:" prop="version">
          <el-select v-model="replaceForm.version" placeholder="please select">
            <el-option
              v-for="(item, index) in versionList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="replaceForm.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operations"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="storedAuth:" prop="storedAuth">
          <el-input
            v-model="replaceForm.storedAuth"
            placeholder="please enter content"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogreplace = false">Cancel</el-button>
        <el-button style="margin-left: 10px" type="primary" @click="replaceDash"
          >Confirm</el-button
        >
      </div>
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
          @click="submitSingle()"
          >Confirm</el-button
        >
      </div>
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
          <el-select v-model.number="multiUpgradeForm.packageId" placeholder="please select">
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
          <el-checkbox v-model="selectAll" @change="handleSelectAll">Select All</el-checkbox>
          <el-cascader
            :options="groupNodesList"
            :props="{ multiple: true}"
            v-model="multiUpgradeForm.groupNodes"
            placeholder="please select"
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
    <new-package
      :dialogPackage="dialogPackage"
      :serviceId="3"
      @changeDialog="changeDialog"
      @getFile="getFile"
    ></new-package>
    <replace-config
      :dialogRepalce="dialogRepalce"
      @changeDialogRepalce="changeDialogRepalce"
    ></replace-config>
    <el-dialog title="Add Local File" size="small" :visible.sync="dialogLocalFile">
      <el-form
        class="create-machine"
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
import {
  bindDepartment,
  departmentList,
  offline,
  getPackList,
  removeConfig,
  bindConfig,
} from "@/api/";
import {
  getFileList,
  getMachineAll,
  getRegionList,
  getclusterList,
  addLocalFile,
} from "@/api/matrix";
import {
  createDashboard,
  getOperation,
  replaceDashboard,
} from "@/api/dashboard";
import {getClusterNames} from "@/api/resource";
import { multiUpgrade } from "@/api/proxy";
import { mapGetters } from "vuex";
import updateAction from "@/components/update-action.vue";
import updateGrade from "@/components/update-grade.vue";
import packageList from "@/components/package-list.vue";
import newPackage from "@/components/new-package.vue";
import replaceConfig from "../../components/replaceConfig.vue";
import openWindow from "@/utils/openWindow";

export default {
  data() {
    return {
      dashboardList: [],
      dashboardHeader: [],
      machineIdList: [],
      clusterNames: [],
      replaceIdList: [],
      storedList: [],
      dialogcopy: false,
      loading: true,
      dialogGrade: false,
      actionForm: {},
      gradeForm: {},
      dialogAction: false,
      dialogPackage: false,
      dialogreplace: false,
      dialogBind: false,
      dialogBindSingle: false,
      serviceId: 3,
      fileList: [],
      operationList: [],
      operations: [],
      versionList: [],
      feList: [],
      regionList: [],
      packList: [],
      regionId: "",
      selectVal: "",
      dialogFE: false,
      packageList: [],
      departmentList: [],
      dialogdashboard: false,
      dialogRepalce: false,
      feServiceId: "",
      dashBoardServiceId: "",
      oldConfig: "",
      newConfig: "",
      pagination: {
        total: 0,
        curPage: 0,
        num: 10,
      },
      searchParams:{
        name: ''
      },
      feForm: {
        clusterName: "",
        serviceId: "",
        packageId: "",
        machineId: [],
        migrateToPaaS: "",
        assignedPort: "",
        operation: "",
      },
      dashBoardForm: {
        clusterName: "",
        serviceId: "",
        packageId: "",
        machineId: [],
        storedId: 0,
        assignedPort: "",
        storedAuth: "",
        operation: "",
        configPackId: "",
        role: "",
      },
      replaceForm: {
        machineId: "",
        version: "",
        clusterId: "",
        storedAuth: "",
        operation: "",
      },
      bindForm: {
        department: "",
      },
      bindConfigForm: {
        configPackId: "",
        clusterId: "",
      },
      options: [
        { text: "Yes", value: true },
        { text: "No", value: false },
      ],
      dashboardRules: {},
      feRules: {
        clusterName: [
          {
            required: true,
            message: "clusterName is require",
            trigger: "blur",
          },
        ],
        configPackId: [
          {
            required: true,
            message: "please select configPackId",
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
        machineId: [
          {
            required: true,
            message: "please select machineId",
            trigger: "change",
          },
        ],
        migrateToPaaS: [
          {
            required: true,
            type: "boolean",
            message: "please select migrateToPaaS",
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
      replaceRule: {
        machineId: [
          {
            required: true,
            message: "please select machineId",
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
        version: [
          {
            required: true,
            message: "please select version",
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
      multiUpgradeForm: {
          operation: "",
          packageId: "",
          updateConfig: "false",
          groupNodes: [],
          multiType: "dashboard",
        },
        selectAll: false,
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
      dialogLocalFile: false,
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
  computed: {
    ...mapGetters(["tableInfo", "selectInfo"]),
  },
  components: {
    updateAction: updateAction,
    updateGrade: updateGrade,
    packageList: packageList,
    newPackage: newPackage,
    replaceConfig: replaceConfig,
  },
  mounted() {
    this.dashboardHeader = this.tableInfo.storedInfos || [];
    this.operationList = this.selectInfo.dashboardOperations || [];
    const storedIdRules = [
      { required: true, message: "please select storedId", trigger: "change" },
    ];
    this.dashboardRules = { ...this.feRules, storedId: storedIdRules };
    this.init();
  },
  methods: {
    init() {
      this.serviceId = this.$route.meta.serviceId || "";
      this.getDashboardList();
      this.getFeList();
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
    handleSelectAll(checked) {
        if (checked) {
          this.multiUpgradeForm.groupNodes = this.getAllNodeValues(this.groupNodesList);
        } else {
          this.multiUpgradeForm.groupNodes = [];
        }
      },
      getAllNodeValues(nodes) {
        let values = [];
        nodes.forEach(group => {
          group.children.forEach(node => {
            values.push([group.value, node.value]);
          });
        });
        return values;
      },
      handleCascaderChange() {
        const allNodes = this.getAllNodeValues(this.groupNodesList);
        this.selectAll = this.multiUpgradeForm.groupNodes.length === allNodes.length;
      },
    search() {
      this.getDashboardList(false)
    },
    handleTableChange(val) {
      this.pagination.curPage = val;
      this.getDashboardList(true);
    },
    getGroupNodesList() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
        department: this.selectVal,
        page: 0,
        num: 1000,
      };
      this.groupNodesList = [];
      getclusterList(params)
        .then((res) => {
          this.groupList = res.rows;
          var glist = new Map();
          this.groupList.forEach((element) => {
            if (element.nodeStatus !== "online") {
              return;
            }
            if (!glist.get(element.clusterName)) {
              glist.set(element.clusterName, [element]);
            } else {
              let value = glist.get(element.clusterName);
              value.push(element);
            }
          });
          glist.forEach((value, key) => {
            const children = [];
            value.forEach((element) => {
              if (element.nodeStatus == "online") {
                children.push({
                  value: element.clusterId,
                  label: `${element.ip} ~ ${element.version} ~ ${element.updateTime}`,
                });
              }
            });

            this.groupNodesList.push({
              value: value[0].clusterId,
              label: key,
              children,
            });
          });
        })
        .catch((err) => {
          console.log(err);
        });
    },
    getDashboardList(isChangePage = false) {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
        department: this.selectVal,
        page: this.pagination.curPage,
        num: this.pagination.num,
        clusterName: this.searchParams.name,
      };
      this.serviceId = serviceId;
      getclusterList(params)
        .then((res) => {
          this.dashboardList = res || [];
          this.dashBoardServiceId = res.serviceId || "";
          this.pagination.total = res.count || 0;
          this.loading = false;
        })
        .catch((err) => {
          console.log(err);
        });
      !isChangePage && this.getGroupNodesList();
    },
    getFeList() {
      // const params = {
      //   isDashboard: false
      // }
      // getStoredList(params).then((res) => {
      //   this.feList = res.rows || []
      //   this.feServiceId = res.serviceId || ''
      // }).catch((err) => {
      //   this.loading = false
      //   console.log(err)
      // })
    },
    /**
     * 获取select
     */
    getRegionList() {
      const serviceId = this.$route.meta.serviceId || "";
      const params = {
        serviceId: serviceId,
      };
      departmentList(params)
        .then((res) => {
          this.departmentList = res || [];
        })
        .catch((err) => {
          console.log(err);
        });
      getPackList(params).then((res) => {
        this.packList = res.configPackList;
      });
      getRegionList(params)
        .then((res) => {
          this.loading = false;
          this.regionList = res.rows || [];
          this.regionId = this.regionList[0] && this.regionList[0].regionId;
          // this.selectRegion()
        })
        .catch((err) => {
          this.loading = false;
          console.log(err);
        });
    },
    /**
     * 获取filelist
     */
    getFileList() {
      const { serviceId } = this.$route.meta || "";
      const params = {
        serviceId: serviceId,
      };
      getFileList(params).then((res) => {
        this.fileList = res || [];
      });
    },
    getFile() {
      this.getFileList();
    },
    changeDialog(val) {
      this.dialogPackage = val;
      this.getFileList();
    },
    newPackage() {
      this.dialogPackage = true;
    },
    changePackageList(val) {
      console.log(val);
      this.getFileList();
    },
    selectRegion() {
      this.getMachineall();
    },
    // selectType() {
    //   const { serviceId } = this.$route.meta || ''
    //   const params = {
    //     department: this.selectVal,
    //     serviceId: serviceId
    //   }
    //   getclusterList(params).then((res) => {
    //     this.dashboardList = res.rows || []
    //     this.dashBoardServiceId = res.serviceId || ''
    //     this.loading = false
    //   }).catch((err) => {
    //     console.log(err)
    //   })
    // },
    getMachineall() {
      const data = {
        regionId: this.dashBoardForm.regionId,
      };
      getMachineAll(data)
        .then((res) => {
          this.machineIdList = res.rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newFE() {
      this.dialogFE = true;
      // this.initCreate()
    },
    // initCreate(status) {
    //   let serviceId = this.feServiceId
    //   console.log(serviceId)
    //   if (status) serviceId = this.dashBoardServiceId
    //   this.getRegionList()
    //   this.getFileList()
    // },
    newDashboard() {
      this.dialogdashboard = true;
      // this.initCreate(true)
    },
    createdashboard(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { serviceId } = this.$route.meta || "";
          this.dashBoardForm.serviceId = Number(serviceId);
          createDashboard(this.dashBoardForm)
            .then(() => {
              this.$message.success("success");
              this.dialogdashboard = false;
              this.pagination.curPage = 0;
              this.getDashboardList();
            })
            .then(() => {
              this.dialogdashboard = false;
            });
        } else {
          this.$message.error("Please enter or select required items!");
          return false;
        }
      });
    },
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
    getActionList() {
      this.init();
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
      if (typeof row.clusterId == "undefined") {
        row.clusterId = 0;
      }
      const params = {
        clusterId: row.clusterId ? row.clusterId : 0,
        configPackId: row.configPackId,
        serviceId: serviceId,
        type: type + " Config",
      };
      //this.$router.push({ path: '/config', query: params })
      openWindow({ path: "/config", query: params });
    },
    replace(row) {
      this.dialogreplace = true;
      this.replaceForm.machineId = row.machineId;
      this.replaceForm.clusterId = row.clusterId;
      const params = {
        regionId: row.regionId,
      };
      getOperation({ serviceId: row.serviceId }).then((res) => {
        this.operations = res.operations || [];
      });
      getFileList({ serviceId: row.serviceId }).then((res) => {
        this.versionList = res;
      });
      getMachineAll(params)
        .then((res) => {
          this.replaceIdList = res.rows;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    replaceDash() {
      this.dialogreplace = false;
      replaceDashboard(this.replaceForm).then((res) => {
        console.log(res);
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
            message: "Bind Success",
          });
          this.init();
        })
        .catch((err) => {
          console.log(err);
        });
      this.dialogBind = false;
    },
    offline(row) {
      this.$confirm("Confirm offline this cluster config?", "Tip", {
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
                message: "Offline Success!",
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
      this.$confirm("Confirm remove?", "Tip", {
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
              this.getDashboardList();
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
          this.getDashboardList();
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
        multiType: "dashboard",
      };
      multiUpgrade(params)
        .then((res) => {
          console.log(res);
          this.getGroupList();
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
          addLocalFile(params)
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
          this.$message.error("Please enter or select required items!");
          return false;
        }
      });
    },
  },
};
</script>

<style scoped lang="scss">
.dashboard-container {
  margin-bottom: 10px;
  .button {
    float: right;
  }
  .create-machine {
    .el-input {
      width: 300px;
    }
    .el-select {
      width: 300px;
    }
  }
  .table {
    margin-top: 30px;
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
