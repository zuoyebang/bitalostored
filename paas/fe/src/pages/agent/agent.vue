<template>
  <div class="agent" v-loading="loading">
    <el-card class="box-card">
      <div class="agent-container">
        <div class="agent-header">
          <el-form class="demo-form-inline">
            <el-form-item label="regionList">
              <span style="margin-right: 10px">count: {{ total }}</span>
              <el-button class="button" type="primary" @click="newRegion()"
                >new region</el-button
              >
            </el-form-item>
          </el-form>
          <el-table
            stripe
            :data="regionList"
            :header-cell-style="{ fontWeight: 600, color: '#606266' }"
          >
            <template v-for="(item, index) in tableHeader">
              <el-table-column align="center" :label="item.text" :key="index">
                <template slot-scope="{ row }">
                  <span>{{ row[item.text] }}</span>
                </template>
              </el-table-column>
            </template>
            <el-table-column align="center" label="operate" width="380">
              <template slot-scope="{ row }">
                <el-button
                  size="mini"
                  type="primary"
                  @click="unbindMachine(row)"
                  >remove-machines</el-button
                >
                <el-button size="mini" type="primary" @click="bindmachine(row)"
                  >region-machine</el-button
                >
                <el-button size="mini" type="primary" @click="remove(row)"
                  >remove</el-button
                >
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-card>
    <el-card class="package">
      <div class="agent-container">
        <div class="agent-header">
          <el-form class="demo-form-inline">
            <el-form-item label="fileList">
              <el-button class="button" type="primary" @click="upGrade()"
                >upgrade agent</el-button
              >
              <el-button class="button" type="primary" @click="newPackage"
                >build</el-button
              >
            </el-form-item>
          </el-form>
          <package-list
            :fileList="fileList"
            @changePackageList="changePackageList"
          ></package-list>
        </div>
      </div>
    </el-card>
    <el-card class="last" style="margin-top: 20px">
      <div class="agent-container">
        <div class="agent-header">
          <el-form class="demo-form-inline">
            <el-form-item label="machineList">
              <span style="margin-right: 10px"
                >count: {{ machineList.count }}</span
              >
              <el-select
                class="select"
                v-model="selectVal"
                placeholder="please select"
                @change="selectMachine"
              >
                <el-option
                  v-for="(item, index) in budgetList"
                  :key="index"
                  :label="item"
                  :value="item"
                ></el-option>
              </el-select>
              <el-input
              label="ip"
              v-model="searchIp"
              style="width:120px; margin-left:5px"
              placeholder="please enter IP"
              @change="selectMachine"
              ></el-input>
              <el-button class="button" type="primary" @click="multiDeleteMachineBtn()"
                >delete machine</el-button
              >
              <el-button class="button" type="primary" @click="markOfflineMachineBtn()"
                >mark off</el-button
              >
              <el-button class="button" type="primary" @click="newMachine()"
                >new machine</el-button
              >
            </el-form-item>
          </el-form>
          <el-table
            stripe
            :data="machineList.machineInfoList"
            :header-cell-style="{ fontWeight: 600, color: '#606266' }"
          >
            <template v-for="(item, index) in machineHeader">
              <el-table-column
                align="center"
                :label="item.text"
                :show-overflow-tooltip="true"
                :key="index"
                v-if="
                  item.text === 'matrix' ||
                  item.text === 'ip' ||
                  item.text === 'proxy' ||
                  item.text === 'bitalos'
                "
                width="200"
              >
                <template slot-scope="{ row }">
                  <span>{{ row[item.text] }}</span>
                </template>
              </el-table-column>
              <el-table-column
                align="center"
                :label="item.text"
                :key="index"
                :show-overflow-tooltip="true"
                v-else
              >
                <template slot-scope="{ row }">
                  <span>{{ row[item.text] }}</span>
                </template>
              </el-table-column>
            </template>
            <el-table-column align="center" label="operate" width="320">
              <template slot-scope="{ row }">
                <el-button size="mini" type="primary" @click="offline(row)"
                  >offline</el-button
                >
                <el-button size="mini" type="primary" @click="update(row)"
                  >update</el-button
                >
                <el-button
                  size="mini"
                  type="primary"
                  @click="removeMachine(row)"
                  >remove</el-button
                >
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-card>
    <el-dialog title="new region" size="large" :visible.sync="dialogregion">
      <el-form
        class="create-region"
        :model="regionForm"
        ref="regionForm"
        :rules="rules"
        label-width="100px"
      >
        <el-form-item label="regionName" prop="regionName">
          <el-input
            v-model="regionForm.regionName"
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item :required="true" label="machineIds" porp="machineIds">
          <el-checkbox-group
            v-model="regionForm.machineIds"
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
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogregion = false">cancel</el-button>
        <el-button type="primary" @click="createRegion('regionForm')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog
      :title="'update machine'"
      size="small"
      :visible.sync="dialogMachine"
    >
      <el-form
        class="create-machine"
        :model="machineForm"
        ref="machineForm"
        :rules="machineRules"
        label-width="100px"
      >
        <el-form-item label="ip" prop="ip">
          <el-input v-model="machineForm.ip" autocomplete="off"></el-input>
        </el-form-item>
        <el-form-item label="idc:" prop="idc">
          <el-select v-model="machineForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="budget">
          <el-input v-model="machineForm.budget" autocomplete="off"></el-input>
        </el-form-item>
        <el-form-item label="cpuTotal">
          <el-input
            v-model.number="machineForm.cpuTotal"
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item label="cpuExMax">
          <el-input
            v-model.number="machineForm.cpuSetMax"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogMachine = false">cancel</el-button>
        <el-button type="primary" @click="updateMachine('machineForm')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog
      :title="'new machine'"
      size="small"
      :visible.sync="dialogNewMachine"
    >
      <el-form
        class="create-machine"
        :model="machineForm"
        ref="machineForm"
        :rules="machineRules"
        label-width="100px"
      >
        <el-form-item label="ip" prop="ip">
          <el-input
            type="textarea"
            v-model="machineForm.ip"
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item label="idc:" prop="idc">
          <el-select v-model="machineForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="budget">
          <el-input v-model="machineForm.budget" autocomplete="off"></el-input>
        </el-form-item>
        <el-form-item label="cpuTotal">
          <el-input
            v-model.number="machineForm.cpuTotal"
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item label="cpuExMax">
          <el-input
            v-model.number="machineForm.cpuSetMax"
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item label="isVirtual">
           <el-select v-model.number="machineForm.isVirtual">
           <el-option
              v-for="(item, index) in virtualOptionList"
              :key="index"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
            </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogNewMachine = false">cancel</el-button>
        <el-button type="primary" @click="createMultiMachine('machineForm')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog
      :title="'mark off machine'"
      size="small"
      :visible.sync="dialogMarkOfflineMachine"
    >
      <el-form
        class="create-machine"
        :model="ipForm"
        ref="ipForm"
        label-width="100px"
      >
        <el-form-item label="ip" prop="ip">
          <el-input
            type="textarea"
            v-model="ipForm.ip"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogMarkOfflineMachine = false">cancel</el-button>
        <el-button type="primary" @click="markOfflineMachine('ipForm')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog
      :title="'delete machine'"
      size="small"
      :visible.sync="dialogMultiDeleteMachine"
    >
      <el-form
        class="create-machine"
        :model="ipForm"
        ref="ipForm"
        label-width="100px"
      >
        <el-form-item label="ip" prop="ip">
          <el-input
            type="textarea"
            v-model="ipForm.ip"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogMultiDeleteMachine = false">cancel</el-button>
        <el-button type="primary" @click="multiDeleteMachine('ipForm')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <new-package
      :dialogPackage="dialogPackage"
      :serviceId="serviceId"
      @changeDialog="changeDialog"
      @getPackage="getPackage"
    ></new-package>
    <el-dialog title="upgrade" size="small" :visible.sync="dialogUpGrade">
      <el-form
        class="update machine"
        :model="upGradeForm"
        :rules="gradeRules"
        label-width="100px"
        ref="gradeRules"
      >
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="upGradeForm.packageId">
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
          <el-select v-model="upGradeForm.regionId">
            <el-option
              v-for="(item, index) in regionList"
              :key="index"
              :label="item.regionName"
              :value="item.regionId"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="updateConfig:" prop="updateConfig">
          <el-select v-model="upGradeForm.updateConfig">
            <el-option
              v-for="(item, index) in options"
              :key="index"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="agentConfig:" prop="agentConfig">
          <el-input
            type="textarea"
            v-model="upGradeForm.agentConfig"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogUpGrade = false">cancel</el-button>
        <el-button type="primary" @click="grade('gradeRules')">confirm</el-button>
      </div>
    </el-dialog>
    <el-dialog title="region-machine" size="large" :visible.sync="dialogBind">
      <el-form
        class="create-machine"
        :model="bindForm"
        :rules="bindRules"
        label-width="100px"
        ref="bindRules"
      >
        <p class="ip-title">{{ regionName }}</p>
        <div class="ip-list">
          <span v-for="(item, index) in regionMachineIdList" :key="index">
            {{ item.ip }}
          </span>
        </div>
        <el-form-item label="machineIds:" prop="machineIds">
          <el-checkbox-group
            v-model="bindForm.machineIds"
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
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogBind = false">cancel</el-button>
        <el-button type="primary" @click="bindMachine('bindRules')"
          >confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog
      title="remove-machines"
      size="large"
      :visible.sync="dialogRemove"
    >
      <el-form
        class="remove-machine"
        :model="bindForm"
        label-width="100px"
        ref="bindRules"
      >
        <p class="ip-title">{{ regionName }}</p>
        <el-form-item prop="machineIds">
          <el-checkbox-group
            v-model="removeForm.machineIdList"
            class="checkbox-group"
          >
            <el-checkbox
              v-for="(item, index) in regionMachineIdList"
              :key="index"
              :label="item.machineId"
            >
              {{ item.ip }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogRemove = false">cancel</el-button>
        <el-button type="primary" @click="unbindMachineTrue()">confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import {
  getRegionList,
  createRegion,
  getMachineAll,
  getFileList,
} from "@/api/matrix";
import {
  getMachineList,
  createMachine,
  apiMarkOfflineMachine,
  apiMultiDeleteMachine,
  agentUpgrade,
  machineUpdate,
  machinesBind,
  machinesUnBind,
  remove,
  offline,
  removeMachine,
  getBudgetList,
  regionRemoveMachines,
} from "@/api/agent";
import { getPackList } from "@/api/";
import { mapGetters } from "vuex";
import packageList from "@/components/package-list.vue";
import newPackage from "@/components/new-package.vue";

export default {
  data() {
    return {
      virtualOptionList: [{label:"No", value:0},{label:"Yes", value:1}],
      regionList: [],
      dialogUpGrade: false,
      loading: true,
      dialogMachine: false,
      dialogNewMachine: false,
      dialogPackage: false,
      dialogMultiDeleteMachine:false,
      dialogMarkOfflineMachine:false,
      updateStatus: false,
      dialogBind: false,
      dialogRemove: false,
      regionId: "",
      serviceId: 0,
      offset: 0,
      page: 0,
      pageSize: 20,
      total: 0,
      idcList: [],
      tableList: [],
      dialogregion: false,
      tableHeader: [],
      machineIdList: [],
      regionMachineIdList: [],
      fileList: [],
      machineList: [],
      machineHeader: [],
      serviceList: [],
      packList: [],
      budgetList: [],
      regionName: "",
      selectVal: "",
      searchIp: "",
      bindForm: {
        regionId: "",
        machineIds: [],
      },
      removeForm: {
        regionId: "",
        machineIdList: [],
      },
      upGradeForm: {
        packageId: "",
        regionId: "",
        updateConfig: "false",
        agentConfig: "",
        config: "",
      },
      regionForm: {
        regionName: "",
        machineIds: [],
      },
      machineForm: {
        idc: "",
        ip: "",
        weight: "",
        budget: "",
        isVirtual: 0,
      },
      ipForm: {
        ip: "",
      },
      gradeRules: {
        packageId: [
          {
            required: true,
            message: "please select packageId",
            trigger: "change",
          },
        ],
        config: [
          {
            required: true,
            message: "please select config",
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
        updateConfig: [
          {
            required: true,
            message: "please select updateConfig",
            trigger: "change",
          },
        ],
      },
      bindRules: {
        machineIds: [
          {
            required: true,
            message: "please check machineIds",
            trigger: "change",
          },
        ],
      },
      rules: {
        regionName: [
          { required: true, message: "regionName is require", trigger: "blur" },
        ],
        machineIds: [
          {
            required: true,
            message: "please check machineIds",
            trigger: "change",
          },
        ],
      },
      machineRules: {
        idc: [
          { required: true, message: "please select idc", trigger: "change" },
        ],
        ip: [{ required: true, message: "ip is require", trigger: "blur" }],
        weight: [
          {
            required: true,
            type: "number",
            message: "weight is not a number",
            trigger: "blur",
          },
        ],
      },
      options: [
        {
          value: "true",
          label: "Yes",
        },
        {
          value: "false",
          label: "No",
        },
      ],
    };
  },
  computed: {
    ...mapGetters(["tableInfo", "selectInfo"]),
  },
  components: {
    packageList: packageList,
    newPackage: newPackage,
  },
  mounted() {
    this.tableHeader = this.tableInfo.regionInfos || [];
    this.machineHeader = this.tableInfo.machineInfoList || [];
    this.idcList = this.selectInfo.idcOptions || [];
    this.getRegionList();
    this.getFileList();
    // this.getMachineList()
  },
  methods: {
    /**
     * get select
     */
    getRegionList() {
      const serviceId = this.$route.meta.serviceId || "";
      const params = {
        serviceId: serviceId,
      };
      getPackList(params).then((res) => {
        this.packList = res.configPackList;
      });
      getBudgetList().then((res) => {
        this.budgetList = res.budgetList;
      });
      getRegionList()
        .then((res) => {
          this.regionList = res.rows || [];
          this.total = res.count;
          if (!this.regionId) {
            this.regionId = this.regionList[0] && this.regionList[0].regionId;
            this.regionName =
              this.regionList[0] && this.regionList[0].regionName;
          }
          this.loading = false;
          if (this.regionId) this.selectRegion();
        })
        .catch((err) => {
          console.log(err);
          this.loading = false;
        });
    },
    getMachineall() {
      getMachineAll()
        .then((res) => {
          this.machineIdList = res.rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    getRegionMachine() {
      const params = {
        regionId: this.regionId,
      };
      getMachineAll(params)
        .then((res) => {
          this.regionMachineIdList = res.rows || [];
        })
        .catch((err) => {
          console.log(err);
        });
    },
    /**
     * get clusterList
     */
    selectRegion() {
      // this.regionList.forEach(item => {
      //   if (item.regionId === val) this.regionName = item.regionName
      // })
      // const params = {
      //   regionId: this.regionId,
      //   limit: this.pageSize,
      //   offset: this.offset
      // }
      // console.log(params)
      // getMachineList(params).then((res) => {
      //  this.tableList = res.rows || []
      //  this.total = res.count || 0
      // }).catch((err) => {
      //   console.log(err)
      // })
    },
    newRegion() {
      this.dialogregion = true;
      this.getMachineall();
    },
    createRegion(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const params = {
            regionName: this.regionForm.regionName,
            machineIds: this.regionForm.machineIds,
          };
          createRegion(params)
            .then(() => {
              this.getRegionList();
              this.dialogregion = false;
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogregion = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    update(row) {
      this.machineForm = row;
      this.dialogMachine = true;
      this.updateStatus = true;
    },
    newMachine() {
      this.dialogMachine = false;
      this.dialogNewMachine = true;
      this.machineForm = {};
    },
    markOfflineMachineBtn() {
      this.dialogMarkOfflineMachine = true;
      this.ipForm = {};
    },
    multiDeleteMachineBtn() {
      this.dialogMultiDeleteMachine = true;
      this.ipForm = {};
    },
    updateMachine(formName) {
      let machineFun = machineUpdate;
      this.$refs[formName].validate((valid) => {
        if (valid) {
          machineFun(this.machineForm)
            .then(() => {
              this.getMachineList();
              this.dialogMachine = false;
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogMachine = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    createMultiMachine(formName) {
      let machineFun = createMachine;
      this.$refs[formName].validate((valid) => {
        if (valid) {
          machineFun(this.machineForm)
            .then(() => {
              this.getMachineList();
              this.dialogNewMachine = false;
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogNewMachine = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    markOfflineMachine(formName) {
      let machineFun = apiMarkOfflineMachine;
      this.$refs[formName].validate((valid) => {
        if (valid) {
          machineFun(this.ipForm)
            .then(() => {
              this.getMachineList();
              this.dialogMarkOfflineMachine = false;
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogMarkOfflineMachine = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    multiDeleteMachine(formName) {
      let machineFun = apiMultiDeleteMachine;
      this.$refs[formName].validate((valid) => {
        if (valid) {
          machineFun(this.ipForm)
            .then(() => {
              this.getMachineList();
              this.dialogMultiDeleteMachine = false;
              this.$message.success("success");
            })
            .catch(() => {
              this.dialogMultiDeleteMachine = false;
            });
        } else {
          this.$message.error("Please input or select required fields!");
          return false;
        }
      });
    },
    getFileList() {
      this.serviceId = this.$route.meta.serviceId || "";
      const params = {
        serviceId: this.serviceId,
      };
      getFileList(params).then((res) => {
        this.fileList = res || [];
      });
    },
    getMachineList() {
      const params = {
        budget: this.selectVal,
        ip: this.searchIp,
      };
      getMachineList(params)
        .then((res) => {
          this.machineList = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newPackage() {
      this.dialogPackage = true;
    },
    changeDialog(val) {
      this.dialogPackage = val;
      this.getFileList();
    },
    upGrade() {
      this.dialogUpGrade = true;
    },
    getPackage() {
      this.getFileList();
    },
    changePackageList() {
      this.getFileList();
    },
    grade(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          agentUpgrade(this.upGradeForm)
            .then(() => {
              this.$message.success("success");
              this.dialogUpGrade = false;
            })
            .then(() => {
              this.dialogUpGrade = false;
            });
        } else {
          this.$message.error("Please select required fields!");
          return false;
        }
      });
    },
    selectService() {
      this.getFileList();
    },
    bindmachine(row) {
      this.dialogBind = true;
      this.regionId = row.regionId;
      this.getMachineall();
      this.getRegionMachine();
    },
    bindMachine(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.bindForm.regionId = this.regionId - 0;
          machinesBind(this.bindForm)
            .then(() => {
              this.dialogBind = false;
              this.getRegionList();
              this.$message.success("success");
            })
            .catch((err) => {
              console.log(err);
              this.dialogBind = false;
            });
        } else {
          this.$message.error("Please select required fields!");
          return false;
        }
      });
    },
    unbind(row) {
      this.$confirm("Are you sure you want to unbind?", "Confirm", {
        confirmButtonText: "confirm",
        cancelButtonText: "cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            regionId: row.regionId,
            machineId: row.machineId,
          };
          machinesUnBind(params)
            .then(() => {
              this.getRegionList();
              this.$message({
                type: "success",
                message: "Unbind success!",
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
    remove(row) {
      this.$confirm("Are you sure you want to remove this region?", "Confirm", {
        confirmButtonText: "confirm",
        cancelButtonText: "cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            regionId: row.regionId,
          };
          remove(params)
            .then(() => {
              this.$message({
                type: "success",
                message: "Remove success!",
              });
              this.getRegionList();
              this.getFileList();
              this.getMachineList();
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
    offline(row) {
      this.$confirm("Are you sure you want to offline this machine?", "Confirm", {
        confirmButtonText: "confirm",
        cancelButtonText: "cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            machineId: row.machineId,
          };
          offline(params)
            .then(() => {
              this.$message({
                type: "success",
                message: "Offline success!",
              });
              this.getRegionList();
              this.getFileList();
              this.getMachineList();
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
    removeMachine(row) {
      this.$confirm("Are you sure you want to remove this machine?", "Confirm", {
        confirmButtonText: "confirm",
        cancelButtonText: "cancel",
        type: "warning",
      })
        .then(() => {
          const params = {
            machineId: row.machineId,
          };
          removeMachine(params)
            .then(() => {
              this.$message({
                type: "success",
                message: "Remove success!",
              });
              this.getRegionList();
              this.getFileList();
              this.getMachineList();
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
    unbindMachine(row) {
      this.dialogRemove = true;
      this.regionId = row.regionId;
      this.removeForm.regionId = row.regionId;
      this.getMachineall();
      this.getRegionMachine();
    },
    unbindMachineTrue() {
      console.log(this.removeForm);
      const params = this.removeForm;
      regionRemoveMachines(params)
        .then(() => {
          this.getRegionList();
          this.$message({
            type: "success",
            message: "Unbind success!",
          });
          this.dialogRemove = false;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    selectMachine() {
      const params = {
        budget: this.selectVal,
        ip: this.searchIp,
      };
      getMachineList(params)
        .then((res) => {
          this.machineList = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
  },
  watch: {
    dialogMachine(val) {
      if (!val) this.updateStatus = false;
    },
  },
};
</script>

<style lang="scss" scoped>
.agent {
  .button {
    float: right;
    margin-left: 20px;
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
  .package {
    margin-top: 15px;
  }
  .pagination-con {
    width: 100%;
    text-align: center;
    .pagination {
      display: inline-block;
      margin-top: 30px;
    }
  }
  .ip-list {
    padding: 10px 0;
    span {
      font-size: 12px;
      display: inline-block;
      width: 110px;
    }
    border-bottom: 1px solid #efefef;
  }
  .ip-title {
    margin-top: -20px;
  }
  .create-machine {
    margin-top: 5px;
    .el-input {
      width: 300px;
    }
    .el-select {
      width: 305px;
    }
  }
}
.el-textarea__inner {
  height: 400px;
}
</style>
