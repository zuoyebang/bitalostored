<template>
  <div class="group-info" v-loading="groupLoading">
    <div class="group-header">
      <div>
        <span>Group Info</span>
        <el-checkbox v-model="autoStatus"> auto refresh </el-checkbox>
      </div>
      <div>
        <el-button class="button" type="primary" @click="multiUpgrade"
          >multi upgrade</el-button
        >
        <el-button class="button" type="primary" @click="dialogAddWitnessFunc"
          >AddWintess</el-button
        >
        <el-button class="button" type="primary" @click="dialogRemoveWitnessFunc"
          >RemoveWintess</el-button
        >
        <el-button class="button" type="primary" @click="removeOffline"
          >Remove offline shards</el-button
        >
        <el-button class="button" type="primary" @click="markOffline"
          >Mark offline nodes</el-button
        >
        <el-button class="button" type="primary" @click="newGroup"
          >new group</el-button
        >
      </div>
    </div>
    <el-card class="machine" v-for="(group, index) in groupList" :key="index">
      <div class="machine-container">
        <span class="title">Group {{ group.groupId }}</span>
        <div>
          <el-button
            class="button"
            type="primary"
            @click="handleGroupMarkOffline(group.groupId)"
            >Mark offline</el-button
          >
          <el-button
            class="button"
            type="primary"
            @click="showGroupcopyDialog(group.groupId)"
            >Expand</el-button
          >
          <el-button
            class="button"
            type="primary"
            @click="newNode(group.groupId)"
            >new node</el-button
          >
        </div>
      </div>
      <el-table
        stripe
        :data="group.nodeInfos"
        :header-cell-style="{ fontWeight: 600, color: '#606266' }"
      >
        <template v-for="(item, index) in groupHeader">
          <el-table-column align="center" :label="item.text" :key="index">
            <template slot-scope="{ row }">
              <span>{{ row[item.text] }}</span>
            </template>
          </el-table-column>
        </template>
        <el-table-column align="center" label="replica">
          <template slot-scope="{ row }">
            <el-switch
              v-model="row.replica"
              @change="change(row)"
              active-color="#13ce66"
              inactive-color="#ff4949"
            >
            </el-switch>
          </template>
        </el-table-column>
        <el-table-column align="center" label="operate" width="480">
          <template slot-scope="{ row }">
            <el-button size="mini" type="primary" @click="upGrade(row)"
              >upgrade</el-button
            >
            <el-button size="mini" type="primary" @click="action(row)"
              >action</el-button
            >
            <el-button size="mini" type="primary" @click="config(row)"
              >config</el-button
            >
            <el-button size="mini" type="danger" @click="single(row)"
              >reraft</el-button
            >
            <el-button
              size="mini"
              type="primary"
              @click="handleServerHistoryList(row)"
              >Operation History</el-button
            >
          </template>
        </el-table-column>
      </el-table>
    </el-card>
    <el-dialog title="new group" size="small" :visible.sync="dialogGroup" :append-to-body="true">
      <el-form
        :model="groupForm"
        :rules="rules"
        ref="groupForm"
        class="create-group"
        label-position="right"
        label-width="95px"
      >
        <el-form-item label="nodeSum: (max 7, nodes per shard)" prop="nodeSum">
          <el-input
            placeholder="nodeSum"
            v-model.number="groupForm.nodeSum"
          ></el-input>
        </el-form-item>
        <el-form-item label="groupSum: (max 24)" prop="groupSum">
          <el-input
            placeholder="groupSum"
            v-model.number="groupForm.groupSum"
          ></el-input>
        </el-form-item>
        <el-form-item label="strategy:" prop="strategy">
          <el-select v-model="groupForm.strategy" :popper-append-to-body="false">
            <el-option
              v-for="(item, index) in strategyList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="groupForm.packageId" :popper-append-to-body="false">
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
          <el-select v-model="groupForm.operation" :popper-append-to-body="false">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="priorityIDC:" prop="priorityIDC">
          <el-select v-model="groupForm.priorityIDC" :popper-append-to-body="false">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="server:" prop="server">
          <el-radio-group v-model="groupForm.server">
            <!-- <el-radio label="matrix" value="matrix"></el-radio> -->
            <el-radio label="bitalos" value="bitalos"></el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="ipList:" prop="ipList">
          <el-input
            type="textarea"
            v-model="groupForm.ipList"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogGroup = false">Cancel</el-button>
        <el-button type="primary" @click="createGroup('groupForm')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog title="Expand" size="mini" :visible.sync="isShowGroupcopyDialog" :append-to-body="true">
      <el-form
        :model="groupcopyForm"
        :rules="groupcopyRules"
        ref="groupcopyForm"
        class="create-group"
        label-position="right"
        label-width="85px"
      >
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="groupcopyForm.packageId">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="isShowGroupcopyDialog = false">Cancel</el-button>
        <el-button type="primary" @click="groupCopyConfirm('groupcopyForm')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
    <el-dialog title="new node" size="small" :visible.sync="dialogNode" :append-to-body="true">
      <el-form
        :model="nodeForm"
        :rules="nodeRules"
        ref="nodeForm"
        class="create-group"
        label-position="right"
        label-width="85px"
      >
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="nodeForm.packageId">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="nodeRole:" prop="nodeRole">
          <el-select v-model="nodeForm.nodeRole">
            <el-option
              v-for="(item, index) in nodeRoleList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="server:" prop="server">
          <el-radio-group v-model="nodeForm.server">
            <el-radio label="bitalos" value="bitalos"></el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="machineId:" prop="machineId">
          <el-select
            v-model="nodeForm.machineId"
            filterable
            placeholder="请选择"
            :multiple="true"
          >
            <el-option
              v-for="item in machineIdOptionList"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
          <!-- <el-radio-group v-model="nodeForm.machineId" class="checkbox-group">
            <el-radio
              v-for="(item, index) in machineIdList"
              :key="index"
              :label="item.machineId"
            >
              {{ item.ip }}
            </el-radio>
          </el-radio-group> -->
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogNode = false">Cancel</el-button>
        <el-button type="primary" @click="createNode('nodeForm')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>

    <el-dialog
      title="cluster expansion"
      size="small"
      :visible.sync="dialogExpansion"
      :append-to-body="true"
    >
      <el-form
        :model="expansionForm"
        :rules="expansionRules"
        ref="nodeForm"
        class="create-group"
        label-position="right"
        label-width="150px"
      >
        <el-form-item label="role:" prop="role">
          <el-select v-model="expansionForm.role">
            <el-option
              v-for="(item, index) in nodeRoleList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Node count to expand:" prop="nodeNum">
          <el-select v-model="expansionForm.nodeNum" placeholder="请选择">
            <el-option
              v-for="item in options"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="expansionForm.packageId">
            <el-option
              v-for="(item, index) in fileList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="IDC:" prop="IDC">
          <el-select v-model="expansionForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="targetMachine:" prop="targetMachine">
          <el-checkbox-group
            v-model="expansionForm.targetMachine"
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
        <el-button @click="dialogExpansion = false">Cancel</el-button>
        <el-button type="primary" @click="expansion()">Confirm</el-button>
      </div>
    </el-dialog>

    <el-dialog
      title="+witness"
      size="small"
      :visible.sync="dialogAddWitness"
      :append-to-body="true"
    >
      <el-form
        :model="addWitnessForm"
        class="create-group"
        label-position="right"
        label-width="150px"
      >
        <el-form-item label="version:" prop="packageId">
          <el-select v-model="addWitnessForm.packageId" placeholder="请选择">
            <el-option
              v-for="(item, index) in packageList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="IDC:" prop="IDC">
          <el-select v-model="addWitnessForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogAddWitness = false">Cancel</el-button>
        <el-button type="primary" @click="addClusterWitness()">Confirm</el-button>
      </div>
    </el-dialog>


    <el-dialog
      title="-witness"
      size="small"
      :visible.sync="dialogRemoveWitness"
      :append-to-body="true"
    >
      <el-form
        :model="removeWitnessForm"
        class="create-group"
        label-position="right"
        label-width="150px"
      >
        <el-form-item label="IDC:" prop="IDC">
          <el-select v-model="removeWitnessForm.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogRemoveWitness = false">Cancel</el-button>
        <el-button type="primary" @click="removeClusterWitness()">Confirm</el-button>
      </div>
    </el-dialog>

    <el-dialog title="config" :visible.sync="dialogConfig" :append-to-body="true">
      <p style="white-space: pre-wrap">{{ configInfo }}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogConfig = false">Cancel</el-button>
        <el-button type="primary" @click="dialogConfig = false"
          >Confirm</el-button
        >
      </span>
    </el-dialog>

    <el-dialog title="reraft" size="small" :visible.sync="dialogSingle" :append-to-body="true">
      <el-form
        :model="singleForm"
        ref="nodeForm"
        class="create-group"
        label-position="right"
        label-width="85px"
      >
        <el-form-item label="token:" prop="token" style="margin-top: 12%">
          <el-input placeholder="选填" v-model="singleForm.token"></el-input>
        </el-form-item>
        <el-form-item label="port:" prop="port" style="margin-top: 6%">
          <el-input placeholder="选填" v-model="singleForm.port"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogSingle = false">Cancel</el-button>
        <el-button type="primary" @click="submitSingle()">Confirm</el-button>
      </div>
    </el-dialog>

    <el-dialog
      title="multi upgrade"
      size="small"
      :visible.sync="dialogMuitiUpgrade"
      :append-to-body="true"
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
              v-for="(item, index) in limitOperationList"
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
              v-for="(item, index) in packageList"
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
            :popper-append-to-body="true"
            placement="top-start"
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
    <el-dialog title="操作历史" width="80%" :visible.sync="dialogHistory" :append-to-body="true">
      <el-table
        v-loading="historyLoading"
        :data="historyData"
        style="width: 100%"
      >
        <el-table-column prop="address" label="Node" width="180"></el-table-column>
        <el-table-column prop="version" label="Version" width="120"></el-table-column>
        <el-table-column prop="type" label="Type" width="200"></el-table-column>
        <el-table-column prop="status" label="Status" width="120"></el-table-column>
        <el-table-column prop="operationTime" label="Operation Time" width="180"></el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer1">
        <el-pagination
          @size-change="handleHistorySizeChange"
          @current-change="handleHistoryCurrentChange"
          :current-page="historyCurrentPage + 1"
          :page-sizes="[10, 20, 50, 100]"
          :page-size="historyPageSize"
          layout="total, sizes, prev, pager, next, jumper"
          :total="historyTotal"
        ></el-pagination>
        <el-button @click="dialogHistory = false">关闭</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import {
  getGroupList,
  groupCreate,
  nodeCreate,
  groupReplica,
  nodeConfig,
  expansion,
  clusterAddWitnessApi,
  clusterRemoveWitnessApi,
  single,
  copy,
  groupMarkOffline,
} from '@/api/group';
import {
  getMachineAll,
  getFileList,
  getPackageList,
  multiUpgrade
} from '@/api/matrix';
import { mapGetters } from 'vuex';
import updateAction from '@/components/update-action.vue';
import updateGrade from '@/components/update-grade.vue';
import { markOffline, deleteOffline } from '@/api/index';
import { getServerHistory } from '@/api/task';

export default {
  data() {
    return {
      groupList: [],
      actionForm: {
        packageId: -1,
        operation: ''
      },
      gradeForm: {},
      autoStatus: false,
      dialogGrade: false,
      dialogAction: false,
      groupLoading: false,
      dialogGroup: false,
      dialogNode: false,
      dialogExpansion: false,
      dialogAddWitness: false,
      dialogRemoveWitness: false,
      dialogConfig: false,
      dialogSingle: false,
      dialogMuitiUpgrade: false,
      isShowGroupcopyDialog: false,
      groupNodesList: [],
      timer: '',
      configInfo: '',
      strategyList: [],
      operationList: [],
      limitOperationList: [],
      packageList: [],
      idcList: [],
      machineIdList: [],
      machineIdOptionList: [],
      nodeRoleList: [],
      fileList: [],
      serviceId: -1,
      groupId: '',
      dialogHistory: false,
      historyLoading: false,
      historyData: [],
      historyTotal: 0,
      historyCurrentPage: 0,
      historyPageSize: 20,
      currentNodeInfo: {},
      groupForm: {
        nodeSum: '',
        groupSum: '',
        strategy: '',
        packageId: '',
        operation: '',
        priorityIDC: '',
        machineIdList: [],
        server: '',
        ipList: ''
      },
      nodeForm: {
        packageId: '',
        machineId: [],
        strategy: '',
        idc: '',
        nodeRole: '',
        server: ''
      },
      groupcopyForm: {
        groupId: '',
        packageId: ''
      },
      migrateForm: {
        targetMachine: [],
        sourceMachine: [],
        clusterId: ''
      },
      expansionForm: {
        targetMachine: [],
        role: '',
        nodeNum: '',
        idc: ''
      },
      addWitnessForm: {
        clusterId: 0,
        packageId: 0,
        idc: ''
      },
      removeWitnessForm: {
        clusterId: 0,
        idc: ''
      },
      singleForm: {
        token: '',
        clusterId: '',
        groupId: '',
        nodeId: '',
        port: ''
      },
      multiUpgradeForm: {
        operation: '',
        packageId: '',
        updateConfig: 'false',
        groupNodes: []
      },
      rules: {
        nodeSum: [
          {
            required: true,
            type: 'number',
            message: 'nodeSum is not a number',
            trigger: 'blur'
          }
        ],
        ipList: [
          {
            required: true,
            message: 'please input ip',
            trigger: 'blur'
          }
        ],
        groupSum: [
          {
            required: true,
            type: 'number',
            message: 'groupSum is not a number',
            trigger: 'blur'
          }
        ],
        strategy: [
          {
            required: true,
            message: 'please select strategy',
            trigger: 'change'
          }
        ],
        packageId: [
          {
            required: true,
            message: 'please select packageId',
            trigger: 'change'
          }
        ],
        operation: [
          {
            required: true,
            message: 'please select operation',
            trigger: 'change'
          }
        ],
        priorityIDC: [
          {
            required: true,
            message: 'please select priorityIDC',
            trigger: 'change'
          }
        ],
        server: [
          {
            required: true,
            message: 'please select server',
            trigger: 'change'
          }
        ]
      },
      expansionRules: {
        role: [
          { required: true, message: 'please select role', trigger: 'change' }
        ],
        packageId: [
          {
            required: true,
            message: 'please select packageId',
            trigger: 'change'
          }
        ],
        nodeNum: [
          {
            required: true,
            message: 'please select nodeNum',
            trigger: 'change'
          }
        ],
        IDC: [
          {
            required: true,
            message: 'please select IDC',
            trigger: 'change'
          }
        ]
      },
      nodeRules: {
        packageId: [
          {
            required: true,
            message: 'please select packageId',
            trigger: 'change'
          }
        ],
        nodeRole: [
          {
            required: true,
            message: 'please select nodeRole',
            trigger: 'change'
          }
        ]
      },
      groupcopyRules: {
        packageId: [
          {
            required: true,
            message: 'please select packageId',
            trigger: 'change'
          }
        ]
      },
      migrateRules: {
        sourceMachine: [
          {
            required: true,
            message: 'please select sourceMachine',
            trigger: 'change'
          }
        ],
        targetMachine: [
          {
            required: true,
            message: 'please select targetMachine',
            trigger: 'change'
          }
        ],
        operation: [
          {
            required: true,
            message: 'please select operation',
            trigger: 'change'
          }
        ],
        packageId: [
          {
            required: true,
            message: 'please select packageId',
            trigger: 'change'
          }
        ]
      },
      multiUpgradeRules: {
        operation: [
          {
            required: true,
            message: 'please select operation',
            trigger: 'change'
          }
        ]
      },
      updateConfigOptions: [
        {
          value: 'true',
          label: 'YES'
        },
        {
          value: 'false',
          label: 'NO'
        }
      ],
      options: [
        {
          value: 1,
          label: '1'
        },
        {
          value: 2,
          label: '2'
        },
        {
          value: 3,
          label: '3'
        }
      ]
    };
  },
  beforeRouteLeave(to, from, next) {
    if (this.timer) clearInterval(this.timer);
    next();
  },
  components: {
    updateAction: updateAction,
    updateGrade: updateGrade
  },
  inject: ['reload'],
  methods: {
    showGroupcopyDialog(groupId) {
      this.isShowGroupcopyDialog = true;
      this.groupcopyForm.groupId = groupId;
      const group = this.groupList.find((item) => item.groupId == groupId);
      this.serviceId = group.serviceId;
      if (this.serviceId === 0) {
        this.serviceId =
          group.nodeInfos.find((item) => item.serviceId !== 0).serviceId || 0;
      }
      this.getPackageMachine(0);
    },
    groupCopyConfirm(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { clusterId } = this.$route.query || '';
          this.isShowGroupcopyDialog = false;
          copy({
            clusterId: Number(clusterId),
            groupId: this.groupcopyForm.groupId,
            packageId: this.groupcopyForm.packageId
          }).then(() => {
            this.$message.success('success');
            this.isShowGroupcopyDialog = false;
          });
        } else {
          this.$message.error('please check params');
          return false;
        }
      });
    },
    getGroupList() {
      const params = this.$route.query;
      this.groupLoading = true;
      getGroupList(params)
        .then((res) => {
          this.serviceId = res.clusterInfo.serviceId;
          this.groupList = res.rows;
          this.groupNodesList = this.groupList.map((element) => {
            const children = [];
            element.nodeInfos.forEach((item) => {
              if (item.nodeStatus == 'online') {
                children.push({
                  value: item.nodeId,
                  label: `${item.ip} ~ ${item.version} ~ ${item.updateTime} ~ ${item.idc} ~ ${item.role}`
                });
              }
            });
            return {
              value: element.groupId,
              label: `Group ${element.groupId}`,
              children
            };
          });
          this.groupLoading = false;
        })
        .catch(() => {
          this.groupLoading = false;
        });
    },
    autoFrfresh() {
      if (this.timer) clearInterval(this.timer);
      this.timer = setInterval(() => {
        if (this.autoStatus) this.getGroupList();
      }, 5000);
    },
    createGroup(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { clusterId } = this.$route.query || '';
          this.groupForm.clusterId = clusterId - 0;
          groupCreate(this.groupForm)
            .then(() => {
              this.$message.success('success');
              this.getGroupList();
              this.dialogGroup = false;
            })
            .then(() => {
              this.dialogGroup = false;
            });
        } else {
          this.$message.error('please check params');
          return false;
        }
      });
    },
    handleGroupMarkOffline(groupId) {
      const { clusterId } = this.$route.query || '';
      groupMarkOffline({ groupId, clusterId: Number(clusterId) }).then(() => {
        this.$message.success('success');
        this.getGroupList();
      });
    },
    handleServerHistoryList(row) {
      this.currentNodeInfo = row;
      this.dialogHistory = true;
      this.historyCurrentPage = 0;
      this.getServerHistoryList();
    },
    getServerHistoryList() {
      this.historyLoading = true;
      const params = {
        clusterId: this.currentNodeInfo.clusterId,
        nodeId: this.currentNodeInfo.nodeId,
        groupId: this.currentNodeInfo.groupId,
        page: this.historyCurrentPage,
        num: this.historyPageSize
      };
      getServerHistory(params)
        .then((res) => {
          this.historyData = res.rows || [];
          this.historyTotal = res.count || 0;
          this.historyLoading = false;
        })
        .catch(() => {
          this.historyLoading = false;
        });
    },
    handleHistorySizeChange(val) {
      this.historyPageSize = val;
      this.historyCurrentPage = 0;
      this.getServerHistoryList();
    },
    
    handleHistoryCurrentChange(val) {
      this.historyCurrentPage = val;
      this.getServerHistoryList();
    },
    createNode(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const { clusterId, serviceId, regionId } = this.$route.query || '';
          this.nodeForm.clusterId = clusterId - 0;
          this.nodeForm.groupId = this.groupId;
          this.nodeForm.serviceId = serviceId - 0;
          this.nodeForm.regionId = regionId - 0;
          if (this.nodeForm.machineId.length === 0) {
            this.nodeForm.machineId = [];
          }
          nodeCreate(this.nodeForm)
            .then(() => {
              this.$message.success('success');
              this.getGroupList();
              this.dialogNode = false;
            })
            .then(() => {
              this.dialogNode = false;
            });
        } else {
          this.$message.error('please check params');
          return false;
        }
      });
    },
    newGroup() {
      this.dialogGroup = true;
      this.getPackageMachine(-1);
    },
    markOffline() {
      this.$confirm('Do you want to detect offline nodes in the current cluster?', 'Confirmation', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          markOffline({ clusterId: this.$route.query.clusterId }).then(() => {
            this.$message.success('Operation successful');
            this.getGroupList();
          });
        })
        .catch(() => {});
    },
    removeOffline() {
      this.$confirm('Do you want to delete offline nodes in the current cluster?', 'Confirmation', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          deleteOffline({ clusterId: this.$route.query.clusterId }).then(() => {
            this.$message.success('Operation successful');
            this.getGroupList();
          });
        })
        .catch(() => {});
    },
    action(row) {
      this.dialogGrade = true;
      this.gradeForm = row;
    },
    upGrade(row) {
      this.actionForm = Object.assign(this.actionForm, row);
      this.dialogAction = true;
    },
    changAction(val) {
      this.dialogAction = val;
    },
    changGrade(val) {
      this.dialogGrade = val;
    },
    getActionList() {
      this.getGroupList();
    },
    getPackageMachine(clusterId) {
      const { regionId } = this.$route.query;
      const params = {
        serviceId: this.serviceId,
        clusterId: clusterId
      };
      const data = {
        regionId: regionId
      };
      Promise.all([getFileList(params), getMachineAll(data)])
        .then((res) => {
          this.fileList = res[0] || [];
          this.machineIdList = res[1].rows || [];
          console.log(this.machineIdList, 'list');
          this.machineIdOptionList = this.machineIdList.map((item) => {
            let obj = {
              label: item.ip,
              value: item.machineId
            };
            return obj;
          });
        })
        .catch((err) => {
          console.log(err);
        });
    },
    newNode(id) {
      this.groupId = id;
      this.dialogNode = true;
      const group = this.groupList.find((item) => item.groupId == this.groupId);
      this.serviceId = group.serviceId;
      if (this.serviceId === 0) {
        this.serviceId =
          group.nodeInfos.find((item) => item.serviceId !== 0).serviceId || 0;
      }
      this.nodeForm.server =
        this.serviceId == 1 ? 'matrix' : this.serviceId == 6 ? 'bitalos' : '';
      this.getPackageMachine(0);
    },
    markExpansion() {
      this.dialogExpansion = true;
      this.getPackageMachine(0);
    },
    dialogAddWitnessFunc() {
      this.dialogAddWitness = true;
      this.getPackageList();
    },
    dialogRemoveWitnessFunc() {
      this.dialogRemoveWitness = true;
    },
    getPackageList() {
      const params = {
        serviceId: this.$route.query.serviceId,
        clusterId: this.$route.query.clusterId
      };
      getPackageList(params)
        .then((res) => {
          this.packageList = res || [];
          if (this.packageList.length > 0) {
            if (
              this.packageList.findIndex(
                (obj) => obj.id === this.multiUpgradeForm.packageId
              ) === -1
            ) {
              this.multiUpgradeForm.packageId = this.packageList[0].id;
            }
          }
        })
        .catch((err) => {
          console.log(err);
        });
    },
    multiUpgrade() {
      this.dialogMuitiUpgrade = true;
      if (this.limitOperationList.length > 0) {
        if (
          !this.limitOperationList.includes(this.multiUpgradeForm.operation)
        ) {
          this.multiUpgradeForm.operation = this.limitOperationList[0];
        }
      }
      this.multiUpgradeForm.groupNodes = [];
      this.getPackageList();
    },
    multiUpgradeAction() {
      this.dialogMuitiUpgrade = false;
      const nodesMap = this.multiUpgradeForm.groupNodes.reduce((prev, cur) => {
        if (prev.has(`${cur[0]}`)) {
          prev.set(`${cur[0]}`, `${prev.get('' + cur[0])},${cur[1]}`);
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
        groupNodes: nodes
      };
      console.log(nodes);
      console.log(params);
      multiUpgrade(params)
        .then((res) => {
          console.log(res);
          this.getGroupList();
        })
        .catch((err) => {
          console.log(err);
        });
    },
    change(row) {
      this.$confirm('Are you sure to modify?', 'Confirmation', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          const params = {
            clusterId: row.clusterId,
            groupId: row.groupId,
            nodeId: row.nodeId,
            replica: row.replica
          };
          groupReplica(params)
            .then((res) => {
              console.log(res);
            })
            .catch((err) => {
              console.log(err);
            });
        })
        .catch(() => {
          row.replica = !row.replica;
        });
    },
    config(row) {
      this.dialogConfig = true;
      const params = {
        clusterId: row.clusterId,
        groupId: row.groupId,
        nodeId: row.nodeId
      };
      nodeConfig(params)
        .then((res) => {
          this.configInfo = res;
        })
        .catch((err) => {
          console.log(err);
        });
    },
    expansion() {
      const { clusterId } = this.$route.query || '';
      this.expansionForm.clusterId = Number(clusterId);
      expansion(this.expansionForm)
        .then((res) => {
          console.log(res);
          this.getGroupList();
        })
        .catch((err) => {
          console.log(err);
        });
      this.dialogExpansion = false;
    },
    addClusterWitness() {
      const { clusterId } = this.$route.query || '';
      this.addWitnessForm.clusterId = Number(clusterId);
      clusterAddWitnessApi(this.addWitnessForm)
        .then((res) => {
          console.log(res);
          this.getGroupList();
        })
        .catch((err) => {
          console.log(err);
        });
      this.dialogAddWitness = false;
    },
    removeClusterWitness() {
      const { clusterId } = this.$route.query || '';
      this.removeWitnessForm.clusterId = Number(clusterId);
      clusterRemoveWitnessApi(this.removeWitnessForm)
        .then((res) => {
          console.log(res);
          this.getGroupList();
        })
        .catch((err) => {
          console.log(err);
        });
      this.dialogRemoveWitness = false;
    },
    single(row) {
      this.singleForm.clusterId = row.clusterId;
      this.singleForm.groupId = row.groupId;
      this.singleForm.nodeId = row.nodeId;
      this.dialogSingle = true;
    },
    submitSingle() {
      this.singleForm.port = Number(this.singleForm.port);
      single(this.singleForm)
        .then((res) => {
          console.log(res);
          this.getGroupList();
        })
        .catch((err) => {
          console.log(err);
        });
      this.singleForm = {
        token: '',
        clusterId: '',
        groupId: '',
        nodeId: '',
        port: ''
      };
      this.dialogSingle = false;
    }
  },
  mounted() {
    this.strategyList = this.selectInfo.strategyList || [];
    const operations = this.selectInfo.matrixOperations || [];
    this.limitOperationList = operations.filter(function (item) {
      if (item == 'supervisor-start') {
        return false;
      } else {
        return true;
      }
    });
    this.operationList = operations;
    this.idcList = this.selectInfo.idcOptions || [];
    this.nodeRoleList = this.selectInfo.nodeRoles || [];
    this.getGroupList();
  },
  computed: {
    ...mapGetters(['tableInfo', 'selectInfo']),
    groupHeader() {
      return this.tableInfo.matrixInfos || [];
    }
  },
  watch: {
    autoStatus(val) {
      if (val) this.autoFrfresh();
    }
  }
};
</script>

<style scoped lang="scss">
.group-info {
  min-height: 500px;
  .group-header {
    display: flex;
    justify-content: space-between;
    height: 40px;
    span {
      margin-right: 50px;
    }
  }
  .create-group {
    height: 80px;
    .el-input {
      width: 260px;
    }
    .el-select {
      width: 260px;
    }
  }
  .multi-upgrade {
    height: 100px;
    .el-input {
      width: 260px;
    }
    .el-select {
      width: 260px;
    }
    .el-cascader {
      width: 260px;
    }
  }
  .machine {
    margin-top: 10px;
    .button {
      margin-bottom: 20px;
    }
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
  .machine-container {
    display: flex;
    justify-content: space-between;
    .button {
      margin-top: -5px;
    }
  }
  .dialog-footer1 {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
}
</style>
