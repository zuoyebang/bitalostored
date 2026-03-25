<template>
  <div class="machine">
    <el-form class="demo-form-inline" inline>
      <el-form-item label="Budget Unit">
        <el-select v-model="budgetArray" placeholder="Please select" multiple>
          <el-option
            v-for="(item, index) in budgetList"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Cloud Provider">
        <el-select v-model="idcArray" placeholder="Please select" multiple>
          <el-option
            v-for="(item, index) in idcOptions"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Virtual Machine">
        <el-select v-model="virtualValue">
          <el-option
            v-for="(item, index) in virtualList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="IP">
        <el-input
          v-model="searchParams.ip"
          placeholder="Please enter IP"
          clearable
          style="width: 200px;"
        ></el-input>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="search()">Search</el-button>
      </el-form-item>
    </el-form>
    <el-card style="font-size:14px;">
    Note: cpuset type (S=Shared, E=Exclusive, N=No CPU binding, Empty=Resource pool not set)
    </el-card>
    <el-card style="font-size:14px;">
      <div>
          <div v-for="[b, machineStat] in budgetMachineStat" :key="b">
            {{ b }}: 
            <span v-for="[idc, count] in machineStat" :key="idc">
            {{idc}}={{count}};
            </span>
          </div>
      </div>
      <div>total: {{total}}</div>
    </el-card>
    <el-card>
      <div class="machine-container">
        <el-table 
          stripe 
          :data="machineData" 
          :header-cell-style="{fontWeight:600,color:'#606266'}"
        >
          <template v-for="(item, index) in machineHeader">
            <el-table-column align="center" :label="item.text" :key="index" :show-overflow-tooltip="true">
              <template slot-scope="scope">
                  <span>{{ scope.row[item.value] }}</span>
              </template>
            </el-table-column>
          </template>
          <el-table-column label="Operation" align="center" fixed="right">
              <template slot-scope="scope">
                <el-button size="mini" type="primary" @click="handleReplicate(scope.row)">Traffic Offload</el-button>
                <el-button size="mini" type="success" @click="handleMigrate(scope.row)">Replace Machine</el-button>
                <el-button size="mini" type="success" @click="handleRemoveProxy(scope.row)">Offline Proxy</el-button>
              </template>
            </el-table-column>
        </el-table>
      </div>
    </el-card>
    <el-dialog title="Replace Machine" :visible.sync="migrateDialogVisible">
    <el-form>
      <el-form-item label="Source Machine">
        <el-input v-model="migrateForm.sourceIp" readonly></el-input>
      </el-form-item>
      <el-form-item label="Target Machine">
        <el-input v-model="migrateForm.targetIp" placeholder="Please enter IP"></el-input>
        <div style="font-size: 12px; color: #999; margin-top: 4px;">
          All nodes from the source machine will be deployed on this machine
        </div>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button @click="handleMigrateCancel">Cancel</el-button>
      <el-button type="primary" @click="handleMigrateConfirm">Confirm</el-button>
    </div>
    </el-dialog>

    <el-dialog title="Offline Proxy" :visible.sync="removeProxyDialogVisible">
      <div>Please confirm that all proxy traffic has been offloaded</div>
      <el-form>
        <el-form-item label="Machine IP">
          <el-input v-model="removeProxyForm.ip" readonly></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="handleRemoveProxyConfirm">Confirm</el-button>
      </div>
    </el-dialog>

    <el-dialog
      title="Traffic Offload Confirmation"
      :visible.sync="replicateDialogVisible"
      :close-on-click-modal="false"
      width="400px"
    >
      <div style="margin-bottom: 12px;">
        Please enter the operation password to perform traffic offload on machine <b>{{ replicateRow && replicateRow.machineIp }}</b>:
      </div>
      <el-input
        v-model="replicateData.token"
        type="text"
        placeholder="Please enter token (md5 of ip)"
        @keyup.enter.native="handleReplicateDialogOk"
      />
      <div slot="footer" class="dialog-footer">
        <el-button @click="handleReplicateDialogCancel">Cancel</el-button>
        <el-button
          type="primary"
          @click="handleReplicateDialogOk"
        >Confirm</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { machineHeader, virtualList } from './constants/machine';
import { mapGetters } from 'vuex';
import { machineInfo } from '@/api/cluster';
import {getBudgetList} from '@/api/agent';
import {replicateMachine, machineMigrate, machineRemoveProxyApi} from '@/api/matrix';

export default {
  data() {
    return {
      budgetArray: [],
      idcArray: [],
      searchParams:{
        budgets: '',
        idcs: '',
        ip: '',
        isVirtual: 0,
      },
      replicateData:{
        token: '',
        ip: '',
      },
      idcOptions: [],
      budgetList: [],
      machineHeader: machineHeader,
      virtualList: virtualList,
      machineData: [],
      budgetMachineStat: {},
      total: 0,
      virtualValue: 0,
      migrateDialogVisible: false,
      removeProxyDialogVisible: false,
      migrateForm: {
        sourceIp: '',
        targetIp: '',
      },
      removeProxyForm: {
        ip: '',
      },
      replicateDialogVisible: false,
      replicateRow: null,
      confirmPassword: '',
    };
  },
  computed: {
    ...mapGetters(['tableInfo', 'selectInfo']),
  },
  methods: {
    search() {
      this.searchParams.budgets = this.budgetArray.join(',');
      this.searchParams.idcs = this.idcArray.join(',');
      this.searchParams.isVirtual = this.virtualValue;
      this.budgetMachineStat = new Map();
      this.total = 0
      machineInfo(this.searchParams)
      .then(res => {
        res.rows.forEach(item => {
          item.machineIp = item.machine.ip;
          item.machineBudget = item.machine.budget;
          item.machineIdc = item.machine.idc;
          item.proxyNode = '';
          item.serverNode = '';
          item.serverNodeNum = 0;
          item.proxyNodeNum = 0;
          item.proxyCpuValue = '';
          item.serverCpuValue = '';
          item.machineInfo = `${item.machineIp}\n${item.machineBudget}\n${item.machineIdc}`;
          this.total = this.total + 1;

          if (!this.budgetMachineStat.get(item.machineBudget)) {
              this.budgetMachineStat.set(item.machineBudget, new Map())
              this.budgetMachineStat.get(item.machineBudget).set(item.machineIdc, 1)
          } else {
             if (!this.budgetMachineStat.get(item.machineBudget).get(item.machineIdc)) {
                this.budgetMachineStat.get(item.machineBudget).set(item.machineIdc, 1)
             } else {
                this.budgetMachineStat.get(item.machineBudget).set(item.machineIdc, this.budgetMachineStat.get(item.machineBudget).get(item.machineIdc) + 1)
             }
          }
          console.log(this.budgetMachineStat)

          if (item.proxy) {
            item.proxy.forEach(el => {
              item.proxyNode += `${el.clustername}: ${el.machineNode}/${el.clusterNode}/${el.totalNode}  \n`;
              item.proxyNodeNum += el.machineNode
            });
            item.proxyNode += `总数：${item.proxyNodeNum} \n`
          }
          if (item.server) {
            item.server.forEach(el => {
              item.serverNode += `${el.clustername}: ${el.machineNode}/${el.clusterNode}/${el.totalNode}/master:${el.masterNum}  \n`;
              item.serverNodeNum += el.machineNode
            });
            item.serverNode += `总数：${item.serverNodeNum}\n主节点数：${item.masterCount}\n繁忙度：${item.busyIndex}`
          }
          if (item.proxyCpu) {
            item.proxyCpu.forEach(el => {
              item.proxyCpuValue += `${el.clusterName}: ${el.port}\n`;
              item.proxyCpuValue += `cpu(${el.cpuNums}${el.cpuSetTypeStr}): ${el.cpuSet}\n`;
            });
            item.proxyCpuValue += `cpu限核：${item.proxyCpuNum}\n`
          }
          if (item.serverCpu) {
            item.serverCpu.forEach(el => {
              item.serverCpuValue += `${el.clusterName}: ${el.port}\n`;
              item.serverCpuValue += `cpu(${el.cpuNums}${el.cpuSetTypeStr}): ${el.cpuSet}\n`;
            });
            item.serverCpuValue += `cpu限核：${item.serverCpuNum}\n`
          }
        })
        this.machineData = res.rows;
      })
    },
    handleReplicate(row) {
      this.replicateRow = row;
      this.replicateDialogVisible = true;
    },
    handleReplicateDialogOk() {
      if (!this.replicateData.token) {
        this.$message.error('Token cannot be empty');
        return;
      }
      this.replicateData.ip = this.replicateRow.machineIp
      replicateMachine(this.replicateData)
        .then((res) => {
          if (Array.isArray(res) && res.length > 0) {
            const msgHtml = `<div style="max-height:300px;overflow:auto;word-break:break-all;">${res.join('<br>')}</div>`;
            this.$alert(msgHtml, 'Traffic Offload Status', {
              confirmButtonText: 'Confirm',
              type: 'success',
              dangerouslyUseHTMLString: true,
              customClass: 'alert-scroll-dialog',
              showClose: true,
              center: false,
            });
          } else {
            this.$message.success('Traffic offload operation successful');
            this.search();
            this.replicateDialogVisible = false;
          }
        })
    },
    handleReplicateDialogCancel() {
      this.replicateDialogVisible = false;
    },
    handleRemoveProxy(row) {
      this.removeProxyForm.ip = row.machineIp;
      this.removeProxyDialogVisible = true;
    },
    handleRemoveProxyConfirm() {
      machineRemoveProxyApi(this.removeProxyForm)
        .then(() => {
            this.$message.success('Starting proxy offline process');
        })
    },
    handleMigrate(row) {
      this.migrateForm.sourceIp = row.machineIp;
      this.migrateForm.targetIp = '';
      this.migrateDialogVisible = true;
    },
    handleMigrateConfirm() {
      if (!this.migrateForm.targetIp) {
        this.$message.error('Target machine IP cannot be empty');
        return;
      }
      machineMigrate(this.migrateForm)
        .then((res) => {
          if (Array.isArray(res) && res.length > 0) {
            const msgHtml = `<div style="max-height:300px;overflow:auto;word-break:break-all;">${res.join('<br>')}</div>`;
            this.$alert(msgHtml, 'Task Creation Status', {
              confirmButtonText: 'Confirm',
              type: 'success',
              dangerouslyUseHTMLString: true,
              customClass: 'alert-scroll-dialog',
              showClose: true,
              center: false,
            });
          } else {
            this.$message.success('Migration task created successfully');
          }
        })
    },
    handleMigrateCancel() {
      this.migrateDialogVisible = false;
    },
  },
  mounted() {
    this.idcOptions = this.selectInfo.idcOptions || [];
    getBudgetList().then((res) => {
        this.budgetList = res.budgetList;
      });
  },
};
</script>

<style lang='scss' scoped>
.table-title {
  margin-bottom: 10px;
}
.machine-container {
  ::v-deep .el-table{
    .cell {
      white-space: pre-wrap;
      max-height: 500px;
      cursor: default;
      overflow: overlay;
    }
  };
}
::v-deep .alert-scroll-dialog .el-message-box {
  width: 600px !important;
  max-width: 90vw;
}
</style>
