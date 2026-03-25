<template>
  <div class="resource">
    <el-form class="demo-form-inline" inline>
      <el-form-item label="Cluster Name">
        <el-select v-model="searchParams.clusterName" placeholder="Please select" @change="search" filterable>
          <el-option
            v-for="(item, index) in clusterList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Service Type">
        <el-select v-model="searchParams.serviceId" @change="search" placeholder="Please select">
          <el-option
            v-for="(item, index) in serviceIdList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Cloud">
        <el-select v-model="searchParams.idc" @change="search" placeholder="Please select">
          <el-option
            v-for="(item, index) in idcList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="CPU Assignment Strategy">
        <el-select v-model="searchParams.isManual" @change="search" placeholder="Please select">
          <el-option
            v-for="(item, index) in manualList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="CPU Binding Type">
        <el-select v-model="searchParams.cpuSetType" @change="search" placeholder="Please select">
          <el-option
            v-for="(item, index) in cpuSetTypeList"
            :key="index"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="search">Search</el-button>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="applyDialog = true">Apply</el-button>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="editDialog = true">Preset Input</el-button>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="costDialog = true">Cost Control</el-button>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="addDialog = true">Add Data</el-button>
      </el-form-item>
    </el-form>
    <el-card>
      <div class="resource-container">
        <el-table 
          stripe 
          :data="resourceData" 
          :header-cell-style="{fontWeight:600,color:'#606266'}"
          @selection-change="handleSelectionChange"
        >
          <el-table-column type="selection" width="55" fixed></el-table-column>
          <template v-for="(item, index) in tableHeader">
            <el-table-column align="center" :label="item.label" :key="index" :show-overflow-tooltip="true">
              <template slot-scope="{ row }">
                <div v-if="item.slot === 'exclusiveCpu'">
                  <el-checkbox 
                    v-model="row.cpuSetType"
                    :true-label="1"
                    :false-label="0"
                    @change="changeCpu($event, row.id)"
                  >
                  </el-checkbox>
                </div>
                <div v-else-if="item.slot === 'shareCpu'">
                  <el-checkbox 
                    v-model="row.cpuSetType"
                    :true-label="2"
                    :false-label="0"
                    @change="changeCpu($event, row.id)"
                  >
                  </el-checkbox>
                </div>
                <div v-else-if="item.slot === 'maxCpu'">
                  <span>{{ row[item.value] == 0 ? 'Not Set' : row[item.value] }}</span>
                </div>
                <div v-else>
                  <span>{{ row[item.value] }}</span>
                </div>
              </template>
            </el-table-column>
          </template>
        </el-table>
      </div>
    </el-card>
    <el-dialog title="Input Preset Value" :visible.sync="editDialog" class="dialog" center>
      <div class="line">
        <span>CPU Count: </span>
        <el-input style="width: 150px" type="number" v-model.number="editParams.manualValue"></el-input>
      </div>
      <div class="line">
        <span>Min CPU Number: </span>
        <el-input style="width: 150px" type="number" v-model.number="editParams.minCpu"></el-input>
      </div>
      <div class="line">
        <span>Max CPU Number: </span>
        <el-input style="width: 150px" type="number" v-model.number="editParams.maxCpu"></el-input>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="editDialog = false">Cancel</el-button>
        <el-button type="primary" @click="editValue">Confirm</el-button>
      </span>
    </el-dialog>
    <el-dialog title="Input CPU Cost" :visible.sync="costDialog" class="dialog" center>
      <div class="line">
        <span>CPU Cores: </span>
        <el-input style="width: 150px" type="number" v-model.number="costParams.cpu"></el-input>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="costDialog = false">Cancel</el-button>
        <el-button type="primary" @click="controlCost">Confirm</el-button>
      </span>
    </el-dialog>
    <el-dialog title="Add Data" :visible.sync="addDialog" class="dialog" center>
      <div class="line">
        <span>Cluster ID: </span>
        <el-input style="width: 150px" type="number" v-model.number="addParams.clusterId"></el-input>
      </div>
      <div class="line">
        <span>Cluster Name: </span>
        <el-input style="width: 150px" type="string" v-model="addParams.clusterName"></el-input>
      </div>
      <div class="line">
        <span>Node Type: </span>
        <el-select v-model="addParams.serviceType">
            <el-option label="proxy" value="proxy">
            </el-option>
            <el-option label="server" value="server">
            </el-option>
          </el-select>
      </div>
      <div class="line">
        <span>Cloud: </span>
        <el-select v-model="addParams.idc">
            <el-option
              v-for="(item, index) in idcList"
              :key="index"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
      </div>
      <div class="line">
        <span>CPU Cores: </span>
        <el-input style="width: 150px" type="number" v-model.number="addParams.cpu"></el-input>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="addDialog = false">Cancel</el-button>
        <el-button type="primary" @click="addResourcePool">Confirm</el-button>
      </span>
    </el-dialog>
    <el-dialog title="Save Manual Preset Value" :visible.sync="applyDialog" class="dialog" center>
      <el-radio v-model="isManual" :label="0">No</el-radio>
      <el-radio v-model="isManual" :label="1">Yes</el-radio>
      <span slot="footer" class="dialog-footer">
        <el-button @click="applyDialog = false">Cancel</el-button>
        <el-button type="primary" @click="apply">Confirm</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { serviceIdList, idcList, manualList, cpuSetTypeList, tableHeader} from './constants/index';
import { getClusterNames, getResourceData, apply, editValue, setCpu, controlCost, addResourcePoolApi } from '@/api/resource';

export default {
  components: {
  },
  data() {
    return {
      serviceIdList: serviceIdList,
      idcList: idcList,
      manualList: manualList,
      tableHeader: tableHeader,
      cpuSetTypeList: cpuSetTypeList,
      clusterList: [{
        label: 'All',
        value: ''
      }],
      searchParams:{
        clusterName: '',
        serviceId: 0,
        idc: 'all',
        isManual: 0,
        cpuSetType: -1
      },
      resourceData: [],
      selectArray: [],
      editDialog: false,
      costDialog: false,
      applyDialog: false,
      addDialog: false,
      editParams: {
      },
      costParams: {
      },
      addParams: {
      },
      isManual: 0,
    };
  },
  computed: {
  },
  methods: {
    search() {
      getResourceData(this.searchParams)
      .then(data => {
        this.resourceData = data.rows;
      })
    },
    apply() {
      if (this.selectArray.length === 0) {
        this.$message.error('No cluster selected!');
        return;
      }
      const ids = [];
      this.selectArray.forEach(item => {
        ids.push(item.id);
      });
      apply({ ids, isManual: this.isManual })
      .then(() => {
        this.$message.success('Apply successfully!');
        this.applyDialog = false;
        this.search();
      })
      .catch((err) => {console.log(err)});
    },
    handleSelectionChange(val) {
      this.selectArray = val;
    },
    changeCpu(cpuSetType, id) {
      this.$confirm('Are you sure to modify?', 'Confirmation', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning',
      })
      .then(() => {
        setCpu({ cpuSetType, id })
      })
      .then(() => {
        this.$message.success("success");
        setTimeout(() => {
          this.search();
        }, 500);
      })
      .catch((err) => {
        console.log(err);
        this.search();
      });
    },
    editValue(){
      if (this.selectArray.length === 0) {
        this.$message.error('No cluster selected!');
        return;
      }
      if (!this.editParams.manualValue) {
        this.$message.error('Please input CPU count!');
        return;
      }
      const manual = this.selectArray.map(item => {
        return {
          id: item.id,
          ...this.editParams,
        }
      })
      editValue({
        manual
      })
      .then(() => {
        this.$message.success("修改预设值成功!");
        this.editDialog = false;
        this.editParams = {};
        this.search();
      })
      .catch((err) => {console.log(err)});
    },
    addResourcePool(){
      if (!this.addParams.clusterId) {
        this.$message.error('Please input clusterId!');
        return;
      }
      if (!this.addParams.clusterName) {
        this.$message.error('Please input clusterName!');
        return;
      }
      const params = {...this.addParams}
      addResourcePoolApi(params)
      .then(() => {
        this.$message.success("Add successful!");
        this.addDialog = false;
        this.addParams = {};
        this.search();
      })
      .catch((err) => {console.log(err)});
    },
    controlCost(){
      if (this.selectArray.length === 0) {
        this.$message.error('No cluster selected!');
        return;
      }
      if (!this.costParams.cpu) {
        this.$message.error('Please input CPU cores!');
        return;
      }
      const manual = this.selectArray.map(item => {
        return {
          id: item.id,
          ...this.costParams,
        }
      })
      controlCost({
        manual
      })
      .then(() => {
        this.$message.success("Update successful!");
        this.costDialog = false;
        this.costParams = {};
        this.search();
      })
      .catch((err) => {console.log(err)});
    }
  },
  mounted() {
  const params = {
      service_id: 6,
  };
    getClusterNames(params)
    .then(data => {
      const clusterList = data.map((item) => ({ label: item, value: item}));
      this.clusterList = this.clusterList.concat(clusterList);
    });
    this.search();
  },
};
</script>

<style lang='scss' scoped>
.table-title {
  margin-bottom: 10px;
}
.resource-container {
  ::v-deep .el-table{
    .cell {
      white-space: pre-wrap;
      max-height: 500px;
      cursor: default;
      overflow: overlay;
    }
  };
}
.line {
  display: flex;
  align-items: center;
  column-gap: 10px;
  margin-bottom: 10px;
  span{
    min-width: 88px;
  }
}
.dialog {
  ::v-deep .zyb-dialog--default{
    min-height: 100px;
    max-width: 300px;
  }
}
</style>
