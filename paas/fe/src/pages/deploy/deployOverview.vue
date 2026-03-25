<template>
  <div class="deployOverview">
    <el-form class="demo-form-inline" inline>
      <el-form-item label="Budget Unit" prop="budget">
        <el-select v-model="budgetArray" placeholder="Please select" multiple>
          <el-option
            v-for="(item, index) in budgetList"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Cloud Provider" prop="idc">
        <el-select v-model="idcArray" placeholder="Please select" multiple>
          <el-option
            v-for="(item, index) in idcOptions"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button class="button" type="primary" @click="search(false)">Search</el-button>
      </el-form-item>
    </el-form>
    <el-card class="overview">
      <div class="overview-container">
        <div class="table-title">Cluster Overview</div>
        <el-table stripe :data="clusterOverview" :header-cell-style="{fontWeight:600,color:'#606266'}">
          <template v-for="(item, index) in overviewHeader">
            <el-table-column align="center" :label="item.text" :key="index" :prop="item.value">
            </el-table-column>
          </template>
        </el-table>
      </div>
    </el-card>
     <el-card class="stat">
      <div class="stat-container">
        <div class="table-title">Cluster Statistics</div>
        <el-table stripe :data="clusterStat" :header-cell-style="{fontWeight:600,color:'#606266'}">
          <el-table-column align="center" label="Cluster Group" prop="clusterGroup"></el-table-column>
          <el-table-column align="center" label="Cluster">
            <template slot-scope="{ row }">
              <a class="button" @click="gotoInfo(row.clusterId)">
                {{ row.clusterName }}
              </a>
            </template>
          </el-table-column>
          <el-table-column align="center" label="Budget Unit" prop="machineBudget"></el-table-column>
          <el-table-column align="center" label="Node Count" prop="nodeNum">
            <template slot-scope="scope">
                <span>{{ scope.row.nodeNum }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-card>
  </div>
</template>

<script>
import { budgetList, overviewHeader, statHeader } from './constants/overview';
import { mapGetters } from 'vuex'
import { deployOverview } from '@/api/cluster'

export default {
  data() {
    return {
      budgetArray: [],
      idcArray: [],
      searchParams:{
        budgets: '',
        idcs: ''
      },
      idcOptions: [],
      budgetList: budgetList,
      overviewHeader: overviewHeader,
      statHeader: statHeader,
      clusterOverview: [],
      clusterStat: [],
    };
  },
  computed: {
    ...mapGetters(['budgets', 'idcs', 'selectInfo']),
  },
  methods: {
    search(isDefaultSearch) {
      if (!isDefaultSearch) {
        this.searchParams.budgets = this.budgetArray.join(',');
        this.searchParams.idcs = this.idcArray.join(',');
        this.$store.dispatch('setBudgets', this.searchParams.budgets);
        this.$store.dispatch('setIdcs', this.searchParams.idcs)
      }
      deployOverview(this.searchParams)
      .then(res => {
        this.clusterOverview = res.clusterOverview || [];
        this.clusterOverview.forEach(item => {
          item.ocrGroup = `${item.ocrGroupNode} / ${item.ocrGroupTotal}`;
          item.yonghuGroup = `${item.yonghuGroupNode} / ${item.yonghuGroupTotal}`;
          item.liveGroup = `${item.liveGroupNode} / ${item.liveGroupTotal}`;
          item.abGroup = `${item.abGroupNode} / ${item.abGroupTotal}`;
        })
        this.clusterStat = res.clusterStat || [];
        this.clusterStat.forEach(item => {
          item.nodeNum = '';
          item.idcList.forEach(el => {
            item.nodeNum += `${el.machineIdc}: ${el.currentNode}/${el.idcNode} \n`;
          });
        })
      })
    },
    gotoInfo(clusterId) {
      const route = this.$router.resolve({ 
        path: '/deploy-info', 
        query: { clusterId } 
      });
      window.open(route.href, '_blank');
    }
  },
  mounted() {
    this.idcOptions = this.selectInfo.idcOptions || [];
    this.searchParams.budgets = this.budgets || '';
    this.searchParams.idcs = this.idcs || '';
    if (this.searchParams.budgets && this.searchParams.idcs) {
      this.budgetArray = this.searchParams.budgets.split(',');
      this.idcArray = this.searchParams.idcs.split(',');
      this.search(true);
    }
  },
};
</script>

<style lang='scss' scoped>
.table-title {
  margin-bottom: 10px;
};
.stat-container {
  .button {
    cursor : pointer;
  }
  ::v-deep .el-table{
    .cell {
      white-space: pre-wrap;
      max-height: 500px;
      overflow: overlay;
    }
  };
}
</style>
