<template>
  <div class="deployDetail">
    <el-card class="detail">
      <div class="detail-container">
        <div class="table-title">Cluster ({{ type }})  {{ clusterName }}</div>
        <template v-for="(item, index) in idcOptions">
          <div class="table-title" :key="'title'+index">Cloud: {{item}}</div>
          <el-table 
            stripe 
            :data="tableData[item]" 
            :header-cell-style="{fontWeight:600,color:'#606266'}" 
            :key="index"
          >
            <el-table-column align="center" label="Budget Unit" prop="budget"></el-table-column>
            <el-table-column align="center" label="Machine List" prop="machineList">
              <template slot-scope="scope">
                <!-- <el-tooltip
                  effect="dark"
                  :content="scope.row.machineList"
                  placement="top"
                > -->
                  <!-- <div slot="content" style="white-space: pre-wrap">{{ scope.row.machineList }}</div> -->
                  <span>{{ scope.row.machineList }}</span>
                <!-- </el-tooltip> -->
              </template>
            </el-table-column>
          </el-table>
        </template>
      </div>
    </el-card>
  </div>
</template>

<script>
import { deployDetail } from '@/api/cluster'
import { mapGetters } from 'vuex'

export default {
  data() {
    return {
      clusterName: '',
      type: '',
      idcOptions: [],
      params: {
        clusterId: 0,
      },
      tableData: {},
    };
  },
  methods: {
    init() {
      const { clusterId, type } = this.$route.query || '';
      this.params.clusterId = Number(clusterId);
      this.type = type;
      deployDetail(this.params)
      .then(res => {
        this.clusterName = res.cluster.clusterName;
        res.serverStat.forEach(item => {
          item.machineList = '';
          item.machines.forEach(el => {
            item.machineList += `${el.machineIp}: ${el.currentNode}/${el.budgetNode}/${el.idcNode}/${el.nodeTotal} \n`
          })
        })
        this.idcOptions.forEach(item => {
          this.tableData[item] = res.serverStat.filter(el => el.idc === item);
        })
      })
    },
  },
  computed: {
    ...mapGetters(['tableInfo', 'selectInfo']),
  },
  mounted() {
    this.idcOptions = this.selectInfo.idcOptions || [];
    this.init();
  },
};
</script>

<style lang='scss' scoped>
.table-title {
  margin-bottom: 10px;
};
.detail-container {
  ::v-deep .el-table{
    .cell {
      white-space: pre-wrap;
      max-height: 500px;
      overflow: overlay;
    }
  };
}
</style>
