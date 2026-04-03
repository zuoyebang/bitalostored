<template>
  <div class="deployInfo">
    <el-card class="info">
      <div class="info-container">
        <div class="table-title">Cluster {{ clusterName }}</div>
        <el-table stripe :data="idcList" :header-cell-style="{fontWeight:600,color:'#606266'}">
          <el-table-column align="center" label="Budget Unit" prop="machineBudget"></el-table-column>
          <el-table-column align="center" label="Cloud Provider" prop="machineIdc"></el-table-column>
          <el-table-column align="center" label="Proxy Node Count" prop="proxyNode">
            <template slot-scope="{ row }">
              <a class="button" @click="gotoDetail(proxyId, 'proxy')">
                {{ row.proxyNode }}
              </a>
            </template>
          </el-table-column>
          <el-table-column align="center" label="Server Node Count">
            <template slot-scope="{ row }">
              <a class="button" @click="gotoDetail(serverId, 'server')">
                {{ row.serverNode }}
              </a>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-card>
  </div>
</template>

<script>
import { deployInfo } from '@/api/cluster'

export default {
  data() {
    return {
      clusterName: '',
      idcList: [],
      params: {
        clusterId: 0,
      },
      proxyId: 0,
      serverId: 0
    };
  },
  methods: {
    init() {
      const { clusterId } = this.$route.query || '';
      this.params.clusterId = Number(clusterId);
      deployInfo(this.params)
      .then(res => {
        this.clusterName = res.cluster.clusterName;
        this.proxyId = res.proxy.clusterId || 0;
        this.serverId = res.cluster.clusterId || 0;
        this.idcList = res.idcList || [];
        this.idcList.forEach(item => {
          item.proxyNode = `${item.proxy.currentNode} / ${item.proxy.nodeTotal}`;
          item.serverNode = `${item.server.currentNode} / ${item.server.nodeTotal}`;
        })
      })
    },
    gotoDetail(id, type) {
      const route = this.$router.resolve({ 
        path: '/deploy-detail', 
        query: { clusterId: id, type } 
      });
      window.open(route.href, '_blank');
    }
  },
  mounted() {
    this.init();
  },
};
</script>

<style lang='scss' scoped>
.button {
  cursor : pointer;
}
.table-title {
  margin-bottom: 10px;
}
</style>
