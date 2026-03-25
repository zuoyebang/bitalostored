<template>
  <div class="box">
     <el-card class="box-card">
     <div class="matrix-container">
       <div class="matrix-header">
         <el-form class="demo-form-inline">
          <el-form-item label="Recent Tasks">
            <el-select class="select" v-model="timeInterval" placeholder="please select" @change="selectTask">
              <el-option
                v-for="item in timeList"
                :key="item.value"
                :label="item.key"
                :value="item.value"
              ></el-option>
            </el-select>
          </el-form-item>
        </el-form>
        <el-table stripe class="tableBox" style="height:100%" :data="tableList" :header-cell-style="{fontWeight:600,color:'#606266'}">
          <template v-for="(item, index) in tableHeader">
            <el-table-column v-if="item.text==='extra'" :resizable="true" align="center" height="300px" min-width="880px" :label="item.text" :key="index">
              <template slot-scope="{ row }">
                <span>{{ row[item.text] }}</span>
              </template>
            </el-table-column>
            <el-table-column v-else align="center" :label="item.text" :key="index">
              <template slot-scope="{ row }">
                <span>{{ row[item.text] }}</span>
              </template>
            </el-table-column>
          </template>
        </el-table>
       </div>
     </div>
    </el-card>
    
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import { getTaskList } from '@/api/task'

export default {
  data() {
    return {
      tableList: [],
      tableHeader: [],
      pageSize: 20,
      total: 0,
      offset: 0,
      timeList: [],
      page: 1,
      timeInterval: '',
    }
  },
  computed: {
    ...mapGetters(['tableInfo', 'selectInfo']),
  },
  mounted() {
    this.tableHeader = this.tableInfo.recentTaskInfos || []
    this.init()
  },
  methods: {
    init() {
      const timeList = this.selectInfo.taskTimeInterval || []
      this.timeInterval = timeList[0] || ''
      this.timeList = []
      timeList.forEach(item => {
        this.timeList.push({
          key: `${item/(24 * 3600)} days`,
          value: item
        })
      })
      this.getTaskList()
    },
     selectTask() {
      this.getTaskList()
    },
    getTaskList() {
      const { clusterId } = this.$route.query || ''
      const params = {
        limit: this.pageSize,
        offset: this.offset,
        clusterId: clusterId,
        timeInterval: this.timeInterval
      }
      getTaskList(params).then(res => {
        this.tableList = res.rows
        this.loading = false
        this.total = res.count
      }).catch(err => {
        this.loading = false
        console.log(err)
      })
    },
  }
}
</script>

<style scoped lang="scss">
.box {
  .pagination-con {
    width: 100%;
    text-align: center;
    .pagination {
      display: inline-block;
      margin-top: 30px;
    }
  }
}
.el-table .cell {
  max-height: 120px !important;
  overflow: hidden !important;} 
</style>
