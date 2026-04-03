<template>
  <div class="task" v-loading="loading">
    <task></task>
    <el-card class="box-publish">
      <div class="public-container">
        <el-form class="demo-form-inline">
          <el-form-item label="Unpublished Tasks">
            <el-button class="button" type="primary" @click="cancel()">cancel</el-button>
            <el-button class="button" type="primary" @click="publish()">publish</el-button>
          </el-form-item>
        </el-form>
      </div>
      <el-table
        ref="multipleTable"
        :header-cell-style="{fontWeight:600,color:'#606266'}"
        :data="unPublishTable"
        @selection-change="handleSelectionChange">
        <el-table-column
          type="selection"
          width="55">
        </el-table-column>
         <template v-for="(item, index) in tableHeader">
            <el-table-column align="center" :label="item.text" :key="index">
              <template slot-scope="{ row }">
                <span>{{ row[item.text] }}</span>
              </template>
            </el-table-column>
          </template>
      </el-table>
    </el-card>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import { getUnpublishTaskList, publishStatus } from '@/api/task'
import task from '@/components/task.vue'

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
      multipleSelection: [],
      unPublishTable: [],
      loading: true
    }
  },
  components: {
    task: task
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
      this.getUnpublishTaskList()
    },
    getUnpublishTaskList() {
      const { clusterId } = this.$route.query || ''
      const params = {
        clusterId: clusterId,
      }
      getUnpublishTaskList(params).then(res => {
        this.loading = false
        this.unPublishTable = res.rows || []
      }).then(err => {
        this.loading = false
        console.log(err)
      })
    },
    publish() {
      const params = {
        taskStatus: 'new'
      }
      this.changeTaskStatus(params)
    },
    cancel() {
      const params = {
        taskStatus: 'cancel'
      }
      this.changeTaskStatus(params)
    },
    changeTaskStatus(params) {
      if (this.multipleSelection.length === 0) {
        this.$message.success('please select')
        return
      }
      let taskIdArr = []
      this.multipleSelection.forEach(item => {
        taskIdArr.push(item.taskId)
      })
      params.taskIds = taskIdArr
      publishStatus(params).then(res => {
        console.log(res)
        this.getUnpublishTaskList()
        this.$message.success('success!')
      }).catch(err => {
        console.log(err)
      })
    },
    handleSelectionChange(val) {
      this.multipleSelection = val
    },
  }
}
</script>

<style scoped lang="scss">
.task {
  .pagination-con {
    width: 100%;
    text-align: center;
    .pagination {
      display: inline-block;
      margin-top: 30px;
    }
  }
  .button {
    float: right;
    margin-left: 10px;
  }
  .box-publish {
    margin-top: 10px;
  }
}
</style>