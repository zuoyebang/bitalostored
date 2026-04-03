<template>
  <div class="operate-history">
    <el-form class="demo-form-inline" inline>
      <el-form-item label="uid">
        <el-select
          v-model="searchParams.uid"
          placeholder="Please select"
          @change="search('init')"
          :clearable="true"
        >
          <el-option
            v-for="(item, index) in uidList"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Module">
        <el-select
          v-model="searchParams.module"
          @change="search('init')"
          :clearable="true"
          placeholder="Please select"
        >
          <el-option
            v-for="(item, index) in moduleList"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-date-picker
          @change="search('init')"
          value-format="timestamp"
          v-model="timeFilter"
          type="daterange"
          range-separator="to"
          start-placeholder="Start Date"
          end-placeholder="End Date"
        >
        </el-date-picker>
      </el-form-item>
    </el-form>
    <p class="tip">Operation records are saved for 6 months</p>
    <el-card>
      <div class="operate-history-container">
        <el-table
          stripe
          :data="tableData"
          :header-cell-style="{ fontWeight: 600, color: '#606266' }"
        >
          <template v-for="(item, index) in tableHeader">
            <el-table-column
              :label="item.label"
              :prop="item.value"
              align="center"
              :show-overflow-tooltip="true"
              :key="index"
            ></el-table-column>
          </template>
        </el-table>
      </div>
      <el-pagination
        class="history-pagination"
        layout="total, prev, pager, next, jumper"
        :total="pagination.total"
        :page-size="pagination.num"
        @current-change="search"
        :current-page.sync="pagination.curPage"
      >
      </el-pagination>
    </el-card>
  </div>
</template>

<script>
import { tableHeader } from './constants/index';
import { getOperationList } from '@/api/operateHistory.js';

export default {
  components: {},
  data() {
    return {
      tableHeader: tableHeader,
      uidList: [],
      moduleList: ['dashboard', 'stored-paas'],
      timeFilter: '',
      searchParams: {
        uid: '',
        module: ''
      },
      pagination: {
        total: 0,
        curPage: 0,
        num: 20
      },
      tableData: []
    };
  },
  computed: {},
  methods: {
    search(type) {
      if (type === 'init') {
        this.pagination = {
          total: 0,
          curPage: 0,
          num: 20
        };
      }
      let startTime = 0;
      let endTime = 0;
      if (this.timeFilter) {
        startTime = this.timeFilter[0] / 1000;
        endTime = this.timeFilter[1] / 1000;
      }
      getOperationList({
        ...this.searchParams,
        startTime: startTime,
        endTime: endTime,
        page: this.pagination.curPage
      }).then((data) => {
        const { uidList = [], rows = [], count } = data;
        this.tableData = rows;
        this.uidList = uidList;
        this.pagination.total = count;
      });
    }
  },
  mounted() {
    this.search();
  }
};
</script>

<style lang='scss' scoped>
.operate-history-container {
  ::v-deep .el-table {
    .cell {
      white-space: pre-wrap;
      max-height: 500px;
      cursor: default;
      overflow: overlay;
    }
  }
}

.tip {
  margin-bottom: 20px;
}
.history-pagination {
  margin-top: 20px;
  text-align: right;
}
</style>
