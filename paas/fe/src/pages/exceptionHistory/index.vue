<template>
  <div class="operate-history">
    <el-form class="demo-form-inline" inline>
      <el-form-item label="Cluster Name">
        <!--
        <el-input
          @input="search('init')"
          v-model="searchParams.clusterName"
          autocomplete="off"
          clearable
        ></el-input>
        -->
        <el-select
          v-model="searchParams.clusterName"
          placeholder="Please select"
          @change="search"
          filterable
          :clearable="true"
        >
          <el-option
            v-for="(item, index) in clusterNameList"
            :key="index"
            :label="item"
            :value="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Status">
        <el-select
          v-model="searchParams.status"
          @change="search('init')"
          placeholder="Please select"
        >
          <el-option
            v-for="(item, index) in statusList"
            :key="index"
            :label="item.name"
            :value="item.status"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Alert Level">
        <el-select
          v-model="searchParams.alertLevel"
          @change="search('init')"
          placeholder="Please select"
        >
          <el-option
            v-for="(item, index) in alertLevelList"
            :key="index"
            :label="item.name"
            :value="item.alertLevel"
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
            >
              <template slot-scope="{ row }">
                <div v-if="item.label === 'Status'">
                  {{ transStatus(row[item.value]) }}
                </div>
                <div v-else-if="item.label === 'Alert Level'">
                  {{ transLevel(row[item.value]) }}
                </div>
                <div v-else>
                  {{ row[item.value] }}
                </div>
              </template>
            </el-table-column>
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
import { tableHeader } from "./constants/index";
import { getExceptionHistroy } from "@/api/exceptionHistroy.js";

export default {
  components: {},
  data() {
    return {
      tableHeader: tableHeader,
      clusterNameList: [],
      statusList: [
        { name: "All", status: -1 },
        { name: "Pending", status: 0 },
        { name: "Processing", status: 1 },
        { name: "Resolved", status: 2 },
        { name: "Ignored", status: 3 },
      ],
      alertLevelList: [
        { name: "All", alertLevel: -1 },
        { name: "P0", alertLevel: 0 },
        { name: "P1", alertLevel: 1 },
        { name: "P2", alertLevel: 2 },
      ],
      timeFilter: "",
      searchParams: {
        clusterName: "",
        status: -1,
        alertLevel: -1,
      },
      pagination: {
        total: 0,
        curPage: 0,
        num: 20,
      },
      tableData: [],
    };
  },
  computed: {},
  methods: {
    transStatus(status) {
      let res = "";
      switch (status) {
        case 0:
          res = "Pending";
          break;
        case 1:
          res = "Processing";
          break;
        case 2:
          res = "Resolved";
          break;
        case 3:
          res = "Ignored";
      }
      return res;
    },
    transLevel(alertLevel) {
      let res = "";
      switch (alertLevel) {
        case 0:
          res = "P0";
          break;
        case 1:
          res = "P1";
          break;
        case 2:
          res = "P2";
          break;
      }
      return res;
    },
    search(type) {
      if (type === "init") {
        this.pagination = {
          total: 0,
          curPage: 0,
          num: 20,
        };
      }
      let startTime = 0;
      let endTime = 0;
      if (this.timeFilter) {
        startTime = this.timeFilter[0] / 1000;
        endTime = this.timeFilter[1] / 1000;
      }
      getExceptionHistroy({
        ...this.searchParams,
        startTime,
        endTime,
        page: this.pagination.curPage,
      }).then((data) => {
        const { clusterNameList = [], rows = [], count } = data;
        this.tableData = rows;
        this.clusterNameList = clusterNameList;
        this.pagination.total = count;
      });
    },
  },
  mounted() {
    this.search();
  },
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
