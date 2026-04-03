<template>
  <div class="package-list">
    <el-table
      stripe
      :data="fileList"
      class="package-table"
      :row-key="getRowKeys"
      :expand-row-keys="expands"
      @expand-change="clickRowHandle"
      :header-cell-style="{fontWeight:600,color:'#606266'}"
    >
      <template v-for="(item, index) in packageHeader">
        <el-table-column align="center" :prop="item.text" :label="item.text" :key="index" :show-overflow-tooltip="item.text === 'clusterList'">
        </el-table-column>
      </template>
      <el-table-column align="center" label="operate" width="200">
        <template slot-scope="{ row }">
          <el-button size="mini" type="primary" @click="remove(row)">remove</el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import { getPackageDetail, removeFile } from '@/api'
import { mapGetters } from 'vuex'
// import update from './update'
export default {
  data() {
    return {
      expands: [],
      packageHeader: [],
      packageInfo: {}
    }
  },
  props: {
    fileList: {
      type: Array,
      required: true,
      default: () => ([]),
    },
  },
  components: {
    // update: update
  },
  computed: {
    ...mapGetters(['tableInfo']),
  },
  mounted() {
    this.packageHeader = this.tableInfo.cosFileList || []
  },
  methods: {
    remove(row) {
      this.$confirm(`This operation will permanently delete this file, continue?`, 'Tip', {
        confirmButtonText: 'Confirm',
        cancelButtonText: 'Cancel',
        type: 'warning'
      }).then(() => {
        const params = {
        id: row.id
      }
      removeFile(params).then(res => {
        console.log(res)
        this.$emit('changePackageList', false)
        this.$message({
          type: 'success',
          message: 'Delete Success!'
        });
      }).catch(err => {
        console.log(err)
      })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: 'Cancelled'
        })
      })
    },
    getRowKeys(row) {
      return row.packageId
    },
    /**
     * View package details
     */
    clickRowHandle(row) {
      if (this.expands.includes(row.packageId)) {
        this.expands = this.expands.filter(val => val !== row.packageId)
      } else {
        this.expands.push(row.packageId)
      }
      this.packageList.forEach((item, index) => {
        if (item.packageId === row.packageId) {
          this.getPackageDetail(index, row.packageId)
          return
        }
      })
    },
    async getPackageDetail(index, packageId) {
      const params = {
        packageId: packageId
      }
      try {
        const packageInfo = await getPackageDetail(params)
        if (packageInfo.rows) {
          this.$set(this.packageList[index], 'innerList',  packageInfo.rows)
        }
      } catch (err) {
        this.$message.error('Failed to get package details')
      }
    },
  }
}
</script>

<style scoped lang="scss">
.package-list {
  margin-top: 10px;
}
</style>>
