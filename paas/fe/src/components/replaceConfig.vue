<template>
  <div class="new-package">
    <el-dialog title="replace Config" :before-close="close" :visible.sync="dialogRepalce">
      <el-row>
        <el-col :span="12" class="col">
          oldConf：<br/>
          <div style="margin: 30px;"></div>
          <table width="100%" height="100%">
            <tr> 
              <td ><el-input type="textarea" :rows="16" v-model="oldConfig" placeholder="please enter content"></el-input></td>
            </tr>
          </table>
        </el-col>
        <el-col :span="12" class="col">
          newConf：<br/>
          <div style="margin: 30px;"></div>
          <table width="100%" height="100%">
            <tr> 
              <td ><el-input type="textarea" :rows="16"  v-model="newConfig" placeholder="please enter content"></el-input></td>
            </tr>
          </table>
        </el-col>
      </el-row>
      <span slot="footer" class="dialog-footer">
        <el-button @click="close()">Cancel</el-button>
        <el-button type="primary" @click="replaceOk">Confirm</el-button>
      </span>
    </el-dialog>
  </div>
</template>
<script>
import { configReplace } from '@/api/'
export default {
  data() {
    return {
      property: 'value',
      oldConfig: '',
      newConfig: ''
    };
  },
  props: {
    dialogRepalce: {
      type: Boolean,
      required: true,
      default: false,
    },
  },
  methods: {
    close() {
      this.$emit('changeDialogRepalce', false)
    },
    replaceOk() {
      const params = {
        oldConf: this.oldConfig,
        newConf: this.newConfig
      }
      configReplace(params).then((res) => {
       console.log(res)
      }).catch((err) => {
        console.log(err)
      })
      this.$emit('changeDialogRepalce', false)
    }
  },
}
</script>

<style lang="scss" scoped>
.el-textarea__inner{
  height: 400px;
}
</style>