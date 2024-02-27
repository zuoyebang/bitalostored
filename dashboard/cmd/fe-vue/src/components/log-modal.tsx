import {Component, Prop, Vue, Watch} from 'vue-property-decorator'
import { getLog } from '@/api'

@Component
export default class LogModal extends Vue {
  @Prop()
  logProxy

  @Watch('logProxy', {immediate: true})
  change(newValue: string){
    this.proxy = newValue;
  }
  
  proxy = ''
  query = ''
  queryTime = Number(localStorage.getItem('queryTime')) || new Date().getTime()
  logData = ''

  pickerOptions = {
    shortcuts: [{
      text: '当前整点',
      onClick(picker) {
        const date = new Date();
        date.setTime(Math.floor(date.getTime() / 3600000) * 3600000 );
        picker.$emit('pick', date);
      }
    }, {
      text: '一小时前整点',
      onClick(picker) {
        const date = new Date();
        date.setTime((Math.floor(date.getTime() / 3600000) - 1) * 3600000 );
        picker.$emit('pick', date);
      }
    }]
  }

  render() {
    return this.proxy && <v-container>
      <v-card-title>日志查询</v-card-title>
      <v-row no-gutters style="height: 80px;">
        <v-col
          cols="4"
        >
          <v-text-field
            v-model={this.proxy}
            label="目标节点"
            outlined
          ></v-text-field>
        </v-col>
        <v-col
          cols="4"
        >
          <v-text-field
            v-model={this.query}
            label="查询条件"
            outlined
          ></v-text-field>
        </v-col>
      </v-row>
      <v-row no-gutters style="height: 50px;">
        <v-col
          cols="8"
        >
          <span>时间</span>
          <el-date-picker
            v-model={this.queryTime}
            type="datetime"
            placeholder="选择日期时间"
            picker-options={this.pickerOptions}
            onchange={this.changeTime}
            value-format="timestamp"
          >
          </el-date-picker>
        </v-col>
      </v-row>
      <v-row no-gutters style="height: 45px;">
        <v-col
          cols="2"
        >
          <v-btn
            onclick={() => (this.query = 'timeout',this.search())}
          >timeout</v-btn>
        </v-col>
        <v-col
          cols="2"
        >
          <v-btn
            onclick={() => (this.query = 'error',this.search())}
          >error</v-btn>
        </v-col>
        <v-col
          cols="2"
        >
          <v-btn
            onclick={() => (this.query = 'slow',this.search())}
          >slow</v-btn>
        </v-col>
        <v-col
          cols="2"
        >
          <v-btn
            onclick={() => (this.query = 'tree',this.search())}
          >tree</v-btn>
        </v-col>
      </v-row>
      <v-btn
        onclick={() => this.search()}
        color="primary"
      >查询</v-btn>
      <div class="data-text" style="margin-top: 20px">
        { this.logData }
      </div>
    </v-container>
  };
  changeTime() {
    localStorage.setItem('queryTime', this.queryTime.toString());
  }
  search() {
    let arr = this.proxy.split(':');
    getLog(this.proxy, arr[0], this.query, this.queryTime / 1000)
    .then((res: any) => {
      const response = res.data;
      if (response.errNo !== 0) {
        this.logData = response.errMsg;
      } else {
        this.logData = response.data || '无日志';
      }
    })
    .catch(err => {
      console.log(err);
    })
  }
}