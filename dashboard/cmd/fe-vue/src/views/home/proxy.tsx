import Vue from 'vue'
import { CLOUD_TYPE_LIST } from '@/constant'
import { addProxy, deleteProxy, syncProxy, forceDel, hotkeys, slowkeys, crossCloud } from '@/api'
import AppMenu from '@/components/app-menu'
import AppModal from '@/components/app-modal'
import { ProxyModels } from '@/interfaces/home'

export default Vue.extend({
  props: ['modelsList', 'statsList', 'readCrossCloud'],
  components: { DeleteMenu: AppMenu, AppModal: AppModal },
  data() {
    return {
      address: '',
      cloudTypeSel: CLOUD_TYPE_LIST[0],
      isShow: true,
      content: {},
      dialog: false,
      val: 0,
      tencentNum: 0,
      aliNum: 0,
      bdNum: 0,
      isShowLog: false,
      logProxy: '',
    }
  },
  render() {
    return <v-card class='mt-2'>
      <v-card-title class={'pb-2'}>Proxy</v-card-title>
      <v-card-text>
        {this.genProxyAddForm()}
        {this.calcProxyNum()}
        {this.genProxyTable()}
        {this.getLog()}
      </v-card-text>
    </v-card>
  },
  methods: {
    calcProxyNum() {
      var txcloudNum = 0
      var aliNum = 0
      var tencentNum = 0
      var bdNum = 0
      this.mergedList.map((i) => {
        if (i.cloudtype === "txcloud") {
          txcloudNum++
        }
        if (i.cloudtype == 'tencent') {
          tencentNum++
        }
        if (i.cloudtype == 'ali') {
          aliNum++
        }
        if (i.cloudtype == 'baidu') {
          bdNum++
        }
      })
      return <v-row>txcloud: {txcloudNum} tencent: {tencentNum} ali:{aliNum} baidu:{bdNum}</v-row>
    },
    genProxyAddForm() {
      return <v-row>
        {
          this.readCrossCloud ?
            <v-btn class={'pb-btn'} onclick={() => this.crossCloud(0)} color={"error"}>forbid read cross cloud</v-btn>
            :
            <v-btn class={'pb-btn'} onclick={() => this.crossCloud(1)} color={'success'}>allow read cross cloud</v-btn>
        }
        <v-dialog v-model={this.dialog} width="500" style="backgroundColor: white">
          <v-card-title style="backgroundColor: white">Prompt</v-card-title>
          <v-card-text style="backgroundColor: white">
            Are you sure about this?
          </v-card-text>
          <v-card-actions style="backgroundColor: white">
            <v-spacer></v-spacer>
            <v-btn onclick={() => this.dialog = false}>cancel</v-btn>
            <v-btn onclick={() => this.clickConfirm()}>confirm</v-btn>
          </v-card-actions>
        </v-dialog>
      </v-row>
    },
    genProxyTable() {
      return <v-simple-table dense>
        <thead>
          <tr>
            <th>ID</th>
            <th>Stats</th>
            <th>Proxy</th>
            <th>Admin</th>
            <th>StartTime</th>
            <th>Version</th>
            <th></th>
            <th>CloudType</th>
            <th>crosscloud</th>
            <th>Sessions</th>
            <th>Commands</th>
          </tr>
        </thead>
        <tbody>
          {this.mergedList.map((i) => <tr class={'text-no-wrap' + (i.error ? ' red darken-4' : '')}>
            <td>{i.id}</td>
            <td>
              <v-btn x-small color='primary' href={i.f} target='_blank'>F</v-btn>
              <v-btn x-small href={i.s} target='_blank' class='ml-1'>S</v-btn>
            </td>
            <td>{i.hostport}</td>
            <td>{i.adminHost}</td>
            <td>{i.startTime}</td>
            <td>{i.version_tag}</td>
            <td>
              <delete-menu
                title={`Reinit and Start proxy:`}
                content={<pre>{JSON.stringify(i, null, 2)}</pre>}
                onconfirm={() => this.onSync(i)}
                activator={(on) => <v-btn x-small color='success' onclick={on.click}>sync</v-btn>}
              />

              <delete-menu
                item={i}
                onconfirm={this.onConfirmDeleteProxy}
                content={
                  <div class={'text-no-wrap'}>
                    <div><b>Remove</b> and <b>Shutdown</b></div>
                    <div>
                      <pre>{JSON.stringify(i, null, 2)}</pre>
                    </div>
                  </div>
                }
                activator={
                  (on) => (
                    <v-btn
                      x-small
                      class='ml-1'
                      color='error'
                      onclick={on.click}
                    >
                      DEL
                    </v-btn>
                  )
                }
              />
              {i.error && <v-icon color='error' onclick={() => forceDel(i)}>mdi-delete</v-icon>}
            </td>
            <td>
              <v-chip small>{i.cloudtype}</v-chip>
            </td>
            <td>{JSON.stringify(i.read_cross_cloud)}</td>
            <td>{JSON.stringify(i.sessions)}</td>
            <td class='text-wrap'>{JSON.stringify(i.cdm_ops)}</td>
          </tr>)}
        </tbody>
      </v-simple-table>
    },
    getLog() {
      return this.isShowLog && <v-dialog
        v-model={this.isShowLog} width="80%">
        <log-modal logProxy={this.logProxy}></log-modal>
      </v-dialog>
    },
    async onAddProxy() {
      await addProxy(this.address, this.cloudTypeSel)
      this.$emit('update')
    },
    async clickHotkeys() {
      this.content = {}
      const hotkeyData = await hotkeys()
      this.content = hotkeyData.data.data
      this.$emit('update')
    },
    async clickSlowkeys() {
      this.content = {}
      const slowkeysData = await slowkeys()
      this.isShow = true
      this.content = slowkeysData.data.data
      this.$emit('update')
    },
    async onSync(item) {
      await syncProxy(item)
      this.$emit('update')
    },
    async onConfirmDeleteProxy(item: ProxyModels) {
      await deleteProxy(item)
      this.$emit('update')
    },
    crossCloud(val: number) {
      this.dialog = true;
      this.val = val;
      // await crossCloud(val)
    },
    async clickConfirm() {
      this.dialog = false;
      await crossCloud(this.val);
    }
  },
  computed: {
    mergedList() {
      return this.modelsList.map((i) => {
        const adminHost = (
          i.hostport ?
            i.hostport.split(':')[0] :
            i.admin_addr.split(':')[0]
        )
          + ':' + i.admin_addr.split(':')[1]
        const stats = this.statsList[i.token].stats || { error: true }
        return {
          ...i,
          ...stats,
          startTime: i.start_time.substring(0, 19),
          f: `http://${adminHost}/proxy/`,
          s: `http://${adminHost}/proxy/stats`,
          adminHost,
        }
      })
    },
  },
})
