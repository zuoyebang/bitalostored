import { Vue, Component, Prop, Watch } from 'vue-property-decorator'
import { getPcConfigList, syncPcConfig, updatePcConfig } from '@/api'
import { PcConfigItem } from '@/interfaces/home'
import AppMenu from '@/components/app-menu'
import { ServerNamespace } from '@/store'
import { ADD_ITEM, SET_OPEN } from '@/store/types'
import { ServerItem } from '@/interfaces/commons'
import { debounceTime, map, pluck } from 'rxjs/operators'

type PcConfigItem2 = PcConfigItem & { contentStr: string }

@Component({ components: { AppMenu } })
export default class PcConfig extends Vue {
  list: PcConfigItem2[] = []

  loading = -1

  isOutOfSync = false

  updateItem: PcConfigItem2 = null

  @ServerNamespace.Action(ADD_ITEM)
  addErrItem

  @ServerNamespace.Action(SET_OPEN)
  openLog

  render() {
    return (
      <v-card class={'mt-2'}>
        <v-card-title>PC Config</v-card-title>
        <v-card-text>
          {this.genPcConfigTable()}
        </v-card-text>
      </v-card>
    )
  }

  mounted() {
    this.onContentStrChange()
    this.getList()
  }

  async getList() {
    const { data: { data } } = await getPcConfigList()
    this.list = data.map((i: PcConfigItem) => ({ ...i, contentStr: JSON.stringify(i.content, null, 2) }))
    // this.checkIsOutOfSync(data)
  }

  checkIsOutOfSync(list: PcConfigItem[]) {
    if (list.some((i) => i.content.black_map)) {
      this.isOutOfSync = true
    }
  }

  async onClickUpdatePcConfig(index: number) {
    if (this.updateItem.content === null) {
      this.addErrItem({
        time: new Date(),
        url: 'null',
        msg: 'Incorrect format of content',
      } as ServerItem)
      this.openLog()
      return
    }
    this.loading = index
    try {
      const { data: { data } } = await updatePcConfig(this.updateItem)
      if (data === 'OK') {
        this.getList()
        this.isOutOfSync = true
      }
    } catch (e) {
      console.error(e)
    } finally {
      this.loading = -1
    }
  }

  async onClickSyncPcConfig() {
    await syncPcConfig()
    this.isOutOfSync = false
    this.getList()
  }

  onClickUpdate(on, i: PcConfigItem2, e) {
    this.updateItem = Object.assign({}, i)
    on.click(e)
  }

  genUpdateContent() {
    const { updateItem: u } = this
    return u && (
      <div class={'mt-5'}>
        <v-text-field
          class={'mb-2'}
          label={'name'}
          value={u.name}
          outlined={true}
          dense={true}
          oninput={(val: string) => this.updateItem.name = val}
        />
        <v-text-field
          className={'mb-2'}
          label={'remark'}
          value={u.remark}
          outlined={true}
          dense={true}
          oninput={(val: string) => this.updateItem.remark = val}
        />
        <v-textarea
          value={u.contentStr}
          label={'content'}
          outlined={true}
          dense={true}
          oninput={(val: string) => this.updateItem.contentStr = val}
          error-messages={this.isParseSuccess ? '' : 'Incorrect format of content'}
        />
      </div>
    )
  }

  genPcConfigTable() {
    const { isOutOfSync } = this
    return (
      <v-simple-table class="text-no-wrap">
        <thead>
          <tr>
            <th>
              {
                isOutOfSync ?
                  <v-btn small={true} onclick={this.onClickSyncPcConfig} color={'error'}>sync (OUT OF SYNC)</v-btn> :
                  'name'
              }
            </th>
            <th>remark</th>
            <th>content</th>
            <th>action</th>
          </tr>
        </thead>
        <tbody>
          {this.list.map((i, index) => (
            <tr>
              <td>{i.name}</td>
              <td>{i.remark}</td>
              <td>{
                this.$createElement(
                  'v-tooltip',
                  {
                    scopedSlots: {
                      activator: ({ on }) => this.$createElement('span', { on: on }, [i.contentStr]),
                    },
                    props: {
                      top: true,
                    },
                  },
                  [<pre>{i.contentStr}</pre>])
              }</td>
              <td>
                <app-menu
                  width={500}
                  title={'Confirm Update'}
                  content={this.genUpdateContent()}
                  activator={
                    (on) => (
                      <v-btn
                        color={'primary'}
                        small={true}
                        onclick={(e) => this.onClickUpdate(on, i, e)}
                        loading={this.loading === index}
                      >
                        <v-icon small={true} left={true}>mdi-update</v-icon>
                        update
                      </v-btn>
                    )
                  }
                  onconfirm={() => this.onClickUpdatePcConfig(index)}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </v-simple-table>
    )
  }

  isParseSuccess = true

  onContentStrChange() {
    this.$watchAsObservable('updateItem.contentStr')
      .pipe(
        debounceTime(500),
        pluck('newValue'),
        map((i: string) => {
          try {
            return JSON.parse(i)
          } catch (e) {
            return false
          }
        }),
      )
      .subscribe((flag) => {
        this.isParseSuccess = !!flag
        if (flag) {
          this.updateItem.content = flag
        } else {
          this.updateItem.content = null
        }
      })
  }
}
