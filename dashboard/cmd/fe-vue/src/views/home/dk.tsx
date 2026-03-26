import {Vue, Component, Prop, Watch} from 'vue-property-decorator'
import {getDkList, createDk, removeDK} from '@/api'
import {DkItem} from '@/interfaces/home'
import AppMenu from '@/components/app-menu'
import {ServerNamespace} from '@/store'
import {ADD_ITEM, SET_OPEN} from '@/store/types'
import {ServerItem} from '@/interfaces/commons'
import {debounceTime, map, pluck} from 'rxjs/operators'

type PcConfigItem2 = DkItem & { contentStr: string }

@Component({components: {AppMenu}})
export default class PcConfig extends Vue {
  list: PcConfigItem2[] = []

  updateItem: PcConfigItem2 = null

  @ServerNamespace.Action(ADD_ITEM)
  addErrItem

  @ServerNamespace.Action(SET_OPEN)
  openLog

  render() {
    return (
      <v-card class={'mt-2'}>
        <v-card-title>
        <v-row align="center" no-gutters>
          <v-col cols="auto">
            Distributed keys
          </v-col>
          <v-col cols="auto" style="margin-left: 16px;">
          <app-menu
                width={500}
                title={'Create'}
                content={this.genCreateContent()}
                activator={
                  (on) => (
                    <v-btn
                      color={'primary'}
                      small={true}
                      onclick={(e) => this.onClickCreate(on, e)}
                    >
                      <v-icon small={true} left={true}>mdi-new-box</v-icon>
                      CREATE
                    </v-btn>
                  )
                }
                onconfirm={() => this.onClickCreateDk()}
              />
          </v-col>
        </v-row>
        </v-card-title>
        <v-card-text>
          {this.genDkTable()}
        </v-card-text>
      </v-card>
    )
  }

  mounted() {
    this.getList()
  }

  async getList() {
    const {data: {data}} = await getDkList()
    this.list = data.map((i: DkItem) => ({...i, contentStr: JSON.stringify(i.groupKeys, null, 2)}))
  }

  async onClickRemoveDk(key: string) {
    try {
      const {data: {data}} = await removeDK(key)
      if (data === 'OK') {
        this.getList()
      }
    } catch (e) {
      console.error(e)
    }
  }

  async onClickCreateDk() {
      try {
        this.updateItem.shardNum = parseInt(this.updateItem.shardNum as any)
        if (this.updateItem.key.length < 1 || this.updateItem.key.length > 512) {
          this.addErrItem({
            time: new Date(),
            url: 'null',
            msg: 'key length must be 1–512 characters',
          } as ServerItem)
          this.openLog()
          return
        }
        if (this.updateItem.shardNum < 1 || this.updateItem.shardNum > 1024) {
          this.addErrItem({
            time: new Date(),
            url: 'null',
            msg: 'shardNum must be between 1 and 1024',
          } as ServerItem)
          this.openLog()
          return
        }
        if (!this.updateItem.dataType) {
          this.addErrItem({
            time: new Date(),
            url: 'null',
            msg: 'dataType is required',
          } as ServerItem)
          this.openLog()
          return
        }
        await createDk(this.updateItem)
        this.getList()
      } catch (e) {
        console.error(e)
      }
  }
  

  onClickCreate(on, e) {
    this.updateItem = Object.assign({key: '', shardNum: 1, dataType: ''})
    on.click(e)
  }

  genCreateContent() {
    const {updateItem: u} = this
    return u && (
      <div class={'mt-5'}>
        <v-text-field
          class={'mb-2'}
          label={'key'}
          value={u.key}
          outlined={true}
          dense={true}
          oninput={(val: string) => this.updateItem.key = val}
        />
        <v-text-field
          className={'mb-2'}
          type="number"
          min="1"
          max="1024"
          label={'shardNum'}
          value={u.shardNum}
          outlined={true}
          dense={true}
          oninput={(val: number) => this.updateItem.shardNum = val}
        />
        <v-select
          className={'mb-2'}
          label={'dataType'}
          value={u.dataType}
          outlined={true}
          dense={true}
          items={['hash', 'set']}
          oninput={(val: string) => this.updateItem.dataType = val}
        />
      </div>
    )
  }

  genDkTable() {
    return (
      <v-simple-table class="text-no-wrap">
        <thead>
        <tr>
          <th>key</th>
          <th>shardNum</th>
          <th>dataType</th>
          <th>groupKeys</th>
          <th>action</th>
        </tr>
        </thead>
        <tbody>
        {this.list.map((i, index) => (
          <tr>
            <td>{i.key}</td>
            <td>{i.shardNum}</td>
            <td>{i.dataType}</td>
            <td>{
              this.$createElement(
                'v-tooltip',
                {
                  scopedSlots: {
                    activator: ({on}) => this.$createElement('span', {on: on}, [i.contentStr]),
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
                title={'Confirm Remove'}
                activator={
                  (on) => (
                    <v-btn
                      color={'primary'}
                      small={true}
                      onclick={on.click}
                    >
                      <v-icon small={true} left={true}>mdi-delete</v-icon>
                      REMOVE
                    </v-btn>
                  )
                }
                onconfirm={() => this.onClickRemoveDk(i.key)}
              />
            </td>
          </tr>
        ))}
        </tbody>
      </v-simple-table>
    )
  }

  isParseSuccess = true
}
