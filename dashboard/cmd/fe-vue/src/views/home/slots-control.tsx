import {Vue, Component, Emit} from 'vue-property-decorator'
import {MigrateRange, MigrateSome} from '@/interfaces/home'
import {createRange, createSome} from '@/api'

@Component
export default class SlotsControl extends Vue {
  range: MigrateRange = {
    from: '',
    to: '',
    group: '',
    migrate:1
  }

  some: MigrateSome = {
    slots: '',
    from: '',
    to: '',
  }

  isOpenRange = false
  isOpenSome = false

  render() {
    return <v-card-text class='mb-0 pb-0'>
      {this.genCreateRange()}
      {/*<v-divider class='mt-1 mb-3'/>*/}
      {this.genCreateSome()}
      {/*<v-divider/>*/}
    </v-card-text>
  }

  genCreateSome() {
    const {some: s, isOpenSome} = this
    return <v-layout style={{height: '66px'}}>
      <v-flex shrink class='pr-2'>
        <v-btn
          color='primary'
          onclick={() => this.isOpenSome = !isOpenSome}
        >Some <v-icon>mdi-chevron-{!isOpenSome ? 'left' : 'right'}</v-icon>
        </v-btn>
      </v-flex>
      <v-scroll-x-transition>
        {isOpenSome &&
        <div class='d-flex'>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='Number Of Slots'
              dense
              outlined value={s.slots}
              oninput={(val: string) => this.some.slots = val}
              persistent-hint
              hint={`[0, 1023]`}
            />
          </v-flex>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='From Group'
              dense
              outlined value={s.from}
              oninput={(val: string) => this.some.from = val}
              persistent-hint
              hint={`[1, 9999]`}
            />
          </v-flex>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='To Group'
              dense
              outlined
              value={s.to}
              oninput={(val: string) => this.some.to = val}
              persistent-hint
              hint={`[1, 9999]`}
            />
          </v-flex>
          <v-flex shrink>
            <v-btn color='primary' onclick={this.migrateSome}>Migrate Some</v-btn>
          </v-flex>
        </div>
        }</v-scroll-x-transition>
    </v-layout>
  }

  genCreateRange() {
    const {range: r, toMax, fromMin, isOpenRange} = this
    return <v-layout style={{height: '66px'}}>
      <v-flex shrink class='pr-2'>
        <v-btn
          color='primary'
          onclick={() => this.isOpenRange = !isOpenRange}
        >Range <v-icon>mdi-chevron-{!isOpenRange ? 'left' : 'right'}</v-icon>
        </v-btn>
      </v-flex>
      <v-scroll-x-transition>
        {isOpenRange && <div class='d-flex'>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='From'
              dense
              outlined value={r.from}
              oninput={(val: string) => this.range.from = val}
              persistent-hint
              hint={`[0, ${toMax}]`}
              rules={[(val: string) => val <= toMax || `≤ ${toMax}`]}
            />
          </v-flex>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='To'
              dense
              outlined value={r.to}
              oninput={(val: string) => this.range.to = val}
              persistent-hint
              hint={`[${fromMin}, 1023]`}
              rules={[(val: string) => val >= fromMin || `≥ ${fromMin}`]}
            />
          </v-flex>
          <v-flex shrink class='pr-2'>
            <v-text-field
              label='Group'
              dense
              outlined
              value={r.group}
              oninput={(val: string) => this.range.group = val}
              persistent-hint
              hint={`[1, 9999]`}
            />
          </v-flex>
          <v-flex shrink class='pr-2'>
            <v-select
              dense
              items={[{key:'是',value:1},{key:'否',value:0}]}
              item-text="key"
              item-value="value"
              oninput={(val: number) => {this.range.migrate = val;console.log(val)}}
              value={1}
              hide-details
              outlined
            />
          </v-flex>
          <v-flex shrink>
            <v-btn color='primary' onclick={this.migrateRange}>Migrate Range</v-btn>
          </v-flex>
        </div>}
      </v-scroll-x-transition>
    </v-layout>
  }

  @Emit('update')
  async migrateRange() {
    await createRange(this.range)
  }

  @Emit('update')
  async migrateSome() {
    await createSome(this.some)
  }

  get toMax() {
    const {range: r} = this
    return r.to ? r.to : 1023
  }

  get fromMin() {
    const {range: r} = this
    return r.from ? r.from : 0
  }
}
