import { Component, Prop, Vue } from 'vue-property-decorator'
import { MigrateTableItem, SlotAction } from '@/interfaces/home'
import { MigratingStatus } from '@/constant'
import { migrate } from '@/api'

@Component
export default class MigrateTable extends Vue {
  @Prop({ default: () => [] })
  list: MigrateTableItem[]

  @Prop()
  slotAction: SlotAction

  headers = [
    { text: 'SlotId', value: 'sid' },
    { text: 'SourceGroupID', value: 'source_group_id' },
    { text: 'TargetGroupID', value: 'target_group_id' },
    { text: 'TargetAddress', value: 'to' },
    { text: 'Total', value: 'total' },
    { text: 'Fails', value: 'fails' },
    { text: 'SuccPercent', value: 'succ_percent' },
    { text: 'Cost/ms', value: 'costs' },
    { text: 'Status', value: 'status' },
    { text: 'CreateTime', value: 'create_time' },
    { text: 'UpdateTime', value: 'update_time' },
  ]
  dialog = false
  type = 0

  clickBtn(type: number) {
    this.dialog = true;
    this.type = type;
    // const data = await migrate(type);
  }

  async clickConfirm() {
    this.dialog = false;
    const data = await migrate(this.type);
  }

  render() {
    const { headers, listGetter } = this
    return (
      <v-card class={'mt-2'}>
        <v-card-title class={'justify-space-between'}>
          Migrate List
          <div>
            {
              this.slotAction && this.slotAction.disabled ?
                <v-btn onclick={() => this.clickBtn(0)} color={"success"}>start migrate</v-btn>
                :
                <v-btn onclick={() => this.clickBtn(1)} color={'error'}>stop migrate</v-btn>
            }
          </div>
        </v-card-title>
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
        <v-card-text>
          {
            this.$createElement('v-data-table', {
              staticClass: 'text-no-wrap',
              props: {
                dense: true,
                headers,
                items: listGetter,
                sortBy: 'update_time',
                sortDesc: true,
              },
              scopedSlots: {
                'item.status': (i) => <v-chip
                  x-small={true}
                  color={i.value === 0 ? 'error' : i.value === 1 ? 'warning' : ''}
                >
                  {MigratingStatus[i.value]}
                </v-chip>,
              },
            })
          }
        </v-card-text>
      </v-card>
    )
  }

  get listGetter() {
    return this.list.map((i) => ({ ...i, ...i.status }))
  }
}
