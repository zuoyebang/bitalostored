import { Component, Prop, Vue } from 'vue-property-decorator';
import { V8SlotHistoryItem } from '@/interfaces/home';

@Component
export default class V8SlotHistoryRender extends Vue {
  @Prop({ default: () => [] })
  list: V8SlotHistoryItem[];


  headers = [
    { text: 'Action', value: 'action' },
    { text: 'StartSlotId', value: 'slotStart' },
    { text: 'EndSlotId', value: 'slotEnd' },
    { text: 'SourceGroupID', value: 'srcGroup' },
    { text: 'TargetGroupID', value: 'dstGroup' },
    { text: 'CreateTime', value: 'createTime' },
    { text: 'UpdateTime', value: 'updateTime' },
  ];

  render() {
    const { headers, listGetter } = this;
    return (
      <v-card class="mt-2">
        <v-card-title class="justify-space-between">
          V8 Slot Migrate
        </v-card-title>
        <v-card-text>
          {this.$createElement('v-data-table', {
            staticClass: 'text-no-wrap',
            props: {
              dense: true,
              headers,
              items: listGetter,
              sortBy: 'update_time',
              sortDesc: true,
            },
          })}
        </v-card-text>
      </v-card>
    );
  }

  get listGetter() {
    this.list.map((i) => ({ ...i }))
    // console.log("v8slot", this.list)
    return this.list
  }
}