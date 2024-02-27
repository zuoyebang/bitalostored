import Vue from 'vue'
import {getColor} from '@/constant'
import {Slots} from '@/interfaces/home'

interface SlotItem {
  groupId: number;
  color: string;
  from: number;
  to: number;
  length: number;
  fromGroupId?: number;
}

const formatList = (list) => list.reduce((d, i) => {
  if (d.data[d.current]) {
    if (d.data[d.current].groupId !== i.group_id) {
      d.current += 1
    } else {
      const from = d.data[d.current].from
      d.data[d.current] = {
        ...d.data[d.current],
        to: i.id,
        length: i.id - from + 1,
      }
      return d
    }
  }
  const preId = d.data[d.current - 1] ? d.data[d.current - 1] : 0
  const left = i.id - preId
  d.data[d.current] = {
    groupId: i.group_id,
    color: getColor(i.group_id),
    from: i.id,
    to: i.id,
    length: 1,
    left: left,
    fromGroupId: i.fromGroupId,
  }
  return d
}, {current: 0, data: []}).data

export default Vue.extend({
  props: ['list'],
  data() {
    return {
      // list: d.data.slots,
      scaleList: Array.from(new Array(1025)).map((i, index) => index).filter((i) => i % 64 === 0),
    }
  },
  render() {
    return this.genSlotsContent()

  },
  methods: {
    genSlotsContent() {
      return <v-card-text class='pb-5 pr-5 pt-0 mt-0'>
        {this.genScaleX()}
        {this.genSlot('Offline', this.offlineList)}
        {this.genSlot('Migrating', this.migratingList)}
        {this.genSlot('Default', this.defaultList)}
      </v-card-text>
    },
    genSlot(title: string, list: SlotItem[]) {
      return <v-row>
        <v-col cols={2} class='text-right slots-title'>{title}</v-col>
        <v-col class='d-flex flex-nowrap relative ma-3 pa-0 slots-state'>
          {
            list.map((i) => (
              this.$createElement('v-menu', {
                  props: {openOnHover: '', top: '', offsetY: '', maxWidth: 150},
                  scopedSlots: {
                    activator: ({on}) => this.$createElement(
                      'div',
                      {
                        staticClass: 'slots-item', on,
                        style: {
                          width: i.length / 10.24 + '%',
                          backgroundColor: i.color,
                          left: i.from / 10.24 + '%',
                        },
                      },
                    ),
                  },
                },
                [
                  <v-card>
                    <v-card-text>
                      Group: {i.groupId}
                      <br/>
                      Slot: [{i.from}, {i.to}]
                      <br/>
                      Count: {i.to - i.from + 1}
                      {i.fromGroupId && <br/>}
                      {i.fromGroupId && 'FromGroup: ' + i.fromGroupId}
                    </v-card-text>
                  </v-card>,
                ])
            ))
          }
        </v-col>
      </v-row>
    },
    genScaleX() {
      return <v-row style={{position: 'relative', bottom: '-127px'}}>
        <v-col cols={2} class='py-0 slots-title'/>
        <v-col class='pa-0 mx-3 relative'>
          {this.scaleList.map((i) => <div class='scale-item' style={{left: i / 10.24 + '%'}}><span>{i}</span></div>)}
        </v-col>
      </v-row>
    },
  },
  computed: {
    offlineList() {
      return this.list.filter((i: Slots) => i.group_id === 0)
    },
    onlineList() {
      return this.list.filter((i: Slots) => i.group_id !== 0)
    },
    migratingList() {
      return formatList(this.onlineList.filter((i: Slots) => i.action && i.action.state === 'pending').map((i) => ({
        ...i,
        'group_id': i.action.target_id,
        fromGroupId: i.group_id,
      })))
    },
    defaultList(): SlotItem[] {
      return formatList(this.onlineList)
    },
  },
})
