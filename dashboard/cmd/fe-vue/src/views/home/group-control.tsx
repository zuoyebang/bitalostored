import { Component, Emit, Prop, Vue } from 'vue-property-decorator'
import { addGroup, addServerToGroup, disableAll, enableAll, getCloudType, syncAll } from '@/api'
import { AddServerToGroupParams } from '@/interfaces/home'
import { SERVER_ROLE } from '@/constant'
import AppMenu from '@/components/app-menu'

@Component({ components: { AppMenu } })
export default class GroupControl extends Vue {
  newGroupId = ''
  CLOUD_TYPE_LIST = []
  addServer: AddServerToGroupParams = {
    cloudType: '',
    server_role: SERVER_ROLE[0],
    server: '',
    groupId: '',
  }
  token: ''
  cloudType = ''
  showAlert = false

  @Prop()
  models
  async created() {
    const data = await getCloudType();
    this.CLOUD_TYPE_LIST = data.data.data.clouds;
    this.addServer.cloudType = this.CLOUD_TYPE_LIST[0]
  }
  render() {
    return <v-card-text>
      {this.genAddGroup()}
      {this.genAddServer()}
      {this.genReplicaControl()}
    </v-card-text>
  }
  genReplicaControl() {
    return (
      <v-layout class='mt-3'>
        <v-flex shrink class='pr-2'>
          <app-menu
            width={400}
            activator={(on) => <v-btn color='primary' onclick={on.click}>Group: Sync All</v-btn>}
            onconfirm={() => this.onClickSyncAll()}
            title={`Resync All Groups: group-[${this.groupIdList.join(', ')}]`}
          />
        </v-flex>
        <v-flex shrink class='pr-2'>
          <app-menu
            activator={(on) => <v-btn color='success' onclick={on.click}>Replica: Enable All</v-btn>}
            onconfirm={() => this.onClickEnableAll()}
            title={`Enable All Groups: group-[${this.groupIdList.join(', ')}]`}
          />
        </v-flex>
        <v-flex shrink class='pr-2'>
          <app-menu
            title={`disable all?:`}
            onconfirm={() => this.onClickDisableAll()}
            activator={(on) => <v-btn color='error' onclick={on.click}>Replica: disable All</v-btn>}
          />
        </v-flex>
      </v-layout>
    )
  }

  genAddServer() {
    return (
      <v-layout class='mt-3'>
        <v-flex shrink class='pr-2'>
          <v-btn disabled={!this.canAddServer} color='primary' onclick={this.onClickAddServerToGroup}>add server</v-btn>
        </v-flex>
        <v-flex shrink class='pr-2'>
          <v-select
            dense
            style="width:150px"
            items={this.CLOUD_TYPE_LIST}
            oninput={(val: string) => this.addServer.cloudType = val}
            value={this.addServer.cloudType}
            hide-details
            outlined
          />
        </v-flex>
        <v-flex shrink class='pr-2'>
          <v-select
            dense
            style="width:220px"
            items={SERVER_ROLE}
            oninput={(val: string) => this.addServer.server_role = val}
            value={this.addServer.server_role}
            hide-details
            outlined
          />
        </v-flex>
        <v-flex shrink class='pr-2'>
          <v-text-field
            hide-details
            dense
            label='Stored Server Address'
            outlined
            value={this.addServer.server}
            oninput={(val: string) => this.addServer.server = val}
          />
        </v-flex>
        <v-flex shrink class='pr-2'>
          <v-text-field
            hide-details
            dense
            label='to GroupId'
            outlined
            value={this.addServer.groupId}
            oninput={(val: string) => this.addServer.groupId = val}
          />
        </v-flex>
      </v-layout>
    )
  }

  genAddGroup() {
    return <v-layout align-center>
      <v-flex shrink class='pr-2'>
        <v-btn disabled={!this.canAddGroup} color='primary' onclick={this.onClickAddGroup}>new group</v-btn>
      </v-flex>
      <v-flex shrink>
        <v-text-field
          hide-details
          dense
          label='GroupId'
          outlined
          value={this.newGroupId}
          oninput={(val: string) => this.newGroupId = val}
        />
      </v-flex>
    </v-layout>
  }

  get groupIdList(): string[] {
    return this.models.map((i) => i.id.toString())
  }

  get canAddServer() {
    const { server, groupId } = this.addServer
    return server && server.length && this.groupIdList.some((id) => id === groupId)
  }

  get canAddGroup() {
    return this.newGroupId &&
      this.newGroupId.length &&
      Number(this.newGroupId) > 0 &&
      this.groupIdList.every((id) => id !== this.newGroupId)
  }
  @Emit('update')
  async onClickSyncAll() {
    await syncAll()
  }

  @Emit('update')
  async onClickEnableAll() {
    await enableAll()
  }

  @Emit('update')
  async onClickDisableAll() {
    await disableAll()
  }

  @Emit('update')
  async onClickAddServerToGroup() {
    await addServerToGroup(this.addServer)
  }

  @Emit('update')
  async onClickAddGroup() {
    try {
      await addGroup(this.newGroupId)
    } catch (e) {
      console.error(e)
    }
  }
}
