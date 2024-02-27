import { Vue, Component, Prop, Emit } from 'vue-property-decorator'
import { GMC, GroupModels, GroupModelsComputed, GroupModelsServer, GroupStats } from '@/interfaces/home'
import { deleteGroup, deleteServerToGroup, compact, logCompact, handleNode, pendingGroupServer, promoteGroup, replicaGroups, syncGroup, degradeGroup, enableGroup, showMembership } from '@/api'
import AppMenu from '@/components/app-menu'
import LogModal from '@/components/log-modal'


@Component({ components: { DeleteMenu: AppMenu, LogModal } })
export default class GroupView extends Vue {
  @Prop()
  host

  @Prop()
  models

  @Prop()
  stats

  loading = {
    index: -1,
  }
  snackbar = false
  degradeLoading = false
  isShowLog = false
  logProxy = ''
  dbtype = 'bdb'


  membershipData = {}
  membership = {}
  render() {
    return <v-card-text class='pt-0 mt-0'>
      <v-snackbar vModel={this.snackbar} timeout={2000} top={true} color={'success'}  >Copy successful</v-snackbar>
      {this.list.map((i, index) => this.genGroupTable2(i, index))}
      {this.getLog()}
    </v-card-text>
  }

  genGroupBtn({ btnTitle, onclick, color, title, content }) {
    return <delete-menu
      title={title}
      activator={(on) => <v-btn x-small={true} color={color} onclick={on.click}>{btnTitle}</v-btn>}
      onconfirm={() => onclick()}
      content={JSON.stringify(content, null, 2)}
    />
  }

  bithashKey(listK, stringK, hashK) {
    var totalK = listK + stringK + hashK
    if (totalK < 1000) {
      return <span>{totalK}</span>
    } else if (totalK > 1000 && totalK < 1000000) {
      return <span>{(totalK / 1000).toFixed(0)}K</span>
    } else {
      return <span>{(totalK / 1000000).toFixed(0)}M</span>
    }
  }

  formatSize(size) {
    if (size < 1024) {
      return <span>{size}B</span>
    } else if (size >= 1024 && size < 1024 * 1024) {
      return <span>{((Number(size) / 1024)).toFixed(2)}KB</span>
    } else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
      return <span>{(Number(size) / 1024 / 1024).toFixed(2)}MB</span>
    } else {
      return <span>{(Number(size) / 1024 / 1024 / 1024).toFixed(2)}GB</span>
    }
  }

  genGroupTable2(i: GroupModelsComputed, index) {
    const content = { id: i.id, servers: i.servers.map((s) => s.server) }
    let allDown: boolean = true
    for (let s of i.servers.values()) {
      let serror: boolean = s.error && true
      if (!serror) {
        allDown = false
        break
      }
    }
    return (
      <v-card class={index === 0 ? 'mt-2' : 'mt-5'}>
        <v-simple-table dense={true} border={true}>
          <thead>
            <tr>
              <th class={'d-flex align-center'} style="min-width: 50px;">
                G{i.id}
                {
                  (allDown || !i.servers || !i.servers.length) &&
                  <v-btn x-small icon onclick={() => this.onClickDeleteGroup(i.id)}>
                    <v-icon small color='error'>mdi-delete</v-icon>
                  </v-btn>
                }
                {
                  i.out_of_sync && <v-btn color={'error'} text x-small>out of sync</v-btn>
                }
                <delete-menu
                  onconfirm={() => this.onClickLogCompact(i.id)}
                  width={'600'}
                  title={'Prompt'}
                  content={
                    <pre>logcompact?</pre>
                  }
                  activator={(on) => <span onclick={on.click}>
                    {this.hoverShowMes('mdi-alpha-l-circle-outline', 'logCompact', 'blue-grey darken-2',)}
                  </span>}
                />
              </th>
              <th style="min-width: 20px;">Replica</th>
              <th style="min-width: 120px;"></th>
              <th style="min-width: 50px">Server/Raft</th>
              <th style="min-width: 20px">Role</th>
              <th style="min-width: 120px;">Cluster</th>
              <th style="min-width: 50px;">Mem/ShrMem</th>
              <th style="min-width: 50px;">DataSize/DiskSize</th>
              <th style="min-width: 100px;">StartTime</th>
            </tr>
          </thead>
          <tbody>
            {i.servers.map((s, index) => (
              <tr class={s.error && 'red darken-4'}>
                <td>
                  {index === 0 ?
                    this.genGroupBtn({
                      title: <span>Resync <b>Group-{i.id}</b></span>,
                      btnTitle: 'sync',
                      color: 'primary',
                      onclick: () => this.onClickGroupItemSync(s, i, index),
                      content,
                    }) :
                    this.genGroupBtn({
                      title: <span>Promote server {s.server} from <b>Group-{i.id}</b></span>,
                      btnTitle: 'promote',
                      color: 'warning',
                      onclick: () => this.onClickGroupItemSync(s, i, index),
                      content,
                    })}
                </td>
                <td>
                  {s.stats.role === 'witness' || s.stats.start_model === 'observer' ?
                    <v-checkbox
                      dense
                      className='ma-0 pa-0'
                      label=''
                      hide-details
                      input-value={s.replica_group}
                      onchange={() => this.onChangeItemReplica(s, i, index)}
                      loading={this.loading.index === index}
                      disabled
                    /> :
                    <v-checkbox
                      dense
                      className='ma-0 pa-0'
                      label=''
                      hide-details
                      input-value={s.replica_group}
                      onchange={() => this.onChangeItemReplica(s, i, index)}
                      loading={this.loading.index === index}
                    />
                  }
                </td>
                <td>
                  {s.stats.start_model && s.stats.start_model !== 'witness' && <delete-menu
                    title={'Content'}
                    content={this.membershipData}
                    activator={(on) => <v-btn
                      small
                      icon
                      onclick={(e) => {
                        on.click(e);
                        this.onClickShowMembership(s, i)
                      }}
                    >
                      <v-icon color='error'>mdi-notebook</v-icon>
                    </v-btn>}
                  />}
                  <delete-menu
                    onconfirm={() => this.onClickDeleteServer(s, i)}
                    title={<span>Remove Server <b>{s.server}</b> from <b>Group-{i.id}</b>:</span>}
                    content={
                      <pre>{JSON.stringify(i, null, 2)}</pre>
                    }
                    activator={(on) => <v-btn
                      small
                      icon
                      onclick={on.click}
                    >
                      <v-icon color='error'>mdi-delete</v-icon>
                    </v-btn>}
                  />

                  {s.stats.start_model && <delete-menu
                    onconfirm={() => this.onClickNode(s, i, 1)}
                    width={'600'}
                    title={'Prompt'}
                    content={
                      <pre>Add a note？</pre>
                    }
                    activator={(on) => <span onclick={on.click}>
                      {this.hoverShowMes('mdi-plus', 'MountNode', 'blue-grey darken-2',)}
                    </span>}
                  />}
                  {s.stats.start_model && <delete-menu
                    onconfirm={() => this.onClickNode(s, i, 3)}
                    content={
                      <pre>This operation will remove this node from the Raft cluster. Please confirm the node's decommission and ensure the cluster can elect a new leader properly.</pre>
                    }
                    activator={(on) => <v-btn
                      small
                      icon
                      onclick={on.click}
                    >
                      <v-icon color='error'>mdi-minus</v-icon>
                    </v-btn>}
                  />}
                  {s.stats.start_model === 'observer' && <delete-menu
                    onconfirm={() => this.onClickNode(s, i, 2)}
                    width={'600'}
                    title={'Prompt'}
                    content={
                      <pre>Join the cluster？</pre>
                    }
                    activator={(on) => <span onclick={on.click}>
                      {this.hoverShowMes('mdi-arrow-up-bold-box-outline', 'MountObserver', 'orange darken-2',)}
                    </span>}
                  />}
                  {s.stats.start_model === 'witness' && <delete-menu
                    onconfirm={() => this.onClickNode(s, i, 4)}
                    width={'600'}
                    title={'Prompt'}
                    content={
                      <pre>Join the cluster？</pre>
                    }
                    activator={(on) => <span onclick={on.click}>
                      {this.hoverShowMes('mdi-gavel', 'MountWitness', 'orange darken-2',)}
                    </span>}
                  />}
                </td>
                <td>
                  <v-btn
                    className={''}
                    x-small={true}
                    color="blue"
                    onclick={() => this.$copyText(s.server).then(() => this.snackbar = true)}
                  >
                    {s.server}
                  </v-btn>
                  <v-btn
                    small={true}
                    icon={true}
                    href={`http://${this.host}/api/topom/group/info/${s.server}`}
                    target='_blank'
                  >
                    <v-icon small={true}>mdi-open-in-new</v-icon>
                  </v-btn>
                  <v-btn
                    small={true}
                    icon={true}
                    href={`http://${this.host}/api/topom/group/debuginfo/${s.server}`}
                    target='_blank'
                  >
                    <v-icon small={true}>mdi-airplane</v-icon>
                  </v-btn>
                  {s.stats.start_model && <v-btn
                    className={''}
                    x-small={true}
                    onclick={() => this.$copyText(s.stats.raft_address).then(() => this.snackbar = true)}
                  >
                    {s.stats.raft_address}
                  </v-btn>}
                </td>
                <td>{s.version_tag}<div style="word-break: break-all;">
                  {s.stats.start_model === 'observer' && "OB"}
                  {s.stats.start_model === 'normal' && "N"}
                  {s.stats.start_model === 'witness' && "W"}
                  {s.stats.start_model === 'master' && "M"}
                  {s.stats.start_model === 'slave' && "S"}
                  /{s.stats.role === 'observer' && "OB"}
                  {s.stats.role === 'normal' && "N"}
                  {s.stats.role === 'witness' && "W"}
                  {s.stats.role === 'master' && "M"}
                  {s.stats.role === 'slave' && "S"}
                </div></td>
                <td>
                  {s.stats.current_node_id}/{s.stats.leader_node_id}<br />
                  <v-chip small>{s.cloudtype}</v-chip><br />
                  {s.stats.status == 'true' && <v-chip small color="green">{s.stats.status}</v-chip>}
                  {s.stats.status == 'false' && <v-chip small color="red">{s.stats.status}</v-chip>}
                  {
                    s.stats.db_sync_running === '1' && this.$createElement(
                      'v-tooltip',
                      {
                        scopedSlots: {
                          activator: ({ on }) => this.$createElement('span', { on: on }, [<v-icon small={true} left={true}>mdi-update</v-icon>]),
                        },
                        props: {
                          top: true,
                        },
                      },
                      ['syncing data from master'])
                  }<br />
                  {s.stats.cluster_id}/{s.stats.cluster_nodes}
                </td>

                <td>{this.formatSize(s.stats.memory_total)}<br />{s.stats.memory_shr ? this.formatSize(s.stats.memory_shr) : 0}</td>
                <td>
                  {this.formatSize(s.stats.disk_data_size)}/{this.formatSize(s.stats.disk_used_size)}<br />
                  meta({this.formatSize(Number(s.stats.string_data_disk_size) + Number(s.stats.string_expire_disk_size))}/{this.formatSize(Number(s.stats.string_data_bithash_file) * 512 * 1024 * 1024)})
                  <br />
                  hash({this.formatSize(s.stats.hash_data_disk_size)})list({this.formatSize(s.stats.list_data_disk_size)})
                  <br />
                  set({this.formatSize(s.stats.set_data_disk_size)})zset({this.formatSize(Number(s.stats.zset_data_disk_size) + Number(s.stats.zset_index_disk_size))})
                </td>

                <td>{s.stats.start_time ? s.stats.start_time.split('.')[0] : ''}</td>
              </tr>
            ))}
          </tbody>
        </v-simple-table>
      </v-card>
    )
  }
  hoverShowMes(butName, attr: string, color, fn = () => { }) {
    return (
      this.$createElement(
        'v-tooltip',
        {
          scopedSlots: {
            activator: ({ on }) => this.$createElement('v-icon', { on: { click: () => { fn() }, ...on }, props: { color } }, [butName]),
          },
          props: {
            top: true,
          },
        },
        [attr])
    )
  }
  getLog() {
    return this.isShowLog && <v-dialog
      v-model={this.isShowLog} width="80%">
      <log-modal logProxy={this.logProxy}></log-modal>
    </v-dialog>
  }
  async onChangeItemReplica(s: GroupModelsServer & GMC & GroupStats, g: GroupModels, index) {
    this.loading.index = index
    const state = s.replica_group
    try {
      const { data: { data } } = await replicaGroups(g.id, s.server, Number(!s.replica_group))
      if (data !== 'OK') {
        s.replica_group = !state
      }
    } catch (e) {
      s.replica_group = !state
    } finally {
      this.loading.index = -1
    }
  }

  @Emit('update')
  async onClickGroupItemSync(s, i, index) {
    await index === 0 ? syncGroup(i.id) : promoteGroup(i.id, s.server)
  }

  @Emit('update')
  async onClickSwitchPending(s: GroupModelsServer & GMC & GroupStats) {
    await pendingGroupServer(s.server, s.isPending)
  }

  @Emit('update')
  async onClickNode(server: GroupModelsServer, g: GroupModels, model: number) {
    await handleNode({ groupId: g.id, server: server.server, raftaddr: server.stats.raft_address, nodeid: server.stats.current_node_id, model })
  }
  // @Emit('update')
  async onClickShowMembership(server: GroupModelsServer, g: GroupModels) {
    const res = await showMembership({ groupId: g.id, server: server.server })
    this.membershipData =
      <v-textarea
        value={JSON.stringify(res.data.data, null, 2)}
        height='500'
        outlined={true}
        dense={true}
      />
    return this.membershipData
  }
  @Emit('update')
  async onClickDeleteServer(server: GroupModelsServer, g: GroupModels) {
    await deleteServerToGroup({ cloudType: server.cloudtype, groupId: g.id, server: server.server, nodeid: server.stats.current_node_id })
  }
  @Emit('update')
  async onClickCompact(server: GroupModelsServer, dbtype: string) {
    await compact(server.server, dbtype)
  }
  @Emit('update')
  async onClickLogCompact(gid: number) {
    await logCompact(gid)
  }
  @Emit('update')
  async onClickEnableOrDisable(server: GroupModelsServer, g: GroupModels) {
    const value = server.stats.use_lru_cache === "false" ? 0 : 1
    await enableGroup({ groupId: g.id, server: server.server, value })
  }
  @Emit('update')
  async onClickDegrade(g: GroupModels) {
    this.degradeLoading = true
    await degradeGroup(g.id, Number(!g.is_degrade_group))
    this.degradeLoading = false
  }
  @Emit('update')
  async onClickDeleteGroup(id: string | number) {
    await deleteGroup(id)
  }

  get list(): GroupModelsComputed[] {
    return this.models.map((i: GroupModels) => {
      i.servers = i.servers.map((s) => {
        let stats = this.stats[s.server]
        // let maxMemory = 'NA'
        // let keys = null
        if (stats && stats.stats) {
          // maxMemory = humanSize(stat::-webkit-scrollbar-thumb:window-inactives.stats.maxmemory)
        } else {
          stats = { stats: {}, error: stats ? stats.error : {} }
          // keys = stats.error ?
          //   <v-btn small text color='error'>error</v-btn> :
          //   <v-btn small text color='warning'>PENDING</v-btn>
        }
        return {
          ...s,
          ...stats,
          // keys,
          // maxMemory,
          // keys,
          isPending: s.action && s.action.state === 'pending',
        }
      })
      return i
    })
  }
}
