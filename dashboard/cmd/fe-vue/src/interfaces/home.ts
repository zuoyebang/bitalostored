import { VNode } from 'vue'

export interface HomeDataResponse {
  complie: string
  config: {
    admin_addr: string
    admin_model: string
    coordinator_addr: string
    coordinator_auth: string
    coordinator_name: string
    product_auth: string
    product_name: string
  }
  model: {
    admin_addr: string
    hostport: string
    pid: number
    product_name: string
    pwd: string
    start_time: string
    sys: string
    token: string
  }
  stats: StateDataResponse
  version: string
  cgroup: string
}

export interface GroupModelsServer {
  action: {
    state: string
    index: number
  }
  cloudtype: string
  datacenter: string
  is_master: boolean
  online: boolean
  replica_group: boolean
  server: string
  stats?: Stats
}
export interface Stats {
  current_node_id: string,
  raft_address?: string
  use_lru_cache?: string
}
export interface GroupModels {
  id: number
  is_degrade_group: boolean
  is_expanding: boolean
  out_of_sync: boolean
  promoting: any
  servers: GroupModelsServer[]
}

export interface GroupModelsComputed extends GroupModels {
  servers: (GroupModelsServer & GMC & GroupStats)[]
}

export interface GMC {
  isPending: boolean
  maxMemory: string
  keys: VNode
  error: any
}

export interface GroupStats {
  unixtime: string
  stats: {
    '# DB': string
    '# Duration': string
    '# Time': string
    Hashes: string
    Lists: string
    Sets: string
    Strings: string
    Zsets: string
    arch_bits: string
    cluster_id: string
    cluster_nodes: string
    compile_date: string
    config_file: string
    connected_clients: string
    current_node_id: string
    db_size: string
    db_size_human: string
    db_sync_running: string
    git_sha: string
    git_version: string
    heap_idle: string
    heap_in_use: string
    heap_in_use_human: string
    heap_total: string
    disk_data_human_size: string
    disk_data_size: string
    disk_used_human_size: string
    disk_used_size: string
    memory_total: string
    memory_total_human: string
    memory_shr: string
    memory_shr_human: string
    heap_total_human: string
    is_bgsaving: string
    is_scaning_keyspace: string
    leader_address: string
    leader_node_id: string
    maxmemory: string
    maxprocs: string
    node_1: string
    node_2: string
    node_3: string
    os: string
    process_id: string
    raft_address: string
    role: string
    start_model: string
    server_address: string
    start_time: string
    status: string
    uptime_in_days: string
    uptime_in_seconds: string
    use_lru_cache: string

    hash_data_free_page: number
    hash_meta_free_page: number
    list_data_free_page: number
    list_meta_free_page: number
    set_data_free_page: number
    set_meta_free_page: number
    string_data_free_page: number
    string_meta_free_page: number
    zset_data_free_page: number
    zset_meta_free_page: number
    zset_index_free_page: number

    string_data_bithash_disk_size: number
    hash_data_bithash_disk_size: number
    list_data_bithash_disk_size: number
    string_data_bithash_add_key: number
    string_data_bithash_delete_key: number
    hash_data_bithash_add_key: number
    hash_data_bithash_delete_key: number
    list_data_bithash_add_key: number
    list_data_bithash_delete_key: number

    hash_data_disk_size: number
    hash_meta_disk_size: string
    list_data_disk_size: number
    list_meta_disk_size: string
    set_data_disk_size: number
    set_meta_disk_size: string
    string_data_disk_size: number
    string_meta_disk_size: string
    string_expire_disk_size: number
    zset_data_disk_size: number
    zset_meta_disk_size: string
    zset_index_disk_size: number

    string_data_bithash_file: string
    list_data_bithash_file: string
    hash_data_bithash_file: string
  }
  version_tag?: string
}

export interface AddServerToGroupParams {
  groupId: number | string,
  server: string,
  nodeid?: string,
  cloudType: string,
  server_role?: string
}

export interface Enable {
  groupId: number | string,
  server: string,
  value: number
}

export interface StateDataResponse {
  closed: boolean
  group: {
    models: GroupModels[]
    stats: {
      [ip: string]: GroupStats
    }
    proxy: {
      models: ProxyModels[]
      stats: {
        async_cal_ops: ProxyStatsOps & { chan_length: number }
        async_direct_ops: ProxyStatsOps & { chan_length: number }
        cdm_ops: ProxyStatsOps
        closed: boolean
        online: true
        sentinels: any
        rusage: {
          cpu: number
          mem: number
          now: string
          raw: {
            cstime: number
            cutime: number
            num_threads: number
            stime: number
            utime: number
            vm_rss: number
            vm_size: number
          }
        }
        sessions: {
          alive: number
          total: number
        }
      }
    }
    slot_action: SlotAction
    slots: Slots[]
  }
}
export interface SlotAction {
  disabled: boolean
  executor: number
  interval: number
  progress: { status: string }
}
export interface Slots {
  action: {
    target_id?: number
    state?: 'pending'
  }
  group_id: number
  id: number
}

export interface ProxyModels {
  admin_addr: string
  cloudtype: string
  datacenter: string
  hostname: string
  hostport: string
  id: number
  pid: number
  product_name: string
  proto_type: string
  proxy_addr: string
  pwd: string
  redis_conf: any
  start_time: string
  sys: string
  token: string
  version_tag: string
}

export interface ProxyStatsOps {
  fails: number
  flush_cost_time: number
  qps: number
  redis: { errors: number }
  total: number
}

export interface MigrateRange {
  from: string
  to: string
  group: string
  migrate: number
}

export interface MigrateSome {
  from: string
  to: string
  slots: string
}

export interface PcConfigItem {
  name: string
  remark: string
  data:any
  content: {
    blacklist: string[]
    whitelist: string[]
    black_map?: any
  }
}

export interface MigrateTableItem {
  create_time: string
  isRootInsert: true
  sid: number
  source_group_id: number
  status: {
    costs: number
    fails: number
    from: string
    slot_id: number
    status: number
    succ_percent: string
    to: string
    total: number
    unixtime: number
  }
  target_group_id: number
  update_time: string

  // merge
  costs: number
  fails: number
  from: string
  slot_id: number
  succ_percent: string
  to: string
  total: number
  unixtime: number
}
