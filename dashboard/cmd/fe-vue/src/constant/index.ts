export const CLOUD_TYPE_LIST = ['baidu', 'tencent']
export const SERVER_ROLE = ["master_slave_node","observer_node","witness_node"]

export const COLORS = [
  '#0003ff',
  '#12e800',
  '#ff0300',
  '#01ffed',
  '#e8e800',
  '#ff02fd',
  '#77ff7d',
  '#e88688',
  '#9394ff',
]

export const getColor = (index: number) => COLORS[index % COLORS.length]

export const ROLE_MAPPER = {
  0: '-- Select --',
  1: 'Super admin: user management, ops management',
  2: 'Ops management role, ops management',
  3: 'Read-only: view data only',
}

export const ROLE_LIST = [
  {value: 0, text: '-- Select --'},
  {value: 1, text: 'Super admin: user management, ops management'},
  {value: 2, text: 'Ops management role, ops management'},
  {value: 3, text: 'Read-only: view data only'},
]

export const MigratingStatus = {
  0: 'Not started',
  1: 'In progress',
  2: 'Completed',
}
