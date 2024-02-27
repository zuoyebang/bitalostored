export const CLOUD_TYPE_LIST = ['baidu', 'tencent']
export const SERVER_ROLE = ["master_slave_node", "observer_node", "witness_node"]

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

export const MigratingStatus = {
  0: 'Not started',
  1: 'In progress',
  2: 'Completed',
}
