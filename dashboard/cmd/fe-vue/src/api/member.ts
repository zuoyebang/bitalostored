import {ajax} from '@/api/index'

export const getList = () => ajax.get('/api/topom/admin/list')
export const addMember = ({username, password, role}) => ajax.put(`/api/topom/admin/add`, {
  username,
  password,
  role,
})
export const delMember = ({username}) => ajax.put(`/api/topom/admin/del/${username}`)
