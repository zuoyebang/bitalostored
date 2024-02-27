import {ajax} from '@/api/index'
import {ajaxGetJSON, ajaxPost} from 'rxjs/internal-compatibility'

export const getClusters = () => ajax('/clusters')
export const getStoreList$ = () => ajax('/clusters')
// export const submit = () => ajaxPost('/switchdashboard')
export const submit = ({opType, name}) => ajax.post('/switchdashboard', {
    opType,
    name,
  })
