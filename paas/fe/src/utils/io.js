import axios from 'axios'

const service = axios.create({
  baseURL: '/',
  timeout: 100000,
  withCredentials: true
})

service.interceptors.request.use(config => {
  return config
}, err => {
  console.log(err)
  return Promise.reject(err)
})

service.interceptors.response.use(res => {
  const data = res.data
  const config = res.config || {}
  const noTip = config.silence || false
  if (data) {
    if (data.status === 0) {
      return data
    } else if (data.status === 30001) {
      !noTip && window.alert('Please login')
      localStorage.setItem('userInfo', '')
      location.reload();
    } else if (data.status === 30003) {
      location.assign(data.msg);
    } else {
      const msg = data.msg
      !noTip && window.alert(`${msg}`)
      return Promise.reject(data)
    }
  } else {
    !noTip && window.alert(`Unknown error`)
    return Promise.reject(data)
  }
}, (err) => {
  if (err.response) {
    const res = err.response
    const { data } = res
    if (data) {
      window.alert('Server error, please try again later')
      return Promise.reject(data)
    }
  } else {
    console.log(err)
    return Promise.reject(err)
  }
})

let io = {
  get: (...args) => {
    let method = 'get'
    let url = args[0]
    let params = args[1] || {}
    let headers = args[2] || {}
    return service({
      url,
      method,
      params,
      headers
    })
  },
  post: (...args) => {
    let method = 'post'
    let url = args[0]
    let data = args[1] || {}
    let headers = args[2] || {}
    let config = args[3] || {}
    return service({
      url,
      method,
      data,
      headers: {
        'Content-Type': 'application/json',
        ...headers
      },
      ...config
    })
  }
}

export default io
