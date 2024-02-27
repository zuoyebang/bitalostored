export interface CommonResponse<T> {
  data: T
  errmsg: { cause: string, stack: any }
  status: number
}

export interface ServerItem {
  time: Date
  url: string
  type: 'request' | 'response'
  msg: string
  data: {
    message: string
    name: string
    stack: string
    config: {
      url: string
      method: 'get' | 'post' | 'put' | 'delete'
      headers: { [text: string]: string }
    }
  }
}
