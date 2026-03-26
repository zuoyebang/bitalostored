module.exports = {
  lintOnSave:false,
  devServer: {
    proxy: {
      //'/api': {target: 'http://172.30.8.42:8041/', pathRewrite: {'^/api': ''}},
      '/': {target: 'http://10.116.48.58:2001/'},
    },
  },
  publicPath: process.env.BASE_URL || '/',
}
