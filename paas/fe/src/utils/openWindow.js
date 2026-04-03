const openWindow = (obj) => {
  const url = window.location.href.replace(window.location.hash, `#${obj.path}`);
  let urlParam = Object.keys(obj.query)
    .map(key => {
      return key + '=' + obj.query[key];
    })
    .join('&');
  window.open(`${url}?${urlParam}`)
}

export default openWindow;