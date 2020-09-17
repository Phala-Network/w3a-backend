const { send_request } = require('./send_request');

function clear_page_views(ts) {
  let payload = {
    "timestamp": ts,
  }
  
  let response = send_request({"ClearPageView": payload});
  if (response.status != "ok") {
    console.log("clear_page_views: response error");
    return;
  }

  //let plain = JSON.parse(response.payload).Plain;
  //let count = JSON.parse(plain).ClearPageView.page_view_count;
  //console.log("clear_page_views: count:", count);
}

module.exports = { clear_page_views }