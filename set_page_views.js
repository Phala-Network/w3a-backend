const { send_request } = require('./send_request');
const { write_key, get_datetime_str, get_seconds_from_date_str } = require('./utils');

const SET_PAGE_VIEW_KEY = "last_set_page_views";
const BATCH_WINDOW = 24 * 60; //in munites

function set_page_views(db, encrypted) {
  let last_set_page_view_in_sec
  const last_set_page_view_value = db.prepare("SELECT * from key_values where key = ?").all(SET_PAGE_VIEW_KEY);
  let page_views
  if (last_set_page_view_value.length == 0) {
    page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length == 0) {
      console.log("set_page_views: no page view");
      return {'success':false, 'start':0, 'end':0};;
    }

    last_set_page_view_in_sec = get_seconds_from_date_str(page_views[0].created_at);
  } else {
    last_set_page_view_in_sec = get_seconds_from_date_str(last_set_page_view_value[0].datetime_value);
  }

  let new_set_page_view_in_sec = last_set_page_view_in_sec + BATCH_WINDOW * 60;
  let now = get_seconds_from_date_str();
  if (new_set_page_view_in_sec > now) new_set_page_view_in_sec = now;

  page_views = db.prepare("SELECT * FROM page_views where created_at >= ? and created_at < ? order by created_at").all(get_datetime_str(last_set_page_view_in_sec*1000), get_datetime_str(new_set_page_view_in_sec*1000));
  if (page_views.length == 0) {
    console.log("set_page_views: no new page view");
    write_key(db, SET_PAGE_VIEW_KEY, last_set_page_view_value.length == 0, new_set_page_view_in_sec);

    return {'success':false, 'start':last_set_page_view_in_sec, 'end':new_set_page_view_in_sec};
  }

  let rows = [];
  for (let i in page_views) {
    let pv = page_views[i];

    let row = {
      "id": pv.id,
      "sid": pv.sid,
      "cid": pv.cid,
      "host": pv.host,
      "path": pv.path,
      "referrer": pv.referrer,
      "ip": pv.ip,
      "user_agent": pv.ua,
      "created_at": Math.floor(new Date(pv.created_at + 'Z').getTime() / 1000)
    }
    
    rows.push(row);
  }

  let payload = {
    "page_views": rows,
    "encrypted": encrypted,
  };

  let response = send_request({"SetPageView": payload});
  if (response.status != "ok") {
    console.log("set_page_views: response error");
    return {'success':false, 'start':0, 'end':0};
  }

  let plain = JSON.parse(response.payload).Plain;
  let count = JSON.parse(plain).SetPageView.page_view_count;

  write_key(db, SET_PAGE_VIEW_KEY, last_set_page_view_value.length == 0, new_set_page_view_in_sec);

  return {'success':count > 0, 'start':last_set_page_view_in_sec, 'end':new_set_page_view_in_sec};
}

module.exports = { set_page_views }