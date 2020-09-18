const { send_request } = require('./send_request');
const { write_key, get_datetime_str, get_date_str, get_seconds_from_date_str } = require('./utils');

const ONLINE_USER_KEY = "last_processed_online_users_timestamp";

function update_online_users(db, end) {
  const last_processed_online_value = db.prepare("SELECT * from key_values where key = ?").all(ONLINE_USER_KEY);
  let last_datetime
  if (last_processed_online_value.length == 0) {
    let page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length > 0) {
      let created_at = page_views[0].created_at;
      last_datetime = get_seconds_from_date_str(created_at);  
    }
  } else {
    last_datetime = get_seconds_from_date_str(last_processed_online_value[0].datetime_value);
  }
  
  if (last_datetime == undefined) {
    console.log('update_online_users: last_datetime == undefined');
    return;
  }

  let now = get_seconds_from_date_str();
  if (now < last_datetime + 60) {
    console.log("update_online_users: too quick request");
    return;
  }

  let payload = {
    "start": last_datetime,
    "end": end,
  }
  
  let response = send_request({"GetOnlineUsers": payload});
  if (response.status != "ok") {
    console.log("update_online_users: response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let online_users = JSON.parse(plain).GetOnlineUsers.online_users;
  if (JSON.parse(plain).GetOnlineUsers.encrypted) {
    for (let ou of online_users) {
      let d = get_datetime_str(ou.timestamp * 1000).substring(0, 19);
      let now_str = get_datetime_str();
      let stmt = db.prepare("INSERT INTO online_users_reports_enc(site_id, unique_cid_count, unique_ip_count, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
      stmt.run(ou.sid, ou.cid_count, ou.ip_count, d, get_date_str(d), now_str, now_str);
    }
  } else {
    for (let ou of online_users) {
      let d = get_datetime_str(ou.timestamp * 1000).substring(0, 19);
      let now_str = get_datetime_str();
      let stmt = db.prepare("INSERT INTO online_users_reports(site_id, unique_cid_count, unique_ip_count, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
      stmt.run(ou.sid, ou.cid_count, ou.ip_count, d, get_date_str(d), now_str, now_str);
    }
  }

  write_key(db, ONLINE_USER_KEY, last_processed_online_value.length == 0, end);
}

module.exports = { update_online_users }