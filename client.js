const sqlite = require('better-sqlite3');
const request = require("sync-request");

const db = new sqlite("../db/development.sqlite3");
const pRuntime_host = "http://localhost:8000";

function set_page_views() {
  const last_read = db.prepare("SELECT * from key_values where key = 'last_read_page_views'").all();
  let page_views
  if (last_read.length == 0) {
    page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
  } else {
    console.log(last_read[0].datetime_value);
    page_views = db.prepare("SELECT * FROM page_views where created_at > ? order by created_at").all(last_read[0].datetime_value);
  }
  //console.log(page_views);
  
  for (let i in page_views) {
    let row = page_views[i];

    let ts = get_seconds_from_date_str(row.created_at);
    let payload = {
      "id": row.id,
      "sid": row.sid,
      "cid": row.cid,
      "host": row.host,
      "path": row.path,
      "referrer": row.referrer,
      "ip": row.ip,
      "user_agent": row.ua,
      "created_at": ts
    }
    console.log('payload:', payload);

    send_request({"SetPageView": payload});
  }

  write_key('last_read_page_views', last_read.length == 0);
}

function update_online_users() {
  const last_processed_online = db.prepare("SELECT * from key_values where key = ?").all('last_processed_online_users_timestamp');
  let last_datetime
  if (last_processed_online.length == 0) {
    let page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length > 0) {
      let created_at = page_views[0].created_at;
      last_datetime = get_seconds_from_date_str(created_at) - 60;  
    }
  } else {
    last_datetime = get_seconds_from_date_str(last_processed_online[0].datetime_value);
  }
  
  if (last_datetime == undefined) {
    console.log('last_datetime == undefined');
    return;
  }

  let now = get_seconds_from_date_str();
  if (now < last_datetime + 60) {
    console.log("too quick request");
    return;
  }

  let payload = {
    "start": last_datetime,
    "end": now,
  }
  console.log('payload:', payload);

  let response = send_request({"GetOnlineUsers": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let online_users = JSON.parse(plain).GetOnlineUsers.online_users;
  console.log(online_users);
  for (let i in online_users) {
    let ou = online_users[i];
    let d = get_date_str(ou.timestamp * 1000).substring(0, 19);
    let now_str = get_date_str();
    let stmt = db.prepare("INSERT INTO online_users_reports(site_id, unique_cid_count, unique_ip_count, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(ou.sid, ou.cid_count, ou.ip_count, d, d.split(' ')[0], now_str, now_str);
  }

  write_key('last_processed_online_users_timestamp', last_processed_online.length == 0);
}

function get_hourly_stats() {
  const last_get_hourly_stats = db.prepare("SELECT * from key_values where key = ?").all('last_processed_hourly_stats_timestamp');
  //console.log(last_get_hourly_stats);
  let last_datetime
  if (last_get_hourly_stats.length == 0) {
    let page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length > 0) {
      last_datetime = get_seconds_from_date_str(page_views[0].created_at);  
    }
  } else {
    last_datetime = get_seconds_from_date_str(last_get_hourly_stats[0].datetime_value);
  }
  
  if (last_datetime == undefined) {
    console.log('last_datetime == undefined');
    return;
  }

  let now = get_seconds_from_date_str();
  if (Math.floor(now/3600) <= Math.floor(last_datetime/3600)) {
    console.log("too quick request");
    return;
  }

  let payload = {
    "start": last_datetime,
    "end": now,
  }
  console.log('payload:', payload);

  let response = send_request({"GetHourlyStats": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let hourly_stats = JSON.parse(plain).GetHourlyStats.hourly_stats;
  console.log(hourly_stats);

  for (let i in hourly_stats) {
    let hs = hourly_stats[i];
    let d = get_date_str(hs.timestamp * 1000).substring(0, 19);
    let now_str = get_date_str();
    let stmt = db.prepare("INSERT INTO hourly_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.pv_count, hs.cid_count, 0, d, d.split(' ')[0], now_str, now_str);
  }

  write_key('last_processed_hourly_stats_timestamp', last_get_hourly_stats.length == 0);
}

function send_request(request_payload) {
  let contract_id = 4;
  let rand_num = Math.ceil(Math.random()*1000000);
  
  let query_body = {
    "contract_id": contract_id,
    "nonce": rand_num,
    "request": request_payload
  }
  //console.log(query_body);

  let query_payload = {
    "Plain": JSON.stringify(query_body)
  }

  let query_data = {
    "query_payload": JSON.stringify(query_payload)
  }
  
  let json = {
    "input": query_data,
    "nonce": { "id":1 }
  }
  console.log("request:", JSON.stringify(json));

  const res = request("POST", pRuntime_host + "/query",  {
    json: json
  });

  console.log("response:", res.getBody('utf8'));

  let response = JSON.parse(res.getBody('utf8'));

  return response;
}

function write_key(key, update) {
  let now_str = get_date_str();
  if (update) {
    let stmt = db.prepare("INSERT INTO key_values(key, value_type, datetime_value, created_at, updated_at) VALUES(?, 2, ?, ?, ?)");
    stmt.run(key, now_str.substr(0, 17) + '00', now_str, now_str);
  } else {
    let stmt = db.prepare("UPDATE key_values set datetime_value = ?, updated_at = ? where key = ?");
    stmt.run(now_str.substr(0, 17) + '00', now_str, key);
  }
}

function get_date_str(timestamp) {
  if (timestamp == undefined) 
    return new Date().toISOString().replace('T', ' ').replace('Z', '');
  else
    return new Date(timestamp).toISOString().replace('T', ' ').replace('Z', '');
}

function get_seconds_from_date_str(date_str) {
  if (date_str == undefined)
    return Math.floor(new Date().getTime() / 60000) * 60;

  return Math.floor(new Date(date_str + 'Z').getTime() / 60000) * 60;
}

//function get_seconds_from_date_str_hourly(date_str) {
//  return Math.round(get_seconds_from_date_str(date_str)/3600)*3600;
//}

async function main() {
  let args = process.argv.slice(2)
  if (args.length >= 1 && args[0] == "set") {
    set_page_views();
    return;
  }

  //update_online_users();
  get_hourly_stats();
}

main().catch(console.error).finally(() => process.exit());

