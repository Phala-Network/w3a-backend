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

    let ts = Math.floor(new Date(row.created_at + 'Z').getTime() / 1000);
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

  let start_of_week = get_first_day_of_week(new Date(last_datetime * 1000), 1);
  let payload = {
    "start": last_datetime,
    "end": now,
    "start_of_week": start_of_week,
  }
  console.log('payload:', payload);

  let response = send_request({"GetHourlyStats": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let hourly_stat = JSON.parse(plain).GetHourlyStat.hourly_stat;
  console.log('hourly_stat:', JSON.stringify(hourly_stat));
  
  let hourly_page_views = hourly_stat.hpv;
  for (let i in hourly_page_views) {
    let hs = hourly_page_views[i];
    let d = get_date_str(hs.timestamp * 1000).substring(0, 19);
    let now_str = get_date_str();
    let stmt = db.prepare("INSERT INTO hourly_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.pv_count, hs.cid_count, hs.avg_duration, d, d.split(' ')[0], now_str, now_str);

    let count = 0;
    let result = db.prepare("SELECT * from total_stats_reports where site_id = ? and timestamp = ? order by created_at desc").all(hs.sid, d);
    if (result.length > 0) {
      count = result[0].pv_count;
    }

    stmt = db.prepare("INSERT INTO total_stats_reports(site_id, clients_count, pv_count, avg_duration_in_seconds, timestamp, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.cid_count, hs.pv_count + count, hs.avg_duration / 2, d, now_str, now_str);
  }

  let site_clients = hourly_stat.sc;
  for (let i in site_clients) {
    let sc = site_clients[i];
    let sid = sc.sid;
    for (let j in sc.cids) {
      let cid = sc.cids[j];
      let result = db.prepare("SELECT * from site_clients where site_id = ? and cid = ?").all(sid, cid);
      let now_str = get_date_str();
      if (result.length == 0) {
        let stmt = db.prepare("INSERT INTO site_clients(site_id, cid, created_at) VALUES(?, ?, ?)");
        stmt.run(sid, cid, now_str);
      }

      result = db.prepare("SELECT * from clients where fingerprint = ?").all(cid);
      if (result.length == 0) {
        let stmt = db.prepare("INSERT INTO clients(fingerprint, created_at, updated_at) VALUES(?, ?, ?)");
        stmt.run(cid, now_str, now_str);
      }
    }
  }

  let weekly_clients = hourly_stat.wc;
  for (let i in weekly_clients) {
    let wc = weekly_clients[i];
    let sid = wc.sid;
    let date_str = get_date_str(wc.timestamp * 1000).split(' ')[0];
    for (let j in wc.cids) {
      let cid = wc.cids[j];
      let result = db.prepare("SELECT * from weekly_clients where site_id = ? and cid = ? and date = ?").all(sid, cid, date_str);
      let now_str = get_date_str();
      if (result.length == 0) {
        let stmt = db.prepare("INSERT INTO weekly_clients(site_id, cid, date, created_at) VALUES(?, ?, ?, ?)");
        stmt.run(sid, cid, date_str, now_str);
      }
    }
  }

  let weekly_sites = hourly_stat.ws;
  for (let i in weekly_sites) {
    let ws = weekly_sites[i];
    let count = ws.count;
    let d = get_date_str(ws.timestamp * 1000).split(' ')[0];
    let now_str = get_date_str();

    let result = db.prepare("SELECT * from weekly_sites_reports where site_id = ? and path = ? and date = ?").all(ws.sid, ws.path, d);
    if (result.length > 0) {
      let last_count = result[0].count;
      db.prepare("UPDATE weekly_sites_reports set count = ? where site_id = ? and path = ? and date = ?").run(last_count + count, ws.sid, ws.path, d);
    } else {
      let stmt = db.prepare("INSERT INTO weekly_sites_reports(site_id, path, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(ws.sid, ws.path, count, d, now_str, now_str);
    }
  }

  let weekly_devices = hourly_stat.wd;
  for (let i in weekly_devices) {
    let wd = weekly_devices[i];
    let count = wd.count;
    let d = get_date_str(wd.timestamp * 1000).split(' ')[0];
    let now_str = get_date_str();

    let result = db.prepare("SELECT * from weekly_devices where site_id = ? and device = ? and date = ?").all(wd.sid, wd.device, d);
    if (result.length > 0) {
      let last_count = result[0].count;
      db.prepare("UPDATE weekly_devices set count = ? where site_id = ? and device = ? and date = ?").run(last_count + count, wd.sid, wd.device, d);
    } else {
      let stmt = db.prepare("INSERT INTO weekly_devices(site_id, device, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(wd.sid, wd.device, count, d, now_str, now_str);
    }
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

function get_first_day_of_week(date, from_monday) {
  let day_of_week = date.getDay();
  let first_day_of_week = new Date(date);
  let diff = day_of_week >= from_monday ? day_of_week - from_monday : 6 - day_of_week;
  first_day_of_week.setDate(date.getDate() - diff);
  first_day_of_week.setHours(0,0,0,0);

  return first_day_of_week.getTime() / 1000;
}


async function main() {
  let args = process.argv.slice(2)
  if (args.length >= 1 && args[0] == "set") {
    set_page_views();
    return;
  }

  if (args.length >= 1 && args[0] == "init") {
    db.prepare("delete from key_values").run();
    
    db.prepare("delete from online_users_reports").run();
    
    db.prepare("delete from hourly_stats_reports").run();
    db.prepare("delete from total_stats_reports").run(); // TODO: stat locally
    db.prepare("delete from clients").run();
    db.prepare("delete from site_clients").run();
    db.prepare("delete from weekly_clients").run();
    db.prepare("delete from weekly_devices").run(); // TODO: device name
    db.prepare("delete from weekly_sites_reports").run();
    
    // TODO:
    // db.prepare("delete from daily_stats_reports").run();
    
    return;
  }

  //update_online_users();

  get_hourly_stats();
}

main().catch(console.error).finally(() => process.exit());

