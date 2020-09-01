const sqlite = require('better-sqlite3');
const { send_request } = require('./send_request');
const { write_key, get_datetime_str, get_minute_str, get_date_str, get_seconds_from_date_str, get_first_day_of_week } = require('./utils');

const sleep = ms => new Promise( res => setTimeout(res, ms));
const db = new sqlite("../db/development.sqlite3");

const SET_PAGE_VIEW_KEY = "last_set_page_views";
const ONLINE_USER_KEY = "last_processed_online_users_timestamp";
const HOURLY_STAT_KEY = "last_processed_hourly_stats_timestamp";

function set_page_views() {
  const last_read = db.prepare("SELECT * from key_values where key = ?").all(SET_PAGE_VIEW_KEY);
  let page_views
  if (last_read.length == 0) {
    page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
  } else {
    let now_str = get_datetime_str();
    page_views = db.prepare("SELECT * FROM page_views where created_at >= ? and created_at < ? order by created_at").all(last_read[0].datetime_value, get_minute_str(now_str));
  }
  //console.log(page_views);
  
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

  if (rows.length == 0) {
    console.log("no new page view");
    return false;
  }

  let payload = {
    "page_views": rows
  };

  let response = send_request({"SetPageView": payload});
  if (response.status != "ok") {
    console.log("response error");
    return false;
  }

  let plain = JSON.parse(response.payload).Plain;
  let count = JSON.parse(plain).SetPageView.page_views;

  write_key(db, SET_PAGE_VIEW_KEY, last_read.length == 0);

  return count > 0;
}

function update_online_users() {
  const last_processed_online = db.prepare("SELECT * from key_values where key = ?").all(ONLINE_USER_KEY);
  let last_datetime
  if (last_processed_online.length == 0) {
    let page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length > 0) {
      let created_at = page_views[0].created_at;
      last_datetime = get_seconds_from_date_str(created_at);  
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
    console.log("update_online_users: too quick request");
    return;
  }

  let payload = {
    "start": last_datetime,
    "end": now,
  }
  //console.log('payload:', payload);

  let response = send_request({"GetOnlineUsers": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let online_users = JSON.parse(plain).GetOnlineUsers.online_users;
  for (let i in online_users) {
    let ou = online_users[i];
    let d = get_datetime_str(ou.timestamp * 1000).substring(0, 19);
    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO online_users_reports(site_id, unique_cid_count, unique_ip_count, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(ou.sid, ou.cid_count, ou.ip_count, d, get_date_str(d), now_str, now_str);
  }

  write_key(db, ONLINE_USER_KEY, last_processed_online.length == 0);
}

function get_hourly_stats() {
  const last_get_hourly_stats = db.prepare("SELECT * from key_values where key = ?").all(HOURLY_STAT_KEY);
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
    console.log("get_hourly_stats: too quick request");
    return;
  }

  let start_of_week = get_first_day_of_week(new Date(last_datetime * 1000), 1);
  let payload = {
    "start": last_datetime,
    "end": now,
    "start_of_week": start_of_week,
  }
  //console.log('payload:', payload);

  let response = send_request({"GetHourlyStats": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let hourly_stat = JSON.parse(plain).GetHourlyStats.hourly_stat;
  console.log('hourly_stat:', JSON.stringify(hourly_stat));
  
  process_hourly_stats(hourly_stat.hourly_page_views);

  process_site_clients(hourly_stat.site_clients);

  process_weekly_clients(hourly_stat.weekly_clients);

  process_weekly_sites(hourly_stat.weekly_sites);

  process_weekly_devices(hourly_stat.weekly_devices);

  write_key(db, HOURLY_STAT_KEY, last_get_hourly_stats.length == 0);
}

function process_hourly_stats(hourly_page_views) {
  for (let i in hourly_page_views) {
    let hs = hourly_page_views[i];
    let d = get_datetime_str(hs.timestamp * 1000).substring(0, 19);
    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO hourly_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.pv_count, hs.cid_count, hs.avg_duration, d, get_date_str(d), now_str, now_str);

    let count = 0;
    let result = db.prepare("SELECT * from total_stats_reports where site_id = ? and timestamp = ? order by created_at desc").all(hs.sid, d);
    if (result.length > 0) {
      count = result[0].pv_count;
    }

    stmt = db.prepare("INSERT INTO total_stats_reports(site_id, clients_count, pv_count, avg_duration_in_seconds, timestamp, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.cid_count, hs.pv_count + count, hs.avg_duration / 2, d, now_str, now_str);
  }
}

function process_site_clients(site_clients) {
  for (let i in site_clients) {
    let sc = site_clients[i];
    let sid = sc.sid;
    for (let j in sc.cids) {
      let cid = sc.cids[j];
      let result = db.prepare("SELECT * from site_clients where site_id = ? and cid = ?").all(sid, cid);
      let now_str = get_datetime_str();
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
}

function process_weekly_clients(weekly_clients) {
  for (let i in weekly_clients) {
    let wc = weekly_clients[i];
    let sid = wc.sid;
    let date_str = get_date_str(get_datetime_str(wc.timestamp * 1000));
    for (let j in wc.cids) {
      let cid = wc.cids[j];
      let result = db.prepare("SELECT * from weekly_clients where site_id = ? and cid = ? and date = ?").all(sid, cid, date_str);
      let now_str = get_datetime_str();
      if (result.length == 0) {
        let stmt = db.prepare("INSERT INTO weekly_clients(site_id, cid, date, created_at) VALUES(?, ?, ?, ?)");
        stmt.run(sid, cid, date_str, now_str);
      }
    }
  }
}

function process_weekly_sites(weekly_sites) {
  for (let i in weekly_sites) {
    let ws = weekly_sites[i];
    let count = ws.count;
    let d = get_date_str(get_datetime_str(ws.timestamp * 1000));
    let now_str = get_datetime_str();

    let result = db.prepare("SELECT * from weekly_sites_reports where site_id = ? and path = ? and date = ?").all(ws.sid, ws.path, d);
    if (result.length > 0) {
      let last_count = result[0].count;
      db.prepare("UPDATE weekly_sites_reports set count = ? where site_id = ? and path = ? and date = ?").run(last_count + count, ws.sid, ws.path, d);
    } else {
      let stmt = db.prepare("INSERT INTO weekly_sites_reports(site_id, path, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(ws.sid, ws.path, count, d, now_str, now_str);
    }
  }
}

function process_weekly_devices(weekly_devices) {
  for (let i in weekly_devices) {
    let wd = weekly_devices[i];
    let count = wd.count;
    let d = get_date_str(get_datetime_str(wd.timestamp * 1000));
    let now_str = get_datetime_str();

    let result = db.prepare("SELECT * from weekly_devices where site_id = ? and device = ? and date = ?").all(wd.sid, wd.device, d);
    if (result.length > 0) {
      let last_count = result[0].count;
      db.prepare("UPDATE weekly_devices set count = ? where site_id = ? and device = ? and date = ?").run(last_count + count, wd.sid, wd.device, d);
    } else {
      let stmt = db.prepare("INSERT INTO weekly_devices(site_id, device, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(wd.sid, wd.device, count, d, now_str, now_str);
    }
  }
}

function get_daily_stats() {
  const last_get_daily_stats = db.prepare("SELECT * from daily_stats_reports order by date desc").all();
  let last_date
  if (last_get_daily_stats.length == 0) {
    let hourly_stats = db.prepare("SELECT * FROM hourly_stats_reports order by date").all();
    if (hourly_stats.length > 0) {
      last_date = new Date(hourly_stats[0].date + 'Z') / 1000;  
    }
  } else {
    last_date = new Date(last_get_daily_stats[0].date + 'Z') / 1000 + 24 * 3600;
  }
  
  if (last_date == undefined) {
    console.log('last_datetime == undefined');
    return;
  }

  let today = Math.floor(new Date().getTime() / 1000 / 24 / 3600) * 24 * 3600;
  if (today <= last_date) {
    console.log("get_daily_stats: too quick request");
    return;
  }

  let last_date_str = get_datetime_str(last_date * 1000);
  let today_str = get_datetime_str(today * 1000);
  let result = db.prepare("SELECT * from hourly_stats_reports where date >= ? and date < ? order by date").all(get_date_str(last_date_str), get_date_str(today_str));
  let stats = [];
  for (let i in result) {
    let hs = {
      "sid" : result[i].site_id.toString(),
      "pv_count": parseInt(result[i].pv_count),
      "cid_count": parseInt(result[i].clients_count),
      "avg_duration": parseInt(result[i].avg_duration_in_seconds),
      "timestamp": new Date(result[i].date + 'Z').getTime() / 1000,
    };

    stats.push(hs);
  }

  let payload = {
    "daily_stat": {"stats": stats}
  }
  //console.log('payload:', payload);

  let response = send_request({"GetDailyStats": payload});
  if (response.status != "ok") {
    console.log("response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let daily_stat = JSON.parse(plain).GetDailyStats.daily_stat;
  console.log('daily_stat:', JSON.stringify(daily_stat));
  stats = daily_stat.stats;
  for (let i in stats) {
    let stat = stats[i];
    let d = get_date_str(get_datetime_str(stat.timestamp * 1000));
    let now_str = get_datetime_str();
    let result = db.prepare("SELECT * from daily_stats_reports where site_id = ? and date = ?").all(stat.sid, d);
    if (result.length == 0) {
      let stmt = db.prepare("INSERT INTO daily_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
      stmt.run(stat.sid, stat.pv_count, stat.cid_count, stat.avg_duration, d, now_str, now_str);
    } else {
      let stmt = db.prepare("UPDATE daily_stats_reports set pv_count = ?, clients_count = ?, avg_duration_in_seconds = ?, updated_at = ?");
      stmt.run(stat.pv_count, stat.cid_count, stat.avg_duration, now_str);
    }
  }
}

function init_db() {
  db.prepare("delete from key_values").run();
    
  db.prepare("delete from online_users_reports").run();
  
  db.prepare("delete from hourly_stats_reports").run();
  db.prepare("delete from total_stats_reports").run(); // TODO: stat locally
  db.prepare("delete from clients").run();
  db.prepare("delete from site_clients").run();
  db.prepare("delete from weekly_clients").run();
  db.prepare("delete from weekly_devices").run(); // TODO: device name
  db.prepare("delete from weekly_sites_reports").run();
  
  db.prepare("delete from daily_stats_reports").run();
}

async function main() {
  let args = process.argv.slice(2)
  if (args.length >= 1 && args[0] == "init") {
    init_db();    
    return;
  }

  let hour = -1;
  let day = -1;
  const interval = 60;
  while (true) {

    let ret = set_page_views();

    if (ret)
      update_online_users();

    if (new Date().getHours() != hour) {
      hour = new Date().getHours();
      get_hourly_stats();
    }

    if (new Date().getDay() != day) {
      day = new Date().getDay();
      get_daily_stats();
    }

    console.log(`[${new Date().toLocaleString()}] wait for ${interval} seconds ...\n`);
    await sleep(interval * 1000);
  }
}

main().catch(console.error).finally(() => process.exit());

