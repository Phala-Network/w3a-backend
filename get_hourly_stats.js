const { send_request } = require('./send_request');
const { write_key, get_datetime_str, get_date_str, get_seconds_from_date_str, get_first_day_of_week } = require('./utils');

const HOURLY_STAT_KEY = "last_processed_hourly_stats_timestamp";

function get_hourly_stats(db, end) {
  const last_get_hourly_stats_value = db.prepare("SELECT * from key_values where key = ?").all(HOURLY_STAT_KEY);
  let last_datetime
  if (last_get_hourly_stats_value.length == 0) {
    let page_views = db.prepare("SELECT * FROM page_views order by created_at").all();
    if (page_views.length > 0) {
      last_datetime = get_seconds_from_date_str(page_views[0].created_at);  
    }
  } else {
    last_datetime = get_seconds_from_date_str(last_get_hourly_stats_value[0].datetime_value);
  }
  
  if (last_datetime == undefined) {
    console.log('get_hourly_stats: last_datetime == undefined');
    return false;
  }


  let now = get_seconds_from_date_str();
  if (Math.floor(now/3600) <= Math.floor(last_datetime/3600)) {
    console.log("get_hourly_stats: too quick request");
    return false;
  }

  let start_of_week = get_first_day_of_week(new Date(last_datetime * 1000), 1);
  let payload = {
    "start": last_datetime,
    "end": end,
    "start_of_week": start_of_week,
  }
  
  let response = send_request({"GetHourlyStats": payload});
  if (response.status != "ok") {
    console.log("get_hourly_stats: response error");
    return false;
  }

  let plain = JSON.parse(response.payload).Plain;
  let encrypted = JSON.parse(plain).GetHourlyStats.encrypted;
  let hourly_stat = JSON.parse(plain).GetHourlyStats.hourly_stat;
  
  process_hourly_stats(db, hourly_stat.hourly_page_view_stats, encrypted);

  process_site_clients(db, hourly_stat.site_clients);

  process_weekly_clients(db, hourly_stat.weekly_clients);

  process_weekly_sites(db, hourly_stat.weekly_sites, encrypted);

  process_weekly_devices(db, hourly_stat.weekly_devices, encrypted);

  write_key(db, HOURLY_STAT_KEY, last_get_hourly_stats_value.length == 0, end);

  return true;
}

function process_hourly_stats(db, hourly_page_view_stats, encrypted) {
  for (let i in hourly_page_view_stats) {
    let hs = hourly_page_view_stats[i];
    let d = get_datetime_str(hs.timestamp * 1000).substring(0, 19);
    let now_str = get_datetime_str();
    if (encrypted) {
      let stmt = db.prepare("INSERT INTO hourly_stats_reports_enc(site_id, pv_count, clients_count, avg_duration_in_seconds, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.run(hs.sid, hs.pv_count, hs.cid_count, hs.avg_duration, d, get_date_str(d), now_str, now_str);
    } else {
      let stmt = db.prepare("INSERT INTO hourly_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, timestamp, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.run(hs.sid, hs.pv_count, hs.cid_count, hs.avg_duration, d, get_date_str(d), now_str, now_str);
    }

    process_total_stat(db, hs, encrypted);
  }
}

function process_total_stat(db, hs, encrypted) {
  let d = get_datetime_str(hs.timestamp * 1000).substring(0, 19);
  if (!encrypted) {
    let count = 0;
    let result = db.prepare("SELECT pv_count from total_stats_reports where site_id = ? and timestamp = ? order by created_at desc").all(hs.sid, d);
    if (result.length > 0) {
      count = parseInt(result[0].pv_count);
    }

    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO total_stats_reports(site_id, clients_count, pv_count, avg_duration_in_seconds, timestamp, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(hs.sid, hs.cid_count, parseInt(hs.pv_count) + count, parseInt(hs.avg_duration) / 2, d, now_str, now_str);
  } else {
    let count = "";
    let result = db.prepare("SELECT id, pv_count from total_stats_reports_enc where site_id = ? and timestamp = ? order by created_at desc").all(hs.sid, d);
    if (result.length > 0) {
      count = result[0].pv_count; //return only one record
    }

    let payload = {
      "count": count,
      "total_stat": hs,
    };

    let response = send_request({"GetTotalStat": payload});
    if (response.status != "ok") {
      console.log("process_total_stat: response error");
      return;
    }

    let plain = JSON.parse(response.payload).Plain;
    if (!JSON.parse(plain).GetTotalStat.encrypted) {
      console.log("process_total_stat: error");
      return;
    }

    if (result.length > 0) {
      db.prepare("DELETE from total_stats_reports_enc where id = ?").run(result[0].id);
    }

    let total_stat = JSON.parse(plain).GetTotalStat.total_stat;
    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO total_stats_reports_enc(site_id, clients_count, pv_count, avg_duration_in_seconds, timestamp, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
    stmt.run(total_stat.sid, total_stat.cid_count, total_stat.pv_count, total_stat.avg_duration, d, now_str, now_str);
  }
}

function process_site_clients(db, site_clients) {
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

function process_weekly_clients(db, weekly_clients) {
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

function process_weekly_sites(db, weekly_sites, encrypted) {
  if (!encrypted) {
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

    return;
  }

  let map = new Map()
  for (let i in weekly_sites) {
    let ws = weekly_sites[i];
    let sid = ws.sid;
    let d = get_date_str(get_datetime_str(ws.timestamp * 1000));
    let key = sid+ '|' + d;
    if (!map.has(key)) {
      let result = db.prepare("SELECT count, path, site_id as sid, date as timestamp from weekly_sites_reports_enc where site_id = ? and date = ?").all(sid, d);
      map.set(key, result);
    }
  }

  let wss_new = [];
  for (let i in weekly_sites) {
    let ws = weekly_sites[i];
    let sid = ws.sid;
    let d = get_date_str(get_datetime_str(ws.timestamp * 1000));
    let key = sid + '|' + d;
    if (map.has(key) && map.get(key).length == 0) { // no data in db
      let now_str = get_datetime_str();
      let stmt = db.prepare("INSERT INTO weekly_sites_reports_enc(site_id, path, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(sid, ws.path, ws.count, d, now_str, now_str);
    } else if (map.has(key) && map.get(key).length > 0) {
      wss_new.push(ws);
    }
  }

  if (wss_new.length == 0) {
    console.log("process_weekly_sites: no new weekly sites");
    return;
  }

  let wss_in_db = []
  for (let key of map.keys()) {
    let value = map.get(key);
    for (let i in value) {
      wss_in_db.push({
        "count": value[i].count.toString(),
        "path": value[i].path,
        "sid":  value[i].sid.toString(),
        "timestamp": get_seconds_from_date_str(value[i].timestamp)
      });
    }
  }

  let payload = {
    "weekly_sites_in_db": wss_in_db,
    "weekly_sites_new": wss_new,
  }
  
  let response = send_request({"GetWeeklySites": payload});
  if (response.status != "ok") {
    console.log("process_weekly_sites: response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let merged_wss = JSON.parse(plain).GetWeeklySites.weekly_sites;
  let keys = []
  for (let i in merged_wss) {
    let ws = merged_wss[i];
    let d = get_date_str(get_datetime_str(ws.timestamp * 1000));
    if (!keys.includes(ws.sid + '|' + d)) {
      db.prepare("DELETE FROM weekly_sites_reports_enc where site_id = ? and date = ?").run(ws.sid, d);
      keys.push(ws.sid + '|' + d);
    }
  }

  for (let i in merged_wss) {
    let ws = merged_wss[i];
    let d = get_date_str(get_datetime_str(ws.timestamp * 1000));
    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO weekly_sites_reports_enc(site_id, path, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
    stmt.run(ws.sid, ws.path, ws.count, d, now_str, now_str);
  }
}

function process_weekly_devices(db, weekly_devices, encrypted) {
  if (!encrypted) {
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

    return;
  }

  let map = new Map()
  for (let i in weekly_devices) {
    let wd = weekly_devices[i];
    let sid = wd.sid;
    let d = get_date_str(get_datetime_str(wd.timestamp * 1000));
    let key = sid+ '|' + d;
    if (!map.has(key)) {
      let result = db.prepare("SELECT count, device, site_id as sid, date as timestamp from weekly_devices_enc where site_id = ? and date = ?").all(sid, d);
      map.set(key, result);
    }
  }

  let wds_new = [];
  for (let i in weekly_devices) {
    let wd = weekly_devices[i];
    let sid = wd.sid;
    let d = get_date_str(get_datetime_str(wd.timestamp * 1000));
    let key = sid + '|' + d;
    if (map.has(key) && map.get(key).length == 0) { // no data in db
      let now_str = get_datetime_str();
      let stmt = db.prepare("INSERT INTO weekly_devices_enc(site_id, device, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
      stmt.run(sid, wd.device, wd.count, d, now_str, now_str);
    } else if (map.has(key) && map.get(key).length > 0) {
      wds_new.push(wd);
    }
  }

  if (wds_new.length == 0) {
    console.log("process_weekly_devices: no new weekly devices");
    return;
  }

  let wds_in_db = []
  for (let key of map.keys()) {
    let value = map.get(key);
    for (let i in value) {
      wds_in_db.push({
        "count": value[i].count.toString(),
        "device": value[i].device,
        "sid":  value[i].sid.toString(),
        "timestamp": get_seconds_from_date_str(value[i].timestamp)
      });
    }
  }

  let payload = {
    "weekly_devices_in_db": wds_in_db,
    "weekly_devices_new": wds_new,
  }
  
  let response = send_request({"GetWeeklyDevices": payload});
  if (response.status != "ok") {
    console.log("process_weekly_devices: response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let merged_wds = JSON.parse(plain).GetWeeklyDevices.weekly_devices;
  console.log('merged_wds:', JSON.stringify(merged_wds));
  
  let keys = []
  for (let i in merged_wds) {
    let wd = merged_wds[i];
    let d = get_date_str(get_datetime_str(wd.timestamp * 1000));
    if (!keys.includes(wd.sid + '|' + d)) {
      db.prepare("DELETE FROM weekly_devices_enc where site_id = ? and date = ?").run(wd.sid, d);
      keys.push(wd.sid + '|' + d);
    }
  }

  for (let i in merged_wds) {
    let wd = merged_wds[i];
    let d = get_date_str(get_datetime_str(wd.timestamp * 1000));
    let now_str = get_datetime_str();
    let stmt = db.prepare("INSERT INTO weekly_devices_enc(site_id, device, count, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?)");
    stmt.run(wd.sid, wd.device, wd.count, d, now_str, now_str);
  }
}

module.exports = { get_hourly_stats }