const { send_request } = require('./send_request');
const { get_datetime_str, get_date_str } = require('./utils');

function get_daily_stats(db, encrypted) {
  let last_date
  if (!encrypted) {
    const last_get_daily_stats = db.prepare("SELECT date from daily_stats_reports order by date desc").all();
    if (last_get_daily_stats.length == 0) {
      let hourly_stats = db.prepare("SELECT date FROM hourly_stats_reports order by date").all();
      if (hourly_stats.length > 0) {
        last_date = new Date(hourly_stats[0].date + 'Z') / 1000;  
      }
    } else {
      last_date = new Date(last_get_daily_stats[0].date + 'Z') / 1000 + 24 * 3600;
    }
  } else {
    const last_get_daily_stats = db.prepare("SELECT date from daily_stats_reports_enc order by date desc").all();
    if (last_get_daily_stats.length == 0) {
      let hourly_stats = db.prepare("SELECT date FROM hourly_stats_reports_enc order by date").all();
      if (hourly_stats.length > 0) {
        last_date = new Date(hourly_stats[0].date + 'Z') / 1000;  
      }
    } else {
      last_date = new Date(last_get_daily_stats[0].date + 'Z') / 1000 + 24 * 3600;
    }
  }
  
  if (last_date == undefined) {
    console.log('get_daily_stats: last_date == undefined');
    return;
  }

  let today = Math.floor(new Date().getTime() / 1000 / 24 / 3600) * 24 * 3600;
  if (today <= last_date) {
    console.log("get_daily_stats: too quick request");
    return;
  }

  let last_date_str = get_datetime_str(last_date * 1000);
  let today_str = get_datetime_str(today * 1000);
  let sql = encrypted ? "SELECT * from hourly_stats_reports_enc where date >= ? and date < ? order by date" : 
                        "SELECT * from hourly_stats_reports where date >= ? and date < ? order by date";
  let result = db.prepare(sql).all(get_date_str(last_date_str), get_date_str(today_str));
  if (result.length == 0) {
    console.log("get_daily_stats: no new hourly stats");
    return;
  }

  let stats = [];
  for (let i in result) {
    let hs = {
      "sid" : result[i].site_id.toString(),
      "pv_count": result[i].pv_count.toString(),
      "cid_count": result[i].clients_count.toString(),
      "avg_duration": result[i].avg_duration_in_seconds.toString(),
      "timestamp": new Date(result[i].date + 'Z').getTime() / 1000,
    };

    stats.push(hs);
  }

  let payload = {
    "daily_stat": {"stats": stats}
  }
  
  let response = send_request({"GetDailyStats": payload});
  if (response.status != "ok") {
    console.log("get_daily_stats: response error");
    return;
  }

  let plain = JSON.parse(response.payload).Plain;
  let daily_stat = JSON.parse(plain).GetDailyStats.daily_stat;
  stats = daily_stat.stats;
  if (encrypted != JSON.parse(plain).GetDailyStats.encrypted) {
    console.log("get_daily_stats: error");
    return;
  }

  if (!encrypted) {
    for (let i in stats) {
      let stat = stats[i];
      let d = get_date_str(get_datetime_str(stat.timestamp * 1000));
      let now_str = get_datetime_str();
      let result = db.prepare("SELECT * from daily_stats_reports where site_id = ? and date = ?").all(stat.sid, d);
      if (result.length == 0) {
        let stmt = db.prepare("INSERT INTO daily_stats_reports(site_id, pv_count, clients_count, avg_duration_in_seconds, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
        stmt.run(stat.sid, stat.pv_count, stat.cid_count, stat.avg_duration, d, now_str, now_str);
      } else {
        let stmt = db.prepare("UPDATE daily_stats_reports set pv_count = ?, clients_count = ?, avg_duration_in_seconds = ?, updated_at = ? where site_id = ? and date = ?");
        stmt.run(stat.pv_count, stat.cid_count, stat.avg_duration, now_str, stat.sid, d);
      }
    }

    return;
  }

  for (let i in stats) {
    let stat = stats[i];
    let d = get_date_str(get_datetime_str(stat.timestamp * 1000));
    let now_str = get_datetime_str();
    let result = db.prepare("SELECT * from daily_stats_reports_enc where site_id = ? and date = ?").all(stat.sid, d);
    if (result.length == 0) {
      let stmt = db.prepare("INSERT INTO daily_stats_reports_enc(site_id, pv_count, clients_count, avg_duration_in_seconds, date, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)");
      stmt.run(stat.sid, stat.pv_count, stat.cid_count, stat.avg_duration, d, now_str, now_str);
    } else {
      let stmt = db.prepare("UPDATE daily_stats_reports_enc set pv_count = ?, clients_count = ?, avg_duration_in_seconds = ?, updated_at = ? where site_id = ? and date = ?");
      stmt.run(stat.pv_count, stat.cid_count, stat.avg_duration, now_str, stat.sid, d);
    }
  }
  
}

module.exports = { get_daily_stats }