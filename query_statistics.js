const sqlite = require('better-sqlite3');
const db = new sqlite("../w3a-gateway/db/development.sqlite3");
const util = require('./decrypt_util');

function main() {
  let args = process.argv.slice(2)
  let decrpyt = args.length == 1 && args[0] == "decrypt";
  disp_online_user(decrpyt);
  disp_hourly_stat(decrpyt);
  disp_daily_stat(decrpyt);
}

function disp_online_user(decrpyt) {
  let result = db.prepare("SELECT * from online_users_reports_enc").all();
  if (result.length == 0) {
    console.log("\nno data in TABLE online_users_reports");
    return;
  }

  console.log(`\nThe data in TABLE online_users_reports (decrpyt: ${decrpyt})`);
  for (row of result) {
    let cid_count = decrpyt?util.decrypt(row.unique_cid_count):row.unique_cid_count;
    let ip_count = decrpyt?util.decrypt(row.unique_ip_count):row.unique_ip_count;
    console.log("sid: " + row.site_id);
    console.log("cid count: " + cid_count);
    console.log("ip count: " + ip_count);
    console.log("time stamp: " + row.timestamp);
    console.log("date: " + row.date);
  }
}

function disp_hourly_stat(decrpyt) {
  let result = db.prepare("SELECT * from hourly_stats_reports_enc").all();
  if (result.length == 0) {
    console.log("\nno data in TABLE hourly_stats_reports");
    return;
  }

  console.log(`\nThe data in TABLE hourly_stats_reports (decrpyt: ${decrpyt})`);
  for (row of result) {
    let pv_count = decrpyt?util.decrypt(row.pv_count):row.pv_count;
    let clients_count = decrpyt?util.decrypt(row.clients_count):row.clients_count;
    let avg_duration_in_seconds = decrpyt?util.decrypt(row.avg_duration_in_seconds):row.avg_duration_in_seconds;
    console.log("sid: " + row.site_id);
    console.log("page view count: " + pv_count);
    console.log("client count: " + clients_count);
    console.log("average duration: " + avg_duration_in_seconds);
    console.log("time stamp: " + row.timestamp);
    console.log("date: " + row.date);
  }
}

function disp_daily_stat(decrpyt) {
  let result = db.prepare("SELECT * from daily_stats_reports_enc").all();
  if (result.length == 0) {
    console.log("\nno data in TABLE daily_stats_reports");
    return;
  }

  console.log(`\nThe data in TABLE daily_stats_reports (decrpyt: ${decrpyt})`);
  for (row of result) {
    let pv_count = decrpyt?util.decrypt(row.pv_count):row.pv_count;
    let clients_count = decrpyt?util.decrypt(row.clients_count):row.clients_count;
    let avg_duration_in_seconds = decrpyt?util.decrypt(row.avg_duration_in_seconds):row.avg_duration_in_seconds;
    console.log("sid: " + row.site_id);
    console.log("page view count: " + pv_count);
    console.log("client count: " + clients_count);
    console.log("average duration: " + avg_duration_in_seconds);
    console.log("date: " + row.date);
  }
}

main()