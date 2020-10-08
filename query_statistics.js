const sqlite = require('better-sqlite3');
const db = new sqlite("../w3a-gateway/db/development.sqlite3");
const util = require('./decrypt_util');

function main() {
  let args = process.argv.slice(2)
  let decrpyt = args.length == 1 && args[0] == "decrypt";
  disp_online_user(decrpyt);
  disp_hourly_stat(decrpyt);
  disp_daily_stat(decrpyt);
  disp_weekly_sites(decrpyt);
  disp_total_stat(decrpyt);
}

function disp_online_user(decrpyt) {
  let result = db.prepare("SELECT * from online_users_reports_enc").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE online_users_reports -----");
    return;
  }

  console.log(`\n===== The data in TABLE online_users_reports (decrpyt: ${decrpyt}) =====`);
  for (let index in result) {
    let row = result[index];
    let cid_count = decrpyt?util.decrypt(row.unique_cid_count):row.unique_cid_count;
    let ip_count = decrpyt?util.decrypt(row.unique_ip_count):row.unique_ip_count;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.site_id);
    console.log("\tcid count: " + cid_count);
    console.log("\tip count: " + ip_count);
    console.log("\tminute: " + row.timestamp);
    console.log("\tdate: " + row.date);
    console.log("");
  }
}

function disp_hourly_stat(decrpyt) {
  let result = db.prepare("SELECT * from hourly_stats_reports_enc").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE hourly_stats_reports -----");
    return;
  }

  console.log(`\n===== The data in TABLE hourly_stats_reports (decrpyt: ${decrpyt}) =====`);
  for (let index in result) {
    let row = result[index];
    let pv_count = decrpyt?util.decrypt(row.pv_count):row.pv_count;
    let clients_count = decrpyt?util.decrypt(row.clients_count):row.clients_count;
    let avg_duration_in_seconds = decrpyt?util.decrypt(row.avg_duration_in_seconds):row.avg_duration_in_seconds;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.site_id);
    console.log("\tpage view count: " + pv_count);
    console.log("\tclient count: " + clients_count);
    console.log("\taverage duration: " + avg_duration_in_seconds);
    console.log("\thour: " + row.timestamp);
    console.log("\tdate: " + row.date);
    console.log("");
  }
}

function disp_daily_stat(decrpyt) {
  let result = db.prepare("SELECT * from daily_stats_reports_enc").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE daily_stats_reports -----");
    return;
  }

  console.log(`\n===== The data in TABLE daily_stats_reports (decrpyt: ${decrpyt}) =====`);
  for (let index in result) {
    let row = result[index];
    let pv_count = decrpyt?util.decrypt(row.pv_count):row.pv_count;
    let clients_count = decrpyt?util.decrypt(row.clients_count):row.clients_count;
    let avg_duration_in_seconds = decrpyt?util.decrypt(row.avg_duration_in_seconds):row.avg_duration_in_seconds;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.site_id);
    console.log("\tpage view count: " + pv_count);
    console.log("\tclient count: " + clients_count);
    console.log("\taverage duration: " + avg_duration_in_seconds);
    console.log("\tdate: " + row.date);
    console.log("");
  }
}

function disp_weekly_sites(decrpyt) {
  let result = db.prepare("SELECT * from weekly_sites_reports_enc").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE weekly_sites_reports -----");
    return;
  }

  console.log(`\n===== The data in TABLE weekly_sites_reports (decrpyt: ${decrpyt}) =====`);
  for (let index in result) {
    let row = result[index];
    let count = decrpyt?util.decrypt(row.count):row.count;
    let path = decrpyt?util.decrypt(row.path):row.path;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.site_id);
    console.log("\tpath: " + path);
    console.log("\tcount: " + count);
    console.log("\tdate: " + row.date);
    console.log("");
  }
}

function disp_total_stat(decrpyt) {
  let result = db.prepare("SELECT * from total_stats_reports_enc").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE total_stats_reports -----");
    return;
  }

  console.log(`\n===== The data in TABLE total_stats_reports (decrpyt: ${decrpyt}) =====`);
  for (let index in result) {
    let row = result[index];
    let pv_count = decrpyt?util.decrypt(row.pv_count):row.pv_count;
    let clients_count = decrpyt?util.decrypt(row.clients_count):row.clients_count;
    let avg_duration_in_seconds = decrpyt?util.decrypt(row.avg_duration_in_seconds):row.avg_duration_in_seconds;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.site_id);
    console.log("\tpage view count: " + pv_count);
    console.log("\tclient count: " + clients_count);
    console.log("\taverage duration: " + avg_duration_in_seconds);
    console.log("\ttime stamp: " + row.timestamp);
    console.log("");
  }
}

main()