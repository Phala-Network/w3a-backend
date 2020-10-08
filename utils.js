function write_key(db, key, update, last_time_in_sec) {
  let last_time_str = get_minute_str(get_datetime_str(last_time_in_sec*1000));
  let now_str = get_datetime_str();
  if (update) {
    let stmt = db.prepare("INSERT INTO key_values(key, value_type, datetime_value, created_at, updated_at) VALUES(?, 2, ?, ?, ?)");
    stmt.run(key, last_time_str, now_str, now_str);
  } else {
    let stmt = db.prepare("UPDATE key_values set datetime_value = ?, updated_at = ? where key = ?");
    stmt.run(last_time_str, now_str, key);
  }
}

function get_datetime_str(timestamp) {
  if (timestamp == undefined) 
    return new Date().toISOString().replace('T', ' ').replace('Z', '');
  else
    return new Date(timestamp).toISOString().replace('T', ' ').replace('Z', '');
}

function get_minute_str(datetime_str) {
  return datetime_str.substr(0, 17) + '00';
}

function get_date_str(datetime_str) {
  return datetime_str.split(' ')[0];
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
  let offset = new Date().getTimezoneOffset() / 60;
  first_day_of_week.setHours(0 - offset, 0, 0, 0);

  return first_day_of_week.getTime() / 1000;
}

function get_day_of_month() {
  let offset = new Date().getTimezoneOffset(); // in minutes
  let d = new Date().getTime() + offset * 60 * 1000;
  return new Date(d).getDate();
}

function init_db(db) {
  db.prepare("delete from key_values").run();
  
  db.prepare("delete from page_views").run();
  db.prepare("delete from online_users_reports").run();
  db.prepare("delete from online_users_reports_enc").run();
  
  db.prepare("delete from hourly_stats_reports").run();
  db.prepare("delete from hourly_stats_reports_enc").run();

  db.prepare("delete from total_stats_reports").run();
  db.prepare("delete from total_stats_reports_enc").run();

  db.prepare("delete from clients").run();
  db.prepare("delete from site_clients").run();
  db.prepare("delete from weekly_clients").run();
  db.prepare("delete from weekly_devices").run();
  db.prepare("delete from weekly_devices_enc").run();
  db.prepare("delete from weekly_sites_reports").run();
  db.prepare("delete from weekly_sites_reports_enc").run();
  
  db.prepare("delete from daily_stats_reports").run();
  db.prepare("delete from daily_stats_reports_enc").run();
}

module.exports = { write_key, get_datetime_str, get_minute_str, get_date_str, get_seconds_from_date_str, get_first_day_of_week, get_day_of_month, init_db }