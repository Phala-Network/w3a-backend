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

module.exports = { write_key, get_date_str, get_seconds_from_date_str, get_first_day_of_week }