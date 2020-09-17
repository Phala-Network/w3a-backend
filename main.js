const sqlite = require('better-sqlite3');

const { get_hourly_stats } = require('./get_hourly_stats')
const { update_online_users } = require('./update_online_users')
const { get_daily_stats } = require('./get_daily_stats')
const { set_page_views } = require('./set_page_views')
const { clear_page_views } = require('./clear_page_views')
const { init_db } = require('./utils');


const sleep = ms => new Promise( res => setTimeout(res, ms));
const db = new sqlite("../db/development.sqlite3");

const ENCRYPTED = false;

async function main() {
  let args = process.argv.slice(2)
  if (args.length >= 1 && args[0] == "init") {
    init_db(db);    
    return;
  }

  if (args.length >= 1 && args[0] == "set") {
    set_page_views(db);    
    return;
  }

  let hour = -1;
  let day = -1;
  const interval = 60; //seconds
  while (true) {

    let result = set_page_views(db, ENCRYPTED);
    if (result.end == 0) break; //no data or has an error 
    let waiting_mode = Math.floor(new Date().getTime() / 60000) == Math.floor(result.end/60);
    if (result.success)
      update_online_users(db, result.end);

    if (new Date().getHours() != hour) {
      if (waiting_mode) hour = new Date().getHours();
      if (get_hourly_stats(db, result.end)) {
        clear_page_views(result.end);
      }
    }

    if (new Date().getDate() != day) {
      if (waiting_mode) day = new Date().getDate();
      get_daily_stats(db, ENCRYPTED);
    }

    if (waiting_mode) {
      console.log(`[${new Date().toLocaleString()}] wait for ${interval} seconds ...\n`);
      await sleep(interval * 1000);
    }
  }
}

main().catch(console.error).finally(() => process.exit());

