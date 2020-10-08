const sqlite = require('better-sqlite3');
const db = new sqlite("../w3a-gateway/db/development.sqlite3");
const util = require('./decrypt_util');

function main() {
  let args = process.argv.slice(2)
  let decrpyt = args.length == 1 && args[0] == "decrypt";
  
  let result = db.prepare("SELECT * from page_views").all();
  if (result.length == 0) {
    console.log("\n----- no data in TABLE page_views -----");
    return;
  }

  console.log(`\n===== The data in TABLE page_views (decrpyt: ${decrpyt}) =====`);
  for (index in result) {
    let row = result[index];
    let host = decrpyt?util.decrypt(row.host):row.host;
    let path = decrpyt?util.decrypt(row.path):row.path;
    let referrer = decrpyt?util.decrypt(row.referrer):row.referrer;
    let ip = decrpyt?util.decrypt(row.ip):row.ip;
    let ua = decrpyt?util.decrypt(row.ua):row.ua;
    console.log((parseInt(index) + 1).toString() + "\tsid: " + row.sid);
    console.log("\tid: " + row.id);
    console.log("\tcid: " + row.cid);
    console.log("\tuid: " + row.uid);
    console.log("\thost: " + host);
    console.log("\tpath: " + path);
    console.log("\treferrer: " + referrer);
    console.log("\tip: " + ip);
    console.log("\tuser agent: " + ua);
    console.log("\tcreated at: " + row.created_at);
    console.log("");
  }
}

main()