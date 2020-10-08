const sqlite = require('better-sqlite3');
const db = new sqlite("../w3a-gateway/db/development.sqlite3");
const util = require('./decrypt_util');

function main() {
  let args = process.argv.slice(2)
  let decrpyt = args.length == 1 && args[0] == "decrypt";
  console.log(decrpyt);

  let result = db.prepare("SELECT * from page_views").all();
  for (row of result) {
    let host = decrpyt?util.decrypt(row.host):row.host;
    let path = decrpyt?util.decrypt(row.path):row.path;
    let referrer = decrpyt?util.decrypt(row.referrer):row.referrer;
    let ip = decrpyt?util.decrypt(row.ip):row.ip;
    let ua = decrpyt?util.decrypt(row.ua):row.ua;
    console.log("\nsid: " + row.sid);
    console.log("id: " + row.id);
    console.log("cid: " + row.cid);
    console.log("host: " + host);
    console.log("path: " + path);
    console.log("referrer: " + referrer);
    console.log("ip: " + ip);
    console.log("user agent: " + ua);
    console.log("created at: " + row.created_at);
  }
}

main()