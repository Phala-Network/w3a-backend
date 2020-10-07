const sqlite = require('better-sqlite3');
const aead = require('./aead');

const db = new sqlite("../w3a-gateway/db/development.sqlite3");

const key = "290c3c5d812a4ba7ce33adf09598a462692a615beb6c80fdafb3f9e3bbef8bc6";

function main() {
  let result = db.prepare("SELECT * from page_views").all();
  for (row of result) {
    console.log("\nsid:" + row.sid);
    console.log("id:" + row.id);
    console.log("cid:" + row.cid);
    console.log("host:" + row.host);
    console.log("path:" + row.path);
    console.log("referrer:" + row.referrer);
    console.log("ip:" + row.ip);
    console.log("user agent:" + row.ua);
    console.log("created at:" + row.created_at);
  }
}

main()