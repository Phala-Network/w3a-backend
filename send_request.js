const request = require("sync-request");
const pRuntime_host = "http://localhost:8000";

function send_request(request_payload) {
  let contract_id = 4;
  let rand_num = Math.ceil(Math.random()*1000000);
  
  let query_body = {
    "contract_id": contract_id,
    "nonce": rand_num,
    "request": request_payload
  }
  //console.log(query_body);

  let query_payload = {
    "Plain": JSON.stringify(query_body)
  }

  let query_data = {
    "query_payload": JSON.stringify(query_payload)
  }
  
  let json = {
    "input": query_data,
    "nonce": { "id":1 }
  }
  console.log("request:", JSON.stringify(json));

  const res = request("POST", pRuntime_host + "/query",  {
    json: json
  });

  console.log("response:", res.getBody('utf8'));

  let response = JSON.parse(res.getBody('utf8'));

  return response;
}

module.exports = { send_request }