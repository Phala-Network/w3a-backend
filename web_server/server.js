const http = require('http');
const express = require("express");  
const app = express();  
app.use(express.static(__dirname + "/public"))

app.get("/", function(req, res){
    res.sendFile(__dirname + "/public/index.html");
})

console.log("listen on port 9000 ...");
var httpServer = http.createServer(app);
httpServer.listen(9000)