cd /root/w3a-gateway
rails s -b 0.0.0.0 -p 7000 &
cd /root/w3a-backend/web_server
node server
