cd /root/w3a-gateway
rails s -b 0.0.0.0 -p 7000 &
cd /root/w3a-backend/web_server
sed 's/localhost/'$DOCKER_IP'/g' public/index.html > index.tmp && mv index.tmp public/index.html
sed 's/localhost/'$DOCKER_IP'/g' public/page1.html > page1.tmp && mv page1.tmp public/page1.html
sed 's/localhost/'$DOCKER_IP'/g' public/page2.html > page2.tmp && mv page2.tmp public/page2.html
node server
