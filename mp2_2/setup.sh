cd /home/csce438/

# Update packages
sudo apt-get update -y

sudo apt-get install apt-utils build-essential -y
#  apt-get install librabbitmq-dev -y
sudo apt-get install vim -y
sudo apt-get install librabbitmq-dev libjsoncpp-dev systemctl curl -y

# Install prerequisites
sudo apt-get install curl gnupg apt-transport-https -y

# Team RabbitMQ's main signing key
curl -1sLf "https://keys.openpgp.org/vks/v1/by-fingerprint/0A9AF2115F4687BD29803A206B73A36E6026DFCA" |  gpg --dearmor | sudo tee /usr/share/keyrings/com.rabbitmq.team.gpg > /dev/null

# Community mirror of Cloudsmith: modern Erlang repository
curl -1sLf https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key |  gpg --dearmor | sudo tee /usr/share/keyrings/rabbitmq.E495BB49CC4BBE5B.gpg > /dev/null

# Community mirror of Cloudsmith: RabbitMQ repository
curl -1sLf https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key |  gpg --dearmor | sudo tee /usr/share/keyrings/rabbitmq.9F4587F226208342.gpg > /dev/null

# Add apt repositories for RabbitMQ, specifying amd64 only to avoid i386 errors
sudo tee /etc/apt/sources.list.d/rabbitmq.list <<EOF
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq.E495BB49CC4BBE5B.gpg] https://ppa1.novemberain.com/rabbitmq/rabbitmq-erlang/deb/ubuntu jammy main
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq.E495BB49CC4BBE5B.gpg] https://ppa2.novemberain.com/rabbitmq/rabbitmq-erlang/deb/ubuntu jammy main
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq.9F4587F226208342.gpg] https://ppa1.novemberain.com/rabbitmq/rabbitmq-server/deb/ubuntu jammy main
deb [arch=amd64 signed-by=/usr/share/keyrings/rabbitmq.9F4587F226208342.gpg] https://ppa2.novemberain.com/rabbitmq/rabbitmq-server/deb/ubuntu jammy main
EOF

# Update package indices
sudo apt-get update -y

# Install Erlang packages
sudo apt-get install -y erlang-base \
   erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets \
   erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key \
   erlang-runtime-tools erlang-snmp erlang-ssl \
   erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl

# Install rabbitmq-server and its dependencies
sudo apt-get install -y rabbitmq-server --fix-missing

# Perform additional cleanup and install Python dependencies
sudo apt autoremove -y
sudo apt install python3-pip -y
sudo pip3 install pyzmq pika

# Enable RabbitMQ plugins and set permissions
# sudo rabbitmq-plugins enable rabbitmq_management
# sudo rabbitmqctl set_user_tags guest administrator
# sudo rabbitmqctl set_permissions -p / guest ".*" ".*" ".*"

# cd /home/csce438/mp2-2-starter-code
#
# # rm send receive
# g++ -o send send.cpp -lrabbitmq
# g++ -o receive receive.cpp -lrabbitmq
# # ./send
# # ./receive

