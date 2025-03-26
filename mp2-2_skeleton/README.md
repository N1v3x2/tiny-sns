# 1. On a ubuntu terminal, create a rabbitmq container that can communicate with your newly created csce438_mp2_2_container

Instead of using one giant container, we use two docker containers as docker containers are supposed to be lightweight. One container that consumes too much resources may be slow.

## 1.1 create a folder for your MP2.2

Like what we did in 2.1, copy the folder of your MP2.1 to a new folder named `mp2_2`. Copy the 7 source code files in this `mp2-2_skeleton` folder to the created `mp2_2` folder: synchronizer.cc, coordinator.proto, Makefile, startup.sh, stop.sh, setup.sh, docker-compose.yml. Enter into the `mp2_2` folder:

    cd mp2_2

## 1.2. create a local docker image with your mp2.1 container
On ubuntu terminal, in `mp2_2` folder, save your `csce438_mp2_1_container` used in MP2.1 into a local docker iamge named `csce438_mp2_2_image`. This `csce438_mp2_2_image` will serves as a key starting point for MP2.2:

    docker commit -m "mp2.2 starting point" -a "Author Name" csce438_mp2_1_container csce438_mp2_2_image

You can use `docker images` to check if you have a local image named `csce438_mp2_2_image`

## 1.3 create 2 containers using the docker compose yaml file

On ubuntu terminal, install the `docker-compose`:

    sudo apt-get install docker-compose -y

Use `docker-compose` to create 2 containers (`rabbitmq_container` and `csce438_mp2_2_container`). The 2 containers can talk through the inter-container communication bridge `rabbitmq_net`:

    docker-compose up -d

Enable rabbitmq_stream and rabbitmq_stream_management plugins for `rabbitmq_container`:

    docker exec rabbitmq_container rabbitmq-plugins enable rabbitmq_stream rabbitmq_stream_management

Enter into the `csce438_mp2_2_container`:

    docker exec -it csce438_mp2_2_container bash -c "cd /home/csce438/mp2_2 && exec /bin/bash"

Make the setup script executable and execute the `setup.sh`. You would be able to see the information shown in the picture below:

    chmod +x setup.sh 
    ./setup.sh 

![configurations of csce438_mp2_2_container](../images/configurations_of_csce438_mp2_2_container.png)


# 2. Viewing the activity in the RabbitMQ queues during development: 

You can visit the RabbitMQ Management Plugin UI at http://localhost:15672/ on your ubuntu VM and login with the credentials: 

    Username: guest
    Password: guest

The management UI is very useful and is implemented as a single page application which relies on the HTTP API. Some of the features include: Declare, list and delete exchanges, queues, bindings, users, virtual hosts and user permissions.

# 3. try compiling and using provided synchronizer:

In the `mp2_2` folder of `csce438_mp2_2_container`, compile the code (provided MP2.2 synchronizer + your MP2.1) using the provided mp2.2 makefile:

    make -j4

To clear the directory (and remove .txt files):
   
    make clean

Use `stop.sh` to kill all processes:

    ./stop.sh

Use `startup.sh` to launch the coordinator, tsd, and synchronizers:

    ./startup.sh

After `startup.sh`, on the RabbitMQ Management Plugin UI at http://localhost:15672/, you should be able to see the similar pic shown below, i.e., 3 x 6 = 18 queues. Now, you can check out `TestCasesMP2.2.xlsx` for test cases and proceed your coding. 

![rabbitmq web ui](../images/rabbitmq_web_visualization.png)

# 4. during development/test, you might find the commands below useful

To avoid the inteferences of residual messages from the message queues, in the `rabbitmq_container`:

    rabbitmqctl stop_app
    rabbitmqctl reset 
    rabbitmqctl start_app

To avoid the inteferences from residual files, in `csce438_mp2_2_container`:

    rm -rf /dev/shm/*
    rm -rf ./cluster_*