# 1. create a docker container with the same environment of MP1

First of all, copy your the folder of your MP1 to a new folder named `mp2_1`. Copy the 3 source code files to the `mp2_1` folder: `coordinator.cc`, `coordinator.proto`, `Makefile`.

Launch a container `csce438_mp2_1_container` with the same environment of MP1

    docker run -it --name csce438_mp2_1_container -v $(pwd)/mp2_1:/home/csce438/mp2_1 liuyidockers/csce438_env:latest

    cd mp2_1

# 2. try using coordinator:

Compile the code (coordinator + your MP1) using the provided mp2.1 makefile:

    make -j4

To clear the directory (and remove .txt files):
   
    make clean

To run the coordinator without glog messages:

    ./coordinator -p 9090

To run the coordinator without glog messages:

    GLOG_logtostderr=1 ./coordinator -p 9090

Check out `TestCasesMP2.1.xlsx` for running server and clients.