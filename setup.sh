#!/bin/bash

ZOOKEEPER_VERSION="3.4.6"
ZOOKEEPER_VERSION="3.5.0-alpha"

kill $(ps aux | grep [z]ookeeper.server | awk '{print $2}')

rm -rf /tmp/zoo*

rm "zookeeper-${ZOOKEEPER_VERSION}.tar.gz"

wget "http://ftp.ps.pl/pub/apache/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz"
tar -xzf "zookeeper-${ZOOKEEPER_VERSION}.tar.gz"
mv "zookeeper-${ZOOKEEPER_VERSION}" /tmp

mkdir /tmp/zookeeper1
mkdir /tmp/zookeeper2
mkdir /tmp/zookeeper3

echo '1' > /tmp/zookeeper1/myid
echo '2' > /tmp/zookeeper2/myid
echo '3' > /tmp/zookeeper3/myid

cp src/test/resources/* "/tmp/zookeeper-${ZOOKEEPER_VERSION}/conf"

cd "/tmp/zookeeper-${ZOOKEEPER_VERSION}"

bin/zkServer.sh start-foreground conf/zoo1.cfg > /tmp/zoo1.log &
bin/zkServer.sh start-foreground conf/zoo2.cfg > /tmp/zoo2.log &
bin/zkServer.sh start-foreground conf/zoo3.cfg > /tmp/zoo3.log &

sleep 5

# dynamically add server 2 and server 3 to the ensemble
bin/zkCli.sh -server localhost:2181 reconfig -add "2=localhost:2882:3882;2182,3=localhost:2883:3883;2183"

echo 'srvr' | nc localhost 2181
echo 'srvr' | nc localhost 2182
echo 'srvr' | nc localhost 2183
