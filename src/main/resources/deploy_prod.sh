#!/bin/bash
#
# Copyright © 1982-2019 Starfox, Inc.
#

if [ $(grep "\"isMaster\": true" /mnt/var/lib/info/instance.json -wc) = 1 ]; then
    echo "Running on the master node."
    mkdir pkgs
    cd pkgs
    aws s3 cp s3://assn-csa-prod-telemetry-data-lake/Huyen/packages/flink1.7.tar .
    aws s3 cp s3://assn-csa-prod-telemetry-data-lake/Huyen/packages/chronos-flink-0.1.jar /home/hadoop/chronos-flink-0.1.jar
    tar -xvf flink1.7.tar
    sudo rm /usr/lib/flink/lib/*
    sudo cp flink-1.7.0/lib/* /usr/lib/flink/lib/
    sudo rm /etc/flink/conf.dist/log4j*
    sudo cp flink-1.7.0/conf/logback* /etc/flink/conf.dist/
    master_ip="$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)"
    cat flink-1.7.0/conf/flink-conf.yaml | sed -e "s/zookeeper.quorum: localhost/zookeeper.quorum: $master_ip/" | sudo tee /etc/flink/conf.dist/flink-conf.yaml > /dev/null
fi
exit 0