#!/bin/bash
#
# Copyright © 1982-2019 Starfox, Inc.
#

if [ $(grep "\"isMaster\": true" /mnt/var/lib/info/instance.json -wc) = 1 ]; then
    echo "Running on the master node."

    pkgPath="s3://assn-huyen/packages"
    flinkTar="flink-1.7.2.tar"
    flinkRoot="flink-1.7.2"
    mainJar="chronos-flink-0.3-snapshot.jar"

    mkdir tmp_pkgs
    cd tmp_pkgs
    aws s3 cp $pkgPath/$flinkTar .
    aws s3 cp $pkgPath/$mainJar /home/hadoop/$mainJar
    tar -xvf $flinkTar
    sudo rm /usr/lib/flink/lib/*
    sudo cp $flinkRoot/lib/* /usr/lib/flink/lib/
    sudo rm /etc/flink/conf.dist/log4j*
    sudo cp $flinkRoot/conf/logback* /etc/flink/conf.dist/
    master_ip="$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)"
    cat $flinkRoot/conf/flink-conf.yaml | sed -e "s/zookeeper.quorum: localhost/zookeeper.quorum: $master_ip/" | sudo tee /etc/flink/conf.dist/flink-conf.yaml > /dev/null
    cd ..
    rm -rf tmp_pkgs
    rm $flinkTar
fi
exit 0
