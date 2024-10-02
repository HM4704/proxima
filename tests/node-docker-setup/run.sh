#!/bin/bash

if [ ! -d "./data" ]; then
    mkdir ./data
    ./update-snapshot.sh
fi

if [ ! -d "./data/proximadb" ]; then
    mkdir ./data/proximadb
fi

if [ ! -d "./data/config" ]; then
    mkdir ./data/config
fi


if [ ! -d "./data/prometheus" ]; then
    mkdir ./data/prometheus
    sudo chown -R 65532:65532 ./data/prometheus
fi

#if [ ! -d "./data/grafana" ]; then
#    mkdir ./data/grafana
#fi

docker compose up
