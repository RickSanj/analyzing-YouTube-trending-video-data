#!/bin/bash
docker run --rm -it \
 --network hw10-network \
 --name spark-submit \
 -v ./app:/opt/app \
 bitnami/spark:latest \
 spark-submit /opt/app/main.py

