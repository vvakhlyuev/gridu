#!/usr/bin/env bash
hadoop fs -mkdir /tmp/purchases_staging
hadoop fs -mkdir /tmp/locations_staging
hadoop fs -mkdir /tmp/purchases
hadoop fs -chmod -R 777 /tmp/purchases_staging
hadoop fs -chmod -R 777 /tmp/locations_staging
hadoop fs -chmod -R 777 /tmp/purchases
bash copyGeoliteDbs.sh
