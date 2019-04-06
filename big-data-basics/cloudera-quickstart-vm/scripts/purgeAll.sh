#!/usr/bin/env bash
beeline -u jdbc:hive2://localhost:10000 cloudera cloudera -f drop_all.sql
# Data for Hive/Spark
hadoop fs -rm -r -f -skipTrash /tmp/purchases_staging
hadoop fs -rm -r -f -skipTrash /tmp/locations_staging
hadoop fs -rm -r -f -skipTrash /tmp/purchases
# Dumps used for Sqoop
hadoop fs -rm -r -f -skipTrash /tmp/csvdump1
hadoop fs -rm -r -f -skipTrash /tmp/csvdump2
hadoop fs -rm -r -f -skipTrash /tmp/csvdump3

mysql --user='root' --password='cloudera' --execute='DROP DATABASE IF EXISTS db;'