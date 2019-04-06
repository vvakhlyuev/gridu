#!/bin/bash

mysql --user='root' --password='cloudera' --execute='DROP DATABASE IF EXISTS db;'
mysql --user='root' --password='cloudera' --execute='CREATE DATABASE IF NOT EXISTS db;'

mysql --user='root' --password='cloudera' --execute='
USE db;
CREATE TABLE top_categories (
   name VARCHAR(100),
   count int);
CREATE TABLE top_products (
   category VARCHAR(100),
   product VARCHAR(200),
   count int,
   rank int);
CREATE TABLE top_countries (
   country VARCHAR(100),
   spent_amount DOUBLE);'

# Top categories
hive -e \
"DROP TABLE IF EXISTS csvdump1;
CREATE TABLE csvdump1
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/tmp/csvdump1'
AS
SELECT category, count(category) as count FROM purchases GROUP BY category ORDER BY count DESC LIMIT 10;"

# Top products
hive -e \
"DROP TABLE IF EXISTS csvdump2;
CREATE TABLE csvdump2
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/tmp/csvdump2'
AS
SELECT * FROM (
    SELECT category, name, count,
        rank() over (partition by category order by count desc) as rank
    FROM (
        SELECT category, name, count(name) as count
        FROM purchases
        GROUP BY category, name
    ) p) p2
WHERE rank < 10;"

# Top countries by money spent
hive -e \
"DROP TABLE IF EXISTS csvdump3;
CREATE TABLE csvdump3
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/tmp/csvdump3'
AS
SELECT country_name, sum(price) as sum
FROM purchases
GROUP BY country_name
ORDER BY sum DESC
LIMIT 10;"

# Export all!
sqoop export \
--connect jdbc:mysql://localhost/db \
--username root \
--password cloudera \
--table top_categories \
--export-dir /tmp/csvdump1

sqoop export \
--connect jdbc:mysql://localhost/db \
--username root \
--password cloudera \
--fields-terminated-by ',' \
--table top_products \
--export-dir /tmp/csvdump2

sqoop export \
--connect jdbc:mysql://localhost/db \
--username root \
--password cloudera \
--table top_countries \
--export-dir /tmp/csvdump3

# See results
mysql --user='root' --password='cloudera' --execute='use db; SELECT * FROM top_categories ORDER BY count DESC;'
mysql --user='root' --password='cloudera' --execute='use db; SELECT * FROM top_products ORDER BY category, count DESC;'
mysql --user='root' --password='cloudera' --execute='use db; SELECT * FROM top_countries ORDER BY spent_amount DESC;'