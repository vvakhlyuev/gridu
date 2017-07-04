SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.exec.max.dynamic.partitions=50000;

--Main "partitioned" table
--Product Name, Product Price, Purchase Date, Product Category, Client IP Address
CREATE EXTERNAL TABLE IF NOT EXISTS purchases_staging(
    name STRING,
    price DOUBLE,
    purchase_dt string,
    category STRING,
    client_ip STRING,
    geoname_id STRING
)
PARTITIONED BY (purchase_date DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/purchases_staging/';

--ADD partitions
ALTER TABLE purchases_staging ADD IF NOT EXISTS
PARTITION (purchase_date='2017-06-01') LOCATION '/tmp/purchases_staging/2017/06/01'
PARTITION (purchase_date='2017-06-02') LOCATION '/tmp/purchases_staging/2017/06/02'
PARTITION (purchase_date='2017-06-03') LOCATION '/tmp/purchases_staging/2017/06/03'
PARTITION (purchase_date='2017-06-04') LOCATION '/tmp/purchases_staging/2017/06/04'
PARTITION (purchase_date='2017-06-05') LOCATION '/tmp/purchases_staging/2017/06/05'
PARTITION (purchase_date='2017-06-06') LOCATION '/tmp/purchases_staging/2017/06/06'
PARTITION (purchase_date='2017-06-07') LOCATION '/tmp/purchases_staging/2017/06/07'
PARTITION (purchase_date='2017-06-08') LOCATION '/tmp/purchases_staging/2017/06/08'
PARTITION (purchase_date='2017-06-09') LOCATION '/tmp/purchases_staging/2017/06/09'
PARTITION (purchase_date='2017-06-10') LOCATION '/tmp/purchases_staging/2017/06/10'
PARTITION (purchase_date='2017-06-11') LOCATION '/tmp/purchases_staging/2017/06/11'
PARTITION (purchase_date='2017-06-12') LOCATION '/tmp/purchases_staging/2017/06/12'
PARTITION (purchase_date='2017-06-13') LOCATION '/tmp/purchases_staging/2017/06/13'
PARTITION (purchase_date='2017-06-14') LOCATION '/tmp/purchases_staging/2017/06/14'
PARTITION (purchase_date='2017-06-15') LOCATION '/tmp/purchases_staging/2017/06/15'
PARTITION (purchase_date='2017-06-16') LOCATION '/tmp/purchases_staging/2017/06/16'
PARTITION (purchase_date='2017-06-17') LOCATION '/tmp/purchases_staging/2017/06/17'
PARTITION (purchase_date='2017-06-18') LOCATION '/tmp/purchases_staging/2017/06/18'
PARTITION (purchase_date='2017-06-19') LOCATION '/tmp/purchases_staging/2017/06/19'
PARTITION (purchase_date='2017-06-20') LOCATION '/tmp/purchases_staging/2017/06/20'
PARTITION (purchase_date='2017-06-21') LOCATION '/tmp/purchases_staging/2017/06/21'
PARTITION (purchase_date='2017-06-22') LOCATION '/tmp/purchases_staging/2017/06/22'
PARTITION (purchase_date='2017-06-23') LOCATION '/tmp/purchases_staging/2017/06/23'
PARTITION (purchase_date='2017-06-24') LOCATION '/tmp/purchases_staging/2017/06/24'
PARTITION (purchase_date='2017-06-25') LOCATION '/tmp/purchases_staging/2017/06/25'
PARTITION (purchase_date='2017-06-26') LOCATION '/tmp/purchases_staging/2017/06/26'
PARTITION (purchase_date='2017-06-27') LOCATION '/tmp/purchases_staging/2017/06/27'
PARTITION (purchase_date='2017-06-28') LOCATION '/tmp/purchases_staging/2017/06/28'
PARTITION (purchase_date='2017-06-29') LOCATION '/tmp/purchases_staging/2017/06/29'
PARTITION (purchase_date='2017-06-30') LOCATION '/tmp/purchases_staging/2017/06/30';

--Locations staging "non-partitioned" table
--geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone
CREATE EXTERNAL TABLE IF NOT EXISTS locations_staging(
    geoname_id STRING,
    locale_code STRING,
    continent_code STRING,
    continent_name STRING,
    country_iso_code STRING,
    country_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/locations_staging/'
TBLPROPERTIES('skip.header.line.count'='1');

--Final merged "partitioned" table of purchases
CREATE TABLE IF NOT EXISTS purchases(
    name STRING,
    price DOUBLE,
    purchase_dt string,
    category STRING,
    client_ip STRING,
    geoname_id STRING
)
PARTITIONED BY (country_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/purchases/';

--Create dynamic partitions via insert
INSERT OVERWRITE TABLE purchases PARTITION(country_name)
SELECT p.name, p.price, p.purchase_dt, p.category, p.client_ip, l.geoname_id, l.country_name
FROM purchases_staging p JOIN locations_staging l ON p.geoname_id = l.geoname_id;