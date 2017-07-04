#!/usr/bin/env bash
cp /media/sf_gridu/event-generator/src/main/resources/GeoLite2-City-CSV_20170606/GeoLite2-City-Locations-en.csv /tmp/geolite2-locations.csv
hadoop fs -put /tmp/geolite2-locations.csv /tmp/locations_staging/