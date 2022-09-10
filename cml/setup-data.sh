#!/bin/bash

setup_environment() {
	# Base directory for training materials used in the Developer course
	SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
	export SETUP_DIR=$(cd ${SCRIPT_DIR}/..; pwd)

	export HADOOP_ROOT_LOGGER="ERROR"
	
	export S3_ROOT=`sed -nr 's/.*(s3a:.*)\/warehouse.*$/\1/p' /etc/hive/conf/hive-site.xml | head -1`
	export HIVE_EXT="`sed -nr 's/.*(s3a:.*)\/warehouse.*$/\1/p' /etc/hive/conf/hive-site.xml | head -1`/warehouse/tablespace/external/hive"
	export PRINCIPAL=`klist | sed -nr 's/^Default principal: (.*)\/.+$/\1/p' | head -1`
	export DATALAKE=`sed -nr 's/^.*https:\/\/(.*)-idbroker.*$/\1/p' /etc/hadoop/conf/core-site.xml | head -1`
	export S3_DATA=https://cml-training.s3.amazonaws.com

	echo "S3_ROOT =  $S3_ROOT"
	echo "HIVE_EXT = $HIVE_EXT"
	echo "PRINCIPAL = $PRINCIPAL"
	echo "DATALAKE = $DATALAKE"
}

download_duocar_data_from_s3() {
	# Download the zipped DuoCar data from S3 and unzip
	cd ~
	curl -s $S3_DATA/duocar.zip --output ./duocar.zip
	rm -r -f duocar
	mkdir duocar
	unzip duocar.zip -d ~/duocar/

    # Download the Earcloud data from S3 and unzip
	cd ~
	curl -s $S3_DATA/earcloud.zip --output ./earcloud.zip
	rm -r -f earcloud
	unzip earcloud.zip

	# Copy demographics data
	hdfs dfs -rm -r -skipTrash $HIVE_EXT/data
	hdfs dfs -mkdir -p $HIVE_EXT/data
	curl -s $S3_DATA/demographics.txt --output ./demographics.txt
	hdfs dfs -put ./demographics.txt $HIVE_EXT/data
}

copy_duocar_data_to_hdfs() {
	
	export HADOOP_USER_NAME="hdfs"
	# Delete all existing data and subdirectories in the /duocar directory in HDFS
	hdfs dfs -rm -r -skipTrash $HIVE_EXT/duocar
	# Recreate the directories in /duocar
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/drivers
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/riders
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/rides
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/ride_reviews
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/weather
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/demographics
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/data_scientists
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/offices
	hdfs dfs -mkdir -p $HIVE_EXT/duocar/raw/ride_routes
	unset HADOOP_USER_NAME
	# Copy all the data to the /duocar/raw directory in HDFS
	cd ~/duocar
	export HADOOP_USER_NAME="hdfs"
	hdfs dfs -put drivers_* $HIVE_EXT/duocar/raw/drivers/
	hdfs dfs -put riders_* $HIVE_EXT/duocar/raw/riders/
	hdfs dfs -put rides_* $HIVE_EXT/duocar/raw/rides/
	hdfs dfs -put ride_reviews_* $HIVE_EXT/duocar/raw/ride_reviews/
	hdfs dfs -put weather* $HIVE_EXT/duocar/raw/weather/
	hdfs dfs -put demographics* $HIVE_EXT/duocar/raw/demographics/
	hdfs dfs -put data_scientists* $HIVE_EXT/duocar/raw/data_scientists/
	hdfs dfs -put offices* $HIVE_EXT/duocar/raw/offices/
	hdfs dfs -put ride_routes_*/* $HIVE_EXT/duocar/raw/ride_routes/
  # Copy the Earcloud data to the /duocar directory in HDFS
	cd ~
	hdfs dfs -put ~/earcloud $HIVE_EXT/duocar
  unset HADOOP_USER_NAME
}

clean_duocar_data() {
	# Download the data cleaning script from S3
	# NOTE: Currently the version of clean_data.py on S3 assumes that
	# the raw ride_routes data files use the tab delimiter. Change it
	# to a comma delimiter later after the updated ride_routes data
	# files are used.
	cd ~
	# Submit the data cleaning script to Spark
	export HADOOP_USER_NAME="hdfs"
	spark-submit cml/clean_data.py $DATALAKE $HIVE_EXT
	unset HADOOP_USER_NAME
}

join_duocar_data() {
	# Download the data joining script from S3
	cd ~
	# Submit the data joining script to Spark
	export HADOOP_USER_NAME="hdfs"
	spark-submit cml/join_data.py $DATALAKE $HIVE_EXT
	unset HADOOP_USER_NAME
}

create_duocar_hive_tables() {
	# Download the Hive loading script from S3
	cd ~
	export HADOOP_USER_NAME="hdfs"
  # Remove any existing data in Hive’s duocar storage directory
	hdfs dfs -rm -r -skipTrash $HIVE_EXT/duocar.db
	# Submit the Hive loading script to Spark
	spark-submit cml/hive_data.py $DATALAKE $HIVE_EXT
	unset HADOOP_USER_NAME
	# Issue an Impala SQL command to invalidate metadata
	# impala-shell -i worker-1:21000 -q 'INVALIDATE METADATA'
	# Protect the tables in the duocar database from being dropped
	# beeline -u jdbc:hive2://master-1:10000/duocar -e "ALTER TABLE drivers ENABLE NO_DROP;ALTER TABLE riders ENABLE NO_DROP;ALTER TABLE rides ENABLE NO_DROP;ALTER TABLE ride_reviews ENABLE NO_DROP;ALTER TABLE joined ENABLE NO_DROP;"
}

create_nycflights13_hive_tables() {
	# Download airlines.txt and flights.parquet from S3
	cd ~
	aws s3 cp s3://cloudera-training-materials/CDSW/airlines.txt .
	aws s3 cp s3://cloudera-training-materials/CDSW/flights.parquet .
	# Delete the /user/hive/warehouse/airlines directory in HDFS and any existing data in it
	hdfs dfs -rm -r -skipTrash /user/hive/warehouse/airlines
	# Create the directory /user/hive/warehouse/airlines in HDFS
	hdfs dfs -mkdir -p /user/hive/warehouse/airlines
	# Upload airlines.txt to the directory
	hdfs dfs -put airlines.txt /user/hive/warehouse/airlines/
	# Delete the /user/hive/warehouse/flights directory in HDFS and any existing data in it
	hdfs dfs -rm -r -skipTrash /user/hive/warehouse/flights
	# Create the directory /user/hive/warehouse/flights in HDFS
	hdfs dfs -mkdir -p /user/hive/warehouse/flights
	# Upload flights.parquet to the directory
	hdfs dfs -put flights.parquet /user/hive/warehouse/flights/
	# Download airlines.sql and flights.sql from S3:
	cd ~
	aws s3 cp s3://cloudera-training-materials/CDSW/airlines.sql .
	aws s3 cp s3://cloudera-training-materials/CDSW/flights.sql .
	# Issue the Impala SQL statement to drop the airlines table if it exists:
	impala-shell -i worker-1:21000 -q 'DROP TABLE IF EXISTS airlines'
	# Issue the Impala SQL statement to create the airlines table:
	impala-shell -i worker-1:21000 -f airlines.sql
	# Issue the Impala SQL statement to drop the flights table if it exists:
	impala-shell -i worker-1:21000 -q 'DROP TABLE IF EXISTS flights'
	# Issue the Impala SQL statement to create the flights table:
	impala-shell -i worker-1:21000 -f flights.sql
	# Protect the airlines and flights tables from being dropped:
	beeline -u jdbc:hive2://master-1:10000 -e "ALTER TABLE flights ENABLE NO_DROP;ALTER TABLE airlines ENABLE NO_DROP;"
}

cleanup_local_data() {
	cd ~
	rm duocar.zip
	rm -rf duocar

	rm earcloud.zip
	rm -rf earcloud

	echo "export S3_ROOT=\"$HIVE_EXT\"" >>.bash_profile
}

create_python_environment() {
	echo "S3_ROOT = \"$HIVE_EXT\"" >env.py
	echo "S3_HOME = \"$S3_ROOT/user/$PRINCIPAL\"" >>env.py
	echo "CONNECTION_NAME = \"$DATALAKE\"" >>env.py
}

setup_environment
download_duocar_data_from_s3
copy_duocar_data_to_hdfs
clean_duocar_data
join_duocar_data
create_duocar_hive_tables
# create_nycflights13_hive_tables
cleanup_local_data
create_python_environment
