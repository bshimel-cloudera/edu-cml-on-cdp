#!/bin/bash

setup_environment() {
	# Base directory for training materials used in the Developer course
	SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
	export SETUP_DIR=$(cd ${SCRIPT_DIR}/..; pwd)

	export HADOOP_ROOT_LOGGER="ERROR"
	
	export S3_ROOT=`sed -nr 's/.*(s3a:.*)\/warehouse.*$/\1/p' /etc/hadoop/conf/hive-site.xml | head -1`
	export HIVE_EXT="`sed -nr 's/.*(s3a:.*)\/warehouse.*$/\1/p' /etc/hadoop/conf/hive-site.xml | head -1`/warehouse/tablespace/external/hive"
	export PRINCIPAL=`klist | sed -nr 's/^Default principal: (.*)\/.+$/\1/p' | head -1`
	export DATALAKE=`sed -nr 's/^.*https:\/\/(.*)-idbroker.*$/\1/p' /etc/hadoop/conf/core-site.xml | head -1`

	echo "S3_ROOT =  $S3_ROOT"
	echo "HIVE_EXT = $HIVE_EXT"
	echo "PRINCIPAL = $PRINCIPAL"
	echo "DATALAKE = $DATALAKE"
}

create_python_environment() {
	echo "S3_ROOT = \"$HIVE_EXT\"" >env.py
	echo "S3_HOME = \"$S3_ROOT/user/$PRINCIPAL\"" >>env.py
	echo "CONNECTION_NAME = \"$DATALAKE\"" >>env.py
}

setup_environment
create_python_environment
