#!/bin/bash

############################################################
# This script fetches Nutritional data from the source URL #
# The Spark programs performs the necessary tranformations #
# and writes the output to HDFS directory				   #
# Finally HTML reports generated out of the Output files   #
# from Spark programs                                      #
############################################################

msd_data_populate(){

JAR_PATH="/Users/hduser/workspace/MsdTest/target"
CONF_PATH="/Users/hduser/bdapps/spark-2.3.3-bin-hadoop2.7/bin"
echo " Deleting if Source files exist"
rm -f Nutrition_data_raw.csv Nutrition_data.csv

echo " Fetching Source File from URL"
# Please use wget in Linux and curl for Mac OS
wget -O Nutrition_data_raw.csv https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD
# curl -o Nutrition_data_raw.csv https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD
sleep 5
echo " Fetching Source File from URL Completed"

awk 'NR>1' Nutrition_data_raw.csv > Nutrition_data.csv

hdfs dfs -mkdir /user/hive/nutrition_data

hdfs dfs -put Nutrition_data.csv /user/hive/nutrition_data

echo " Creating Source Hive table"
hive -f input_table_ddl.hql
echo " Creating Source Hive table Completed"

echo " Running ETL Now"
sh spark-submit  --class com.msd.test.AvgvalAllAgeGrp ${JAR_PATH}/MsdTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${CONF_PATH}/application.conf
echo " ETL Completed.. Creating Output Tables"

hive -f output_table_ddl.hql
echo "Output Tables Created"

echo " Creating Reports"
sh create_report.sh 
echo " Reports Created"

}

msd_data_populate
