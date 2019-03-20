
msd_data_populate(){

echo " Deleting if Source files exist"
rm -f Nutrition_data_raw.csv Nutrition_data.csv

echo " Fetching Source File from URL"
# Please use wget in Linix and curl for Mac OS
#wget -O Nutrition_data_raw.csv https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD
curl -o Nutrition_data_raw.csv https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD
sleep 5
echo " Fetching Source File from URL Completed"

awk 'NR>1' Nutrition_data_raw.csv > Nutrition_data.csv

sh /Users/hduser/bdapps/hadoop-2.7.7/bin/hdfs dfs -mkdir /user/hive/nutrition_data

sh /Users/hduser/bdapps/hadoop-2.7.7/bin/hdfs dfs -put Nutrition_data.csv /user/hive/nutrition_data

echo " Creating Source Hive table"
sh /Users/hduser/bdapps/apache-hive-1.2.2-bin/bin/hive -f input_table_ddl.hql
echo " Creating Source Hive table Completed"

echo " Running ETL Now"
sh /Users/hduser/bdapps/spark-2.3.3-bin-hadoop2.7/bin/spark-submit  --class com.msd.test.AvgvalAllAgeGrp /Users/hduser/workspace/MsdTest/target/MsdTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar /Users/hduser/bdapps/spark-2.3.3-bin-hadoop2.7/bin/application.conf
echo " ETL Completed.. Creating Output Tables"

sh /Users/hduser/bdapps/apache-hive-1.2.2-bin/bin/hive -f output_table_ddl.hql
echo "Output Tables Created"

echo " Creating Reports"
sh create_report.sh 
echo " Reports Created"

}

msd_data_populate

exit 0