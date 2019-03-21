# Msd_Test

Solution approach and assumptions are as below.

Steps Followed 
--------------------

1. Download the data file from the source link.
2. Remove the header from the file before pushing the file to HDFS.
3. Create the source Hive table (external table).
4. Running the ETL ( as mentioned in the email) from Spark (Scala)on the data pushed to HDFS in Step 2.
	Basically the ETL will write the data in to destination location in to 2 tables. ( table names avg_data_val_all_age_grp and avg_data_val_female)

5. Creating output Hive tables ( external table) on top of the directory in Step 4
6. Generating report (HTML report ) on from the data generated in Step 4. ( Sample Html ( file name my_html_file.html) report available in repository )
	under the directory the script is running.

Assumptions:
------------
1. for Q1 - Average of each Question’s "Data_Value" by year for all age groups
 Since the YearStart and YearEnd are same for all the rows hence the grouping is done on YearStart column.

2. Below hdfs directories should not exist in the running environment 
	/user/hive/nutrition_data
	/user/hive/msd_export_female
	/user/hive/msd_export_all



Versions followed to build this project are as follows
------------------------------------------------------
Java 1.8,
Scala 2.11,
Apache Spark 2.3.3,
Apache Hadoop 2.7.3,
Apache Hive 1.2.2

Building and running the program.
---------------------------------
The project can be build in  Scala IDE with above version dependencies.
Once project is build the JAR_PATH and CONF_PATH variables need to be changed in the msd_data_populate.sh script as per the paths where jar ( jar with dependencies) and application.conf is present in the environment its running.
The hostName  need to be changed as per the environment its running ( e.g. hostName = "hdfs://0.0.0.0:9000" in my case).

The following hdfs directories need to be deleted in case they exist in the running environment.

/user/hive/nutrition_data, /user/hive/msd_export_female and /user/hive/msd_export_all


To run the ETL flow issue the below command where the script is located.

sh -x msd_data_populate.sh > msd.log 2>&1
