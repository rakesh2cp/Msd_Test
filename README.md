# Msd_Test

My solution approach and assumptions are as below.

Steps To be Followed 
--------------------

1. Download the file from the source link.
2. Remove the header from the file before pushing the file to HDFS.
3. Create the source Hive table (external table).
4. Running the ETL ( as mentioned in the email) from Spark (Scala)on the data pushed to HDFS in Step 2.
	Basically the ETL will write the data in to destination location in to 2 tables. ( table names avg_data_val_all_age_grp and avg_data_val_female)

5. Creating output Hive tables ( external table) on top of directory in Step 4
6. Generating report (HTML report ) on from the data generated in Step 4. ( Sample Html ( file name my_html_file.html) report available in repository )
	under the directory the script is running.

