create database if not exists msd;
use msd;
drop table if exists msd.avg_data_val_all_age_grp;
CREATE external TABLE `msd.avg_data_val_all_age_grp`(
  `question` string, 
  `yearstart` string, 
  `age_in_mnth` string, 
  `avg_data_value` double)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
("separatorChar" = ",")
stored as textfile
location '/user/hive/msd_export_all';

drop table if exists msd.avg_data_val_female;
CREATE external TABLE  `msd.avg_data_val_female`(
  `gender` string, 
  `question` string, 
  `yearstart` string, 
  `avg_data_value` double)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
("separatorChar" = ",")
stored as textfile
location '/user/hive/msd_export_female';
