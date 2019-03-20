create database if not exists msd;
use msd;
create external table if not exists msd.nutrition_data(
YearStart string,
YearEnd string,	
LocationAbbr string,
LocationDesc string,
Datasource string,
Class string,
Topic string,
Question string,
Data_Value_Unit	 string,
Data_Value_Type	 string,
Data_Value  string,
Data_Value_Alt string,
Data_Value_Footnote_Symbol  string,	
Data_Value_Footnote	 string,
Low_Confidence_Limit  string,
High_Confidence_Limit string,
Sample_Size	 string,
Total  string,
Age_in_mnth	 string,
Gender string,
Race_Ethnicity	 string,
GeoLocation	 string,
ClassID	 string,
TopicID	 string,
QuestionID	 string,
DataValueTypeID	 string,
LocationID	 string,
StratificationCategory1	 string,
Stratification1	 string,
StratificationCategoryId1  string,	
StratificationID1  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
("separatorChar" = ",")
stored as textfile
location '/user/hive/nutrition_data';