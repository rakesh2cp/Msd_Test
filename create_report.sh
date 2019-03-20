#!/bin/bash


echo "###################################################################"
#######################################
# Prepare output file name and other params #
#######################################

my_func(){
out_filename=my_html_file
echo "Out filename: ${out_filename}.html"
  
HEADERFILE='<html>
<head>
<title>Report1</title>
<STYLE TYPE="text/css">
<!--
TD{font-family: Arial; font-size: 10pt;}
TH{font-family: Arial; font-size: 10pt; font-weight: bold; color:green; text-align: center; width:100px}
H6{font-family: Arial; font-size: 10pt; color:green; border: none; text-align: left;}
body{font-family: Arial; font-size: 10pt;}
--->
</STYLE>
</head>
<body>'
TABLE_BODY_PBR='
<br>
<br>
<table border="1">
<tr style="text-align:center"><th style="text-align:center" colspan="4">All Report</th></tr>
<tr style="text-align:center"><th> Question</th><th> YearStart</th><th> Age_In_Mnth</th><th>Avg_Data_Value</th></tr>'

TABLE_BODY_SET='
<br>
<br>
<table border="1">
<tr style="text-align:center"><th style="text-align:center" colspan="4">Female Report</th></tr>
<tr style="text-align:center"><th> Gender</th><th> Question</th><th> YearStart</th><th>Avg_Data_Value</th></tr>'

  TABLE_TRAILERFILE='</table>'
  
  BODY_TRAILERFILE='</body>
</html>'

  
  HTMLFILE=${out_filename}.html
  rm -f ${out_filename}.html
  ALL_FILE_CSV=${out_filename}.csv
  FEM_FILE_CSV=${out_filename}_2.csv

cd /Users/hduser/bdapps/hadoop-2.7.7/bin

./hdfs dfs -cat /user/hive/msd_export_all/*.csv > ${ALL_FILE_CSV}

./hdfs dfs -cat /user/hive/msd_export_female/*.csv > ${FEM_FILE_CSV}



########################################
#           Prepare-Report             #
########################################


echo ${TABLE_BODY_PBR} >> ${HTMLFILE}


	if [ -f "${ALL_FILE_CSV}" ]
	then
		sed "s/,/<\/td><td>/g" ${ALL_FILE_CSV} | while read line
		do
		printf "<tr><td>${line}</td></tr>\n" >> ${HTMLFILE}
		done
	fi
	
echo ${TABLE_TRAILERFILE} >> ${HTMLFILE}


echo ${TABLE_BODY_SET} >> ${HTMLFILE}


	if [ -f "${FEM_FILE_CSV}" ]
	then
		sed "s/,/<\/td><td>/g" ${FEM_FILE_CSV} | while read line
		do
		printf "<tr><td>${line}</td></tr>\n" >> ${HTMLFILE}
		done
	fi
echo ${TABLE_TRAILERFILE} >> ${HTMLFILE}

}


my_func

exit 0