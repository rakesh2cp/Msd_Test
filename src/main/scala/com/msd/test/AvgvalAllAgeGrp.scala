package com.msd.test
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.{SQLContext, SaveMode}
import com.typesafe.config.{ ConfigFactory }
import java.io.File

object AvgvalAllAgeGrp {
  
  def main(args: Array[String]): Unit = {
    
  // Config file with all the parameters 
  var applicationConf: com.typesafe.config.Config = ConfigFactory.load("application.conf")

    if (args.length == 1) {
      applicationConf = ConfigFactory.parseFile(new File(args(0)))
    }

      var hostName = applicationConf.getString("spark.hostName")
      var inputPath = applicationConf.getString("spark.inputPath")
      var outputPathFem = applicationConf.getString("spark.outputPathFem")
      var outputPathAll = applicationConf.getString("spark.outputPathAll")
      
   // Spark Session Initialization and defining App Name
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MSD-ETL-App")
      .getOrCreate()
      
// Defining Schema for Nutrition Data set      
val Schema = StructType(Seq(
StructField("YearStart" , StringType ,  true),
StructField("YearEnd" , StringType ,  true),
StructField("LocationAbbr" , StringType ,  true),
StructField("LocationDesc" , StringType ,  true),
StructField("Datasource" , StringType ,  true),
StructField("Class" , StringType ,  true),
StructField("Topic" , StringType ,  true),
StructField("Question" , StringType ,  true),
StructField("Data_Value_Unit" , StringType ,  true),
StructField("Data_Value_Type" , StringType ,  true),
StructField("Data_Value" , StringType ,  true),
StructField("Data_Value_Alt" , StringType ,  true),
StructField("Data_Value_Footnote_Symbol" , StringType ,  true),
StructField("Data_Value_Footnote" , StringType ,  true),
StructField("Low_Confidence_Limit" , StringType ,  true),
StructField("High_Confidence_Limit" , StringType ,  true),
StructField("Sample_Size" , StringType ,  true),
StructField("Total" , StringType ,  true),
StructField("Age_in_mnth" , StringType ,  true),
StructField("Gender" , StringType ,  true),
StructField("Race_Ethnicity" , StringType ,  true),
StructField("GeoLocation" , StringType ,  true),
StructField("ClassID" , StringType ,  true),
StructField("TopicID" , StringType ,  true),
StructField("QuestionID" , StringType ,  true),
StructField("DataValueTypeID" , StringType ,  true),
StructField("LocationID" , StringType ,  true),
StructField("StratificationCategory1" , StringType ,  true),
StructField("Stratification1" , StringType ,  true),
StructField("StratificationCategoryId1" , StringType ,  true),
StructField("StratificationID1" , StringType, true)))

// Source file path on Local
val fileSystemPath = "/Users/hduser/bdapps/msd/Nutrition_data.csv" 

// Source file path on HDFS 
val hdfsInputFilePath = hostName + inputPath + "/Nutrition_data.csv"

// Destination File Path on HDFS
val hdfsOutPathAll = hostName + outputPathAll 
val hdfsOutPathFem = hostName + outputPathFem 
println(hdfsInputFilePath)
println(hdfsOutPathAll)
println(hdfsOutPathFem)

// Reading source file in to input_data dataframe
val input_data = spark.read.option("delimiter",",").option("header","false").schema(Schema).csv(hdfsInputFilePath)
  
// Creating Temporary view on the Source data
spark.read.option("delimiter",",").option("header","false").schema(Schema).csv(hdfsInputFilePath).createOrReplaceTempView("nutrition_data")

// Show 20 rows for Average of each Question’s "Data_Value" by year for all age groups
spark.sql("select question,yearstart,age_in_mnth,round(avg(data_value),2) as avg_data_value from nutrition_data where nutrition_data.Age_in_mnth !='' group by nutrition_data.Question,nutrition_data.YearStart,nutrition_data.Age_in_mnth order by YearStart desc").show()

// Creating a data frame Average of each Question’s "Data_Value" by year for all age groups
val avgvalallage = spark.sql("select question,yearstart,age_in_mnth,round(avg(data_value),2) as avg_data_value from nutrition_data where nutrition_data.Age_in_mnth !='' group by nutrition_data.Question,nutrition_data.YearStart,nutrition_data.Age_in_mnth order by YearStart desc")

// Writing data frame to HDFS OutPut directory
avgvalallage.write.mode(SaveMode.Overwrite).csv(hdfsOutPathAll)

// Show 20 rows for Average of each Question’s "Data_Value" by year for female only
spark.sql("select Gender,question,yearstart,round(avg(data_value),2) as avg_data_value from nutrition_data where Gender ='Female' group by question,yearstart,Gender").show()

// Creating a dataframe Average of each Question’s "Data_Value" by year for female only
val avgvalfemale = spark.sql("select Gender,question,yearstart,round(avg(data_value),2) as avg_data_value from nutrition_data where Gender ='Female' group by question,yearstart,Gender")

// Writing data frame to HDFS output directory
avgvalfemale.write.mode(SaveMode.Overwrite).csv(hdfsOutPathFem)

println("================= OutPut Files Written to HDFS Successfully =====================: " )
spark.stop()

  }
}