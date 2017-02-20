package org.excel.readers.main
import org.apache.spark.SparkConf
// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, LongType};

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.excel.readers.main.CSVReader
import org.excel.readers.main.ExcelReader
import org.apache.spark.sql.DataFrame

object ReaderMain {
  def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("SparkProgram").setMaster("local")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     //import sqlContext.implicits._

     
// Create an dataframe for fwd_risk_1
val fwdRisk_1File = sc.textFile("C:/sagan/fractal/My Workspaces/TrainingWKS/ExcelCSVReader/FRB_FWDRisk1.csv")
// The schema is encoded in a string
val fwdRisk_1SchemaString = "scenario:String prod_dsc:String cusip:String valuation_date:String portfolio_date:String All_200:Double "+
"base:Double basebalance:Double pv01:Double pv200:Double proceeds:Double"
// Generate the schema based on the string of schema
val fwdRisk_1Schema =
  StructType(
    fwdRisk_1SchemaString.split(" ").map( fieldName => if(fieldName.split(":")(1)=="String")StructField(fieldName.split(":")(0), StringType, true) 
        else if(fieldName.split(":")(1)=="Double") StructField(fieldName.split(":")(0), DoubleType, true) else StructField(fieldName.split(":")(0), LongType, true)))
// Convert records of the RDD (people) to Rows.
val fwdRisk_1RowRDD = fwdRisk_1File.map(_.split(",")).map(p => Row(p(0),p(1),p(2),monthEndDate(p(3)),monthEndDate(p(4)),p(5).toDouble,p(6).toDouble,
    p(7).toDouble,p(8).toDouble,p(9).toDouble,p(10).toDouble ))
// Apply the schema to the RDD.
var fwdRisk_1DataFrame = sqlContext.createDataFrame(fwdRisk_1RowRDD, fwdRisk_1Schema)
// Register the DataFrames as a table.
fwdRisk_1DataFrame.registerTempTable("fwdRisk_1")


// Create an dataframe for fwd_arm_prepay
val fwd_arm_prepayFile = sc.textFile("C:/sagan/fractal/My Workspaces/TrainingWKS/ExcelCSVReader/FRB_FWDRisk1.csv")
// The schema is encoded in a string
val fwd_arm_prepaySchemaString = "scenario:String prod_dsc:String cusip:String valuation_date:String portfolio_date:String CPR:Double CDR:Double survival:Double balance:Double"
// Generate the schema based on the string of schema
val fwd_arm_prepaySchema =
  StructType(
    fwd_arm_prepaySchemaString.split(" ").map( fieldName => if(fieldName.split(":")(1)=="String")StructField(fieldName.split(":")(0), StringType, true) 
        else if(fieldName.split(":")(1)=="Double") StructField(fieldName.split(":")(0), DoubleType, true) else StructField(fieldName.split(":")(0), LongType, true)))
// Convert records of the RDD (people) to Rows.
val fwd_arm_prepayRowRDD = fwdRisk_1File.map(_.split(",")).map(p => Row(p(0),p(1),p(2),monthEndDate(p(3)),monthEndDate(p(4)),p(5).toDouble,p(6).toDouble,p(7).toDouble,
    p(8).toDouble))

// Apply the schema to the RDD.
val fwd_arm_prepayDataFrame = sqlContext.createDataFrame(fwd_arm_prepayRowRDD, fwd_arm_prepaySchema)
// Register the DataFrames as a table.
fwd_arm_prepayDataFrame.registerTempTable("fwd_arm_prepay")


  fwd_arm_prepayDataFrame.show()
  fwdRisk_1DataFrame.show()
  
  
  //Step 1.1 to get more columns 
  fwdRisk_1DataFrame = sqlContext.sql("select aa.* ,aa.basebalance as nv, basebalance/1000 as nv_mm, base*100 as price, basebalance*base as mv, (basebalance*base)/1000000 as mv_mm,"+ 
  " pv01/1000 as pv01_k, pv200 as u200, pv200/1000000 as u200_mm  from fwdRisk_1 aa")
  
  fwdRisk_1DataFrame.registerTempTable("fwdRisk_1")
  fwdRisk_1DataFrame.show()
  
  //combine the risk files 
  val combineRiskDF = sqlContext.sql("select aa.*, bb.CPR, bb.CDR,  bb.survival, bb.balance from  fwdRisk_1 aa join fwd_arm_prepay bb on aa.scenario = bb.scenario and aa.prod_dsc = bb.prod_dsc and aa.cusip = bb.cusip and aa.valuation_date = bb.valuation_date and aa.portfolio_date = bb.portfolio_date ")
  combineRiskDF.registerTempTable("combine_risk")
  combineRiskDF.show()
  
  //Step 2 Aggregation
  val totalNV =  sqlContext.sql("select sum(nv) from combine_risk").first()(0)
  val aggRiskDF = sqlContext.sql("select scenario, portfolio_date, valuation_date, prod_dsc, sum(nv) as nv, sum(nv_mm) as nv_mm, sum(mv) as mv, sum(nv)/"+totalNV+" as price, sum(mv_mm) as mv_mm, sum(pv01) as pv01,"+ 
" sum(pv01_k) as pv01_k, sum(u200) as u200, sum(u200_mm) as u200_mm from combine_risk group by scenario, portfolio_date, valuation_date, prod_dsc ")
  aggRiskDF.registerTempTable("agg_risk")
  aggRiskDF.show()
  
 
  }
  
  
  //to convert month and portfolio date to end date format
  def monthEndDate(date:String) :String = {
   if(date.length()==7)
     return date+"-31"
   else
    return date.substring(0, 4)+"-"+date.substring(4, 6)+"-"+date.substring(6, 8) 
  }
  
  
 
}