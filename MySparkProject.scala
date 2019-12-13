import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object MySparkProject extends App{

	// Read csv input file from HDFS
	val inputfile = sc.textFile("/sparkfiles/trans_log.csv")

	//Split record based on comma
	val csv = inputfile.map(_.split(","))

	// This import is required for implicit conversion of RDD to Dataframe
	import sqlContext.implicits._
	
	// Filter data based on only required codes from the transactions
	val impData = csv.filter(x=>(x(1) == "TT" || x(1) == "LL" || x(1) == "PP"))

	//Separate TT records and get those in an RDD
	val TT = impData.filter(x=> (x(1) == "TT")).map(x=> Row(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10)))
	
	//Separate PP records and get those in an RDD
	val PP = impData.filter(x=> (x(1) == "PP")).map(x=> Row(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6)))
	
	//Separate LL records and get those in an RDD
	val LL = impData.filter(x=> (x(1) == "LL")).map(x=> Row(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6)))

	//Lets generate the schema for each RDD.
	//We can also use case class but that allows only 22 fields
	
	val ttSchema = StructType(Seq(StructField("seq_num", IntegerType, true),
	StructField("trans_code", StringType, true),
	StructField("item_seq", IntegerType, true),
	StructField("product_cd", StringType, true),
	StructField("amt", DoubleType, true),
	StructField("disc_amt", DoubleType, true),
	StructField("add_flg", IntegerType, true),
	StructField("store", StringType, true),
	StructField("emp_num", IntegerType, true),
	StructField("lane", IntegerType, true),
	StructField("t_stmp", StringType, true) ) )

	val llSchema = StructType(Seq(StructField("seq_num", IntegerType, true),
	StructField("trans_code", StringType, true),
	StructField("loyalty_cd", StringType, true),
	StructField("store", StringType, true),
	StructField("emp_num", IntegerType, true),
	StructField("lane", IntegerType, true),
	StructField("t_stmp", StringType, true) ) )

	val ppSchema = StructType(Seq(StructField("seq_num", IntegerType, true),
	StructField("trans_code", StringType, true),
	StructField("promo_cd", StringType, true),
	StructField("store", StringType, true),
	StructField("emp_num", IntegerType, true),
	StructField("lane", IntegerType, true),
	StructField("t_stmp", StringType, true) ) )

	//Apply schema to RDD to generate dataframe
	val ttDf = sqlContext.createDataFrame(TT, ttSchema)
	val llDf = sqlContext.createDataFrame(LL, llSchema)
	val ppDf = sqlContext.createDataFrame(PP, ppSchema)

	//Register dataframes as temporary hive table so that we can query it
	ttDf.registerTempTable("ttStageTable")
	llDf.registerTempTable("llStageTable")
	ppDf.registerTempTable("ppStageTable")

	//Write a UDF to generate transaction id
	
	def getTransId(ts:String, d_store_id:Int, d_lane:Int, d_trans_seq:Int): Long = {
	  val daydate = ts.replaceAll("-","").split(" ")(0)
	  val store_id = "%05d".format(d_store_id)
	  val lane = "%02d".format(d_lane)
	  val trans_seq = "%04d".format(d_trans_seq)
	  (daydate + store_id.toString + lane.toString + trans_seq.toString).toLong
	}

	//Register UDF 
	val transId = getTransId(_:String,_:Int,_:Int,_:Int)
	val tid = sqlContext.udf.register("getTransId", transId)

	//Generate transaction id using UDF registered above
	val new_tt = sqlContext.sql("select getTransId(t_stmp, store_id, lane, seq_num) as txid, product_cd, amt, disc_amt, add_flg, store_id, emp_num, t_stmp from ttStageTable t JOIN  store s on t.store = s.store_num")
	new_tt.registerTempTable("new_tt_tbl")

	//Now we need to delete all the returned items from transactions
	sqlcontext.context("create table ttdelete as select * from new_tt_tbl where add_flg=-1)
	sqlcontext.sql("insert overwrite new_tt_tbl as select * from new_tt_tbl where add_flg=1")

	//User may have bought multiple similer items but returned only few of them hence we generate ranks for each scanned items and based on that we delete those
	
	sqlContext.sql("drop if exists table ttstage; create table ttstage as select seq_num,product_cd ,rank() (over partition by seq_num,product_cd order by ) as rank from new_tt_tbl")
	
	sqlContext.sql("drop if exists table ttstagedelete; create table ttstagedelete as select seq_num,product_cd ,rank() (over partition by seq_num,product_cd order by ) as rank from ttdelete")
	
	sqlContext.sql("insert overwrite table ttstage select stage.* from ttstage stage left join ttstagedelete del on del.seq_num=stage.seq_num and del.product_cd=stage.product_cd and del.rnk=stage.rnk where del.seq_num is null")

	sqlContext.sql("insert overwrite table new_tt_tbl select nt.* from new_tt_tbl nt join ttstage tt on nt.seq_num = tt.seq_num and nt.product_cd = tt.product_cd")
	
	//Join with dimension tables to resolve numbers into ids
	
	
	
	//Finally populate the partitioned target table
	
	
}