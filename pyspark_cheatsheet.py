import pyspark
from pyspark.sql import SparkSession
from pypark.conf import SparkConf
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.functions import count,desc, when, col, substring
from pyspark.sql types import *
from libs_common import *

def shape(self):
	"""
	Prints the number of rows and columns in a spark DataFrame
	"""

	return self.count(), len(self.schema.names)
pyspark.sql.DataFrame.shape = shape


def info(self, show = True):
  '''
  Print concise summary of a pyspark.sql.DataFrame
  This method prints information about a DataFrame
  including the index dtype and columns, non-null values
  
  Args:
    show(bool): prints dataframe
    
  Returns:
    pyspark.sql.DataFrame

  '''
  
  subset = self.schema.names
  total_rows = self.count()
  _non_null = self.select([(total_rows - f.sum(f.when(f.col(col).isNull(),1).otherwise(0))).alias(col) for col in subset])\
    .toPandas().transpose().reset_index().rename(columns={'index':'Column', 0:'Non-Null Count'})
  _non_null = spark.createDataFrame(_non_null)
  _dtype = spark.createDataFrame(self.dtypes).withColumnRenamed('_1','Column').withColumnRenamed('_2','Dtype')
  result = _dtype.join(_non_null, on = 'Column').select('Column', 'Non-Null Count', 'Dtype')
  
  if show:
    return result.show()
  else:
    return result
pyspark.sql.DataFrame.info = info

def value_counts(self, subset, normalize = True, sort = True, ascending = False, show = True):
  '''
  Prints frequencies and proportion of unique value in a DataFrame
  
  Args:
    subset(list): column to be used.
    normalize(bool): returns proportion
    sort(bool): sort by frequencies
    ascending(bool): sort in ascending order.
    
  Return:
    pyspark.sql.DataFrame
  '''
  
  w = Window.partitionBy().rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  
  self = self.groupby(subset).count()
  
  if normalize:
    self = self.withColumn('pct', f.round(f.col('count')/f.sum('count').over(w),4))
  else:
    pass
  
  if sort:
    self = self.sort('count', ascending = ascending)
  else:
    self = self.sort(subset)
    
  if show:
    return self.show()
  else:
    return self
pyspark.sql.DataFrame.value_counts = valueCounts

df_sp = spark.read.parquet("people.parquet")


# query from a datafile
# Parquet files can also be used to create a temporary view and then used in SQL statements.
df_sp.createOrReplaceTempView("parquetFile")
df_query = spark.sql("SELECT name FROM df_sp WHERE age >= 13 AND age <= 19")
df_query.show()

# using query directly from DB
import pyspark
from pyspark.sql imort SparkSession

# DB INFO
db_info = {
	'WP':{
						'ur':'jdbc:oracle:thin:@xx.xxx.xxx.xx:###/WP'
						'user':'P'
						'password':'###'
					 }
	'WW':{
						'ur':'jdbc:oracle:thin:@xx.xxx.xxx.xx:###/WW'
						'user':'W'
						'password':'###'
					 }
}

odjbc_path = '/home/woori/workspace/notebook/ojdbc8.jar'
driver='oracle.jdbc.driver.OracleDriver'

spark = SparkSession \
.builder \
.config('spark.driver.extraClassPath',ojdbc_path) \
.config('spark.executor.extraClassPath',ojdbc_path) \
.config('spark.driver.memory','50g')
.config('spark.executor.cores', '4')
.config('spark.executor.memory', '5g')
.config('spark.app.name', 'dsl')
.getOrCreate()

def read_table(query,db='WDMDP',db_info=db_info, driver=driver, spark=spark, num_partitions=100):
	url =db_info[db]['url']
	user =db_info[db]['user']
	password =db_info[db]['password']

df = spark.read.format('jdbc')\ 
					.option('driver', driver) \ 
					.option('url', url) \ 
					.option('dtable', dtable) \ 
					.option('user', user) \ 
					.option('password', password) \ 
					.option('numPartitions', num_partitions) \
					.load()
	return df 

query = """(
SELECT * FROM TABLENM
)"""

df = read_table(query)
df.show(2)

# Nunique()
from pyspark.sql.functions import countDistinct
df.select(countDistinct("col")).show()

# save to single file
df.repartition(1).write.format(‘parquet’).mode(‘append’).save(‘balance.parquet’)

# to pandas dataframe
df_pd = df.toPandas()

# save as feather
feather.write_feather(df_pd, 'example_feather')

# create date
import datetime
from pyspark.sql.functions import Row

def get_date_df():
   initial_date = datetime.date(2018, 1, 1)
   days = 365
   one_day = datetime.timedelta(days=1)
   all_days = [{"date": initial_date + i * one_day} for i in range(days)]
   return spark.createDataFrame(Row(**x) for x in all_days)

# create dummy date
df = spark.createDataFrame([ (123,1, "2021-01-01")
                            , (123,1, "2021-01-06")
                            , (123,1, "2021-02-10")
                            , (123,0, "2021-02-28")
                            , (777,1, "2021-01-01")
                            , (777,1, "2021-01-01")
			], ["id", "flag", "date"])


# change string to date type
df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))


# time related features
from pyspark.sql.functions import year,month, quarter,weekofyear, dayofweek,date_format, to_date
df = df.withColumn("year", year(df.date))
df = df.withColumn("month", month(df.date))
df = df.withColumn("quarter", quarter(df.date))
df = df.withColumn("day_of_month", date_format(col("date"), "d")
df = df.withColumn("day_of_year", date_format(col("date"), "D")
df = df.withColumn("week_of_year", weekofyear(df.date)) # deprecated in spark3
df = df.withColumn("day_of_year", dayofweek(df.date))
		   
# window functions: rolling + forward
df = f.Window()
		   
# Read from file
# Read from DB
# Query from loaded file

# drop 
df_query=df_query.drop("cust_id", "inst_id", "account_num","seq_num", "currency")

# from pandas dataframe to spark dataframe
df_s = spark.createDataFrame(df)
		   
# concat: 2 methods
df = df.select("*", concat(df.colA, lit("_"), df.colB)).alias("colNew") 
df = df.select("*", concat(col("colA"), lit("_"), col("colB"))).alias("colNew")

# windowing
