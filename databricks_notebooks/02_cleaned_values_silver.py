'''
#Imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

#configuraions

spark.conf.set(
  "fs.azure.account.key.ecommercestoragee.dfs.core.windows.net",
  "Aadl2XvCpB5mjmEm+jW3zDGI0FIBlnBKTKRIAIwam8eBOTd+XBMm9nQx0iFqOErP/OlE13CR6g57+AStP5V1+g=="
)



bronze_path = "abfss://ecommerce@ecommercestoragee.dfs.core.windows.net/bronze"

silver_path = "abfss://ecommerce@ecommercestoragee.dfs.core.windows.net/silver"

#Reading raw data from bronze
df_bronze=(
  spark.readStream
  .format("delta")
  .load(bronze_path)
)

#Clean and enrich
df_clean=(
  df_bronze
  .withColumn("timestamp",to_timestamp("timestamp"))
  .withColumn("price",when(col("price").isNull(),0.0).otherwise(col("price")))
  .withColumn("quantity",when(col("quantity").isNull(),1).otherwise(col("quantity")))
  .withColumn("total_amount",col("price")*col("quantity"))
  .dropDuplicates(["order_id"])
  .filter(col("country")=="USA")
  .filter(col("state").isNotNull())
)
#Write to Silver Layer
(
  df_clean.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation",silver_path+"/_checkpoint")
 .start(silver_path)
 )




'''
