'''
#Library
from pyspark.types import *
from pyspark.sql.functions import *

#Schema of dataset 

schema = StructType([
    StructField('order_id', StringType()),
    StructField('timestamp', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('category', StringType()),
    StructField('price', DoubleType()),
    StructField('quantity', IntegerType()),
    StructField('total_amount', DoubleType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('country', StringType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType()),
    StructField('delivery_status', StringType()),
])


# Azure Event Hub Configuration
event_hub_namespace = "ecommerce-namespacee.servicebus.windows.net"
event_hub_name = "ecommerce-orders"
event_hub_conn_str = "Endpoint=sb://ecommerce-namespacee.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=3dACFKl5IwhGimA5h+eV/B59Qdr/c1OS++AEhKVH39w=

eh_conf = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Stream from Event Hub
df_raw = (
    spark.readStream
    .format('kafka')
    .options(**eh_conf)
    .load()
)

#Parse JSON from kafka stream
df_orders = (
    df_raw.selectExpr("Cast(value AS STRING) as json")
    .select(from_json("json",schema).alias("data"))
    .select("data.*")
)

spark.conf.set(
  "fs.azure.account.key.ecommercestoragee.dfs.core.windows.net",
  "Aadl2XvCpB5mjmEm+jW3zDGI0FIBlnBKTKRIAIwam8eBOTd+XBMm9nQx0iFqOErP/OlE13CR6g57+AStP5V1+g=="
)


#bronze_path = "abfss://ecommerce@ecommercestoragee.dfs.core.windows.net/bronze"

(
    df_orders.writeStream
    .format('delta')
    .outputMode("append")
    .option("checkpointLocation",bronze_path+"/_checkpoint")
    .start(bronze_path)
)
'''
