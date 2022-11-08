from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col 
from pyspark.sql.functions import udf,col,sum,avg,count,unix_timestamp

spark = SparkSession.builder.appName ("Analytics project session").getOrCreate()

#this method to put data in spark is giving errors (all columns inferred as string, though inferschema is true)
header = spark.read \
  .format("csv") \
  .option("header", "true") \
  .load("/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/cleaned_data.csv/headers.csv") 

# csv_df = spark.read.csv ('final_data.csv',sep = ',', header = True, inferSchema= True)
csv_df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema","true") \
  .load('/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/cleaned_data.csv') \
  .toDF(*header.columns)
  
# csv_df.printSchema()
# csv_df.show(10)

#changing datatype of dataset to timestamp from string
csv_df = csv_df.withColumn('order_purchase_timestamp', csv_df['order_purchase_timestamp'].cast('timestamp'))
csv_df = csv_df.withColumn('order_aproved_at', csv_df['order_aproved_at'].cast('timestamp'))
csv_df = csv_df.withColumn('order_delivered_customer_date', csv_df['order_delivered_customer_date'].cast('timestamp'))

UDF_year = udf(lambda x: x.isocalendar()[0])
UDF_week = udf(lambda x: x.isocalendar()[1])
UDF_weekday = udf(lambda x: x.isocalendar()[2])
UDF_date = udf(lambda x: x.date().isoformat())

csv_df = csv_df.withColumn("order_purchase_year", UDF_year(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_week", UDF_week(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_weekday", UDF_weekday(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_date", UDF_date(col("order_purchase_timestamp")).cast('date'))

# csv_df.show(2,vertical=True)

# csv_df.createOrReplaceTempView("ecommerce")


total_sales = csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(sum(csv_df.order_items_qty * (csv_df.order_products_value + csv_df.order_freight_value))\
    .alias("Total_sales"))\
    .orderBy("order_purchase_year","order_purchase_date")    
total_sales.show(10,vertical=False)

# quey 2, Total sales in each city
total_sales_city_wise=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_date")\
    .agg( sum ( csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_date")
# total_sales_city_wise.show(5)

#quey 3, Total sales in each state
total_sales_state_wise=csv_df.groupBy("customer_state","order_purchase_year","order_purchase_date")\
    .agg(sum(csv_df.order_items_qty*csv_df.order_products_value+csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_date")
# total_sales_state_wise.show(5)

#Orders
#Query 1, Total no of order
total_orders=csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orderss"))\
    .orderBy("order_purchase_year","order_purchase_date")
# total_orders.show(5)

#Query 2, Total no of order city wise
total_orders_city_wise=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_date")
# total_orders_city_wise.show(5, vertical= True)

#Query 3, Total no of order state wise
total_orders_state_wise = csv_df.groupBy("customer_state","order_purchase_year","order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_date")
# total_orders_state_wise.show(5)

#Query 4, AVG review_score per order
avg_score_per_order=csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(avg("review_score")\
    .alias("AVG_review_score"))\
    .orderBy("order_purchase_year","order_purchase_date")
# avg_score_per_order.show(5)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order=csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(avg("order_freight_value")\
    .alias("Aavg_freight_charges_per_order"))\
    .orderBy("order_purchase_year","order_purchase_date")
# avg_freight_charges_per_order.show(5)

#Query 6, AVG time taken to approve the order
avg_time_to_approve=csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(avg((unix_timestamp(csv_df.order_aproved_at) - unix_timestamp(csv_df.order_purchase_timestamp))/3600)\
    .alias("avg_time_to_approve_in_hour"))\
    .orderBy("order_purchase_year","order_purchase_date")
# avg_time_to_approve.show(5)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time=csv_df.groupBy("order_purchase_year","order_purchase_date")\
    .agg(avg((unix_timestamp(csv_df.order_delivered_customer_date) - unix_timestamp(csv_df.order_purchase_timestamp))/24*3600)\
    .alias("avg_order_delivery_time_in_day"))\
    .orderBy("order_purchase_year","order_purchase_date")
# avg_order_delivery_time.show(5)

#Weekly insight
#quey 1, Total sales
total_sales=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_sales.show(5)

# quey 2, Total sales in each city
total_sales_city_wise=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
# total_sales_city_wise.show(5)

#quey 3, Total sales in each state
total_sales_state_wise=csv_df.groupBy("customer_state","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_week")
# total_sales_state_wise.show(5)

#WEEKLY Insight for orders
#Orders
#Query 1, Total no of order
total_orders=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orderss"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_orders.show(5)

#Query 2, Total no of order city wise
total_orders_city_wise=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
# total_orders_city_wise.show(5)

#Query 3, Total no of order state wise
total_orders_state_wise=csv_df.groupBy("customer_state","order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_week")
# total_orders_state_wise.show(5)

#Query 4, AVG review_score per order
avg_score_per_order=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg("review_score")\
    .alias("AVG_review_score"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_score_per_order.show(5)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg("order_freight_value")\
    .alias("Aavg_freight_charges_per_order"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_freight_charges_per_order.show(5)

#Query 6, AVG time taken to approve the order
avg_time_to_approve=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg((unix_timestamp(csv_df.order_aproved_at) - unix_timestamp(csv_df.order_purchase_timestamp))/3600)\
    .alias("avg_time_to_approve_in_hour"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_time_to_approve.show(5)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg((unix_timestamp(csv_df.order_delivered_customer_date) - unix_timestamp(csv_df.order_purchase_timestamp))/24*3600)\
    .alias("avg_order_delivery_time_in_day"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_order_delivery_time.show(5, truncate= False)

#Query c, Total freight charges
total_freight_charges=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_freight_value)\
    .alias("total_freight_charges"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_freight_charges.show(5)

#Query d, Total freight charges city wise
total_freight_charges_city_wise=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_freight_value)\
    .alias("total_freight_charges"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
# total_freight_charges_city_wise.show(5)



#  Exporting data from Queries to NO SQLDB

# csv_df.write\
#     .format('com.mongodb.spark.sql.DefaultSource')\
#     .mode("append")\
#     .option( "uri", "mongodb+srv://user:password@cluster1.s5tuva0.mongodb.net/my_database.my_collection?retryWrites=true&w=majority") \
#     .save()


# write files (spark df) to amazon s3

# csv_df.write.json("s3a://bucket-name/amazon-mock-project",mode="overwrite")