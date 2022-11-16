from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,sum,avg,count,unix_timestamp,round
import pandas as pd
import boto3
from io import StringIO
from utilities.AWS_credentials import SECRET_ACCESS_KEY, ACCESS_KEY_ID 
# boto3 connects AWS services with python application
#stringIO provides pythons main facility to provide input facilities

# creating a spark session with mongoDB connectors for data export at the end
try :
    spark = SparkSession.builder.appName ("Analytics project session")\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/mock-project.dataCollection") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/mock-project.dataCollection") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
except Exception as e:
    print (" Exeception occured: " + str(e))


# Reading header file seperately as exported CSV does not have header
try :
    header = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/cleaned_data.csv/headers.csv") 
except Exception as e:
    print (" Exeception occured: " + str(e))


##following is the code to import exported CSV with seperate hearder file.
try:
    csv_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema","true") \
    .load('/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/cleaned_data.csv') \
    .toDF(*header.columns)
except Exception as e:
    print (" Exeception occured: " + str(e))    

## below code reads CSV file which has headers included
# csv_df = spark.read.csv ('final_data.csv',sep = ',', header = True, inferSchema= True)

# csv_df.printSchema()
# csv_df.show(10)

#changing datatype of dataset date columns to timestamp from string
csv_df = csv_df.withColumn('order_purchase_timestamp', csv_df['order_purchase_timestamp'].cast('timestamp'))
csv_df = csv_df.withColumn('order_aproved_at', csv_df['order_aproved_at'].cast('timestamp'))
csv_df = csv_df.withColumn('order_delivered_customer_date', csv_df['order_delivered_customer_date'].cast('timestamp'))

# declaring some functions to export date,year,weekday,week from the timestamp so that we can add seperate columns on it.
# this will help in writing smaller and easier queries.

UDF_year = udf(lambda x: x.isocalendar()[0])
UDF_week = udf(lambda x: x.isocalendar()[1])
UDF_weekday = udf(lambda x: x.isocalendar()[2])
UDF_date = udf(lambda x: x.date().isoformat())

# adding some extra columns based on the data from above UDFs
csv_df = csv_df.withColumn("order_purchase_year", UDF_year(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_week", UDF_week(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_weekday", UDF_weekday(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_date", UDF_date(col("order_purchase_timestamp")).cast('date'))

# csv_df.show(2,vertical=True)
# csv_df.createOrReplaceTempView("ecommerce")

#Daily insight
#Sales
#Query 1, Total sales
total_sales_daily= csv_df.groupBy("order_purchase_date")\
    .agg(round(sum(csv_df.order_items_qty * (csv_df.order_products_value + csv_df.order_freight_value)),2)\
    .alias("Total_sales"))\
    .orderBy("order_purchase_date")    
# total_sales_daily.show(10,vertical=False)

#Query 2, Total sales in each city
total_sales_city_wise_daily=csv_df.groupBy("customer_city","order_purchase_date")\
    .agg( sum ( csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_city","order_purchase_date")
# total_sales_city_wise_daily.show(5)

#Query 3, Total sales in each state
total_sales_state_wise_daily=csv_df.groupBy("customer_state","order_purchase_date")\
    .agg(sum(csv_df.order_items_qty*csv_df.order_products_value+csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_state","order_purchase_date")
# total_sales_state_wise_daily.show(5)

#Orders
#Query 1, Total no of order
total_orders_daily=csv_df.groupBy("order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("order_purchase_date")
# total_orders_daily.show(5)

#Query 2, Total no of order city wise
total_orders_city_wise_daily=csv_df.groupBy("customer_city","order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_city","order_purchase_date")
# total_orders_city_wise_daily.show(5, vertical= True)

#Query 3, Total no of order state wise
total_orders_state_wise_daily = csv_df.groupBy("customer_state","order_purchase_date")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_state","order_purchase_date")
# total_orders_state_wise_daily.show(5)

#Query 4, AVG review_score per order
avg_score_per_order_daily=csv_df.groupBy("order_purchase_date")\
    .agg(avg("review_score")\
    .alias("AVG_review_score"))\
    .orderBy("order_purchase_date")
# avg_score_per_order_daily.show(5)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order_daily=csv_df.groupBy("order_purchase_date")\
    .agg(avg("order_freight_value")\
    .alias("Aavg_freight_charges_per_order"))\
    .orderBy("order_purchase_date")
# avg_freight_charges_per_order_daily.show(5)

#Query 6, AVG time taken to approve the order
avg_time_to_approve_daily=csv_df.groupBy("order_purchase_date")\
    .agg(avg((unix_timestamp(csv_df.order_aproved_at) - unix_timestamp(csv_df.order_purchase_timestamp))/3600)\
    .alias("avg_time_to_approve_in_hour"))\
    .orderBy("order_purchase_date")
# avg_time_to_approve_daily.show(5)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time_daily=csv_df.groupBy("order_purchase_date")\
    .agg(avg((unix_timestamp(csv_df.order_delivered_customer_date) - unix_timestamp(csv_df.order_purchase_timestamp))/24*3600)\
    .alias("avg_order_delivery_time_in_day"))\
    .orderBy("order_purchase_date")
# avg_order_delivery_time_daily.show(5)

#Weekly insight for sales
#Sales
#Query 1, Total sales
total_sales_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_sales_weekly.show(5)

# Query 2, Total sales in each city
total_sales_city_wise_weekly=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
# total_sales_city_wise_weekly.show(5)

#Query 3, Total sales in each state
total_sales_state_wise_weekly=csv_df.groupBy("customer_state","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_items_qty * csv_df.order_products_value + csv_df.order_freight_value)\
    .alias("Total_sales"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_week")
# total_sales_state_wise_weekly.show(5)

#WEEKLY Insight for orders
#Orders
#Query 1, Total no of order
total_orders_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orderss"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_orders_weekly.show(5)

#Query 2, Total no of order city wise
total_orders_city_wise_weekly=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
# total_orders_city_wise_weekly.show(5)

#Query 3, Total no of order state wise
total_orders_state_wise_weekly=csv_df.groupBy("customer_state","order_purchase_year","order_purchase_week")\
    .agg(count("order_items_qty")\
    .alias("Total_orders"))\
    .orderBy("customer_state","order_purchase_year","order_purchase_week")
# total_orders_state_wise_weekly.show(5)

#Query 4, AVG review_score per order
avg_score_per_order_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg("review_score")\
    .alias("AVG_review_score"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_score_per_order_weekly.show(5)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg("order_freight_value")\
    .alias("Aavg_freight_charges_per_order"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_freight_charges_per_order_weekly.show(5)

#Query 6, AVG time taken to approve the order
avg_time_to_approve_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg((unix_timestamp(csv_df.order_aproved_at) - unix_timestamp(csv_df.order_purchase_timestamp))/3600)\
    .alias("avg_time_to_approve_in_hour"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_time_to_approve_weekly.show(5)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(avg((unix_timestamp(csv_df.order_delivered_customer_date) - unix_timestamp(csv_df.order_purchase_timestamp))/24*3600)\
    .alias("avg_order_delivery_time_in_day"))\
    .orderBy("order_purchase_year","order_purchase_week")
# avg_order_delivery_time_weekly.show(5, truncate= False)

#Query c, Total freight charges
total_freight_charges_weekly=csv_df.groupBy("order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_freight_value)\
    .alias("total_freight_charges"))\
    .orderBy("order_purchase_year","order_purchase_week")
# total_freight_charges_weekly.show(5)

#Query d, Total freight charges city wise
total_freight_charges_city_wise_weekly=csv_df.groupBy("customer_city","order_purchase_year","order_purchase_week")\
    .agg(sum(csv_df.order_freight_value)\
    .alias("total_freight_charges"))\
    .orderBy("customer_city","order_purchase_year","order_purchase_week")
total_freight_charges_city_wise_weekly.show(5)

# making dictionary for the queries output where values are spark-dataframes.
df_names = { 'total_sales_daily':total_sales_daily, 
             'total_sales_city_wise_daily':total_sales_city_wise_daily, 
             'total_sales_state_wise_daily':total_sales_state_wise_daily, 
             'total_orders_daily':total_orders_daily, 
             'total_orders_city_wise_daily':total_orders_city_wise_daily, 
             'total_orders_state_wise_daily':total_orders_state_wise_daily, 
             'avg_score_per_order_daily':avg_score_per_order_daily, 
             'avg_freight_charges_per_order_daily':avg_freight_charges_per_order_daily, 
             'avg_time_to_approve_daily':avg_time_to_approve_daily, 
             'avg_order_delivery_time_daily':avg_order_delivery_time_daily,
             'total_sales_weekly':total_sales_weekly, 
             'total_sales_city_wise_weekly':total_sales_city_wise_weekly, 
             'total_sales_state_wise_weekly':total_sales_state_wise_weekly, 
             'total_orders_weekly':total_orders_weekly, 
             'total_orders_city_wise_weekly':total_orders_city_wise_weekly, 
             'total_orders_state_wise_weekly':total_orders_state_wise_weekly, 
             'avg_score_per_order_weekly':avg_score_per_order_weekly, 
             'avg_freight_charges_per_order_weekly':avg_freight_charges_per_order_weekly,
             'avg_time_to_approve_weekly':avg_time_to_approve_weekly, 
             'avg_order_delivery_time_weekly':avg_order_delivery_time_weekly, 
             'total_freight_charges_weekly':total_freight_charges_weekly, 
             'total_freight_charges_city_wise_weekly':total_freight_charges_city_wise_weekly
              }

print(len(df_names))

# #exporting query data to local storage in CSV format
# try:
#     for key,value in df_names.items():
#         value.write.mode('overwrite').csv(path = "/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/Output_CSVs/" + key, header= True)
# except Exception as e:
#     print (" Exception occured: " + str(e))


## exporting query data to S3 in CSV format

# def upload_s3(df,i):
#     s3 = boto3.client("s3",aws_access_key_id = ACCESS_KEY_ID ,aws_secret_access_key = SECRET_ACCESS_KEY)
#     csv_buf = StringIO()
#     df.to_csv(csv_buf, header=True, index=False)
#     csv_buf.seek(0)   
#     s3.put_object(Bucket="rohithya-mockproject", Body=csv_buf.getvalue(), Key='output/'+i)

# try:
#     for key,value in df_names.items():
#         upload_s3(value.toPandas(),str(key)+'.csv')
# except Exception as e:
#     print (" Exception occured: " + str(e))


# Exporting query data to mongo-DB in json format

# try:
#     for key,value in df_names.items():
#         value.write\
#             .format('com.mongodb.spark.sql.DefaultSource')\
#             .option('uri', 'mongodb://127.0.0.1:27017/mock-project-amazon.' + key) \
#             .save()
# except Exception as e:
#     print (" Exception occured: " + str(e))            