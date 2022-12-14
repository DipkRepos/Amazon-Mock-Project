from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col 

spark = SparkSession.builder.appName (" Analytics project session").getOrCreate()

#this method to put data in spark is giving error

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

UDF_year = udf(lambda ts: ts.isocalendar()[0])
UDF_week = udf(lambda ts: ts.isocalendar()[1])
UDF_weekday = udf(lambda ts: ts.isocalendar()[2])
UDF_date = udf(lambda ts: ts.date().isoformat())

csv_df = csv_df.withColumn("order_purchase_year", UDF_year(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_week", UDF_week(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_weekday", UDF_weekday(col("order_purchase_timestamp")))
csv_df = csv_df.withColumn("order_purchase_date", UDF_date(col("order_purchase_timestamp")).cast('date'))

# csv_df.show(2,vertical=True)

csv_df.createOrReplaceTempView("ecommerce")

# spark.sql("select * from ecommerce").show(2,vertical=True)

# Query 1: total sales
total_sales="select order_purchase_year as year,order_purchase_date as date,\
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by order_purchase_year,order_purchase_date\
    order by order_purchase_year,order_purchase_date"
spark.sql(total_sales).show(10,vertical=False)

#quey 2, Total sales in each city
total_sales_city_wise="select customer_city as city,order_purchase_year as year,order_purchase_date as date,\
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by customer_city,order_purchase_year,order_purchase_date\
    order by order_purchase_year,order_purchase_date"
# spark.sql(total_sales_city_wise).show(20,vertical = False)

#quey 3, Total sales in each state
total_sales_state_wise="select customer_state as state,order_purchase_year as year,order_purchase_date as date, \
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by customer_state,order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
# spark.sql(total_sales_state_wise).show(2,vertical=True)

#Orders
#Query 1, Total no of order
total_orders="select order_purchase_year as year,order_purchase_date as date, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
# spark.sql(total_orders).show(2,vertical=True)

#Query 2, Total no of order city wise
total_orders_city_wise="select customer_city as city,order_purchase_year as year,order_purchase_date as date, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by customer_city,order_purchase_year,order_purchase_date \
    order by customer_city,order_purchase_year,order_purchase_date"
# spark.sql(total_orders_city_wise).show(20,vertical=False)

#Query 3, Total no of order state wise
total_orders_state_wise="select customer_state as state,order_purchase_year as year,order_purchase_date as date, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by customer_state,order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
# spark.sql(total_orders_state_wise).show(2,vertical=True)

#Query 4, AVG review_score per order
avg_score_per_order="select order_purchase_year as year,order_purchase_date as date, \
    AVG(review_score) as AVG_review_score\
    from ecommerce group by order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
# spark.sql(avg_score_per_order).show(10,vertical=True)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order="select order_purchase_year as year,order_purchase_date as date, \
    AVG(order_freight_value) as avg_freight_charges_per_order\
    from ecommerce group by order_purchase_year,order_purchase_date\
    order by order_purchase_year,order_purchase_date"
# spark.sql(avg_freight_charges_per_order).show(2,vertical=True)

#Query 6, AVG time taken to approve the order
avg_time_to_approve="select order_purchase_year as year,order_purchase_date, \
    AVG(TIMESTAMPDIFF(HOUR,order_purchase_timestamp,order_aproved_at)) as avg_time_to_approve_in_hour\
    from ecommerce group by order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
# spark.sql(avg_time_to_approve).show(2,vertical=True)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time="select order_purchase_year as year,order_purchase_date, \
    AVG(TIMESTAMPDIFF(DAY,order_purchase_timestamp,order_delivered_customer_date)) as avg_order_delivery_time_in_day\
    from ecommerce group by order_purchase_year,order_purchase_date \
    order by order_purchase_year,order_purchase_date"
#spark.sql(avg_order_delivery_time).show(2,vertical=True)

#Weekly insight
#quey 1, Total sales
total_sales="select order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_sales).show(10,vertical=True)

#quey 2, Total sales in each city
total_sales_city_wise="select customer_city as city,order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by customer_city,order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_sales_city_wise).show(2,vertical=True)

#quey 3, Total sales in each state
total_sales_state_wise="select customer_state as state,order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty*(order_products_value+order_freight_value)) as Total_sale\
    from ecommerce group by customer_state,order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_sales_state_wise).show(2,vertical=True)

#WEEKLY Insight for orders
#Orders (one row in dataset is one order)
#Query 1, Total no of order
total_orders="select order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_orders).show(10,vertical=False) 

#Query 2, Total no of order city wise
total_orders_city_wise="select customer_city as city,order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by customer_city,order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_orders_city_wise).show(10,vertical=False)

#Query 3, Total no of order state wise
total_orders_state_wise="select customer_state as state,order_purchase_year as year,order_purchase_week as week, \
    SUM(order_items_qty) as Total_order\
    from ecommerce group by customer_state,order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_orders_state_wise).show(10,vertical=True)

#Query 4, AVG review_score per order
avg_score_per_order="select order_purchase_year as year,order_purchase_week as week, \
    AVG(review_score) as AVG_review_score\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(avg_score_per_order).show(10,vertical=True)

#Query 5, AVG freight charges per order
avg_freight_charges_per_order="select order_purchase_year as year,order_purchase_week as week, \
    AVG(order_freight_value) as avg_freight_charges_per_order\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(avg_freight_charges_per_order).show(10,vertical=True)

#Query 6, AVG time taken to approve the order
avg_time_to_approve="select order_purchase_year as year,order_purchase_week as week, \
    AVG(TIMESTAMPDIFF(HOUR,order_purchase_timestamp,order_aproved_at)) as avg_time_to_approve_in_hour\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(avg_time_to_approve).show(10,vertical=True)

#Query 7, AVG time taken to deliver the order
avg_order_delivery_time="select order_purchase_year as year,order_purchase_week as week, \
    AVG(TIMESTAMPDIFF(DAY,order_purchase_timestamp,order_delivered_customer_date)) as avg_order_delivery_time_in_day\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(avg_order_delivery_time).show(10,vertical=True)

#Query c, Total freight charges
total_freight_charges="select order_purchase_year as year,order_purchase_week as week, \
    SUM(order_freight_value) as Total_freight_charges\
    from ecommerce group by order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_freight_charges).show(10,vertical=True)

#Query d, Total freight charges city wise
total_freight_charges_city_wise="select customer_city as city,order_purchase_year as year,order_purchase_week as week, \
    SUM(order_freight_value) as Total_freight_charges\
    from ecommerce group by customer_city,order_purchase_year,order_purchase_week \
    order by order_purchase_year,order_purchase_week"
# spark.sql(total_freight_charges_city_wise).show(10,vertical=True)

#exporting spark dataframe to json format
# csv_df.coalesce(1).write.format('json').save('/Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data/output.json')