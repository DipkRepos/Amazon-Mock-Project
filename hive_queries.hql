set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition=true;
set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

CREATE EXTERNAL TABLE amazon_table 
(
id int,
order_status string,
order_products_value double,
order_freight_value double,
order_items_qty double,
customer_city string,
customer_state string,
customer_zip_code_prefix int,
product_name_length int,
product_description_length int,
product_photos_qty int,
review_score double,
order_purchase_timestamp timestamp,
order_aproved_at timestamp,
order_delivered_customer_date timestamp
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/Storage_HDFS';  

ALTER TABLE amazon_table SET SERDEPROPERTIES ("timestamp.formats"="DD/MM/YY HH:mm");

INSERT OVERWRITE DIRECTORY '/Storage_HDFS_output/cleaned_data.csv' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM amazon_table;

!hadoop fs -get /Storage_HDFS_output/cleaned_data.csv /Storage;
