# Amazon-Mock-Project

## Devlopers : Rohithya, Nikhil, Deepak, Aniket

## Mentor: Senthilanathan Kalyanasundaram

## Manager/Client: Namdam Karthik

## EPIC:

 We work for an e-commerce company as a prominent data consultant. Our job entails analyzing sales data. The company operates at a number of locations around the world. They want us to analyze the data from their daily and weekly sales transactions and derive significant insights to understand their sales in various cities and states. we've also been asked to include a variety of other details (that are provided below) about the product evaluation.

## STORIES:

- Setting up AWS S3 Bucket
  - Create AWS s3 bucket to load the given sales data csv file into it
- Setting up the Hive environment
  - Install Hive locally to load the data
- Defining Schema for hive table
  - Create a table with specific schema so that we can load csv file data into the table without any issue
- Setting up the HDFS environment
  - Install Hadoop ecosystem for HDFS setup to store the csv file data
- Setting up the Spark environment
  - Install pyspark to run queries on sales data
- Setting up the NOSQL DB environment
  - Set up NOSQL DB(MongoDB) locally to save the queries output
- Loading csv file into S3 bucket
  - Upload the csv file into the AWS S3 bucket
- Loading csv file into Hive
  - Upload the csv file into the Hive table which we have already created
- Loading hive table into HDFS
  - Export the hive table into HDFS by creating a new directory
- Querying data city wise
  - Total sales and order distribution per day and week for each city
- Querying data state wise
  - Total sales and order distribution per day and week for each state
- Querying data for average reports
  - Average review score, average freight value, average order approval, and delivery time
- Querying data for freight charges
  - The freight charges per city and total freight charges
- Defining document structure for NOSQL DB
  - Define query output document structure for NOSQL DB
- Converting query output into the required format
  - Convert the query output into the document structure defined above and save it locally
- Exporting query output to NOSQL DB
  - Loading query results into NOSQL DB after changing the format for insights
- Exporting query output to HDFS
  - Loading query results into HDFS for insights
- Exporting query output to S3
  - Loading query results into S3 for easy access

## Procedure to run the project

## Commands (Snowflake)

```
create or replace file format mycsvformat
  type = 'CSV'
  field_delimiter = ','
  skip_header = 0;
  
create or replace stage project_stage
  file_format = mycsvformat 
  url='s3://mock-project-bucket/olist_public_dataset.csv';

show stages;

drop stage my_csv_stage; //droping unnecesssary stages

show stages;

list @project_stage;

create or replace table amazon_data_table 
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
 order_purchase_timestamp date_part(<date_or_time_part>, <date_or_time_expr>),
 order_aproved_at date_part(<date_or_time_part>, <date_or_time_expr>),
 order_delivered_customer_date date_part(<date_or_time_part>, <date_or_time_expr>) 
); 

copy into amazon_data_table from @project_stage 
pattern='.*.csv' 
file_format = (type = csv field_delimiter = ',' skip_header = 0);

copy into amazon_data_table
  from @project_stage/olist_public_dataset.csv
  on_error = 'skip_file';


select * from amazon_data_table;
```

# Setting up local environment ( Docker, Hive, HDFS, SPARK )

DOCKER

```
docker pull cloudera/quickstart:latest

docker images

docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8080:50070 -p 8081:50075 -p 8020:8020 -p 9000:9000 -v /Users/_charjan/Desktop/Training/Mock_project/Amazon-Mock-Project/data:/Storage 4239cd /usr/bin/docker-quickstart

docker exec -it ed sh                                
```

HDFS

```
hadoop fs -mkdir /Storage_HDFS
hadoop fs -copyFromLocal Storage/olist_public_dataset.csv /Storage_HDFS/
```

HIVE

```
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
```

SPARK

```
Download and install any IDE such as PyCharm community edition or Visual Studio Code 
Create a folder where you will like to store the project files
Open the folder using any of the IDE that you installed

PyCharm has an advantage of creating its virtual environment on its own.
For VS Code, we need to create a virtual environment by following the below commands:
python3 -m venv mock-p
source mock-p/bin/activate
This below command is used to install pyspark in both the IDE’s
pip install pyspark

I would better suggest using pycharm because it will create a virtual environment on its own.
```

[Results of city-wise queries](https://docs.google.com/document/d/1cAs5sppIqbSO3OZIO7Z-aUHW9mwMmtCMGS4FbYxkyhA/edit#)

[Results of state-wise queries](https://docs.google.com/document/d/1EkTXpiRELX4A6mU1Sc6Y43wPwDEEx4MhTS1Qgt2pEKc/edit)
