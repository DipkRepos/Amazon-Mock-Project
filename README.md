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

 

## Commands
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
 id int,order_status string,
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
