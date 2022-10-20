# Amazon-Mock-Project

## Devlopers : Rohitya, Nikhil, Deepak, Aniket 
## Mentor: Senthilanathan Kalyanasundaram
## Manager/Client: Namdam Karthik


## EPIC:
 You work for an e-commerce company as a prominent data consultant. Your job entails analyzing sales data. The company operates at a number of locations around the world. They want you to analyze the data from their daily and weekly sales transactions and derive significant insights to understand their sales in various cities and states. You've also been asked to include a variety of other details (that are provided below) about the product evaluation.

## STORIES: 

- Setting up the environment for the group & Configure the necessary dependencies.
    - Creating GitHub repo and cloning it into everyone’s local system, 
    - Setting up  AWS for S3 buckets, EMR, dynamo-DB and other services, 
    - Installing and setting up Spark IDLE/Shell to write the SQL queries,
    - Logging into putty to set up the Hadoop cluster.
    - Import necessary packages in the different environments mentioned above.

- Data Ingestion:
    - Create a bucket (for example S3 and Azure Blob) and upload the CSV file
    - Load the data from the bucket into the Hive table
    - Create a new directory in HDFS and copy the data from Hive into HDFS.
    - Check if the data has been successfully loaded in the HDFS path
 
- Writing Queries:
    - Total sales and order distribution per day and week for each city
    - Total sales and order distribution per day and week for each state.
    - Average review score, average freight value, average order approval, and delivery time.
    - The freight charges per city and total freight charges.

- Data analysis and visualization:
    - Write the results into HDFS (refer to QUE)
    - Save the final dataset into object storage service per the cloud platform
    - Create a DB cluster that is also a NoSQL using the relevant service on the cloud platform
    - Save insights in the NoSQL DB mentioned in the previous step
 

## Questions:
- Why do we need putty if we're using AWS EMR?
- Is the data diff from CSV for streaming?
- Why do we need to put results into HDFS and then into object storage like S3?
















t




