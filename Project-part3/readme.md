# **Project part 3**

## work flow
Business Requirement:

Factors that effect user behaviour to be created (Assume from DS team)
1. user (user_feature_1, user_feature_2)
2. product (prd_features)
3. preference = user + product (up_features)

High levels in Solution:
![](./part3-work-flow.jpeg)

## AWS Glue DataBrew 
Data Preporcessing by Glue DataBrew. AWS Glue DataBrew is a visual data preparation tool that enables users to clean and normalize data without writing any code. DataBrew is serverless, can explore and transform terabytes of raw data without needing to create clusters or manage any infrastructure.

## 1. Create dataset
![](/Project_part3_v3/assets/create-dataset.png)

## 2. Create Projects with recipe (step function)
![](/Project_part3_v3/assets/create-projects.png)

## 3. Create jobs to load in S3 
![](/Project_part3_v3/assets/create-jobs.png)

## AWS glue development endpoint
For ETL scripts with outputs see [pySpark.ipynb](/Project_part3_v3/pySpark.ipynb)

```py
# Create data frames from the source tables 
prd_feature = spark.read.parquet("s3://data-lake-bucket-imba/features/prd_feature_db")
prd_feature.show(10)
prd_feature.printSchema()
print("Count: ", prd_feature.count())

up_features = spark.read.parquet("s3://data-lake-bucket-imba/features/up_features_db")
up_features.show(10)
up_features.printSchema()
print("Count: ", up_features.count())

user_features_1 = spark.read.parquet("s3://data-lake-bucket-imba/features/user_feature1_db")
user_features_1.show(10)
user_features_1.printSchema()
print("Count: ", user_features_1.count())

user_features_2 = spark.read.parquet("s3://data-lake-bucket-imba/features/user_features_2_db")
user_features_2.show(10)
user_features_2.printSchema()
print("Count: ", user_features_2.count())

# Join the data frames 
joinDF = ((up_features.join(prd_feature, "product_id")).join(user_features_1, "user_id")).join(user_features_2, "user_id")
joinDF.show(10)
joinDF.printSchema()
print("Count: ", joinDF.count())

# Write out a single file to S3 directory "finaltable"
singleDF = joinDF.repartition(1)
singleDF.write.csv("s3://data-lake-bucket-imba/features/finaltable", header = "true")
```
