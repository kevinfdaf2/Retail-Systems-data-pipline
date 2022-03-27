import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



def main():
    prd_feature = spark.read.parquet("s3://imba-kevin0019/features/prd_feature_db/")
    up_features = spark.read.parquet("s3://imba-kevin0019/features/up_feature_db")
    user_features_1 = spark.read.parquet("s3://imba-kevin0019/features/user_feature_1_db")
    user_features_2 = spark.read.parquet("s3://imba-kevin0019/features/user_feature_2_db")
    
    # join everything together
    joinDF = ((up_features.join(prd_feature, "product_id")).join(user_features_1, "user_id")).join(user_features_2, "user_id")
    
    singleDF = joinDF.repartition(1)
    singleDF.write.csv("s3://imba-kevin0019/features/features_join", header = "true")

    
if __name__ == '__main__':
    main()
