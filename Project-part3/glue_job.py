from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


prd_feature = spark.read.parquet("s3://imba-kevin0019/features/prd_feature_db/")
up_features = spark.read.parquet("s3://imba-kevin0019/features/up_feature_db")
user_features_1 = spark.read.parquet("s3://imba-kevin0019/features/user_feature_1_db")
user_features_2 = spark.read.parquet("s3://imba-kevin0019/features/user_feature_2_db")

# join everything together
joinDF = ((up_features.join(prd_feature, "product_id")).join(user_features_1, "user_id")).join(user_features_2, "user_id")

singleDF = joinDF.repartition(1)
singleDF.write.mode('overwrite').csv("s3://imba-kevin0019/features/features_join", header = "true")

