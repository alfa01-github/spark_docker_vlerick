# PREREQUISITES FOR DOCKER?
RUN python -m pip install -r requirementsfile.txt

# SECTION ONE OF THE ASSIGNMENT
import os
print(os.environ["AWS_ACCESS_KEY_ID"])

from pyspark import SparkConf
from pyspark.sql import SparkSession

BUCKET = "dmacademy-course-assets"
KEYafter = "vlerick/after_release.csv"
KEYpre = "vlerick/pre_release.csv"

config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

dfpre = spark.read.csv(f"s3a://{BUCKET}/{KEYpre}", header=True)
dfpre.show()
dfafter = spark.read.csv(f"s3a://{BUCKET}/{KEYafter}", header=True)
dfafter.show()

# SECTION TWO OF THE ASSIGNMENT

import pandas as pd 

pre = dfpre.toPandas()
after = dfafter.toPandas()

# SECTION FIVE OF THE ASSIGNMENT


