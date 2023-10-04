import os
os.environ["SPARK_VERSION"] = '3.3'

#Import necessary libraries
from pydeequ.analyzers import Completeness, Size, AnalysisRunner, Distinctness, AnalyzerContext, Compliance , Mean
from pyspark.sql import SparkSession
from pydeequ.verification import VerificationSuite
from pydeequ.checks import Check, CheckLevel
import pydeequ



# Initialize a Spark session
spark = SparkSession.builder \
    .appName("DataQualityCheck") \
    .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
    .getOrCreate()

# Load your dataset into a Spark DataFrame
data = spark.read.csv("data/train.csv", header=True, inferSchema=True)

# Create a VerificationSuite
verification_suite = VerificationSuite(spark)

# Define data quality checks
analysisResult = AnalysisRunner(spark) \
    .onData(data) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("Age")) \
    .addAnalyzer(Completeness("Cabin")) \
    .addAnalyzer(Distinctness("PassengerId")) \
    .addAnalyzer(Mean("Age")) \
    .addAnalyzer(Compliance("Sex","Sex == 'male' or Sex == 'female'")) \
    .addAnalyzer(Completeness("Sex")) \
    .run()

# Run the data quality analysis
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(
    spark, analysisResult)
analysisResult_df.show()


# Stop the SparkSession
spark.stop()
