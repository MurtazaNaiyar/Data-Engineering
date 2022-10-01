import re
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType,DoubleType
from pyspark.sql.functions import explode
from pyspark.sql.functions import countDistinct
from utils import process_source,write_output

output_path='/dataset/output/'

def get_top_n_job_posting(df: DataFrame, n: int) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :param n: n to be used to get the top n values
    :return: dataframe values
    """
    df1 = df.groupby("job_category").agg(countDistinct("job_id").alias("number_of_jobs"),
                                     sum("no_of_positions").alias("no_of_positions"))
    windowSpec  = Window.orderBy(col("number_of_jobs").desc(),col("no_of_positions").desc())
    result = df1.withColumn("row_number",row_number().over(windowSpec)).repartition(4).filter("row_number<="+str(n))
    return result

def process_op_n_job_posting():
    df=process_source("/dataset/nyc-jobs.csv")
    result = get_top_n_job_posting(df,10).drop("row_number")
    write_output(result,output_path+'top_n_job_posting/')

if __name__ == "__main__":
    process_op_n_job_posting()