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

def get_top_job_posting_salary_agency(df: DataFrame) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :return: dataframe values
    """
    windowSpec  = Window.partitionBy("agency").orderBy(((col("salary_range_from").\
                                                         cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).desc())
    result = df.withColumn("row_number",row_number().over(windowSpec)).filter("row_number=1").select("agency","job_id","business_title","salary_range_from","salary_range_to",((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("avg_salary"))
    return result


def process_avg_salary_agency():
    df=process_source("/dataset/nyc-jobs.csv")
    result = get_top_job_posting_salary_agency(df)
    write_output(result,output_path+'job_posting_salary_agency/')
    
if __name__ == "__main__":
    process_avg_salary_agency()