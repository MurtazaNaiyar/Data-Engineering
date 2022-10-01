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


def get_avg_salary_per_category_per_frequency(df: DataFrame) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :return: dataframe values
    """
    result = df.groupBy("job_category","salary_frequency")\
               .agg(f.avg((col("salary_range_from").cast(DoubleType())+col("salary_range_to").\
                         cast(DoubleType()))/2).alias("avg_salary")
                    ).select("job_category","salary_frequency","avg_salary")
    
    return result


def process_avg_salary_per_category_per_frequency():
    df=process_source("/dataset/nyc-jobs.csv")
    result = get_avg_salary_per_category_per_frequency(df)
    write_output(result,output_path+'salary_per_category_per_frequency/')
    
if __name__ == "__main__":
    process_avg_salary_per_category_per_frequency()
