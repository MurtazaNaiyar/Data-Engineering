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


def get_degree(minimum_Qual):
    if not minimum_Qual:
        return "NotFound"
    elif minimum_Qual.lower().find('degree') == -1:
        return "NotFound"
    else:
        m = re.findall("[^\s]+(?=\sdegree)", minimum_Qual.lower())
        return ','.join(m)
get_degree = udf(get_degree,StringType())

def get_avg_salary_per_degree(df: DataFrame) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :return: dataframe values
    """
    result = df.select(get_degree("minimum_qual_requirements").alias('degree'),"salary_range_from","salary_range_to")\
               .withColumn("degree", explode(split(col("degree"), ",")))
    result_df = result.filter(~result["degree"].isin(['NotFound','baccalaureate'])).groupBy("degree")\
                      .agg(min((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Min_Salary")\
                           ,max((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Max_Salary")\
                           ,avg((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Avg_Salary"))
    return result_df

def process_avg_salary_per_degree():
    df=process_source("/dataset/nyc-jobs.csv")
    result = get_avg_salary_per_degree(df)
    write_output(result,output_path+'avg_salary_per_degree/')
    
if __name__ == "__main__":
    process_avg_salary_per_degree()