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


def get_avg_salary_agency(df: DataFrame, current_Date: bool) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :param date: input date (Ture,False)
    :return: dataframe values
    """
    if current_Date:
        date = "current_date()"
    else:
        date = df.selectExpr("max(to_date(`posting_date`,'yyyy-MM-dd')) as date").collect()[0]['date']

    result = df.where(f"to_date(`posting_date`,'yyyy-MM-dd')>=add_months('{date}',-24)").\
    groupBy("agency").\
    agg(avg("salary_range_from").alias("avg_salary_range_from"),avg("salary_range_to").alias("avg_salary_range_to"),avg((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("avg_Salary")).\
    select("agency","avg_salary_range_from","avg_salary_range_to","avg_salary")
    
    return result

def process_avg_salary_agency():
    df=process_source("/dataset/nyc-jobs.csv")
    result = get_avg_salary_agency(df,False)
    write_output(result,output_path+'avg_salary_agency/')
    
if __name__ == "__main__":
    process_avg_salary_agency()