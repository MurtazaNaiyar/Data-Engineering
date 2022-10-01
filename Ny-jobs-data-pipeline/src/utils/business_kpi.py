import re
import os
import sys
import json
import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from utils.utilities import spark_session
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import explode
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StringType,DoubleType


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

def get_degree(minimum_Qual):
    if not minimum_Qual:
        return "Others"
    elif minimum_Qual.lower().find('degree') == -1:
        return "Others"
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
    result_df = result.filter(~result["degree"].isin(['a','s'])).groupBy("degree")\
                      .agg(min((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Min_Salary")\
                           ,max((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Max_Salary")\
                           ,avg((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("Avg_Salary"))
    return result_df


def get_top_job_posting_salary_agency(df: DataFrame) -> DataFrame :
    """
    Return the top n job postings
    :param df: input dataframe
    :return: dataframe values
    """
    windowSpec  = Window.partitionBy("agency").orderBy(((col("salary_range_from").\
                                                         cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).desc())
    result = df.withColumn("row_number",row_number().over(windowSpec)).filter("row_number=1").\
    select("agency","job_id","business_title","salary_range_from","salary_range_to",((col("salary_range_from").cast(DoubleType())+col("salary_range_to").cast(DoubleType()))/2).alias("avg_salary"))
    return result

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


def get_highest_paid_skill(df:DataFrame) -> str:     
    """
    Return the Highest paid Skills from the dataframe.
    :param df : Input Dataframe
    :return str: Return the result string with details of Hoghest Paid Skills
    """
    highest_paid_skills=df.withColumn('avg_salary', (f.col('salary_range_from') + f.col('salary_range_to')) / 2).\
    withColumn("annual_salary" , f.expr("CASE WHEN `salary_frequency` = 'Daily'  THEN avg_salary * 365\
                                              WHEN `salary_frequency` = 'Hourly' THEN  avg_salary * 365 *24\
                                         ELSE avg_salary END")).select('preferred_skills').distinct().\
    orderBy('avg_salary',ascending=False).limit(1)
    df_highest_paid_skills = highest_paid_skills.toJSON().map(json.loads).collect()
    df_highest_paid_skills = pd.DataFrame(df_highest_paid_skills)
    text = df_highest_paid_skills['preferred_skills'][0]
    results =" ".join(text.split())
    return f"The highest paid skills in the US market is: {results}"
