import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


cl_columns=['civil_service_title','job_description','minimum_qual_requirements','preferred_skills','additional_information','to_apply','hours/shift']
output_path='/dataset/output/'

def spark_session(app_name="pyspark_utils"):
    """Fixture for creating a spark context."""
    return SparkSession.builder.master("local").appName(app_name).getOrCreate()

def read_file(path: str) -> DataFrame:
    """
    Return the Dataframe after reading a csv file from the given input file location
    :param df: input path
    :return: Dataframe
    """
    spark = spark_session()
    df = spark.read\
    .option("quote", "\"")\
    .option("escape", "\"")\
    .csv(path, header=True,inferSchema = True)
    return df

def write_output(df:DataFrame,output_path:str) -> DataFrame:
    """
    Write the dataframe to the specified output location
    :param df: Input dataframe to be written
    :output path : Output path to write the dataframe
    :return: True
    """
    df.repartition(1).write.mode('overwrite').csv(output_path)

def clean_columns(df: DataFrame) -> DataFrame :
    """
    Clean the column names after removing special characters from column name 
    and converting it to small case.
    :param df: Input dataframe
    :return: Return dataframe with clean names
    """

    df = df.select([f.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    df=df.toDF(*[c.lower() for c in df.columns])
    df = df.withColumnRenamed("#_of_positions", "no_of_positions")
    return df


def remove_duplicates(df: DataFrame) -> DataFrame :
    """
    Return dataframe after removing duplicates from the input dataframe.
    :param : Dataframe
    :return : Dataframe with unique records.
    """
    return df.dropDuplicates()


def clean_str_cols(df: DataFrame,cl_columns:list) -> DataFrame:
    for cols in cl_columns:
        df = df.withColumn(cols, regexp_replace(cols, "[^0-9a-zA-Z_\-/]+", " "))
    return df


def process_source(path:str) -> bool:
    df=read_file(path)
    df=clean_columns(df)
    df=remove_duplicates(df)
    df=clean_str_cols(df,cl_columns)
    write_output(df,output_path/+'cleaned/')