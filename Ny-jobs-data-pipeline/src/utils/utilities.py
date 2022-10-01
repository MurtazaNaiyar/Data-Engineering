import os 
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


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
    df.coalesce(1).write.csv(output_path,mode='overwrite',header=True)

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
    :param df: Dataframe
    :return df: Dataframe with unique records.
    """
    return df.dropDuplicates()

def clean_str_cols(df: DataFrame,cl_columns:list) -> DataFrame:
    """
    Return dataframe with list of columns to be cleaned for special characters
    :param df: Dataframe to be cleaned with
    :Param list: List of columns to clean for special characters 
    :return df : Return the dataframe after cleaning the list of columns
    with special characters
    """
    for cols in cl_columns:
        df = df.withColumn(cols, regexp_replace(cols, "[^0-9a-zA-Z_\-/]+", " "))
    return df

def remove_nulls(df: DataFrame, column: str) -> DataFrame :
    """
    Return dataframe after dropping null rows for a specified column
    :param df: input dataframe
    :param column: column to be used for subset
    :return: Cleaned dataframe
   """
    clean_df = df.na.drop(subset=[column])
    return clean_df