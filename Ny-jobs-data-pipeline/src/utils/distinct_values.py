from pyspark.sql import DataFrame


def get_distinct_values(df: DataFrame, column: str) -> list :
    """
    Return the distinct values for a given column in a dataframe
    :param df: input dataframe
    :param column: column to be used to get the distinct values
    :return: list of string values
    """
    row_list = df.select(column).distinct().collect()
    return [row[column] for row in row_list]
