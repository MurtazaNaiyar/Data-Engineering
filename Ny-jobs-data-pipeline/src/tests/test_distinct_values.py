from utils.distinct_values import get_distinct_values
from utils.utilities import spark_session

def test_get_distinct_values():
    spark = spark_session()
    mock_data: list = [('X', 'Annual'), ('Y', 'Daily'),('Z', 'Daily'),('V', 'Yearly')]
    expected_result: list = ['Annual', 'Daily','Yearly']
    schema: list = ['A', 'B']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    assert get_distinct_values(df=mock_df, column='B') == expected_result
