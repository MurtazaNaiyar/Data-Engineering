from utils.utilities import remove_duplicates, read_file
from utils.utilities import spark_session



def test_remove_duplicates():
    spark = spark_session()
    mock_data: list = [('X', 'Annual'), ('X', 'Annual')]
    schema: list = ['A', 'B']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df=remove_duplicates(df=mock_df)
    assert out_df.count() == 1

# def test_read_file():
#     path="/dataset/cleaned/"
#     out_df=read_file(path)
#     assert out_df.count()>1
#     assert type(out_df) == 'pyspark.sql.dataframe.DataFrame'
