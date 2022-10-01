from utils.utilities import spark_session
from utils.distinct_values import get_distinct_values
from utils.business_kpi import get_avg_salary_agency
from utils.business_kpi import get_avg_salary_per_degree
from utils.business_kpi import get_top_job_posting_salary_agency
from utils.business_kpi import get_top_n_job_posting
from utils.business_kpi import get_avg_salary_per_category_per_frequency



def test_get_avg_salary_agency():
    spark = spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    mock_data: list = [('abc','2022-06-24 00:00:00',200,300), ('def','2022-04-25 00:00:00',300,400)]
    schema: list = ['agency', 'posting_date','salary_range_from','salary_range_to']
    expected_schema: list=['agency','avg_salary_range_from','avg_salary_range_to','avg_salary']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df = get_avg_salary_agency(mock_df,False)
    assert out_df.count() == 2
    assert out_df.count() > 0
    assert out_df.count() < 5
    assert out_df.columns == expected_schema

def test_get_avg_salary_per_degree():
    spark = spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    mock_data: list = [('Masters Degree',200,300), ('Bachelors Degree',300,400)]
    expected_schema: list = ['degree', 'Min_Salary','Max_Salary','Avg_Salary']
    schema: list=['minimum_qual_requirements','salary_range_from','salary_range_to']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df = get_avg_salary_per_degree(mock_df)
    assert out_df.count() == 2
    assert out_df.columns == expected_schema


def test_get_top_job_posting_salary_agency():
    spark = spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    mock_data: list = [('agency_abc',123,'inspector',25463,39873), 
    ('agency_abc',123,'inspector',25463,28873),('agency_abc',345,'IG',34463,39873),
    ('agency_def',673,'SP',32124,35682),('agency_def',546,'inspector',25463,39873)]
    schema: list = ['agency','job_id','business_title','salary_range_from','salary_range_to']
    expected_schema: list=['agency','job_id','business_title','salary_range_from','salary_range_to','avg_salary']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df = get_top_job_posting_salary_agency(mock_df)
    assert out_df.count() == 2
    assert out_df.columns == expected_schema
    assert len(out_df.columns) == 6


def test_get_top_n_job_posting():
    spark = spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    mock_data: list = [('IAS',123,30), 
    ('IPS',123,34),('IPS',345,54),
    ('IAS',673,23),('IPS',546,75)]
    schema: list = ['job_category','job_id','no_of_positions']
    expected_schema: list=['job_category','number_of_jobs','no_of_positions']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df = get_top_n_job_posting(mock_df,10)
    out_df=out_df.drop("row_number")
    out_cols=out_df.columns
    assert out_df.count()  <= 10
    assert out_df.columns == expected_schema
    assert len(out_df.columns) == 3


def test_get_avg_salary_per_category_per_frequency():
    spark = spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    mock_data: list = [('IAS','Yearly',96000,120000),('IAS','Yearly',65778,80000), 
    ('IAS','Monthly',7000,9000),('IAS','Weekly',2500,2800),
    ('IPS','Monthly',8543,9200),('IPS','Weekly',4300,5600),('IPS','Weekly',4500,5600)]
    schema: list = ['job_category','salary_frequency','salary_range_from','salary_range_to']
    expected_schema: list=['job_category','salary_frequency','avg_salary']
    expected_result: list = ['Monthly','Yearly','Weekly']
    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    out_df = get_avg_salary_per_category_per_frequency(mock_df)
    assert get_distinct_values(df=out_df, column='salary_frequency') == expected_result
    assert out_df.count()  == 5
    assert out_df.columns == expected_schema
    assert len(out_df.columns) == 3
