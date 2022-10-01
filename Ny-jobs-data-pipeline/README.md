# Coding Challenge

This Code assessment represents a coding challenge for Data Engineering roles.

## Purpose

- Evaluate your coding abilities and your software engineering skills
- Judge your technical experience
- Understand how you design the solution 
- Have a technical conversation after the code submission & review is done

## Challenge 

In this challenge, we will use a dataset having the current job posting hosted by the City of New york which is available on the City of New York's official jobs site (https://www1.nyc.gov/jobs/index.page).

- Internal postings availble to city employees and external postings availbale to the public are included. 
- Data is accesible in the CSV file [nyc-jobs.csv](https://github.com/projectforyou/project1/blob/main/dataset/nyc-jobs.csv)

This challenge is composed of following steps: 

### Data Exploration

- Provide a detailed analysis of source data: Column values (eg: Numerical vs character), categorical columns, etc. 
- List of KPIs to be resolved:
  - What's the number of jobs posting per category (Top 10)? 
  - What's the salary distribution per job category? 
  - Is there any correlation between the higher degree and the salary?
  - What's the job posting having the highest salary per agency? 
  - What's the job postings average salary per agency for the last 2 years? 
  - What are the highest paid skills in the US market? 

### Data Processing
 
- Create functions to process your dataset (Cleaning, column pre-processing, data wrangling, transformation etc) 
- Apply atleast 3 feature engineering techniques 
- Features removal based on the exploration/ profiling.
- Store your processed data into a target file

## Expectations

- Above mentioned challenge
- Test cases : you need to create/run your own test cases for every functions you build.
- Code Comments 
- If any deployment to be done, please document the proposals of the deployment steps in [MyDocument.md](https://github.com/projectforyou/project1/blob/main/MyDocument.md)
- If you had to trigger your code, please suggest your approach in [MyDocument.md](https://github.com/projectforyou/project1/blob/main/MyDocument.md)

## Coding Instructions

- PySpark should be used
- Feel free to use any libraries (you can use pip install, if needed)
- Use a visualization library to present your analysis results.
- Any learnings/ challenges/ considerations/ assumptions, please document in [MyDocument.md](https://github.com/projectforyou/project1/blob/main/MyDocument.md)

## Technical Support:

1. Use your own Github account to fork this repository.
2. Follow the installation instructions in [INSTALL.md](https://github.com/projectforyou/project1/blob/main/INSTALL.md)
3. Open the Jupyter notebook home page, using the link similar to the one highlighted in the picture 

  ![jupyter-notebook](https://github.com/projectforyou/project1/blob/main/pictures/docker-compose.png)

4. Open the notebook (assesment_notebook.ipynb). 
5. Run all the cells
6. After creating spark session, you should be able to see an application **pyspark-assesment** running in the Spark Master UI as below. 

  ![spark-application](https://github.com/projectforyou/project1/blob/main/pictures/spark-application.png)

By now you would already have every setup needed in your own machine :

- Spark cluster with a master and 2 worker nodes.
- Notebook to start working, with the basic libraries imported (Please feel free to import any new library, if required). 
- Dataset read that is required for your activity.
- Sample function within the notebook arriving at a basic KPI. [distinct_values.py](https://github.com/projectforyou/project1/blob/main/src/utils/distinct_values.py)
- Sample test case for testing the function. [test_distinct_values.py](https://github.com/projectforyou/project1/blob/main/src/tests/test_distinct_values.py)
- Runnable test cases using '**make test**'

## Submission:
 
Once the activity is completed:
1. Commit/ push your code into your own Git. 
2. DONT try to raise a Pull Request to this repo please.
3. Make your GitHub repo as private. (GitHub -> Settings -> General -> Danger Zone -> Change repository visibility -> Change Visibility -> Make Private)
4. Share the access projectforyou20@gmail.com (GitHub -> Settings -> Collaborators -> Manage Access -> Add People -> Add the email address projectforyou20@gmail.com)
5. Share the GitHub repository URL with hiring team.

We will review your work and get back to you!

# Happy Coding!! 
