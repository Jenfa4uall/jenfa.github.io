from bs4 import BeautifulSoup
import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

#Getting the page html
url = 'https://en.wikipedia.org/wiki/List_of_largest_consumer_markets'
response = requests.get(url)

# defining the database engine
username = 'postgres'
password = '*****' 
host = 'localhost'
database = 'ml'
port = 5432
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

#Extracting the data from the html page
def extract():    
#scrapping the website
    print('scrapping the website has started')
    soup = BeautifulSoup(response.text,'html')
# find the needed table(consumer markets of the world)
    print('locating the consumer markets of the world')
    consumer_markets = soup.find_all('table')[1]

    print('getting the consumer market table header')
    table_header=consumer_markets.find_all('th')
#storing the headers to a dataframe
    print('cleaning the table header tag and storing in a dataframe')
    header =[row.text.strip() for row in table_header]
    title= pd.DataFrame(columns= header)
# droping some column list off
    print('droping off column not needed')
    df =title.drop(['World','48,793,177','58%','2018'], axis= 1)

# getting the table data
    print('Table header extraction completed')
    print('table records being extracted')
    table_row=consumer_markets.find_all('td')
    record =[row.text.strip() for row in table_row]
#The record output came out as sigle list
# The values are put on separate list at every 4th jump
    rows = [record[i:i + 4] for i in range(0, len(record), 4)]

#appending the header and the rows
    print('Appending of header and rows ongoing')
    for row in rows:
        df.loc[len(df)] = row
    
    return df

#Storing the data in a dataframe
df =extract()

# Transforming the data by renaming of columns
def transform():
    new_colums = ['Country', 'Consmptn_Expend','%_of_GDP','Year']
    df.columns =new_colums
    return df


#loading the data to the database
def load(df):
    df.to_sql('consumption_expenditure', con=engine,if_exists='replace', index=False)
    print('data loaded to the database')


## Creating DAGs arguments
default_args ={
    'owner': 'Me',
    'start_date': days_ago(0),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Defining the DAGs
dag = DAG(
    dag_id='Scraping',
    default_args=default_args,
    description='Web Scraping Household Consumption Data',
    schedule_interval=timedelta(days=1),
)

#Defining the first task
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

#defining the second task
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

#Defining the third task
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

#Pipeline
execute_extract >> execute_transform >> execute_load