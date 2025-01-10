from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import pandahouse as ph
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# параметры подключения к БД Clickhouse для выгрузки данных
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20240420'
}

# параметры подключения к БД для загрузки в базу test
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database': 'test',
                   'user': 'student-rw',
                   'password': '656e2b0c9c'}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's-tsedrik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 19),
}

# Интервал запуска DAG
schedule_interval = '0 16 * * *'

# создам dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def tsedrik_dag():

# создал запрос из таблицы feed_actions
    @task()
    def extract_feed():
        query = '''SELECT toDate(time) as event_date,
                    user_id,
                    os,
                    gender,
                    age,
                    countIf(action = 'view') AS views,
                    countIf(action = 'like') AS likes
                FROM simulator_20241120.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY event_date,user_id,os,gender,age'''
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed
 
    #извлекаем данные из таблицы сообщений
    @task()
    def extract_message():
        query = '''SELECT s.*,
                    r.messages_received,
                    r.users_received
                FROM(
                    SELECT
                      toDate(time) as event_date,
                      user_id,
                      gender,
                      os,
                      age,
                      COUNT(receiver_id) AS messages_sent,
                      COUNT(DISTINCT receiver_id) AS users_sent
                    FROM
                      simulator_20241120.message_actions
                    WHERE
                      event_date = yesterday()
                    GROUP BY
                      event_date,
                      user_id,
                      gender,
                      os,
                      age) AS s
                LEFT JOIN (
                    SELECT
                      toDate(time) as event_date,
                      receiver_id AS user_id,
                      COUNT(user_id) AS messages_received,
                      COUNT(DISTINCT user_id) AS users_received
                    FROM
                      simulator_20241120.message_actions
                    WHERE
                      event_date = yesterday()
                    GROUP BY
                      event_date,
                      user_id) AS r 
                    using user_id'''
  
        df_message = ph.read_clickhouse(query, connection=connection)
        return df_message
    
    # объединяем таблицы
    @task()
    def general_table(df_feed, df_message):
        df = df_feed.merge(df_message, how='outer', on = ['event_date', 'user_id', 'os', 'gender', 'age']).fillna(0)
        return df
        
    # посчитаем по платформам
    @task()
    def transform_os(df):
        df_os = (
        df[['event_date', 'os','views', 'likes', 'messages_sent', 'users_sent','messages_received', 'users_received']]
        .groupby(['event_date', 'os']).sum().reset_index())
        df_os.insert(1, 'dimension', 'os')
        df_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        return df_os
    
    # посчитаем по полу
    @task()
    def transform_gender(df):
        df_gender = (
        df[['event_date', 'gender','views', 'likes', 'messages_sent', 'users_sent','messages_received', 'users_received']]
        .groupby(['event_date', 'gender']).sum().reset_index())
        df_gender.insert(1, 'dimension', 'gender')
        df_gender.rename(columns={'os': 'dimension_value'}, inplace=True)
        return df_gender
    
    # посчитаем по возрасту
    @task()
    def transform_age(df):
        df_age = (
        df[['event_date', 'age','views', 'likes', 'messages_sent', 'users_sent','messages_received', 'users_received']]
        .groupby(['event_date', 'age']).sum().reset_index())
        df_age.insert(1, 'dimension', 'age')
        df_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        return df_age
    
    @task()
    def unity(df_os, df_gender, df_age):
        df_total = pd.concat([df_os, df_gender, df_age], axis= 0)

        df_total = df_total.astype({
            'views': 'int',
            'likes': 'int',
            'messages_received': 'int',
            'messages_sent': 'int',
            'users_received': 'int',
            'users_sent': 'int'})

        # Указываем желаемый порядок столбцов
        new_order = ['event_date', 
                     'dimension', 
                     'dimension_value', 
                     'views', 
                     'likes', 
                     'messages_received', 
                     'messages_sent', 
                     'users_received', 
                     'users_sent']

        df_total = df_total[new_order]
        return df_total
    
    
    #выгружаем таблицу
    @task()
    def load(df_total):
        query_test = '''CREATE TABLE IF NOT EXISTS test.tsedrik
                    (event_date Date,
                    dimension String,
                    dimension_value String,    
                    views Int64,         
                    likes Int64,         
                    messages_received Int64,         
                    messages_sent Int64,         
                    users_received Int64,         
                    users_sent Int64)
                    ENGINE = MergeTree()
                    ORDER BY event_date
                    '''
        ph.execute(query_test, connection=connection_test)
        ph.to_clickhouse(df=df_total, table="tsedrik", index=False, connection=connection_test)

    
    df_feed = extract_feed()
    df_message = extract_message()
    df = general_table(df_feed, df_message)
    df_os = transform_os(df)
    df_gender = transform_gender(df)
    df_age = transform_age(df)
    df_total = unity(df_os, df_gender, df_age)
    load(df_total)
    
tsedrik_dag = tsedrik_dag()
