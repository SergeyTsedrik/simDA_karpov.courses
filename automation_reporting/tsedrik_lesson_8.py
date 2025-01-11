# pip install telegram
# pip install python-telegram-bot

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# параметры подключения к БД Clickhouse для выгрузки данных
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20240420'
    }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's-tsedrik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 19),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

my_token = '7585901231:AAGB0U7iiG8MrtQbw0OTgaHh4C3eCKiNS74'
bot = telegram.Bot(token= my_token)

chat_id = -938659451

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tsedrik():
    
               
    @task()
    def extract_data():
        query = '''
        SELECT toDate(time) AS event_date,
            COUNT(DISTINCT user_id) DAU, 
            COUNTIf(action = 'view') AS views,
            COUNTIf(action = 'like') AS likes,
            ROUND(COUNTIf(action = 'like') / COUNTIf(action = 'view') *100, 2) AS CTR
        FROM simulator_20241120.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY event_date
        ORDER BY event_date
        '''
        data = ph.read_clickhouse(query, connection=connection)
        return data
    
    
    @task()
    def last_day(data):
        # отправка сообщения
        msg = f"""
        Информация о значениях ключевых метрик за: {data['event_date'].iloc[-1].strftime('%Y-%m-%d')}
        -----------------------------------------------------
        DAU = {data['DAU'].iloc[-1]}
        Количество просмотров = {data['views'].iloc[-1]}
        Количество лайков = {data['likes'].iloc[-1]}
        CTR = {data['CTR'].iloc[-1]}%"""
        return msg
        
        
    @task()
    def last_week(data):
        # Создание фигуры и подграфиков
        fig, axs = plt.subplots(2, 2, figsize=(20, 16))

        # Первый график
        sns.lineplot(data=data, x='event_date', y='DAU', ax=axs[0, 0])
        axs[0, 0].set_title('График DAU за неделю')
        # Поворот меток на оси X
        axs[0, 0].tick_params(axis='x', rotation=45)
        # Добавление сетки
        axs[0, 0].grid(True)
        # Ось х
        axs[0, 0].set_xlabel('Дата')
        # Добавление точек на пересечении значений
        axs[0, 0].scatter(data['event_date'], data['DAU'])  

        # Второй график
        sns.lineplot(data=data, x='event_date', y='CTR', ax=axs[0, 1])
        axs[0, 1].set_title('График CTR за неделю')
        # Поворот меток на оси X
        axs[0, 1].tick_params(axis='x', rotation=45)
        # Добавление сетки
        axs[0, 1].grid(True)
        # Ось х
        axs[0, 1].set_xlabel('Дата')
        # Добавление точек на пересечении значений
        axs[0, 1].scatter(data['event_date'], data['CTR'])

        # Третий график
        sns.lineplot(data=data, x='event_date', y='views', ax=axs[1, 0])
        axs[1, 0].set_title('График просмотров за неделю')
        # Поворот меток на оси X
        axs[1, 0].tick_params(axis='x', rotation=45)
        # Добавление сетки
        axs[1, 0].grid(True)
        # Ось х
        axs[1, 0].set_xlabel('Дата')
        # Добавление точек на пересечении значений
        axs[1, 0].scatter(data['event_date'], data['views'])

        # Четвертый график
        sns.lineplot(data=data, x='event_date', y='likes', ax=axs[1, 1])
        axs[1, 1].set_title('График лайков за неделю')
        # Поворот меток на оси X
        axs[1, 1].tick_params(axis='x', rotation=45)
        # Добавление сетки
        axs[1, 1].grid(True)
        # Ось х
        axs[1, 1].set_xlabel('Дата')
        # Добавление точек на пересечении значений
        axs[1, 1].scatter(data['event_date'], data['likes'])

        plt.tight_layout()
        
        plot_object = io.BytesIO() # создаем файловый объект
        plt.savefig(plot_object )
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        return plot_object
        
    
    @task()
    def send_bot(msg, plot_object):
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    data = extract_data()
    msg = last_day(data)
    plot_object = last_week(data)
    send_bot(msg, plot_object)
    
    
dag_tsedrik = dag_tsedrik()  