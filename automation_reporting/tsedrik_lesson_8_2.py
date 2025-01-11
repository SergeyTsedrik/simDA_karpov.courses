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

my_token = '7981000615:AAEucWIbE5qfgmLfE5az_Wq0myVAcGADi7g'
bot = telegram.Bot(token= my_token)
chat_id =  -938659451

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tsedrik_2():
    
    # извлекаем только пользователей ленты
    @task()
    def only_action_data():
        query= '''SELECT COUNT(DISTINCT user_id) AS user
        FROM simulator_20241120.feed_actions
        WHERE user_id NOT IN (SELECT user_id FROM simulator_20241120.message_actions)'''
        only_action = ph.read_clickhouse(query, connection=connection)
        return only_action
    
    # извлекаем только пользователей месседжера
    @task()
    def only_message_data():
        query= '''SELECT COUNT(DISTINCT user_id) AS user
        FROM simulator_20241120.message_actions
        WHERE user_id NOT IN (SELECT user_id FROM simulator_20241120.feed_actions)'''
        only_message = ph.read_clickhouse(query, connection=connection)
        return only_message
    
    # извлекаем пользователей которые пользуются двумя сервисами
    @task()
    def all_service_data():
        query= '''SELECT COUNT(DISTINCT user_id) AS user
        FROM simulator_20241120.feed_actions AS f
        JOIN simulator_20241120.message_actions AS m ON f.user_id = m.user_id'''
        all_service = ph.read_clickhouse(query, connection=connection)
        return all_service
    
    # извлекаем всех пользователей 
    @task()
    def all_users_data():
        query= '''SELECT COUNT(DISTINCT user_id) AS user
        FROM simulator_20241120.feed_actions AS f
        LEFT JOIN simulator_20241120.message_actions AS m ON f.user_id = m.user_id'''
        all_users = ph.read_clickhouse(query, connection=connection)
        return all_users
    
    # отправляем сообщение боту
    @task()
    def last_day(only_action, only_message, all_service, all_users):
        # отправка сообщения
        msg = f"""Статистика пользователей на вчерашний день:
        Пользователи, использующие только ленту: {only_action['user'].loc[0]}
        Пользователи, использующие только месседжер: {only_message['user'].loc[0]}
        Пользователи, использующие оба сервиса: {all_service['user'].loc[0]}
        Общее количество пользователей: {all_users['user'].loc[0]}"""
        return msg
        
     # извлекаем MAU пользователей 
    @task()
    def mau_data():
        query= '''SELECT 
            f.month, 
            f.users_act, 
            m.users_mess
        FROM (
            SELECT 
                EXTRACT(MONTH FROM toDate(time)) AS month,
                COUNT(DISTINCT user_id) AS users_act
            FROM simulator_20241120.feed_actions
            GROUP BY month
            ORDER BY month
        ) AS f
        LEFT JOIN (
            SELECT 
                EXTRACT(MONTH FROM toDate(time)) AS month,
                COUNT(DISTINCT user_id) AS users_mess
            FROM simulator_20241120.message_actions
            GROUP BY month
            ORDER BY month
        ) AS m 
        ON f.month = m.month
        '''
        mau = ph.read_clickhouse(query, connection=connection)
        return mau
        
    # строю графики MAU    
    @task()
    def mau_graph(mau):
        plt.figure(figsize=(16, 8))

        # Первый график для MAU пользователей ленты
        sns.lineplot(data=mau, x='month', y='users_act', label='MAU пользователей ленты', color='blue')
        plt.scatter(mau['month'], mau['users_act'], color='blue')

        # Второй график для MAU пользователей месседжера
        sns.lineplot(data=mau, x='month', y='users_mess', label='MAU пользователей месседжера', color='orange')
        plt.scatter(mau['month'], mau['users_mess'], color='orange')

        plt.title('График MAU пользователей ленты и месседжера')
        plt.xlabel('Месяц')
        plt.ylabel('Количество пользователей')
        plt.grid(True)
        plt.legend()        
        # создал файловый объект
        mau_object = io.BytesIO() 
        plt.savefig(mau_object)
        mau_object.seek(0)
        mau_object.name = 'MAU.png'
        plt.close()
        return mau_object
        
    # воронка конверсии по платформам: просмотры → лайки → сообщения.    
    @task()
    def conversion_os():
        query='''SELECT event_date, 
            os, 
            views, 
            likes, 
            count_letter, 
            CTR,
            (count_letter / likes) AS CR
        FROM (
            SELECT 
                toDate(time) AS event_date,
                os, 
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                countIf(action = 'like') / countIf(action = 'view') AS CTR
            FROM simulator_20241120.feed_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY event_date, os
        ) AS f
        JOIN (
            SELECT 
                toDate(time) AS event_date, 
                os,
                COUNT(receiver_id) AS count_letter
            FROM simulator_20241120.message_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY event_date, os
        ) AS m ON f.event_date = m.event_date AND f.os = m.os
        ORDER BY f.event_date DESC, f.os'''
        cr_os = ph.read_clickhouse(query, connection=connection)
        return cr_os
    
    # Создал фигуры и график    
    @task()
    def conversion_os_graph(cr_os):
        fig, axs = plt.subplots(2, 2, figsize=(20, 16))

        # Первый график
        sns.barplot(data=cr_os, x=cr_os['event_date'].dt.strftime('%Y-%m-%d'), y='views', hue='os', ax=axs[0, 0])
        axs[0, 0].set_title('График просмотров за неделю')
        axs[0, 0].tick_params(axis='x', rotation=45)
        axs[0, 0].grid(True)
        axs[0, 0].set_xlabel('Дата')

        # Второй график
        sns.barplot(data=cr_os, x=cr_os['event_date'].dt.strftime('%Y-%m-%d'), y='likes', hue='os', ax=axs[0, 1])
        axs[0, 1].set_title('График лайков за неделю')
        axs[0, 1].tick_params(axis='x', rotation=45)
        axs[0, 1].grid(True)
        axs[0, 1].set_xlabel('Дата')

        # Третий график
        sns.barplot(data=cr_os, x=cr_os['event_date'].dt.strftime('%Y-%m-%d'), y='count_letter', hue='os', ax=axs[1, 0])
        axs[1, 0].set_title('График отправленных сообщений за неделю')
        axs[1, 0].tick_params(axis='x', rotation=45)
        axs[1, 0].grid(True)
        axs[1, 0].set_xlabel('Дата')

        # Четвертый график
        sns.lineplot(data=cr_os, x='event_date', y='CTR', hue='os', ax=axs[1, 1])
        axs[1, 1].set_title('График CTR за неделю')
        axs[1, 1].tick_params(axis='x', rotation=45)
        axs[1, 1].grid(True)
        axs[1, 1].set_xlabel('Дата')

        # Общий заголовок
        fig.suptitle('Основные метрики в разрезе платформ', fontsize=20)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()
        # создал файловый объект
        os_object = io.BytesIO() 
        plt.savefig(os_object)
        os_object.seek(0)
        os_object.name = 'os_object.png'
        plt.close()
        return os_object
        
    # воронка конверсии по трафику: просмотры → лайки → сообщения.    
    @task()
    def conversion_source():
        query='''SELECT event_date, 
            source, 
            views, 
            likes, 
            count_letter, 
            CTR,
            (count_letter / likes) AS CR
        FROM (
            SELECT 
                toDate(time) AS event_date,
                source, 
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                countIf(action = 'like') / countIf(action = 'view') AS CTR
            FROM simulator_20241120.feed_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY event_date, source
        ) AS f
        JOIN (
            SELECT 
                toDate(time) AS event_date, 
                source,
                COUNT(receiver_id) AS count_letter
            FROM simulator_20241120.message_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            GROUP BY event_date, source
        ) AS m ON f.event_date = m.event_date AND f.source = m.source
        ORDER BY f.event_date DESC, f.source'''
        cr_source = ph.read_clickhouse(query, connection=connection)
        return cr_source
    
    @task()
    def conversion_source_graph(cr_source):
        # Создал фигуры и график
        fig, axs = plt.subplots(2, 2, figsize=(20, 16))

        # Первый график
        sns.barplot(data=cr_source, x=cr_source['event_date'].dt.strftime('%Y-%m-%d'), y='views', hue='source', ax=axs[0, 0])
        axs[0, 0].set_title('График просмотров за неделю')
        axs[0, 0].tick_params(axis='x', rotation=45)
        axs[0, 0].grid(True)
        axs[0, 0].set_xlabel('Дата')

        # Второй график
        sns.barplot(data=cr_source, x=cr_source['event_date'].dt.strftime('%Y-%m-%d'), y='likes', hue='source', ax=axs[0, 1])
        axs[0, 1].set_title('График лайков за неделю')
        axs[0, 1].tick_params(axis='x', rotation=45)
        axs[0, 1].grid(True)
        axs[0, 1].set_xlabel('Дата')

        # Третий график
        sns.barplot(data=cr_source, x=cr_source['event_date'].dt.strftime('%Y-%m-%d'), y='count_letter', hue='source', ax=axs[1, 0])
        axs[1, 0].set_title('График отправленных сообщений за неделю')
        axs[1, 0].tick_params(axis='x', rotation=45)
        axs[1, 0].grid(True)
        axs[1, 0].set_xlabel('Дата')

        # Четвертый график
        sns.lineplot(data=cr_source, x='event_date', y='CTR', hue='source', ax=axs[1, 1])
        axs[1, 1].set_title('График CTR за неделю')
        axs[1, 1].tick_params(axis='x', rotation=45)
        axs[1, 1].grid(True)
        axs[1, 1].set_xlabel('Дата')
        
        fig.suptitle('Основные метрики в разрезе платформ', fontsize=20)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()
        # создал файловый объект
        source_object = io.BytesIO() 
        plt.savefig(source_object)
        source_object.seek(0)
        source_object.name = 'source_object.png'
        plt.close()
        return source_object
    
    # отправим боту
    @task()
    def send_bot(msg, mau_object, os_object, source_object):
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=mau_object)
        bot.sendPhoto(chat_id=chat_id, photo=os_object)
        bot.sendPhoto(chat_id=chat_id, photo=source_object)
            
    only_action = only_action_data()
    only_message = only_message_data()
    all_service = all_service_data()
    all_users = all_users_data()
    msg = last_day(only_action, only_message, all_service, all_users) 
    mau = mau_data()
    mau_object = mau_graph(mau) 
    cr_os = conversion_os()
    os_object = conversion_os_graph(cr_os)
    cr_source = conversion_source()
    source_object = conversion_source_graph(cr_source)
    send_bot(msg, mau_object, os_object, source_object)

    
dag_tsedrik_2 = dag_tsedrik_2()