# pip install telegram
# pip install python-telegram-bot
# импорт необходимых библиотек для работы с Telegram 
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

# Параметры подключения к БД Clickhouse для выгрузки данных
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
    'start_date': datetime(2024, 6, 27),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'
# токен бота Telegram для отправки сообщений
my_token = '7278742467:AAFHVHoCuWg2QoAmictv4p2tNZRxEeGTKK0'
bot = telegram.Bot(token=my_token)
chat_id = 704216283


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tsedrik_3():
    # декоратор для выгрузки данных из таблицы feed_actions
    @task()
    def load_feed():
        query = '''
        SELECT toStartOfFifteenMinutes(time) AS ts,
               toDate(time) AS date,
               formatDateTime(ts, '%R') AS hm,
               uniqExact(user_id) AS users_feed,
               countIf(action = 'view') AS views,
               countIf(action = 'like') AS likes
        FROM simulator_20241120.feed_actions
        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts DESC
        '''
        feed = ph.read_clickhouse(query, connection=connection)
        return feed
    # декоратор для выгрузки данных из таблицы message_actions
    @task()
    def load_message():
        query = '''
        SELECT toStartOfFifteenMinutes(time) AS ts,
               toDate(time) AS date,
               formatDateTime(ts, '%R') AS hm,
               uniqExact(user_id) AS users_mess,
               count(receiver_id) AS mess
        FROM simulator_20241120.message_actions
        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts DESC
        '''
        message = ph.read_clickhouse(query, connection=connection)
        return message
    # декоратор для выявления аномалий в ленте приложения
    @task()
    def check_and_run_alert_feed(feed, a=3, n=5):
        metrics_list = ['users_feed', 'views', 'likes']
        for metric in metrics_list:
            df = feed[['ts', 'date', 'hm', metric]].copy()
            # вычисление квартилей и интерквартильного размаха (IQR)
            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
            df['iqr'] = df['q75'] - df['q25']
            # определение границ для аномалий
            df['up'] = df['q75'] + a * df['iqr']
            df['low'] = df['q25'] - a * df['iqr']
            # применение скользящего среднего к границам аномалий
            df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
            df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
            
            # проверка на наличие аномалий в текущем значении метрики
            if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
                msg = f"Метрика {metric}:\nТекущее значение: {df[metric].iloc[-1]:.2f}\nОтклонение: {abs(1 - df[metric].iloc[-1] / df[metric].iloc[-2]):.2%}"
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()
                sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                sns.lineplot(x=df['ts'], y=df['up'], label='upper bound')
                sns.lineplot(x=df['ts'], y=df['low'], label='lower bound')
                plt.xticks(rotation=45)
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f"{metric}.png"
                plt.close()
                # отправка сообщения в Telegram-чат с информацией об аномалии
                bot.sendMessage(chat_id=chat_id, text=msg)
                # отправка графика в Telegram-чат
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    # декоратор для выявления аномалий в месседжере
    @task()
    def check_and_run_alert_message(message, a=3, n=5):
        metrics_list = ['users_mess', 'mess']
        for metric in metrics_list:
            df = message[['ts', 'date', 'hm', metric]].copy()
            # вычисление квартилей и интерквартильного размаха (IQR)
            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
            df['iqr'] = df['q75'] - df['q25']
            # определение границ для аномалий
            df['up'] = df['q75'] + a * df['iqr']
            df['low'] = df['q25'] - a * df['iqr']
            # применение скользящего среднего к границам аномалий
            df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
            df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

            # проверка на наличие аномалий в текущем значении метрики
            if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
                msg = f"Метрика {metric}:\nТекущее значение: {df[metric].iloc[-1]:.2f}\nОтклонение: {abs(1 - df[metric].iloc[-1] / df[metric].iloc[-2]):.2%}"
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()
                sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                sns.lineplot(x=df['ts'], y=df['up'], label='upper bound')
                sns.lineplot(x=df['ts'], y=df['low'], label='lower bound')
                plt.xticks(rotation=45)
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f"{metric}.png"
                plt.close()
                # отправка сообщения в Telegram-чат с информацией об аномалии
                bot.sendMessage(chat_id=chat_id, text=msg)
                # отправка графика в Telegram-чат
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    feed = load_feed()
    message = load_message()
    check_and_run_alert_feed(feed, a=3, n=5)
    check_and_run_alert_message(message, a=3, n=5)

dag_tsedrik_3 = dag_tsedrik_3()
