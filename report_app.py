from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import io
import pandahouse as ph
from airflow.decorators import dag, task

connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20260520'
}

default_args = {
    'owner': 'i_maksutov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 7, 22)
}

@dag(default_args=default_args,
     schedule_interval='0 11 * * *',
     catchup=False)
def dag_report_app_ilgam():

    @task()
    def get_feed_data():
        q = """
        SELECT
            toDate(time) as date,
            count(DISTINCT user_id) as dau_feed,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(countIf(action = 'like') /
                  countIf(action = 'view'), 4) as CTR
        FROM simulator_20260520.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        return ph.read_clickhouse(q, connection=connection).to_dict()

    @task()
    def get_messages_data():
        q = """
        SELECT
            toDate(time) as date,
            count(DISTINCT user_id) as dau_messages,
            count() as messages_sent,
            uniq(receiver_id) as active_receivers
        FROM simulator_20260520.message_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """
        return ph.read_clickhouse(q, connection=connection).to_dict()

    @task()
    def get_app_dau():
        q = """
        SELECT
            date,
            uniq(user_id) as dau_app
        FROM (
            SELECT
                toDate(time) as date,
                user_id
            FROM simulator_20260520.feed_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
            UNION ALL
            SELECT
                toDate(time) as date,
                user_id
            FROM simulator_20260520.message_actions
            WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        )
        GROUP BY date
        ORDER BY date
        """
        return ph.read_clickhouse(q, connection=connection).to_dict()

    @task()
    def send_report(feed_data, messages_data, app_dau_data):
        df_feed = pd.DataFrame(feed_data)
        df_msg = pd.DataFrame(messages_data)
        df_app = pd.DataFrame(app_dau_data)

        # Берём данные за вчера
        yesterday_feed = df_feed.iloc[-1]
        yesterday_msg = df_msg.iloc[-1]
        yesterday_app = df_app.iloc[-1]

        date = yesterday_feed['date']

        msg = f"""
📱 Отчёт по приложению за {date}

👥 АУДИТОРИЯ:
- DAU приложения: {int(yesterday_app['dau_app']):,}
- DAU ленты: {int(yesterday_feed['dau_feed']):,}
- DAU мессенджера: {int(yesterday_msg['dau_messages']):,}

📰 ЛЕНТА:
- Просмотры: {int(yesterday_feed['views']):,}
- Лайки: {int(yesterday_feed['likes']):,}
- CTR: {yesterday_feed['CTR']:.2%}

💬 МЕССЕНДЖЕР:
- Сообщений отправлено: {int(yesterday_msg['messages_sent']):,}
- Активных получателей: {int(yesterday_msg['active_receivers']):,}
        """
        print(msg)

        # Графики
        fig, axes = plt.subplots(2, 3, figsize=(18, 10))
        fig.suptitle(f'Метрики приложения за последние 7 дней',
                     fontsize=16, fontweight='bold')

        sns.set_theme(style='whitegrid')

        # DAU
        axes[0, 0].plot(df_app['date'], df_app['dau_app'],
                        color='steelblue', marker='o', linewidth=2,
                        label='DAU app')
        axes[0, 0].plot(df_feed['date'], df_feed['dau_feed'],
                        color='coral', marker='o', linewidth=2,
                        label='DAU лента')
        axes[0, 0].plot(df_msg['date'], df_msg['dau_messages'],
                        color='green', marker='o', linewidth=2,
                        label='DAU мессенджер')
        axes[0, 0].set_title('DAU по сервисам')
        axes[0, 0].legend(fontsize=8)
        axes[0, 0].tick_params(axis='x', rotation=45)
        axes[0, 0].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        # Просмотры
        axes[0, 1].plot(df_feed['date'], df_feed['views'],
                        color='coral', marker='o', linewidth=2)
        axes[0, 1].set_title('Просмотры (лента)')
        axes[0, 1].tick_params(axis='x', rotation=45)
        axes[0, 1].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        # Лайки
        axes[0, 2].plot(df_feed['date'], df_feed['likes'],
                        color='green', marker='o', linewidth=2)
        axes[0, 2].set_title('Лайки (лента)')
        axes[0, 2].tick_params(axis='x', rotation=45)
        axes[0, 2].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        # CTR
        axes[1, 0].plot(df_feed['date'], df_feed['CTR'],
                        color='purple', marker='o', linewidth=2)
        axes[1, 0].set_title('CTR (лента)')
        axes[1, 0].tick_params(axis='x', rotation=45)
        axes[1, 0].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        # Сообщения
        axes[1, 1].plot(df_msg['date'], df_msg['messages_sent'],
                        color='steelblue', marker='o', linewidth=2)
        axes[1, 1].set_title('Сообщений отправлено')
        axes[1, 1].tick_params(axis='x', rotation=45)
        axes[1, 1].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        # Активные получатели
        axes[1, 2].plot(df_msg['date'], df_msg['active_receivers'],
                        color='orange', marker='o', linewidth=2)
        axes[1, 2].set_title('Активных получателей')
        axes[1, 2].tick_params(axis='x', rotation=45)
        axes[1, 2].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        print("График приложения успешно сохранён")
        plt.close()

    feed_data = get_feed_data()
    messages_data = get_messages_data()
    app_dau_data = get_app_dau()

    send_report(feed_data, messages_data, app_dau_data)

dag_report_app_ilgam = dag_report_app_ilgam()
