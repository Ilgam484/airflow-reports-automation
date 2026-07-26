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
def dag_report_feed_ilgam():

    @task()
    def get_data():
        q_yesterday = """
        SELECT
            toDate(time) as date,
            count(DISTINCT user_id) as DAU,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(countIf(action = 'like') /
                  countIf(action = 'view'), 4) as CTR
        FROM simulator_20260520.feed_actions
        WHERE toDate(time) = today() - 1
        GROUP BY date
        """

        q_week = """
        SELECT
            toDate(time) as date,
            count(DISTINCT user_id) as DAU,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(countIf(action = 'like') /
                  countIf(action = 'view'), 4) as CTR
        FROM simulator_20260520.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
        GROUP BY date
        ORDER BY date
        """

        df_yesterday = ph.read_clickhouse(q_yesterday, connection=connection)
        df_week = ph.read_clickhouse(q_week, connection=connection)

        return {'yesterday': df_yesterday.to_dict(),
                'week': df_week.to_dict()}

    @task()
    def send_report(data):
        df_yesterday = pd.DataFrame(data['yesterday'])
        df_week = pd.DataFrame(data['week'])

        date = df_yesterday['date'].iloc[0]
        dau = df_yesterday['DAU'].iloc[0]
        views = df_yesterday['views'].iloc[0]
        likes = df_yesterday['likes'].iloc[0]
        ctr = df_yesterday['CTR'].iloc[0]

        msg = f"""
📊 Отчёт по ленте за {date}

👥 DAU: {dau:,}
👁 Просмотры: {views:,}
❤️ Лайки: {likes:,}
📈 CTR: {ctr:.2%}
        """
        print(msg)

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Метрики ленты за последние 7 дней',
                     fontsize=16, fontweight='bold')

        sns.set_theme(style='whitegrid')

        metrics = [
            ('DAU', 'DAU', axes[0, 0], 'steelblue'),
            ('views', 'Просмотры', axes[0, 1], 'coral'),
            ('likes', 'Лайки', axes[1, 0], 'green'),
            ('CTR', 'CTR', axes[1, 1], 'purple'),
        ]

        for col, title, ax, color in metrics:
            ax.plot(df_week['date'], df_week[col],
                    color=color, marker='o', linewidth=2)
            ax.set_title(title, fontsize=12)
            ax.tick_params(axis='x', rotation=45)
            ax.xaxis.set_major_formatter(
                mdates.DateFormatter('%d.%m'))

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150,
                    bbox_inches='tight')
        buf.seek(0)
        print("График успешно сохранён")
        plt.close()

    data = get_data()
    send_report(data)

dag_report_feed_ilgam = dag_report_feed_ilgam()
