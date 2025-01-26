from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Defina a função que será executada como tarefa
def minha_tarefa():
    print("Olá, Airflow! Agendamento funcionando!")

# Crie o DAG
with DAG(
    dag_id='meu_dag_agendado',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Exemplo de DAG agendado no Airflow',
    schedule_interval='0 9 * * *',  # Cron: Todos os dias às 9h
    start_date=datetime(2025, 1, 1),  # Data inicial
    catchup=False,  # Não executa tarefas anteriores à data inicial
) as dag:
    
    # Defina uma tarefa
    tarefa = PythonOperator(
        task_id='minha_primeira_tarefa',
        python_callable=minha_tarefa,
    )
