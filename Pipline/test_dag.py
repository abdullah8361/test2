from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='P3',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    with TaskGroup('product_name_quality_check') as product_name_quality_check_group:
        product_name_quality_check_task = BigQueryInsertJobOperator(
            task_id='product_name_quality_check_task',
            configuration={
                "query": {
                    "query": """
                    WITH ProductNameQualityCheck AS (
                        SELECT 
                            CASE 
                                WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Fail'
                                ELSE 'Pass'
                            END AS ProductNameCheckResult
                        FROM 
                            products
                    )
                    SELECT * FROM ProductNameQualityCheck
                    """,
                    "useLegacySql": False,
                }
            },
        )

    with TaskGroup('ranked_products') as ranked_products_group:
        ranked_products_task = BigQueryInsertJobOperator(
            task_id='ranked_products_task',
            configuration={
                "query": {
                    "query": """
                    WITH RankedProducts AS (
                        SELECT 
                            product_id,
                            product_name,
                            total_quantity_sold,
                            ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS rank
                        FROM 
                            SalesData
                    )
                    SELECT 
                        product_id,
                        product_name,
                        total_quantity_sold
                    FROM 
                        RankedProducts
                    WHERE 
                        rank <= 5
                    """,
                    "useLegacySql": False,
                }
            },
        )

    product_name_quality_check_task >> ranked_products_task