
import json

import pendulum

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def tutorial_taskflow_api():
    """
    ### TaskFlow API
    Esta es una simple data pipeline que demustra el uso de TaskFlow API utilizando tres simples tareas para Extract, Transform, y Load.
    """
    @task()
    def extract():
        """
        #### Extract task
        Obtiene datos para el resto de las tareas.
        En este caso se obtienen datos simulados leyendo de una cadena JSON.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        Una simple tarea de transformación que toma como entrada la colacción de datos y calcula un valor total de la orden.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        #### Load task
        Una simple tarea de carga que toma el resultado de la transformación, y en este caso se imprime a la pantalla.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


tutorial_taskflow_api()
