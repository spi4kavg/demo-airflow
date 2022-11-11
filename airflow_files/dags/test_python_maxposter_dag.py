import datetime
import logging
import os
import time

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "test", "start_date": datetime.datetime(2018, 11, 1), "provide_context": True}


def extract_and_load_brands():
    response = requests.get("https://api.maxposter.ru/partners-api/directories/vehicle-brands")
    df = pd.DataFrame.from_dict(response.json()["data"]["vehicleBrands"])
    df.to_csv("files/brands.csv")


def extract_and_load_categories():
    response = requests.get("https://api.maxposter.ru/partners-api/directories/vehicle-categories")
    df = pd.DataFrame.from_dict(response.json()["data"]["vehicleCategories"])
    df.to_csv("files/categories.csv")


def extract_and_load_models():
    response = requests.get("https://api.maxposter.ru/partners-api/directories/vehicle-models")
    df = pd.DataFrame.from_dict(response.json()["data"]["vehicleModels"])
    df.to_csv("files/models.csv")


def transform_data(**kwargs):
    ti = kwargs["ti"]

    brands = pd.read_csv("files/brands.csv")
    categories = pd.read_csv("files/categories.csv")
    models = pd.read_csv("files/models.csv")

    models.merge(brands, left_on="brandId", right_on="id")
    models.merge(categories, left_on="categoryId", right_on="id")

    models.to_csv("files/dataset.csv")

    count_of_active = len(models.loc[models.isActive])
    count_of_inactive = len(models) - count_of_active

    ti.xcom_push(key="maxposter_counts", value={"a": count_of_active, "ina": count_of_inactive})


def load_data(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(key="maxposter_counts", task_ids=["transform_data"])[0]

    df = pd.DataFrame.from_dict([data])
    df.to_csv("files/active_inactive_result.csv")


with DAG(
    "load_maxposter_data",
    description="load_maxposter_data",
    schedule_interval="*/30 * * * *",
    catchup=False,
    default_args=args,
) as dag:  # 0 * * * *   */1 * * * *

    extract_and_load_brands = PythonOperator(
        task_id="extract_and_load_brands", python_callable=extract_and_load_brands
    )
    extract_and_load_categories = PythonOperator(
        task_id="extract_and_load_categories", python_callable=extract_and_load_categories
    )
    extract_and_load_models = PythonOperator(
        task_id="extract_and_load_models", python_callable=extract_and_load_models
    )

    transform_data = PythonOperator(task_id="transform_data", python_callable=transform_data)
    load_data = PythonOperator(task_id="load_data", python_callable=load_data)

    extract_and_load_brands >> transform_data
    extract_and_load_categories >> transform_data
    extract_and_load_models >> transform_data

    transform_data >> load_data
