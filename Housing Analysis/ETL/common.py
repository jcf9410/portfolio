import datetime
import logging

import boto3
import psycopg2
from pandas import read_sql
from sqlalchemy import create_engine


def execute_query(q, params=None):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    engine = create_engine(f"postgresql://postgres:{db_pass}@localhost:5432/Housing")

    logging.debug("Executing query")
    df = read_sql(q, engine, params=params)

    return df


def execute_update_query(q_update, params=None):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    db_connection = psycopg2.connect(database="Housing",
                                     host="localhost",
                                     user="postgres",
                                     password=db_pass,
                                     port=5432)
    cursor = db_connection.cursor()

    try:
        cursor.execute(q_update, params)
        db_connection.commit()
    except Exception as e:
        db_connection.rollback()
        raise e
    finally:
        cursor.close()
        db_connection.close()


def upload_to_table(df, table_name):
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    engine = create_engine(f"postgresql://postgres:{db_pass}@localhost:5432/Housing")

    logging.info(f"Uploading {df.shape[0]} elements to {table_name}")
    df["timestamp"] = str(datetime.datetime.now(datetime.timezone.utc))
    df.to_sql(name=table_name, con=engine, index=False, if_exists="append")
