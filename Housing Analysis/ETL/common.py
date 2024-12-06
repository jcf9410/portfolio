import datetime
import logging

import boto3
import psycopg2
from pandas import read_sql
from sqlalchemy import create_engine, text


def create_sql_engine():
    ssm = boto3.client("ssm", region_name="eu-west-1")
    db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
    db_pass = db_pass["Parameter"]["Value"]

    engine = create_engine(f"postgresql://postgres:{db_pass}@localhost:5432/Housing")

    return engine


def execute_query(q, params=None):
    engine = create_sql_engine()

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


def upload_to_table(df, table_name, replace_existing_values=None):
    engine = create_sql_engine()

    logging.info(f"Uploading {df.shape[0]} elements to {table_name}")
    df["timestamp"] = str(datetime.datetime.now(datetime.timezone.utc))

    if replace_existing_values:
        ids = df["id"].values.tolist()
        placeholders = ",".join(["%s"] * len(ids))
        q_select_existing = f"""
            SELECT id
            FROM {table_name}
            WHERE id in ({placeholders})
        """
        existing_ids = execute_query(q_select_existing, params=tuple(ids))
        if not existing_ids.empty:
            existing_ids = existing_ids["id"].values.tolist()
            df_update = df.loc[df["id"].isin(existing_ids), :]
            data = df_update.to_dict("records")

            columns = df_update.columns
            values_clause = ", ".join([
                f"({', '.join(map(repr, row.values()))})" for row in data
            ])
            column_names = ", ".join(columns)

            set_clause = []
            for col in columns:
                if col == "timestamp":
                    set_clause.append(f"{col} = CAST(updates.{col} AS TIMETZ)")
                elif col != "id":
                    set_clause.append(f"{col} = updates.{col}")

            q_update = f"""
            WITH updates ({column_names}) AS (
                VALUES {values_clause}
            )
            UPDATE {table_name}
            SET {', '.join(set_clause)}
            FROM updates
            WHERE {table_name}.id = updates.id
            """
            execute_update_query(q_update)

        df_load = df.loc[~df["id"].isin(existing_ids), :]
    else:
        df_load = df
    df_load.to_sql(name=table_name, con=engine, index=False, if_exists="append")
