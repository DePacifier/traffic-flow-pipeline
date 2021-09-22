from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime

from scripts.logger_creator import CreateLogger
from scripts.sql_handler import SQL_Handler

logger = CreateLogger('DATA-Extract-LOAD-DAG', handlers=1)
logger = logger.get_default_logger()

# DECLARING Airflow DAG CONFIGURATION
DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['milkybekele@gmail.com'],
    'email_on_failure': True,
    'schedule_interval': '@daily',
}

# Declaring DAG used functions
# Kafka Data Reader


def _check_mysql_connection():
    try:
        sql = SQL_Handler(host='localhost', port=3306,
                          username='root', password='password')

        logger.info(
            'CONNECTION SUCCESSFUL TO MySQL DATABASE, DAG PROCEEDING TO EXTRACTING AND LOADING DATA ...')
        return True

    except Exception as e:
        logger.exception(
            'CONNECTION TO MySQL DATABASE FAILED, EXITING DAG ...')
        return False


def _create_database():
    try:
        sql = SQL_Handler(host='localhost', port=3306,
                          username='root', password='password')

        sql.sql_query("CREATE DATABASE IF NOT EXISTS traffic_flow_db")

        logger.info(
            'DAG CREATED(ALREADY AVAILABLE) DATABASE traffic_flow_db SUCCESSFULLY')
        return True

    except Exception as e:
        logger.exception('DAG FAILED TO CREATE DATABASE "traffic_flow_db"')
        return False


def _remove_previous_tables():
    try:
        sql = SQL_Handler(host='localhost', port=3306,
                          username='root', password='password')

        sql.execute_query_on_db(
            "DROP TABLE IF EXISTS traffic_flow", database='traffic_flow_db')

        sql.execute_query_on_db(
            "DROP TABLE IF EXISTS stations", database='traffic_flow_db')

        logger.info(
            'DAG SUCCESSFULLY REMOVED PREVIOUSLY CREATED "traffic_flow" AND "stations" TABLES')

        return True

    except Exception as e:
        logger.exception(
            'DAG FAILED TO REMOVE PREVIOUSLY CREATED "traffic_flow" AND "stations" TABLES')
        return False


def _create_stations_table():
    try:
        sql = SQL_Handler(host='localhost', port=3306,
                          username='root', password='password')

        create_stations_table_query = """
            CREATE TABLE IF NOT EXISTS stations(
                id INT PRIMARY KEY,
                FWY INT,
                direction VARCHAR(2),
                district INT,
                county INT,
                city INT,
                state_pm VARCHAR(10),
                abs_pm FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                length FLOAT,
                type VARCHAR(3),
                lanes INT,
                name VARCHAR(50),
                user_id_1 VARCHAR(6),
                user_id_2 VARCHAR(20),
                user_id_3 INT,
                user_id_4 INT
            )
        """

        sql.insert_table(
            table_create_query=create_stations_table_query, database='traffic_flow_db')

        insert_stations_query = """
            INSERT INTO stations
            (id,FWY,direction,district,county,city,state_pm,abs_pm,latitude,
            longitude,length,type,lanes,name,user_id_1,user_id_2,user_id_3,user_id_4)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)
        """

        sql.insert_values(database='traffic_flow_db',
                          insert_query=insert_stations_query, file_path='../data/I80_stations.csv')

        logger.info('DAG SUCCESSFULLY CREATED TABLE "stations"')

        return True

    except Exception as e:
        logger.exception('DAG FAILED TO CREATE TABLE "stations"')
        return False


def _create_traffic_flow_table():
    try:
        sql = SQL_Handler(host='localhost', port=3306,
                          username='root', password='password')
        create_traffic_flow_table_query = """
                CREATE TABLE IF NOT EXISTS traffic_flow(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    source_id INT,
                    date DATE,
                    time TIME,
                    primary_link_source_flag FLOAT,
                    avg_speed FLOAT,
                    avg_flow FLOAT,
                    avg_occ FLOAT,
                    avg_freeflow_speed FLOAT,
                    avg_travel_time FLOAT,
                    samples_below_100pct_ff FLOAT,
                    samples_below_95pct_ff FLOAT,
                    samples_below_90pct_ff FLOAT,
                    samples_below_85pct_ff FLOAT,
                    samples_below_80pct_ff FLOAT,
                    samples_below_75pct_ff FLOAT,
                    samples_below_70pct_ff FLOAT,
                    samples_below_65pct_ff FLOAT,
                    samples_below_60pct_ff FLOAT,
                    samples_below_55pct_ff FLOAT,
                    samples_below_50pct_ff FLOAT,
                    samples_below_45pct_ff FLOAT,
                    samples_below_40pct_ff FLOAT,
                    samples_below_35pct_ff FLOAT,
                    samples_below_30pct_ff FLOAT,
                    samples_below_25pct_ff FLOAT,
                    samples_below_20pct_ff FLOAT,
                    samples_below_15pct_ff FLOAT,
                    FOREIGN KEY(source_id) REFERENCES stations(id)
                )
            """

        sql.insert_table(
            table_create_query=create_traffic_flow_table_query, database='traffic_flow_db')

        insert_traffic_flow_query = """
                INSERT INTO traffic_flow
                (source_id,date,time,primary_link_source_flag,avg_speed,avg_flow,avg_occ,avg_freeflow_speed,avg_travel_time,samples_below_100pct_ff,
                samples_below_95pct_ff,samples_below_90pct_ff,samples_below_85pct_ff,samples_below_80pct_ff,
                samples_below_75pct_ff,samples_below_70pct_ff,samples_below_65pct_ff,samples_below_60pct_ff,
                samples_below_55pct_ff,samples_below_50pct_ff,samples_below_45pct_ff,samples_below_40pct_ff,
                samples_below_35pct_ff,samples_below_30pct_ff,samples_below_25pct_ff,samples_below_20pct_ff,samples_below_15pct_ff)
                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)
            """

        sql.insert_values(database='traffic_flow_db',
                          insert_query=insert_traffic_flow_query, file_path='../data/I80_davis.txt')

        logger.info('DAG SUCCESSFULLY CREATED TABLE "traffic_flow"')

    except Exception as e:
        logger.exception('DAG FAILED TO CREATE TABLE "traffic_flow"')


# DAG
with DAG('RAW-DATA-EXTRACTOR-AND-LOADER', catchup=False, default_args=DAG_CONFIG) as dag:
    # check if database connection is working properly
    checking_db_connection = ShortCircuitOperator(
        task_id='checking_db_connection',
        python_callable=_check_mysql_connection
    )

    # create the database to store the tables on
    creating_db = ShortCircuitOperator(
        task_id='creating_db',
        python_callable=_create_database
    )

    # remove previously created tables if the database and tables existed
    removing_previous_tables = ShortCircuitOperator(
        task_id='removing_previous_tables',
        python_callable=_remove_previous_tables
    )

    # create the stations table from the CSV file
    creating_stations_table = ShortCircuitOperator(
        task_id='creating_stations_table',
        python_callable=_create_stations_table
    )

    # create the traffic_flow table from the text file
    creating_traffic_flow_table = ShortCircuitOperator(
        task_id='creating_traffic_flow_table',
        python_callable=_create_traffic_flow_table
    )

    checking_db_connection >> creating_db >> removing_previous_tables >> creating_stations_table >> creating_traffic_flow_table
