# -*- coding: utf-8 -*-

###############################################################################
#
# File:      crud.py
# Project:   talentphare
# Author(s): Matthieu Saussaye
# Scope:     script to create tables in postgres database
#
# Created:       28 July 2023
# Last modified: 28 July 2023
#
# Copyright (c) 2015-2021, sigmapulse.ch Limited. All Rights Reserved.
#
# The contents of this software are proprietary and confidential to the
# copyright holder. No part of this program may be photocopied, reproduced, or
# translated into another programming language without prior written consent of
# the copyright holder.
#
###############################################################################
import argparse
import copy
import logging
import logging.config
import numpy as np
import pandas as pd
import contextlib
import sqlalchemy.ext.declarative as sqld
import json
import time

sqla_base = sqld.declarative_base()

from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import MetaData, text
from clients import power_futures
from etl import models
from sqlalchemy_utils import create_database, drop_database, database_exists
from utils import defines, logger, ret
from utils.config import get_internal_db_settings
from sqlalchemy.ext.declarative import declarative_base
from loaders import DbSessionLoader, DbLoader

logger = logging.getLogger(__name__)
Base = declarative_base()


def connect_to_mongo_db(dbconfig, db_name):
    """Connecting to mongo cluster"""
    
    db_session = DbSessionLoader(DbLoader(tags=db_name))

    return {
        "client": db_session.get_db_client_session(write_permission=True),
        "target": db_session.get_db_target_session(write_permission=True)
    }


def create_tables(engine, tables: list):
    """
    Creates the tables defined in models.py into the database
    If the database is not created it will create a new database with the params defined in config.py
    - table answer (user_key|question_i|question_text|answer)
    - table user (user_name|user_key|age|prompt_information)
    """
    logger.info('Creating tables in the db')
    for table_name in tables:
        logger.info(f'Creating table {table_name}')
        try:
            if table_name.endswith('_answer'):
                models.get_table_answer(table_name).__table__.create(bind=engine)
            elif table_name.endswith('_user'):
                models.get_table_user(table_name).__table__.create(bind=engine)
            else:
                pass
            # models.Base.metadata.create_all(engine)
            # models.create_models(table_name)
        except ProgrammingError as err:
            logger.warning(f"Table already exists, error: {err}")
            raise Exception(f"Table already exists, error: {err}")

        except OperationalError as err:
            logger.error(f'There was a problem creating the table, error: \n{err}')
            error = str(err.__dict__['orig'])
            if f'database "{defines.DB_NAME}" does not exist' in error:
                logger.info(f'Creating database {defines.DB_NAME}')
                create_database(engine.url)
            else:
                raise Exception(f'There was a problem creating the table \n{err}')


def drop_tables(engine, tables: list):
    """
    Deletes the tables that are created in the DB
        tables: list with the names of the tables to drop

    """
    logger.info('Dropping and creating tables in the db')
    assert tables, "Tables list to drop is empty"

    with engine.begin() as connection:
        logger.info('engin begin ok')
        for table_name in tables:
            logger.info('table droping %s'%(table_name))
            try:
                query = text(f"""   DROP TABLE IF EXISTS {table_name}; """)
                logger.debug(f"The query is: \n {query}")
                connection.execute(query)
            except OperationalError as err:
                logger.error(f'There was a problem deleting the tables \n{err}')
                raise Exception(f'There was a problem deleting the tables \n{err}')

def clean_tables(engine, tables):
    """
    Cleans the tables that are created in the DB
    without dropping them
        tables: list with the names of the tables to clean
    """
    logger.info('Cleaning  tables in the db')
    assert tables, "Tables list to clean is empty"

    with engine.begin() as connection:
        for table_name in tables:
            try:
                query = text(f"""   DELETE FROM {table_name} """)
                logger.debug(f"The query is: \n {query}")
                connection.execute(query)
            except OperationalError as err:
                logger.error(f'There was a problem cleaning the tables \n{err}')
                raise Exception(f'There was a problem cleaning the tables \n{err}')

def recreate_tables(engine, tables):
    """
    First deletes and then creates again the tables in the database
    """
    logger.info('Recreating tables in the db')
    drop_tables(engine, tables)
    sleep(0.5)
    create_tables(engine, tables)


def delete_database(engine):
    """
    Deletes the database
    """
    logger.info('Deleting Database')
    try:
        drop_database(engine.url)
    except ProgrammingError as err:
        # if isinstance(err.orig, InvalidCatalogName):
        #     logger.error(f'Database not found (continuing anyway)\n{err}')
        # else:
        logger.error(f'There was a problem deleting the database \n{err}')
        raise Exception(f'There was a problem deleting the database \n{err}')

    except OperationalError as err:
        logger.error(f'There was a problem connecting to the database \n{err}')
        raise Exception(f'There was a problem connecting to the database \n{err}')


def export_azure_data_json(engine, table_name, start_date, end_date):
    """
    This functino will export the data from azure db as json file to be used in our own API
    Args:
        engine:
        database:
        table_name:
        start_date:
        end_date:

    Returns:
        values:  json file with values of the table
    """
    logger.info(f"Preparing to export data from azure as json")
    with engine.begin() as connection:
        logger.info(f"Starting the query")
        query = text(f""" SELECT * FROM {table_name} 
                            WHERE date >= '{start_date}' AND date <= '{end_date}' """)
        logger.debug(f"The query is: \n {query}")

        pandas_results = pd.read_sql(con=connection, sql=query)
        logger.debug(f"The df is {pandas_results}")
        logger.debug(f"The df as json is {pandas_results.to_json(orient='records')}")

    return pandas_results.to_json(orient='records')
def create_view(engine,
                table_name: str,
                show_features_names: bool = True):
    """
    Creates the views in the DB used for the PowerBi diagrams
    Args:
        engine: engine connection
        table_name: string with the name of the table from where it will create the view
        show_features_names: bool to hide features names in the explainability view (last line of code WHERE feature = feature_group)

    Returns:

    """
    logger.info(f"Creating views in db")
    with engine.begin() as connection:
        logger.info(f"Creating view v_user")
        query = text(f"""    CREATE OR ALTER VIEW [dbo].[v_user]
                             AS
                             SELECT *
                               FROM {table_name} as  fct
                             """)
        logger.debug(f"The query is: \n {query}")
        connection.execute(query)
        query = text(f"""    CREATE OR ALTER VIEW [dbo].[v_answer]
                                     AS
                                     SELECT *
                                       FROM {table_name} as  fct
                                     """)
        logger.debug(f"The query is: \n {query}")
        connection.execute(query)

def write_to_table(df: pd.DataFrame,
                   engine,
                   table_name: str,
                   schema: list,
                   use_index: bool,
                   upsert_tables,
                   date : str = None):
    """
    Args:
        df (pd.Dataframe):  dataframe with all the information that we will write to the database
        engine (engine): connection to the DB
        table_name (str) : name of the table that we will write to the database
        schema (list): name of the columns that the table will have in the database and to subset the df
        use_index (bool) : to define if the table will use df index or not
        date (str) : the date of the data we want to insert
        upsert_tables (bool) : to upsert_tables
    """
    # Check that the dataframe is not empty
    logger.debug(f'The columns are {list(df.columns)} and the schema is {schema}')
    if df.shape[0] == 0:
        logger.error("Data frame is empty")
        raise Exception("Nothing to write to database!")

    # check if the column exist in the df, if not create and fill it with Null
    for c in schema:
        if c not in df:
            df[c] = None
    
    df = df[schema].apply(copy.deepcopy)
    df.columns = schema

    if table_name.endswith('answer'):
        df.drop_duplicates(subset=['user_key', 'answer'], keep='last', inplace=True)
    elif table_name.endswith('user'):
        df.drop_duplicates(subset=['user_key', 'user_name'], keep='last', inplace=True)
    else :
        pass
    # converting to floats to avoid issues with psycopg2
    if table_name.endswith('answer'):
        df['question_i'] = pd.to_numeric(df['question_i'], errors='coerce')

    if upsert_tables:
        logger.info(f'Upserting Table {table_name}')

        with engine.begin() as connection:
            logger.info(f'Creating temporary table')
            
            if date is not None :
                df_filtered = df[(df['date'] == date)]
                logger.info(f'Will write {df_filtered.shape[0]} rows to the db')
                logger.info(f'It corresponds to the date {date}')

            else :
                df_filtered = df
                logger.info(f'Will write {df_filtered.shape[0]} rows to the db')
                
            df_filtered.to_sql('temporary_table', con=connection, index=False, if_exists='append',
                      method='multi', chunksize=1000)
            
            ## changer chunksize, voir si le temps est lent / ou non rester sur merge
            ## sinon faire plutot un insert avec 1 000 000 de ligne , faire insert du fichier dans un table tempo sql ('toto') puis faire le merge via sql

            
            if table_name.endswith('user'):
                query = text(f"""   MERGE {table_name} as destination
                                        USING (SELECT date, user_key, user_name, infos FROM temporary_table) as source
                                        ON  destination.date=source.date and destination.product=source.product
                                    WHEN MATCHED THEN
                                        UPDATE SET 
                                            infos = source.infos,
                                            user_name = source.user_name
                                    WHEN NOT MATCHED THEN
                                        INSERT (date,user_key, user_name, infos)
                                        VALUES (source.date,source.user_key, source.user_name, source.infos);
                                    """)
            elif table_name.endswith('anwser'):
                query = text(f"""   MERGE {table_name} as destination 
                                        USING (SELECT date, user_key, question_i, question_text, answer FROM temporary_table) as source
                                        ON  destination.date=source.date and destination.product=source.product
                                    WHEN MATCHED THEN
                                        UPDATE SET 
                                            date = source.date,
                                            user_key = source.user_key,
                                            question_i = source.question_i,
                                            question_text = source.question_text,
                                            answer = source.answer
                                    WHEN NOT MATCHED THEN
                                        INSERT (date, user_key, question_i, question_text, answer)
                                        VALUES (source.date,source.user_key, source.question_i, source.question_text, source.answer);
                                    """)
            logger.debug(f"The query is: \n {query}")
            connection.execute(query)
            logger.debug(f"Total number of rows upserted: {df.shape[0]}")

            # delete temporary table
            logger.debug(f"Dropping temporary table ")
            query = text(f"""   DROP TABLE IF EXISTS temporary_table; """)
            logger.debug(f"The query is: \n {query}")
            connection.execute(query)
    else:
        try:
            with engine.begin() as connection:
                logger.info(f'Writing to table {table_name} in DB')
                if table_name.endswith('text_fields'):
                    # First delete the info in the text fields table
                    logger.info(f'Cleaning table {table_name} in DB')
                    query = text(f"""IF EXISTS 
                                    (SELECT object_id FROM sys.tables where name = '{table_name}')
                                      DELETE FROM {table_name}; """)
                    logger.info(f"The query is: \n {query}")
                    connection.execute(query)
                    df.to_sql(table_name, con=connection, index=False, if_exists='append',
                              method='multi', chunksize=1000)
                else:
                    if use_index:
                        logger.info(f'Using index ')
                        df.to_sql(table_name, con=connection, index=True, if_exists='append',
                                  method='multi', chunksize=1000)
                    else:
                        logger.info(f'Not using index ')
                        df.to_sql(table_name, con=connection, index=False, if_exists='append',
                                  method='multi', chunksize=1000)
                logger.info(f'Writing to table {table_name} finished, total rows written {df.shape[0]}')
        except Exception as e:
            logger.error(f'Something went wrong during writing to db {e}')
            raise Exception(f'Something went wrong during writing to db {e}')


def prepare_db(engine,
               drop_db: bool,
               bool_recreate_tables: bool,
               create_db: bool,
               bool_clean_tables: bool,
               tables: list):
    """

    Args:
        engine (engine): connection to the DB
        drop_db (bool): To drop database
        bool_recreate_tables (bool): To recreate tables in the database
        bool_clean_tables (bool): To clean tables in the database
        create_db (bool): To create database
        tables (list): list with the name of tables to affect
    Returns: None

    """
    if drop_db:
        delete_database(engine)

    if create_db:
        logger.info("Trying to create Database")
        try:
            if not database_exists(engine.url):
                create_database(engine.url)
            logger.info("DataBase created")
            create_tables(engine, tables)
        except Exception as err:
            logger.error(f"Error trying to create database {err}")
            raise Exception(f"Error trying to create database {err}")

    if bool_recreate_tables:
        recreate_tables(engine, tables)

    if bool_clean_tables:
        clean_tables(engine, tables)


def write_data_database(engine,
                        data: pd.DataFrame,
                        table_name: str,
                        raw_schema: list,
                        use_index: bool,
                        upsert_tables: bool,
                        date: str = None) -> None:
    
    """ This functions writes the raw data to the Database
    Args:
        engine (engine): connection to the db
        data (pd.DataFrame): pandas dataframe with the data to write to the Database
        table_name (str): name of the table to write to the database
        raw_schema (list): raw schema of the table (names of the columns)
        use_index (bool): to define if we use index of pandas df or not
        upsert_tables (bool): to define if we upsert tables or not (keep in mind that this parameter is working only for MySql DB)
        date (str) : the date of the data we want to insert
    Returns: None

    """

    logger.info('Writing Data into DB')
    write_to_table(data, engine, table_name, raw_schema, use_index, upsert_tables, date=date)


def get_internal_db_engine(db_config_file, db_name: str):
    """

    Args:
        db_config_file: Config file of the Database (uri, user, password, port)
        db_name (str): Name of the Database

    Returns: Engine

    """

    db_settings = get_internal_db_settings(db_config_file)[ret.AUTOMATE_FIELD_RES]
    logger.info('Starting to create Engine to connect to the db')
    db_settings[defines.DB_URI] = db_settings[defines.DB_URI] % db_name
    engine = create_engine(db_settings[defines.DB_URI], echo=False)
    # prepare_db(table_name, engine, drop_db, recreate_tables, create_db, clean_tables)
    return engine


def pivot_table(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to Pivot the data to the correct format
    Args:
        data (pd.DataFrame): Pandas dataframe with the data

    Returns (pd.DataFrame): Pandas dataframe with the pivot data

    """
    # Check that the dataframe is not empty
    if data.shape[0] == 0:
        logger.error("Data in pivot_table is empty!")
        raise Exception("Data in pivot_table is empty!")

    # Pivot the data
    df = data.melt(id_vars=['product'], var_name='feature', ignore_index=False)
    df.reset_index(inplace=True)
    df.rename(columns={'od': 'date'}, inplace=True)
    df.sort_values(by=['date', 'product'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    df['value'] *= 1

    # replacing the False and True values to integers
    logger.debug(f'The columns of the dataframe are {df.columns}')

    return df


def main(args):
    # Get data from CSV file to DataFrame(Pandas)
    logger.info('Reading Data')
    # data = pd.read_excel('../../data/CVG US RTG APAC for Q321_small.xlsx')
    data = pd.read_excel(args.input_file)
    data.drop(data.filter(regex='Unnamed').columns, axis=1, inplace=True)

    logger.info('Dropping Features in data')
    data.drop(columns=power_futures.FEATURES_TO_DROP,
              inplace=True)
    data.set_index(power_futures.DATE_COL, inplace=True)
    data.rename(columns={power_futures.PRODUCT_NAME: 'product'}, inplace=True)
    pivot_data = pivot_table(data)
    # Writing to DB
    logger.info('Preparing to write to DB')
    db_engine = get_internal_db_engine(args, args.config_db)
    logger.info('Writing Data Into the DB')
    write_data_database(db_engine, data=pivot_data)
    logger.info('Finished writing data')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--create_db', action='store_true', default=False)
    parser.add_argument('--drop_db', action='store_true', default=False)
    parser.add_argument('--recreate_tables', action='store_true', default=False)
    parser.add_argument('--clean_tables', action='store_true', default=False)
    parser.add_argument('--input_file', action='store',
                        default='../data/SHAP_PROD_HISTORIC_BLENDPROD_V4_1_H10_T10_20220510.csv')
    parser.add_argument('--config_db', action='store', default='../config/config_db.cfg')

    args = parser.parse_args()
    main(args)
    
    
    
