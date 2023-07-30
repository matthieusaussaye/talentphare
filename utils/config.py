# -*- coding: utf-8 -*-
"""
################################################################################
#
# File:     config.py
#
# Product:  PL BI
# Author:   Andres Montero
# Date:     13 July 2021
#
# Scope:
#
#
# Copyright (c) 2015-2021, Predictive Layer Limited.  All Rights Reserved.
#
# The contents of this software are proprietary and confidential to the author.
# No part of this program may be photocopied,  reproduced, or translated into
# another programming language without prior written consent of the author.
#
#
#
################################################################################
"""

import os
import configparser
import logging
from utils import ret, defines

import configparser
from pymongo.mongo_client import MongoClient

from utils.logger import logging

logger = logging.getLogger(__name__)

######################### INTERNAL DB CONFIG #############################

def read_internal_db_config(dbconfig):
    return read_config(dbconfig)

def parse_internal_db_config(config_file):
    for required_section in [defines.INTERNAL_DB_CONFIG_SECTION_DEFAULT]:
        if not config_file.has_section(required_section):
            msg = 'Can not find section %s in etl config file (found %s)' % (required_section, config_file.sections())
            logger.error(msg)
            return ret.init_result(ret.AUTOMATE_RES_KO, msg)

    db_section = config_file[defines.INTERNAL_DB_CONFIG_SECTION_DEFAULT]

    db_settings= {}
    db_settings[defines.DB_URI] = db_section[defines.DB_URI]
    db_settings[defines.DB_HOST] = db_section[defines.DB_HOST]
    db_settings[defines.DB_PORT] = db_section[defines.DB_PORT]
    db_settings[defines.DB_USER] = db_section[defines.DB_USER]
    db_settings[defines.DB_PASSWORD] = db_section[defines.DB_PASSWORD]
    return db_settings

def get_internal_db_settings(dbconfig):
    result = ret.init_result()

    config_file = read_internal_db_config(dbconfig)
    if config_file is None:
        msg = 'Can not find config file: %s' % dbconfig
        logger.error(msg)
        return ret.init_result(ret.AUTOMATE_RES_KO, msg)

    try:
        db_settings = parse_internal_db_config(config_file)
    except BaseException as e:
        msg = str(e)
        logger.error(msg)
        return ret.init_result(ret.AUTOMATE_RES_KO, msg=msg)
    result[ret.AUTOMATE_FIELD_RES] = db_settings
    return result

# =======================================================================================================================
def get_mail_config(mailconfig):
    logger.info('Start reading email configuration.')
    config = configparser.RawConfigParser()
    config.read(mailconfig)
    res = {}
    res[defines.CONFIG_FILE_MAIL_ADMIN] = config.get(defines.CONFIG_FILE_MAIL_TAG, defines.CONFIG_FILE_MAIL_ADMIN)
    res[defines.CONFIG_FILE_MAIL_FORECAST] = config.get(defines.CONFIG_FILE_MAIL_TAG, defines.CONFIG_FILE_MAIL_FORECAST)
    res[defines.CONFIG_FILE_MAIL_REPLY_TO] = config.get(defines.CONFIG_FILE_MAIL_TAG, defines.CONFIG_FILE_MAIL_REPLY_TO)
    res[defines.CONFIG_FILE_MAIL_KPI_REPORT] = config.get(defines.CONFIG_FILE_MAIL_TAG, defines.CONFIG_FILE_MAIL_KPI_REPORT)
    return ret.init_result(ret=res)


# =======================================================================================================================
def get_smtp_config(mailconfig):
    logger.info('Start reading SMTP configuration.')
    config = configparser.RawConfigParser()
    config.read(mailconfig)
    res = {}
    # Primary SMTP
    res[defines.CONFIG_FILE_SMTP_USER] = config.get(defines.CONFIG_FILE_SMTP_TAG, defines.CONFIG_FILE_SMTP_USER)
    res[defines.CONFIG_FILE_SMTP_PASSWORD] = config.get(defines.CONFIG_FILE_SMTP_TAG, defines.CONFIG_FILE_SMTP_PASSWORD)
    res[defines.CONFIG_FILE_SMTP_SENDER] = config.get(defines.CONFIG_FILE_SMTP_TAG, defines.CONFIG_FILE_SMTP_SENDER)
    res[defines.CONFIG_FILE_SMTP_SERVER] = config.get(defines.CONFIG_FILE_SMTP_TAG, defines.CONFIG_FILE_SMTP_SERVER)

    # Backup SMTP
    res[defines.CONFIG_FILE_SMTP_BACKUP_TAG + defines.CONFIG_FILE_SMTP_USER] = config.get(defines.CONFIG_FILE_SMTP_BACKUP_TAG, defines.CONFIG_FILE_SMTP_USER)
    res[defines.CONFIG_FILE_SMTP_BACKUP_TAG + defines.CONFIG_FILE_SMTP_PASSWORD] = config.get(defines.CONFIG_FILE_SMTP_BACKUP_TAG, defines.CONFIG_FILE_SMTP_PASSWORD)
    res[defines.CONFIG_FILE_SMTP_BACKUP_TAG + defines.CONFIG_FILE_SMTP_SENDER] = config.get(defines.CONFIG_FILE_SMTP_BACKUP_TAG, defines.CONFIG_FILE_SMTP_SENDER)
    res[defines.CONFIG_FILE_SMTP_BACKUP_TAG + defines.CONFIG_FILE_SMTP_SERVER] = config.get(defines.CONFIG_FILE_SMTP_BACKUP_TAG, defines.CONFIG_FILE_SMTP_SERVER)

    return ret.init_result(ret=res)

# =======================================================================================================================
def get_db_session(dbconfig):
    # Load the database parameters and open up the connection
    #
    result = ret.init_result()
    logger.info('Start opening a connection with the database.')
    try:
        db_uri_mapper=DbURIMapper(dbconfig.replace("db","db_uri"))
        config = configparser.RawConfigParser()
        config.read(dbconfig)
        dbname_geniusfi = config.get(defines.CONFIG_FILE_DB_TAG, defines.CONFIG_FILE_GENIUSFI_DB)
        dbname_plkbrest = config.get(defines.CONFIG_FILE_DB_TAG, defines.CONFIG_FILE_PLKBREST_DB)
        database_geniusfi = db_uri_mapper.get_db_session(dbname_geniusfi)
        database_plkbrest = db_uri_mapper.get_db_session(dbname_plkbrest)
        result[ret.AUTOMATE_FIELD_RES] = {
                                          defines.CONFIG_FILE_GENIUSFI_DB: database_geniusfi,
                                          defines.CONFIG_FILE_PLKBREST_DB:database_plkbrest
                                          }
    except Exception as e:
        logger.warning('We can not connect to the database')
        return ret.init_result(ret.AUTOMATE_RES_KO, str(e))
    return result

# =======================================================================================================================
def get_geniusfinance_config(dbconfig):
    # Load the database parameters and open up the connection
    #
    result = ret.init_result()
    logger.info('Start opening a connection with the database.')
    try:
        config = configparser.RawConfigParser()
        config.read(dbconfig)
        geniusfinance_user = config.get(defines.CONFIG_FILE_GENIUSFINANCE_TAG, defines.GENIUSFINANCE_USER)
        geniusfinance_password = config.get(defines.CONFIG_FILE_GENIUSFINANCE_TAG, defines.GENIUSFINANCE_PASSWORD)
        result[ret.AUTOMATE_FIELD_RES] = {
                                          defines.GENIUSFINANCE_USER: geniusfinance_user,
                                          defines.GENIUSFINANCE_PASSWORD:geniusfinance_password
                                          }
    except Exception as e:
        logger.warning('We can not connect to the database')
        return ret.init_result(ret.AUTOMATE_RES_KO, str(e))
    return result


######################### GENERIC FUNCTIONS #############################

def read_config(config):
    if os.path.exists(config) is False:
        return None
    config_file = configparser.ConfigParser(allow_no_value=True, strict=True, default_section="default")
    config_file.read(config)
    return config_file


class DbURIMapper(object):
    """
    The class is used for making the link between a db name, and the associated uri to connect to the db
    """
    def __init__(self, dbconfig):
        """
        Initialize the object with the file containing the mapping rules.
        :param dbconfig: A uri config mapping file
        """
        if os.path.exists(dbconfig) is False:
            logger.error('Can not find URI config file: %s' % dbconfig)
            return ret.init_result(ret.AUTOMATE_RES_KO)
        config = configparser.ConfigParser(strict=False)
        config.read(dbconfig)
        # Do not go thru defines in order to limit modifications to existing projects ...
        #
        self._uri = config.get('models', 'uri')
        self._production = config.getboolean('models', 'production', fallback=False)
        data_reader_env_user = config.get('models', 'data_reader_env_user')
        data_reader_env_pass = config.get('models', 'data_reader_env_pass')

        if 'replicaSet' in self._uri:
            if self._production is True:
                model_writer_env_user = config.get('models', 'model_writer_env_user')
                model_writer_env_pass = config.get('models', 'model_writer_env_pass')
                self._model_writer_env_user = os.getenv(model_writer_env_user, None)
                self._model_writer_env_pass = os.getenv(model_writer_env_pass, None)
            self._data_reader_env_user = os.getenv(data_reader_env_user, None)
            self._data_reader_env_pass = os.getenv(data_reader_env_pass, None)


    def get_db_uri(self, dbname):
        # Load the database parameters and open up the connection
        #
        if (dbname.endswith('_pl') is True):
            logger.info('Providing Read/Write Access for DB: %s' % dbname)
            if self._production is False:
                # When not in production, force localhost for PL dbs
                #
                return 'mongodb://localhost:27017/' + dbname
            if 'replicaSet' in self._uri:
                return self._uri % (self._model_writer_env_user, self._model_writer_env_pass, dbname)
            else:
                return self._uri + '/' + dbname
        else:
            logger.info('Providing Read Only Access for DB: %s' % dbname)
            if 'replicaSet' in self._uri:
                return self._uri % (self._data_reader_env_user, self._data_reader_env_pass, dbname)
            else:
                logger.info('Providing Read Only Access for DB: %s' % dbname)
                return self._uri + '/' + dbname

    def get_db_session(self, dbname):
        uri = self.get_db_uri(dbname)
        client= MongoClient(uri)
        return client[dbname]





if __name__ == "__main__":
    pass
