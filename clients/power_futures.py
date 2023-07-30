# -*- coding: utf-8 -*-

###############################################################################
#
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

import sys

sys.path.insert(0, '..')

import argparse
import copy
import logging
from etl import crud, data_extender
import pandas as pd
import numpy as np

from utils import defines
from utils.df_utils import transform_price, filtering_ds, compute_target, transform_target, prepare_hi_data
from utils.price_M1_Q1 import *

from datetime import datetime

from Power_Groups_FCT_v5_v6 import create_subgroup_feat_dict_shap_big as create_subgroup_feat_dict_shap_big_v5_v6
from Power_Groups_FCT import create_subgroup_feat_dict_shap_big

# =====================================================================
# PARAMETERS ==========================================================
# =====================================================================
# Name of column in data that should go to the 'product' column in the dB
DICT_MATCHING_NAMES = {'name': 'product'}
# Features to drop ===============================
FEATURES_TO_DROP = ['vd', 'AV_PROBA', 'AV_PRED', 'AV_LOGIT', 'AV_LOGIT_TO_PROBA']
# DATE COLUMN ===============================
DATE_COL = 'od'
# SCHEMA ===============================
RAW_SCHEMA = ['date', 'product', 'feature', 'feature_group', 'value', 'feature_value', 'info']
RAW_SCHEMA_PRICE_TARGET = ['date', 'product', 'price', 'target']
RAW_SCHEMA_TEXT = ['index', 'text']
RAW_SCHEMA_HI = ['date','product','AV_CHI2_PCA95','AV_CHI2_PCA80','AV_CHI2W','AV_CHI2','AV_OOR']
# PARA FOR GROUPS ===============================
FRACDIFF = '_FRACDIFF_'
LVL1 = 'lvl1'
LVL2 = 'lvl2'
LVL3 = 'lvl3'
LVL1to2 = LVL1 + '_' + LVL2
LVL2to3 = LVL2 + '_' + LVL3
CLIENT_LVL = 'client_lvl'
DEF_LVL = 'default_lvl'
##########
DICT_MATCHING_INFO = {}
# CLIENT GROUP CONFIG ===========================
config_group_axpo = {
    DEF_LVL: LVL2,  # lvl2, lvl3
    LVL1to2: [],
    LVL2to3: [],
}
# FIX AS INPUT FCT
config_group_client = copy.deepcopy(config_group_axpo)
logger = logging.getLogger(__name__)


# ===============================================
# FCT FOR GROUPS ================================
# ===============================================
def convert_datetime(dt):
    return datetime.strftime(dt, '%d/%m/%Y')


# REVERSE MAP GROUP
def reverse_map_group(dict_map):
    list_feat = [el2 for el1 in list(dict_map.keys()) for el2 in dict_map[el1]]
    list_feat = list(set(list_feat))
    list_groups = list(dict_map.keys())
    # ======================================
    dict_opposite_map = {}
    for feat in list_feat:
        dict_opposite_map[feat] = []
        for group in list_groups:
            if feat in dict_map[group]:
                dict_opposite_map[feat] = dict_opposite_map[feat] + [group]
    return dict_opposite_map


# CREATE ALL GROUP HIERARCHY
def create_group_hierarchy(list_features, version):
    map_group_all = {}
    
    # BUILD ALL GROUP LVL ->  {GROUP : FEAT}
    
    if version == "v5" or version == "v6" or version =="v7"  :
        logger.info('version v5, v6 or v7')
        group3, group2, group1 = create_subgroup_feat_dict_shap_big_v5_v6(list_features)
        
    elif version == "v4" :
        logger.info('version v4')
        group3, group2, group1 = create_subgroup_feat_dict_shap_big(list_features)

    # REVERSE THE GROUPS -> {FEAT : GROUP}
    rgroup3 = reverse_map_group(group3)
    rgroup2 = reverse_map_group(group2)
    rgroup1 = reverse_map_group(group1)

    # CREATE THE GROUP HIER -> {FEAT : {LVL1 : GROUP1, LVL2 : GROUP2, LVL3 : GROUP3,}}
    for el in list_features:
        map_group_all[el] = {
            LVL1: rgroup1[el][0],
            LVL2: rgroup2[el][0],
            LVL3: rgroup3[el][0],
        }

    return map_group_all


# ===============================================
# FCT FOR CLIENTS GROUPS ========================
# ===============================================
# PERSONALIZE GROUP MAP FOR CLIENTS
def find_client_lvl(config_group_client, dict_map_groups):
    dict_map_client = copy.deepcopy(dict_map_groups)
    # INSERT FIELD "CLIENT LEVEL" FOR ALL FEATURES ====
    default_lvl = config_group_client[DEF_LVL]
    # Default value ===================================
    for key_ in list(dict_map_client.keys()):
        dict_map_client[key_][CLIENT_LVL] = default_lvl
    # IMPOVE LVL1 to LVL2
    if config_group_client[LVL1to2]:
        for el in config_group_client[LVL1to2]:
            for key_ in list(dict_map_client.keys()):
                if dict_map_client[key_][LVL1] == el:
                    dict_map_client[key_][CLIENT_LVL] = LVL2
    # IMPOVE LVL2 to LVL3
    if config_group_client[LVL2to3]:
        for el in config_group_client[LVL2to3]:
            for key_ in list(dict_map_client.keys()):
                if dict_map_client[key_][LVL2] == el:
                    dict_map_client[key_][CLIENT_LVL] = LVL3
    return dict_map_client


# CREATE PERSONALIZED GROUP MAP
def creat_map_client(dict_map_client):
    dict_map_client_only = {}
    for key_ in list(dict_map_client.keys()):
        lvl_client = dict_map_client[key_][CLIENT_LVL]
        dict_map_client_only[key_] = dict_map_client[key_][lvl_client]
    return dict_map_client_only


# ===============================================
# FCT FOR PIVOT DATA ============================
# ===============================================
# ADD GROUP TO DF PIVOT DATA
def add_data_group(pivot_data : pd.DataFrame, rgroup) -> pd.DataFrame:
    pivot_data['feature_group'] = pivot_data['feature'].apply(lambda x: rgroup.get(x, x))
    return pivot_data


# FETCH FILE, CLEAN & PIVOT, BUILD GROUP SHAP
def prepare_data(input_file: str, version) -> pd.DataFrame:
    """
    Args:
        input_file: path to reading the csv file

    Returns:
        pandas dataframe with data ready to be written to the DB
    """
    # FETCH FILE ======================================================
    # Get data from CSV file to DataFrame(Pandas)
    logger.info('Reading Data')
    # data = pd.read_excel(input_file)
    data = pd.read_csv(input_file)
    data.drop(data.filter(regex='Unnamed').columns, axis=1, inplace=True)

    # CLEANING (DROP + RENAME) ========================================
    # DROP USELESS
    logger.info('Dropping Features in data')
    data.drop(columns=FEATURES_TO_DROP,
              inplace=True)
    if 'Target_Num' in list(data.columns):
        data.drop(columns='Target_Num', inplace=True)
    data.set_index(DATE_COL, inplace=True)
    # RENAMING COLUMNS
    logger.info('Renaming Features')
    for key, value in DICT_MATCHING_NAMES.items():
        data.rename(columns={key: value}, inplace=True)

    # PIVOT TABLE =====================================================
    logger.info('PIVOT TABLE')
    pivot_data = crud.pivot_table(data)
    list_features = list(data.columns)
    if 'product' in list_features:
        list_features.remove('product')

    # CREATE MAP GROUP =================================================
    logger.info('create_group_hierarchy')
    dict_map_groups = create_group_hierarchy(list_features, version)
    logger.info('find_client_lvl')
    dict_map_groups_client = find_client_lvl(config_group_client, dict_map_groups)
    logger.info('creat_map_client')
    dict_map_groups_client_final = creat_map_client(dict_map_groups_client)

    # ADD GROUP IN PIVOT DATA =================================================
    logger.info('add_data_group to pivot_table')
    pivot_data = add_data_group(pivot_data, dict_map_groups_client_final)

    return pivot_data


def main(args):

    # Decoding Args parameters =========================

    mongo_db = args.mongo_db
    create_db = args.create_db
    drop_db = args.drop_db
    db_config_file = args.config_db
    db_mongo_config_file = args.config_mongo_db

    input_file_hi = args.input_file_hi
    input_file_shap = args.input_file_shap
    input_file_feature = args.input_file_feature

    date = args.date
    version = args.version
    horizon = int(args.horizon)
    
    recreate_tables = args.recreate_tables
    clean_tables = args.clean_tables
    upsert_tables = args.upsert_tables

    db_name = args.db_name

    show_feature_names_explainability = args.show_feature_names_explainability
    create_views = args.create_views
    collections_mongo = args.collections_mongo

    # TABLE NAME that will be created in DB ===============================
    TABLE_NAME = f'power_t{horizon}_futures'

    # DB_NAME to create the database ===============================
    if db_name != None  :
        DB_NAME = db_name
    else :
        DB_NAME = f'power_t{horizon}'
        
    logger.info('DB_NAME %s'%(DB_NAME))
    
    DB_NAME = db_name
    
    # TABLE names inside the database ===============================
    tables = [f'power_t{horizon}_futures',
              f'power_t{horizon}_futures_text_fields',
              f'power_t{horizon}_futures_price_target',
              f'power_t{horizon}_futures_HI']

    # Prepare data =====================================
    logger.info('Preparing HI Data')
    hi_data = prepare_hi_data(input_file_hi)
    logger.info('DF pivot with grouping done')
    
    logger.info('Preparing SHAP Data')
    shap_pivot_data = prepare_data(input_file_shap, version)
    logger.info('DF pivot with grouping done')

    logger.info('Preparing Features Data')
    feature_pivot_data = prepare_data(input_file_feature, version)
    logger.info('Merging Features Data & SHAP')
    shap_pivot_data['feature_value'] = feature_pivot_data['value']

    logger.info('Adding extra treatments')
    data = data_extender.add_data_extra_info(shap_pivot_data,
                                            'feature_group',
                                            'info',
                                            DICT_MATCHING_INFO)
    
    # Reading data from mongo ====================================
    
    logger.info('reading data from mongo db')
    
    mongo_db = crud.connect_to_mongo_db(db_mongo_config_file, mongo_db)
    data_mongo_concatenated = []
    
    for collection in collections_mongo:
        mycol = mongo_db['client'][collection]
        data_mongo = pd.DataFrame(list(mycol.find()))
        data_mongo_concatenated.append(data_mongo[['date', 'name', 's']])

    data_mongo_concatenated = pd.concat(data_mongo_concatenated)
    data_mongo_concatenated.rename(columns={'name': 'product'}, inplace=True)

    logger.info('computing the target')

    data_mongo_filtered = filtering_ds(data_mongo_concatenated = data_mongo_concatenated,
                                       country_codes = ['DE', 'FD', 'F7'],
                                       load = ['B'],
                                       period_codes = ['Q', 'Y', 'M'],
                                       years = [19, 20, 21, 22, 23, 24, 25, 26],
                                       price_type='s')
    
    print("data_mongo_filtered")
    print(data_mongo_filtered)

    data_targets = compute_target(data_mongo_filtered=data_mongo_filtered,
                                  horizon=horizon)
    
    final_targets = transform_target(data_targets)
    
    final_prices = transform_price(data_mongo_filtered)
    
    final_prices = final_prices.dropna()
    
    logger.info('joining price data with targets')
        

    price_target = final_prices.merge(final_targets,
                                      left_on=['product','date'],
                                      right_on = ['product','date'],
                                      how='left')

    price_target = add_M1_Q1_to_price_target_table(price_target=price_target,
                                                   horizon=horizon,
                                                   safety=5,
                                                   type='power')
    
    logger.info('price_target with M1, Q1')

    logger.info(price_target)

    # Writing to DB ====================================

    logger.info('Preparing to write to DB')
    

   db_engine = crud.get_internal_db_engine(db_config_file, DB_NAME)

    crud.prepare_db(engine=db_engine,
                   create_db=create_db,
                   drop_db=drop_db,
                   bool_recreate_tables=recreate_tables,
                   bool_clean_tables=clean_tables,
                   tables=tables)

    logger.info('Writing Shap Data Into the DB')

    crud.write_data_database(engine=db_engine,
                             data=data,
                             table_name=TABLE_NAME,
                             raw_schema=RAW_SCHEMA,
                             use_index=False,
                             upsert_tables=upsert_tables,
                             date=date)

    logger.info('Finished writing data')
    
    logger.info('Writing HI Data Into the DB')
    
    logger.info(hi_data)

    crud.write_data_database(engine=db_engine,
                             data=hi_data,
                             table_name=TABLE_NAME + '_HI',
                             raw_schema=RAW_SCHEMA_HI,
                             use_index=False,
                             upsert_tables=upsert_tables,
                             date=date)
    
    logger.info(f'Writing price_target Into the DB')

    logger.info(price_target)
    
    crud.write_data_database(engine=db_engine,
                             data=price_target,
                             table_name=TABLE_NAME + '_price_target',
                             raw_schema=RAW_SCHEMA_PRICE_TARGET,
                             use_index=False,
                             upsert_tables=upsert_tables,
                             date=None)

    logger.info('Finished writing data')

    logger.info('Writing Text fields Data Into the DB')

    text_data_df = pd.DataFrame.from_dict(defines.TEXT_FIELDS_DATA,
                                         orient='index',
                                         columns=['text']).reset_index()

    crud.write_data_database(db_engine,
                            data=text_data_df,
                            table_name=TABLE_NAME + '_text_fields',
                            raw_schema=RAW_SCHEMA_TEXT,
                            use_index=False,
                            upsert_tables=False)

    logger.info('Finished writing data')

   if create_views:
       logger.info("Starting creation of views")
       crud.create_view(engine=db_engine,
                      table_name=TABLE_NAME,
                      show_features_names=show_feature_names_explainability)
      logger.info("Finished creating views")


if __name__ == '__main__':
    # Rem: IP for nrj4 & prod can access Azure db. On nrj4, in /Andres/pl_bi/clients/apox...
    parser = argparse.ArgumentParser()

    parser.add_argument('--create_db', action='store_true', default=False)
    parser.add_argument('--drop_db', action='store_true', default=False)
    parser.add_argument('--recreate_tables', action='store_true', default=False)
    # Replace all db by new data, while keeping view based on the db
    parser.add_argument('--clean_tables', action='store_true', default=False)
    # Only for part of the Data, but will replace with new one if any duplicate exist
    parser.add_argument('--upsert_tables', action='store_true', default=False)
    
    parser.add_argument('--version',
                        action='store',
                        default="V6")
    
    parser.add_argument('--date',
                        action='store',
                        default=None)

    parser.add_argument('--db_name',
                        action='store',
                        default=None)

    parser.add_argument('--horizon',
                        action='store',
                        default=5)
    
    parser.add_argument('--input_file_hi',
                        action='store',
                        default='../data_healthindicator/HEALTH_INDICATORS/MONITOR2_BLENDPROD_V4_1_H5_T5_04_07_2022__15_48_41.csv')
    
    parser.add_argument('--input_file_shap',
                        action='store',
                        default='../data/SHAP_PROD_HISTORIC_BLENDPROD_V4_1_H5_T5_20220510.csv')
    
    parser.add_argument('--input_file_feature',
                        action='store',
                        default='../data/DATA_PROD_HISTORIC_BLENDPROD_V4_1_H5_T5_20220510.csv')
    
    parser.add_argument('--config_db',
                        action='store',
                        default='../config/config_db.cfg')

    parser.add_argument('--mongo_db',
                        nargs='*',
                        default=['database'])
    
    parser.add_argument('--config_mongo_db',
                        action='store',
                        default='../config/config_db_mongo.cfg')


    parser.add_argument('--collections_mongo',
                        nargs='*',
                        default=['futures_eod_de',
                                 'futures_eod_fr',
                                 'futures_eod_it'])

    parser.add_argument('--show_feature_names_explainability',
                        action='store_true',
                        default=True)

    parser.add_argument('--create_views',
                        action='store_true',
                        default=False)

    args = parser.parse_args()
    main(args)
    logger.info("Program finished, Thank you ")