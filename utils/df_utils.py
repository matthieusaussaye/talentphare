# -*- coding: utf-8 -*-
"""
################################################################################
#
# File:     df_utils.py
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
"""
import sys

sys.path.insert(0, '..')

import pandas as pd
import numpy as np
import logging
import logging.config
from etl import crud

DICT_MATCHING_NAMES = {'name': 'product'}
DATE_COL = 'od'


logger = logging.getLogger(__name__)

def filtering_ds(data_mongo_concatenated,
                 country_codes=['DE','FD','F7'],
                 load=['B'],
                 period_codes = ['Q','Y','M'],
                 years = [19,20,21,22,23,24,25,26],
                 price_type = 's') :
    
    """
    Filter the concatened mongo df by country_codes, load (peak/based), period_codes and years.

    Returns:
        pd.Dataframe: the filtered dataframe
    """
    
    products = [c+l+p for c in country_codes for l in load for p in period_codes]
    years = [str(x) for x in years]
    products_filter = '|'.join(products)
    years_filter = '|'.join(years)

    data_mongo_concatenated = data_mongo_concatenated.pivot_table(columns="product",
                                                                  values=price_type,
                                                                  index='date')

    data_mongo_filtered = data_mongo_concatenated.filter(regex=products_filter).filter(regex=years_filter)

    return data_mongo_filtered


def compute_target(data_mongo_filtered,
                   horizon=5) : 
    """
    Return the target for a given horizon and filtered price data.

    Args:
        data_mongo_filtered (pd.DataFrame): filtered prices
        horizon (int, optional): horizon of forecasting, default to 5.
    
    Return:
        target (pd.Dataframe): price difference between the price in "horizon" days and today.
    """
    

    def diff(x):
        return x.iloc[-1] - x.iloc[0]

    w=horizon+1
    
    data_mongo_copy = data_mongo_filtered.copy()
    
    for col in data_mongo_filtered.columns :
        data_mongo_copy_target = data_mongo_copy[[col]].rolling(window=w).apply(diff).shift(-horizon)
        data_mongo_copy_target = data_mongo_copy_target[[col]].rename(columns={col:col+"_target"})
        data_mongo_copy = pd.concat([data_mongo_copy, data_mongo_copy_target],axis=1)

    return data_mongo_copy

def transform_price(data) :
    """
    Transform the price df

    Args:
        data (pd.Dataframe): price df
    
    Returns:
        The transformed price.
    """
    
    final_prices = data.copy().unstack().reset_index()
    final_prices.columns = ['product','date','price']
    
    return final_prices

def transform_target(data):
    """transform the target df

    Args:
        data (pd.Dataframe): targets df
    
    Returns:
        The transformed dataframe.
    """
    
    data_targets = data.copy()
    
    data_targets = data_targets.copy().filter(regex='_target').copy()
    logger.info('filtering')

    data_targets = data_targets.copy().unstack().reset_index()
    logger.info('transforming')

    data_targets.columns = ['product','date','target']
    data_targets['product'] = data_targets['product'].apply(lambda x: x.replace("_target", ""))
    data_targets['date'] = pd.to_datetime(data_targets['date'])
    
    return data_targets


def adding_columns(dataset : pd.DataFrame) -> pd.DataFrame :
    """
    Adding country, periods, months, year of contracts & a boolean that is equal to:
        sign_shap*sign_target / 2
    Args:
        dataset (pd.DataFrame): The full dataframe with price & target.

    Returns:
        full_data (pd.DataFrame): The full dataframe with price, target and added columns.
    """
    full_data = dataset.copy()
    
    full_data['country'] = full_data['product'].apply(lambda x: x[:3] if x[-2:]!='M1' and x[-2:]!='Q1' else 'other')
    full_data['periods'] = full_data['product'].apply(lambda x: x[3])
    full_data['months'] = full_data['product'].apply(lambda x: x[4])
    full_data['year'] = full_data['product'].apply(lambda x: int(x[-2:]) if x[-2:]!='M1' and x[-2:]!='Q1' else 0)
    full_data['feature_value'] = full_data['feature_value'].astype(float)
    full_data['feature_is_right'] = (np.sign(full_data['target'])*np.sign(full_data['feature_value'])+1)/2

    gp = full_data.groupby(by=['country','periods','year'])['feature_is_right']
    full_data['cum_success'] =  gp.cumsum()/gp.cumcount()
    
    return full_data



# FETCH FILE, CLEAN & PIVOT, BUILD HI DATA
def prepare_hi_data(input_file: str) -> pd.DataFrame:
    """
    Args:
        input_file: path to reading the csv file

    Returns:
        pandas dataframe with data ready to be written to the DB
    """
    # FETCH FILE ======================================================
    # Get data from CSV file to DataFrame(Pandas)
    logger.info('Reading Data')
    data = pd.read_csv(input_file)
    data.drop(data.filter(regex='Unnamed').columns, axis=1, inplace=True)

    # CLEANING (SELECT & RENAME) ========================================
    
    # DROP USELESS
    data = data[['od','name','AV_CHI2_PCA95','AV_CHI2_PCA80','AV_CHI2W','AV_CHI2','AV_OOR']]
    data.set_index(DATE_COL, inplace=True)
    # RENAMING COLUMNS
    logger.info('Renaming Features')
    for key, value in DICT_MATCHING_NAMES.items():
        data.rename(columns={key: value}, inplace=True)

    # Check that the dataframe is not empty
    if data.shape[0] == 0:
        logger.error("Data in pivot_table is empty!")
        raise Exception("Data in pivot_table is empty!")

    # Pivot the data
    data.reset_index(inplace=True)
    data.rename(columns={'od': 'date'}, inplace=True)
    data.sort_values(by=['date', 'product'], inplace=True)
    data.reset_index(inplace=True, drop=True)

    logger.debug(f'The columns of the dataframe are {data.columns}')

    return data