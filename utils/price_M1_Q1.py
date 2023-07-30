# -*- coding: utf-8 -*-
"""
################################################################################
#
# File:     price_M1_Q1.py
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
import copy

import pandas as pd
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import DateOffset
import logging
import logging.config
from etl import crud
logger = logging.getLogger(__name__)


def add_dd(price_target : pd.DataFrame,
           dict_month : dict,
           type : str ='power'):
    """Add delivery date column

    Args:
        price_target (pd.Dataframe): dataframe with the prices and the target
        dict_month (dict): dict with the finance futures calendar month correspondance

    Returns:
        df_temp: dataframe with the price and the delivery date
    """
    
    df_temp = price_target
    if type == 'gas' :
        df_temp.loc[:,'dd'] = df_temp["product"].apply(lambda x : datetime(int("20"+x[-2:]), dict_month[x[3]], 1))
    elif type == 'crude' :
        df_temp.loc[:,'dd'] = df_temp["product"].apply(lambda x : datetime(int("20"+x[-2:]), dict_month[x[3]], 1))
    else : 
        df_temp.loc[:,'dd'] = df_temp["product"].apply(lambda x : datetime(int("20"+x[-2:]), dict_month[x[4]], 1))

    return (df_temp)
    
    
    
def nb_months_column(df_temp : pd.DataFrame) :
    """Compute the nb of months between the two columns

    Args:
        df_temp (pd.Dataframe): _description_
    """
    df_temp.loc[:,'nb_months'] = 12 * (df_temp.dd.dt.year - df_temp.date.dt.year) + (df_temp.dd.dt.month - df_temp.date.dt.month)
    return df_temp

def compute_M1_M2(data_mongo_filtered,
                  type='power'):
    """Add M1 bool & M2 bool  (Q1, Q2) to the dataframe

    Args:
        data_mongo_filtered (df): filtered prices and target from mongo

    Returns:
        df_temp: dataframe with boolean M1, M2, Q1, Q2
    """
    
    df_temp = data_mongo_filtered
    df_temp = nb_months_column(df_temp)

    if type == 'gas' :
        df_temp.loc[:,'is_M1'] = df_temp.apply(lambda x : (x['nb_months']==1), axis=1)
        df_temp.loc[:,'is_M2'] = df_temp.apply(lambda x : (x['nb_months']==2), axis=1)
        df_temp.loc[:,'is_M3'] = df_temp.apply(lambda x : (x['nb_months']==3), axis=1)
        df_temp.loc[:,'is_M4'] = df_temp.apply(lambda x : (x['nb_months']==4), axis=1)
        df_temp.loc[:,'is_M5'] = df_temp.apply(lambda x : (x['nb_months']==5), axis=1)

    elif type == 'crude' :
        df_temp.loc[:,'is_M2'] = df_temp.apply(lambda x : (x['nb_months']==2), axis=1)
        df_temp.loc[:,'is_M3'] = df_temp.apply(lambda x : (x['nb_months']==3), axis=1)
        df_temp.loc[:,'is_M4'] = df_temp.apply(lambda x : (x['nb_months']==4), axis=1)
        df_temp.loc[:,'is_M5'] = df_temp.apply(lambda x : (x['nb_months']==5), axis=1)

    else :
        df_temp.loc[:,'is_M1'] = df_temp.apply(lambda x : (x['product'][3]=='M') & (x['nb_months']==1), axis=1)
        df_temp.loc[:,'is_M2'] = df_temp.apply(lambda x : (x['product'][3]=='M') & (x['nb_months']==2), axis=1)

        df_temp.loc[:,'is_Q1'] = df_temp.apply(lambda x : (x['product'][3]=='Q') & (x['nb_months']<=3), axis=1)
        df_temp.loc[:,'is_Q2'] = df_temp.apply(lambda x : (x['product'][3]=='Q') & (x['nb_months']>3) & (x['nb_months']<=6), axis=1)

        df_temp.loc[:,'is_Y1'] = df_temp.apply(lambda x : (x['product'][3]=='Y') & (x['nb_months']<=12), axis=1)
        df_temp.loc[:,'is_Y2'] = df_temp.apply(lambda x : (x['product'][3]=='Y') & (x['nb_months']>12) & (x['nb_months']<=24), axis=1)
    
    return (df_temp)


def build_BDs2DD(df, date='date', dd='dd'):
    """Build the delivery date in business day

    Args:
        df (pd.DataFrame): dataframe with the prices
        date (str, optional): date. Defaults to 'date'.
        dd (str, optional): delivery date. Defaults to 'dd'.

    Returns:
        _type_: _description_
    """
    dftmp = copy.deepcopy(df[[date, dd]])
    dftmp.loc[:,'bd2dd'] = dftmp.apply(
        lambda x: np.busday_count(datetime.date(x[date]), datetime.date(x[dd])) - 1, axis=1)
    return dftmp['bd2dd']

def add_months(date, x) : 
    """Add one month

    Args:
        date (datetype): date

    Returns:
        date + one month
    """
    return date+DateOffset(months=x)

def rolling_dates(df : pd.DataFrame, horizon : int, safety=int(5), date='date', type = 'power'):
    """Compute the rolling dates booleans

    Args:
        df (pd.Dataframe): dataframe with the prices & targets
        horizon (_type_): horizon of forecast
        safety (_type_, optional): safety days. Defaults to int(5).
        date (str, optional): date of the price. Defaults to 'date'.

    Returns:
        dftmp: dataframe with the prices and the boolean of M1, Q1.
    """

    dftmp = copy.deepcopy(df[[date]])
    
    # modify M2 with delivery<=Horizon BD + Rolling to M1 ================================
    # 1st Step : Build dd of M1 for all dates
    dftmp.loc[:,'ddM1'] = dftmp[date].apply(lambda x : add_months(datetime(x.year, x.month, 1), 1) )
    # 2nd Step : Build BDs left to ddM1
    dftmp.loc[:,'bd2ddM1'] = dftmp.apply(
        lambda x: np.busday_count(datetime.date(x[date]), datetime.date(x['ddM1'])) - 1, axis=1)
    # 3rd Step : Build bool " is this day a roll for month"
    dftmp.loc[:,'is_roll_M'] =  dftmp['bd2ddM1'] <= horizon + safety

    if type == 'gas' or type == 'crude':
        return dftmp['is_roll_M']
    else :
        # 4th Step : Build bool " is this month a roll for quarter"
        dftmp.loc[:,'is_M_roll_Q'] =  dftmp[date].apply(lambda x : x.month%3 == 0)
        dftmp.loc[:,'is_roll_Q'] = dftmp['is_roll_M'] & dftmp['is_M_roll_Q']
        # 4th Step : Build bool " is this month a roll for year"
        dftmp.loc[:,'is_M_roll_Y'] =  dftmp[date].apply(lambda x : x.month%12 == 0)
        dftmp.loc[:,'is_roll_Y'] = dftmp['is_roll_M'] & dftmp['is_M_roll_Y']
        
        return dftmp['is_roll_M'], dftmp['is_roll_Q'], dftmp['is_roll_Y']
        

# ================================================
# EXTRA OPTION : BUILD M1 & Q1 PROD FOR SHAP + RM BD2DD < HORIZON + 2
def add_M1_Q1_to_price_target_table(price_target : pd.DataFrame,safety : int = int(5), horizon : int = 5, type : str = "power") :
    """Final function that map to M1 & Q1 prices/target table

    Args:
        price_target (pd.DataFrame): price target table
        safety (int, optional): safety days. Defaults to int(5).
        horizon (int, optional): horizon of forecast. Defaults to 5.

    Returns:
        price_target (pd.DataFrame): price target table with M1 & Q1 
    """

    
    months = ["F","G","H","J","K","M","N","Q","U","V","X","Z"]
    dict_month={e:i+1 for i,e in enumerate(months)}
    
    logger.info('Adding delivery date to price_target')
    
    price_target_dd = add_dd(price_target=price_target,
                             dict_month=dict_month,
                             type=type)
    
    logger.info(price_target_dd)
    
    price_target_dd_M1_M2 = compute_M1_M2(data_mongo_filtered=price_target_dd,
                                          type=type)

    logger.info("price_target_dd_M1_M2")

    logger.info(price_target_dd_M1_M2)

    bool_keep = build_BDs2DD(price_target_dd_M1_M2) > horizon + int(2)
    ind_extra = copy.deepcopy(price_target_dd_M1_M2.loc[bool_keep])

    # modify M2 with delivery<=Horizon BD + Rolling to M1 ================================
    if type == 'gas' or type == 'crude':
        indM_roll = rolling_dates(ind_extra,
                                  horizon=horizon,
                                  safety=safety,
                                  type=type)
    else :
        indM_roll, indQ_roll, indY_roll = rolling_dates(ind_extra,horizon=horizon,safety=safety,type=type)
    
    if type == 'gas' :
        # ROLL M1, M2 & M3 ==================================================================
        ind_extra.loc[ind_extra['is_M1'] & indM_roll, 'is_M1'] = False
        ind_extra.loc[ind_extra['is_M2'] & indM_roll, 'is_M1'] = True
        ind_extra.loc[ind_extra['is_M2'] & indM_roll, 'is_M2'] = False

        ind_extra.loc[ind_extra['is_M3'] & indM_roll, 'is_M2'] = True
        ind_extra.loc[ind_extra['is_M3'] & indM_roll, 'is_M3'] = False
        
        ind_extra.loc[ind_extra['is_M4'] & indM_roll, 'is_M3'] = True
        ind_extra.loc[ind_extra['is_M4'] & indM_roll, 'is_M4'] = False
        
        ind_extra.loc[ind_extra['is_M5'] & indM_roll, 'is_M4'] = True
        ind_extra.loc[ind_extra['is_M5'] & indM_roll, 'is_M5'] = False
    elif type == 'crude' :
        ind_extra.loc[ind_extra['is_M2'] & indM_roll, 'is_M2'] = False
        ind_extra.loc[ind_extra['is_M3'] & indM_roll, 'is_M2'] = True
        ind_extra.loc[ind_extra['is_M3'] & indM_roll, 'is_M3'] = False

        ind_extra.loc[ind_extra['is_M4'] & indM_roll, 'is_M3'] = True
        ind_extra.loc[ind_extra['is_M4'] & indM_roll, 'is_M4'] = False

        ind_extra.loc[ind_extra['is_M5'] & indM_roll, 'is_M4'] = True
        ind_extra.loc[ind_extra['is_M5'] & indM_roll, 'is_M5'] = False
    else :
        # ROLL M1, M2 & M3 ==================================================================
        ind_extra.loc[ind_extra['is_M1'] & indM_roll, 'is_M1'] = False
        ind_extra.loc[ind_extra['is_M2'] & indM_roll, 'is_M1'] = True
        ind_extra.loc[ind_extra['is_M2'] & indM_roll, 'is_M2'] = False

        # ROLL Q1, Q2 & Q3 ==================================================================
        ind_extra.loc[ind_extra['is_Q1'] & indQ_roll, 'is_Q1'] = False
        ind_extra.loc[ind_extra['is_Q2'] & indQ_roll, 'is_Q1'] = True
        ind_extra.loc[ind_extra['is_Q2'] & indQ_roll, 'is_Q2'] = False
        # ROLL Y1, Y2 & Y3 ==================================================================
        ind_extra.loc[ind_extra['is_Y1'] & indY_roll, 'is_Y1'] = False
        ind_extra.loc[ind_extra['is_Y2'] & indY_roll, 'is_Y1'] = True
        ind_extra.loc[ind_extra['is_Y2'] & indY_roll, 'is_Y2'] = False

    # NAMING M1, Q1  ==================================================================
    if type == 'gas' :
        df_M1 = copy.deepcopy(ind_extra[ind_extra['is_M1']==True])
        df_M1.loc[:,'product'] = df_M1['product'].str[:2]+"BM1"

        df_M2 = copy.deepcopy(ind_extra[ind_extra['is_M2']==True])
        df_M2.loc[:,'product']= df_M2['product'].str[:2]+"BM2"
        
        df_M3 = copy.deepcopy(ind_extra[ind_extra['is_M3']==True])
        df_M3.loc[:,'product'] = df_M3['product'].str[:2]+"BM3"
        
        df_M4 = copy.deepcopy(ind_extra[ind_extra['is_M4']==True])
        df_M4.loc[:,'product'] = df_M4['product'].str[:2]+"BM4"
    elif type == 'crude':
        df_M2 = copy.deepcopy(ind_extra[ind_extra['is_M2'] == True])
        df_M2.loc[:,'product'] = df_M2['product'].str[:3] + "M2"

        df_M3 = copy.deepcopy(ind_extra[ind_extra['is_M3'] == True])
        df_M3.loc[:,'product'] = df_M3['product'].str[:3] + "M3"

        df_M4 = copy.deepcopy(ind_extra[ind_extra['is_M4'] == True])
        df_M4.loc[:,'product'] = df_M4['product'].str[:3] + "M4"
    else :
        df_M1 = copy.deepcopy(ind_extra[ind_extra['is_M1']==True])
        df_M1.loc[:,'product'] = df_M1['product'].str[:3]+"M1"
        
        df_Q1 = copy.deepcopy(ind_extra[ind_extra['is_Q1']==True])
        df_Q1.loc[:,'product'] = df_Q1['product'].str[:3]+"Q1"
        
        df_Y1 = copy.deepcopy(ind_extra[ind_extra['is_Y1']==True])
        df_Y1.loc[:,'product'] = df_Y1['product'].str[:3]+"Y1"
    
    # CONCATENATION ===========
    if type == 'gas' :
        df_M1234 = pd.concat([df_M1, df_M2, df_M3, df_M4], ignore_index=True, sort=False)
        price_target = pd.concat([df_M1234,ind_extra])
        price_target.drop(['dd','is_M1','is_M2','is_M3','is_M4','is_M5','nb_months'], inplace=True, axis=1)
    elif type == 'crude' :
        df_M1234 = pd.concat([df_M2, df_M3, df_M4], ignore_index=True, sort=False)
        price_target = pd.concat([df_M1234,ind_extra])
        price_target.drop(['dd','is_M2','is_M3','is_M4','is_M5','nb_months'], inplace=True, axis=1)
    else :
        df_MQY1 = pd.concat([df_Q1, df_M1, df_Y1], ignore_index=True, sort=False)
        price_target = pd.concat([df_MQY1,ind_extra])
        price_target.drop(['dd','is_M1','is_M2','is_Q1','is_Q2','is_Y1','is_Y2','nb_months'], inplace=True, axis=1)
        
        logger.info('price_target après rolling')
        logger.info(price_target.loc[price_target['product'] == 'DEBM1'].sort_values(by='date'))

    return price_target