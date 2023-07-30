# -*- coding: utf-8 -*-

###############################################################################
#
# File:      apox_futures_power.py
# Project:   Pl_Bi
# Author(s): Andres Montero
# Scope:     file to crete Postgres DB for Apox_futures_power project
#
# Created:       14 July 2021
# Last modified: 14 July 2021
#
# Copyright (c) 2015-2021, Predictive Layer Limited. All Rights Reserved.
#
# The contents of this software are proprietary and confidential to the
# copyright holder. No part of this program may be photocopied, reproduced, or
# translated into another programming language without prior written consent of
# the copyright holder.
#
###############################################################################


import logging
import pandas as pd
from etl import crud
import argparse
import copy

logger = logging.getLogger(__name__)


def add_data_extra_info(data: pd.DataFrame, column: str, new_column: str, DICT_MATCHING: dict) -> pd.DataFrame:
    """

    Args:
        data: data(pd.DataFrame): pandas df
        column: (str) name of current column in the data to use
        new_column: (str) name of the new column in the data
        DICT_MATCHING: (str) dict with the matching of the features

    Returns:
        pandas dataframe with the extra column for the extra treatments
    """

    data[new_column] = data[column].apply(lambda x: DICT_MATCHING.get(x, x))

    return data
