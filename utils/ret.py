# -*- coding: utf-8 -*-
################################################################################
#
# File:     ret.py
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


AUTOMATE_FIELD_CODE= 'code'
AUTOMATE_FIELD_MSG= 'msg'
AUTOMATE_FIELD_RES= 'res'

AUTOMATE_RES_OK= 0
AUTOMATE_RES_KO= 1
AUTOMATE_RES_NO_FORECASTS= 2
AUTOMATE_RES_NO_AVAIL_FORECAST= 3


#=======================================================================================================================
def init_result(code=AUTOMATE_RES_OK,  msg='', ret= None):
    # Simple structure that can be used for reporting results between different function calls
    #
    result= {}
    result[AUTOMATE_FIELD_CODE]= code
    if type(msg) is list:
       result[AUTOMATE_FIELD_MSG]= msg
    else:
       result[AUTOMATE_FIELD_MSG]= [msg]
    result[AUTOMATE_FIELD_RES]= ret
    return result
