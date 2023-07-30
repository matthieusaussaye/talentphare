# create the code to connect to genius finance
# -*- coding: utf-8 -*-

###############################################################################
#
# File:      geniusfi_connector.py
# Project:   Pl_Bi
# Author(s): Andres Montero
# Scope:     file to crete return json from genius finance
#
# Created:       05 Aug 2021
# Last modified: 05 Aug 2021
#
# Copyright (c) 2015-2021, Predictive Layer Limited. All Rights Reserved.
#
# The contents of this software are proprietary and confidential to the
# copyright holder. No part of this program may be photocopied, reproduced, or
# translated into another programming language without prior written consent of
# the copyright holder.
#
###############################################################################


import datetime
import json
import logging
import time
import urllib

import requests

from utils import ret, config, defines

logger = logging.getLogger(__name__)

logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL)


def load_shap_from_model(dbconfig, geniusficonfig, model, date_start, nb_values):
    # First load the config to get db params
    res = config.get_db_session(dbconfig)
    if res[ret.AUTOMATE_FIELD_CODE]:
        logger.error('Can not get the database configuration.')
        return res
    db_geniusfi = res[ret.AUTOMATE_FIELD_RES][defines.CONFIG_FILE_GENIUSFI_DB]
    db_plkbrest = res[ret.AUTOMATE_FIELD_RES][defines.CONFIG_FILE_PLKBREST_DB]
    # Then load the config to get the genius finance params
    res = config.get_geniusfinance_config(geniusficonfig)
    if res[ret.AUTOMATE_FIELD_CODE]:
        logger.error('Can not get the genius finance configuration.')
        return res
    genius_finance_user = res[ret.AUTOMATE_FIELD_RES][defines.GENIUSFINANCE_USER]
    genius_finance_password = res[ret.AUTOMATE_FIELD_RES][defines.GENIUSFINANCE_PASSWORD]

    # Create session to genius finance
    #
    session = requests.session()
    payload = {'password': genius_finance_password, 'username': genius_finance_user}
    r = session.post(defines.GENIUSFINANCE_URL_LOGIN, json=payload)
    if r.status_code != 200:
        msg = r.content
        logger.error('Fails to log in to. Check user/password %s - Msg: %s' % (defines.GENIUSFINANCE_URL_LOGIN, msg))
        return ret.init_result(code=ret.AUTOMATE_RES_KO)
    # Load the model config from geniusfinance db
    query_model = {defines.GENIUSFI_M_FIELD_TARGET: model[0], defines.GENIUSFI_M_FIELD_DEFINEID: model[2],
                   defines.GENIUSFI_M_FIELD_HORIZON: model[1]}
    model_entry = db_geniusfi[defines.COLLECTION_MODELS].find_one(query_model)
    # Get the id of the model
    id = model_entry["_id"]

    # Now that we have the model, we need to find the user and keys
    #
    api_key_entry = db_plkbrest[defines.COLLECTION_API_KEYS].find_one({defines.PLKBREST_K_FIELD_MODEL: id})
    if not api_key_entry:
        logger.warning('No key for model: %s' % id)
    user = api_key_entry[defines.PLKBREST_K_FIELD_USER]
    zekey = api_key_entry[defines.PLKBREST_K_FIELD_KEY]
    zeid = api_key_entry['_id']

    # Now, try to find the user of the model
    #
    user = db_plkbrest[defines.COLLECTION_USERS].find_one({'_id': user})
    if not user:
        logger.warning('No user for model: %s key:%s' % (id, api_key_entry[defines.PLKBREST_K_FIELD_USER]))
    username = user[defines.PLKBREST_U_FIELD_USERNAME]
    passwd = urllib.parse.quote_plus(user[defines.PLKBREST_U_FIELD_PASSWORD])

    # So far so good, ready to query the SHAP
    # We first need to timestamp the datetime in the js format
    timestamp_datetime = int(time.mktime(date_start.timetuple())) * 1000
    url = defines.GENIUSFINANCE_URL_SHAP % (
    model[0], model[1], model[2], id, passwd, username, zekey, timestamp_datetime, nb_values)

    # Call the SHAP route
    r = session.get(url)
    if r.status_code != 200:
        logger.warning('Fails to call shap method. Please investigate on id= %s' % id)
    shap_result = r.json()

    # Log out and ensure the session is closed
    r = session.get(defines.GENIUSFINANCE_URL_LOGOUT)
    if r.status_code != 200:
        logger.warning('Fails to log out properly. Msg: %s' % r.content)
    else:
        logger.info('Log out of the application')

    return shap_result


if __name__ == '__main__':
    data_json = load_shap_from_model(dbconfig="../config/config_db.cfg", geniusficonfig="../config/config_geniusfi.cfg")
    with open('data_genius_finance.json', 'w', encoding='utf-8') as f:
        json.dump(data_json, f, ensure_ascii=False, indent=4)
