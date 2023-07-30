# -*- coding: utf-8 -*-
"""
################################################################################
#
# File:     defines.py
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

########### PG DB FIELDS ############
DB_PORT = 'db_port'
DB_NAME = 'db_name'
DB_USER = 'db_user'
DB_PASSWORD = 'db_password'
DB_HOST = 'db_host'

DB_URI = "db_uri"

INTERNAL_DB_CONFIG_SECTION_DEFAULT = "DEFAULT"

########## GENIUS_FI DB FIELDS ########

# Definitions for accessing database configuration file
#

CONFIG_FILE_DB_TAG = 'database'
CONFIG_FILE_DB_TYPE = 'type'

CONFIG_FILE_GENIUSFI_DB = 'geniusfi_name'
CONFIG_FILE_PLKBREST_DB = 'plkbrest_name'


COLLECTION_USERS="users"
COLLECTION_API_KEYS="api_keys"

COLLECTION_FINANCE_INFO="finance_info"
COLLECTION_MODELS="models"

GENIUSFI_M_FIELD_TARGET='target'
GENIUSFI_M_FIELD_DEFINEID='definedId'
GENIUSFI_M_FIELD_HORIZON='horizon_target'
GENIUSFI_M_FIELD_CLIENT='client'

PLKBREST_K_FIELD_MODEL="model"

PLKBREST_K_FIELD_MODEL="model"
PLKBREST_K_FIELD_USER="user"
PLKBREST_K_FIELD_KEY="key"

PLKBREST_U_FIELD_USERNAME="username"
PLKBREST_U_FIELD_PASSWORD="password"


############### CONFIG MAIL ######################

MAIL_CONFIG_FILE = 'config/config_mail.cfg'

CONFIG_FILE_MAIL_TAG = 'mail'
CONFIG_FILE_MAIL_ADMIN = 'admin_list'
CONFIG_FILE_MAIL_FORECAST = 'forecast_list'
CONFIG_FILE_MAIL_REPLY_TO = 'reply_to'
CONFIG_FILE_MAIL_KPI_REPORT = 'kpi_list'

# Definitions for accessing SMTP configuration file
#
SMTP_CONFIG_FILE = 'config/config_smtp.cfg'
CONFIG_FILE_SMTP_TAG = 'smtp'
CONFIG_FILE_SMTP_USER = 'user'
CONFIG_FILE_SMTP_PASSWORD = 'password'
CONFIG_FILE_SMTP_SENDER = 'sender'
CONFIG_FILE_SMTP_SERVER = 'server'

# Definitions for accessing SMTP backup configuration file
#
CONFIG_FILE_SMTP_BACKUP_TAG = 'smtp_backup'

################## CONFIG SMS ########################

# Definitions for accessing SMS configuration file
#
SMS_CONFIG_FILE = 'config/config_sms.cfg'

CONFIG_FILE_SMS_TAG = 'mobile'
CONFIG_FILE_SMS_MOBILE = 'mob'
CONFIG_FILE_SMS_FROM = 'from'


################################## GENIUS FINANCE ETL ##################################################################

# Config Tag
CONFIG_FILE_GENIUSFINANCE_TAG='geniusfinance'
# Config Entries
GENIUSFINANCE_USER="user"
GENIUSFINANCE_PASSWORD="password"

GENIUSFINANCE_URL_LOGIN = 'https://finance.predictivelayer.com/auth/signin'
GENIUSFINANCE_URL_LOGOUT = 'https://finance.predictivelayer.com/auth/signout'

GENIUSFINANCE_URL_SHAP = 'https://finance.predictivelayer.com/shap/%s/%s/%s/%s?apiPwd=%s&apiUser=%s&key=%s&start=%s&nb=%s'


################################## DASHBOARD FEATURES ###################################################################

HEALTH_INDICATORS="Daily forecasts health indicators, the lower the better. A health indicator too high alerts on unknown patterns & market conditions."
HEALTH_INDICATORS_2="Red = Abnormal Condition / Orange = Uncertain Condition / Green = Safe Condition"
HEALTH_INDICATORS_EVOLUTION="Health indicators over time"
GROUP_CONTRIBUTIONS="Daily forecasts contributions of all the input data/features, aggregated by groups (positive = bullish, negative = bearish)."
FEATURE_CONTRIBUTIONS="Daily forecasts contributions of all the input data/features, in a selected group (positive = bullish, negative = bearish)."
FEATURE_RANKING="Feature contributions over time. Follow specific market drivers evolution over time."
PERIOD_COMPARISON="Average contributions of features on the Product market evolution over 2 specific periods. Monitor market drivers on desired time windows."
MODEL_EXPLAINABILITY="Historical scatter plot of an input data/feature contribution. X-axis : value of the data/feature. Y-axis : forecast contribution."



TEXT_FIELDS_DATA = {
                 "HEALTH_INDICATORS": HEALTH_INDICATORS,
                 "HEALTH_INDICATORS_2": HEALTH_INDICATORS_2,
                 "HEALTH_INDICATORS_EVOLUTION": HEALTH_INDICATORS_EVOLUTION,
                 "GROUP_CONTRIBUTIONS": GROUP_CONTRIBUTIONS,
                 "FEATURE_CONTRIBUTIONS":FEATURE_CONTRIBUTIONS,
                 "FEATURE_RANKING":FEATURE_RANKING,
                 "PERIOD_COMPARISON":PERIOD_COMPARISON,
                 "MODEL_EXPLAINABILITY":MODEL_EXPLAINABILITY}
