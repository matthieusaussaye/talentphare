# Convention: Always use logger with file name as key
#
import logging
import urllib

from bson.objectid import ObjectId

import requests
from utils import ret, config, defines


# CUSTOMER= 'BRS-PL Trading'
#
# URL= 'http://finance.predictivelayer.com:3100/maintenance/forecast/model/%(id)s?type=cron&user=cron-user&pwd=Cr0n-pwd@2015?'
#
#
#
# PLTAM_URI='http://13.36.117.60:8000/auth/signin'
# MODEL_API_ROUTE=''
# SIGN_IN_ROUTE='/models/api/v2.1/auth/signin'
#
# USER='brs-wmd5k'
# PASSWORD='6ryMbh2TcXUScXa8P4ehS+BolzFFpeNPpiZxjmpo3BM38LzPEu8zkRDI47HFgM2vUeuWBkjQvgpxPNGHEoFpUQ==' 
#
#
# MODEL_ID= -1
# MODEL_PORTFOLIO= -1
#
# MODEL_HORIZON= -2
# MODEL_TARGET= -3
#
# MAX_SCORING = 18
# TEMPO_BETWEEN_SCORE = 35
# Convention: Always use logger with file name as key
#
logger = logging.getLogger('test_genius_fi')

logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL)

URL_LOGIN= 'https://finance.predictivelayer.com/auth/signin'
URL_LOGOUT= 'https://finance.predictivelayer.com/auth/signout'

import datetime
import time

date_test=int(time.mktime(datetime.datetime(2021,6,1).timetuple()))*1000
nb=10

URL_SANITY= 'https://finance.predictivelayer.com/shap/%s/%s/%s/%s?apiPwd=%s&apiUser=%s&key=%s&start=%s&nb=%s'

USER= ''
PASSWORD= ''


LIST_MODELS=[('orange',10,'presentation')]

def sanity_check_geniusfi(dbconfig):
    # Access db configuration
    #
    res = config.get_db_session(dbconfig)
    if res[ret.AUTOMATE_FIELD_CODE]:
        logger.error('Can not get the database configuration.')
        return res
    db_geniusfi = res[ret.AUTOMATE_FIELD_RES][defines.CONFIG_FILE_GENIUSFI_DB]
    db_plkbrest = res[ret.AUTOMATE_FIELD_RES][defines.CONFIG_FILE_PLKBREST_DB]

    # Only create the session when we need it:-)
    #
    session = requests.session()
    payload = {'password': PASSWORD, 'username': USER}
    r = session.post(URL_LOGIN, json=payload)
    if r.status_code != 200:
        msg= r.content
        logger.error('Fails to log in to. Check user/password %s - Msg: %s' % (URL_LOGIN, msg))
        return ret.init_result(code=ret.AUTOMATE_RES_KO)

    for model in LIST_MODELS:
        # Double check that we are dealing with a model that has an AUC
        #
        query_model= {defines.GENIUSFI_M_FIELD_TARGET: model[0], defines.GENIUSFI_M_FIELD_DEFINEID: model[2], defines.GENIUSFI_M_FIELD_HORIZON: model[1]}
        model_entry= db_geniusfi[defines.COLLECTION_MODELS].find_one(query_model)
        # client=model_entry[defines.GENIUSFI_M_FIELD_CLIENT]
        id=model_entry["_id"]
        # Now that we have the model, we need to find the user and keys
        #
        api_key_entry= db_plkbrest[defines.COLLECTION_API_KEYS].find_one({ defines.PLKBREST_K_FIELD_MODEL: id})
        if not api_key_entry:
            logger.warning('No key for model: %s' % id)
            continue
        user= api_key_entry[defines.PLKBREST_K_FIELD_USER]
        zekey= api_key_entry[defines.PLKBREST_K_FIELD_KEY]
        zeid= api_key_entry['_id']

        # Now, try to find the user of the model
        #
        user= db_plkbrest[defines.COLLECTION_USERS].find_one({'_id': user})
        if not user:
            logger.warning('No user for model: %s key:%s' % (id, api_key_entry[defines.PLKBREST_K_FIELD_USER]))
            continue
        username= user[defines.PLKBREST_U_FIELD_USERNAME]
        passwd = urllib.parse.quote_plus(user[defines.PLKBREST_U_FIELD_PASSWORD])

        # So far so good, ready to delete
        #
        url= URL_SANITY % (model[0],model[1],model[2],id,passwd, username, zekey,date_test,nb)
        r = session.get(url)
        if r.status_code != 200:
            logger.warning('Fails to call sanity-check method. Please investigate on id= %s' % id)
            continue
        sanity_check_json=r.json()
    r = session.get(URL_LOGOUT)
    if r.status_code != 200:
        logger.warning('Fails to log out properly. Msg: %s' % r.content)
    else:
        logger.info('Log out of the application')
    return sanity_check_json


if __name__ == '__main__':
    sanity_check_geniusfi(dbconfig="../config/config_db.cfg")
    
    
    