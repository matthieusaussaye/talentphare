
import logging
import os
import re

from pymongo import MongoClient

from loaders.db_loader import DbLoader
from loaders.db_uri_loader import DbUriLoader

logger = logging.getLogger(__name__)

def password_obfuscator(uri):
    """
    Small utility function to avoid printing password in db url when provided
    :param uri:
    :return: the obfuscated string
    """
    # Markers for password
    #
    start = ':'
    end = '@'
    result = re.search('\://(.*)%s(.*)%s' % (start, end), uri)
    if result is not None:
        if len(result.groups()) >= 2:
            passwd = result.group(2)
            uri = uri.replace(passwd, 'X' * len(passwd))
    return uri

class DbSessionLoader:
    def __init__(self,
                 db_loader: DbLoader = None,
                 db_uri_loader: DbUriLoader = None) -> None:
        logger.debug(f'{__class__}.__init__ function')
        if db_loader is None:
            db_loader = DbLoader()
        if not isinstance(db_loader, DbLoader):
            raise ValueError(f'db_loader param must be DbLoader instance')
        if db_uri_loader is None:
            db_uri_loader = DbUriLoader()
        if not isinstance(db_uri_loader, DbUriLoader):
            raise ValueError(
                f'db_uri_loader param must be DbUriLoader instance')
        self.db_loader = db_loader
        self.db_uri_loader = db_uri_loader
        self.init_env_values()
        self.set_sessions()

    def init_env_values(
            self,
            data_reader_env_user=DbUriLoader.
                CONFIG_FILE_DB_URI_DATA_READER_MONGO_USER,
            data_reader_env_pass=DbUriLoader.
                CONFIG_FILE_DB_URI_DATA_READER_MONGO_PASSWD,
            model_writer_env_user=DbUriLoader.
                CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_USER,
            model_writer_env_pass=DbUriLoader.
                CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_PASSWD
    ) -> None:
        logger.debug(f'{__class__}.init_env_values function')
        for attr, env in [
            ("__data_reader_env_user", data_reader_env_user),
            ("__data_reader_env_pass", data_reader_env_pass),
            ("__model_writer_env_user", model_writer_env_user),
            ("__model_writer_env_pass", model_writer_env_pass),
        ]:
            env_var = self.db_uri_loader[env]
            value = os.environ.get(env_var)
            setattr(self, "_" + self.__class__.__name__ + attr, value)
            if value is None:
                logger.warning(
                    f'{attr} environment var not set, need to define in your environment {env_var}.'
                )
            else:
                logger.info(
                    f'{attr} environment var successfully loaded, using environment {env_var} var.'
                )

    def get_db_uri(self,
                   target: bool = False,
                   write_permission: bool = False) -> str:
        logger.debug(f'{__class__}.get_db_uri function')
        production = self.is_production()
        uri = self.get_uri()
        if target and production:
            return f'mongodb://localhost:27017'
        elif 'replicaSet' in uri:
            if write_permission:
                return uri % (self.__model_writer_env_user,
                              self.__model_writer_env_pass)
            else:
                return uri % (self.__data_reader_env_user,
                              self.__data_reader_env_pass)
        return f'{uri}'

    def get_uri(self) -> str:
        logger.debug(f'{__class__}.get_uri function')
        return self.db_uri_loader[DbUriLoader.CONFIG_FILE_DB_URI_URI]

    def is_production(self) -> bool:
        logger.debug(f'{__class__}.is_production function')
        return self.db_uri_loader[DbUriLoader.CONFIG_FILE_DB_URI_PRODUCTION]

    def set_sessions(self):
        logger.debug(f'{__class__}.set_sessions function')
        self.set_db_client_sessions()
        self.set_db_target_sessions()

    @property
    def client_db(self, ):
        logger.debug(f'{__class__}.client_db property')
        return self.db_loader[DbLoader.CONFIG_FILE_MONGO_ETL_DB_CLIENT]

    @property
    def target_db(self, ):
        logger.debug(f'{__class__}.target_db property')
        return self.db_loader[DbLoader.CONFIG_FILE_MONGO_ETL_DB_TARGET]

    def set_db_client_sessions(self, ) -> None:
        logger.debug(f'{__class__}.set_db_client_sessions function')
        uri = self.get_db_uri(write_permission=False)
        print(password_obfuscator(uri))
        mongo_client = MongoClient(uri)
        db = mongo_client[self.client_db]
        self.__client_session = MongoClient(
            self.get_db_uri(write_permission=False))[self.client_db]
        self.__client_write_session = MongoClient(
            self.get_db_uri(write_permission=True))[self.client_db]

    def set_db_target_sessions(self, ) -> None:
        logger.debug(f'{__class__}.set_db_target_sessions function')
        self.__target_session = MongoClient(self.get_db_uri(
            True, False))[self.target_db]
        self.__target_write_session = MongoClient(self.get_db_uri(
            True, True))[self.target_db]

    def get_db_client_session(self,
                              write_permission: bool = False) -> MongoClient:
        logger.debug(f'{__class__}.get_db_client_session function')
        return self.__client_write_session if write_permission else self.__client_session

    def get_db_target_session(self,
                              write_permission: bool = False) -> MongoClient:
        logger.debug(f'{__class__}.get_db_target_session function')
        return self.__target_write_session if write_permission else self.__target_session

