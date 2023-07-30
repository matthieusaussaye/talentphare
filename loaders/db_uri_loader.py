import logging
from typing import List, Tuple, Union

from loaders.loader import Loader

logger = logging.getLogger(__name__)


class DbUriLoader(Loader):
    # Definitions for accessing DB
    DB_URI_CONFIG_FILE = '../config/config_db_uri.cfg'
    CONFIG_FILE_DB_URI_TAG = 'models'
    CONFIG_FILE_DB_URI_URI = 'uri'
    CONFIG_FILE_DB_URI_DATA_READER_MONGO_USER = 'data_reader_env_user'
    CONFIG_FILE_DB_URI_DATA_READER_MONGO_PASSWD = 'data_reader_env_pass'
    CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_USER = 'model_writer_env_user'
    CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_PASSWD = 'model_writer_env_pass'
    CONFIG_FILE_DB_URI_PRODUCTION = 'production'

    DEFAULT_PARAMS = [
        CONFIG_FILE_DB_URI_URI,
        CONFIG_FILE_DB_URI_DATA_READER_MONGO_USER,
        CONFIG_FILE_DB_URI_DATA_READER_MONGO_PASSWD,
        CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_USER,
        CONFIG_FILE_DB_URI_MODEL_WRITER_MONGO_PASSWD,
        (CONFIG_FILE_DB_URI_PRODUCTION, False),
    ]

    def __init__(
        self,
        config_file_path: str = DB_URI_CONFIG_FILE,
        tags: Union[str, List[str]] = CONFIG_FILE_DB_URI_TAG,
        params: List[Union[str, Tuple[str, bool]]] = DEFAULT_PARAMS,
    ) -> None:
        logger.debug(f'{__class__}.__init__ function')
        logger.info(f'Running Loader.__init__ function with params: {{'
                    f'config_file_path: {config_file_path}, '
                    f'tags: {tags}, '
                    f'params: {params}}}')
        super().__init__(config_file_path, tags, params)
