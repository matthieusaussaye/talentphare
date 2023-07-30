import logging
from typing import List, Tuple, Union

from loaders.loader import Loader

logger = logging.getLogger(__name__)


class DbLoader(Loader) :
    # Definitions for accessing DB
    DB_CONFIG_FILE = '../config/config_db_mongo.cfg'
    CONFIG_FILE_DB_TAG = "database"
    CONFIG_FILE_DB_TYPE = 'type'
    CONFIG_FILE_MONGO_ETL_DB_TARGET = 'dbname_target'
    CONFIG_FILE_MONGO_ETL_DB_CLIENT = 'dbname_client'

    DEFAULT_PARAMS = [
        CONFIG_FILE_MONGO_ETL_DB_TARGET,
        CONFIG_FILE_MONGO_ETL_DB_CLIENT,
    ]

    def __init__(
        self,
        config_file_path: str = DB_CONFIG_FILE,
        tags: Union[str, List[str]] = CONFIG_FILE_DB_TAG,
        params: List[Union[str, Tuple[str, bool]]] = DEFAULT_PARAMS,
    ) -> None:
        logger.debug(f'{__class__}.__init__ function')
        logger.info(f'Running {__name__}.__init__ function with params: {{'
                    f'config_file_path: {config_file_path}, '
                    f'tags: {tags}, '
                    f'params: {params}}}')
        super().__init__(config_file_path, tags, params)
