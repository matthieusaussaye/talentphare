import logging
import os
from abc import ABC
from configparser import ConfigParser
from typing import Any, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)


class Loader(ABC):
    def __init__(self, config_file_path: str, tags: Union[str, List[str]],
                 params: List[Union[str, Tuple[str, bool]]]) -> None:
        logger.debug(f'{__class__}.__init__ function')
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f'{config_file_path} not found')
        config_parser = ConfigParser(strict=False)
        config_parser.read(config_file_path)
        self.__config_parser = config_parser
        self.__tags = [tags] if isinstance(tags, str) else tags
        self.__params = params
        self.__set_config()

    def __set_config(self):
        logger.debug(f'{__class__}.__set_config function')
        try:
            self.__config = {}
            for tag in self.tags:
                self.__config[tag] = {}
                for param in self.params:
                    if isinstance(param, str):
                        self.__config[tag].update({
                            param:
                            self.__config_parser.get(tag, param, fallback=None)
                        })
                    else:
                        self.__config[tag].update({
                            param[0]:
                            self.__config_parser.getboolean(tag,
                                                            param[0],
                                                            fallback=param[1])
                        })
        except Exception as e:
            logger.error('Config file not properly configured')
            raise e

    @property
    def config(self) -> Dict:
        logger.debug(f'{__class__}.config property')
        if len(self.tags) == 1:
            return self.__config[self.tags[0]]
        else:
            return self.__config

    @property
    def tags(self, ):
        logger.debug(f'{__class__}.tags property')
        return self.__tags

    @property
    def params(self, ):
        logger.debug(f'{__class__}.params property')
        return self.__params

    def __getitem__(self, __name: str) -> Any:
        logger.debug(f'{__class__}.__getitem__ function')
        return self.config.get(__name)
