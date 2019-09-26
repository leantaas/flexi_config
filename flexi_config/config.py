from os.path import join, dirname, abspath
import logging
import yaml
import os
from .aws_secrets import get_secret, get_specific_secret
import jmespath

logger = logging.getLogger(__name__)


class Config(object):
    profile_value = os.getenv('CONTEXT_ENV', 'local')
    profile = profile_value or 'local'
    yaml_config = None

    @classmethod
    def set_config_path(cls, path):
        if os.path.exists:
            yaml_conf_path = abspath(join(
                path,
                f'{cls.profile}-env.yaml'
            ))
            with open(yaml_conf_path, 'r') as stream:
                try:
                    cls.yaml_config = yaml.safe_load(stream)
                except yaml.YAMLError:
                    logger.exception()
        else:
            raise RuntimeError('Invalid path specified')

    @classmethod
    def get(cls, key):
        if not cls.yaml_config:
            raise RuntimeError('Config path must be set with `set_config_path(path)`')
        value = jmespath.search(key, cls.yaml_config)
        if value is not None and isinstance(value, str):
            if value.startswith('aws'):
                parts_of_key = value.split(":")
                if len(parts_of_key) == 2:
                    return get_secret(parts_of_key[1])
                elif len(parts_of_key) == 3:
                    return get_specific_secret(parts_of_key[2], parts_of_key[1])
        elif value is None:
            key_parts = key.split('.')
            for i in range(1, len(key_parts)):
                k = '.'.join(key_parts[:-i])
                v = cls.get(k)
                if v is not None and i < len(key_parts) - 1:
                    value = jmespath.search('.'.join(key_parts[i + 1:]), v)
                    break
            else:
                return None
        return value


