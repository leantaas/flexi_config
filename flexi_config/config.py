from os.path import join, dirname, abspath
import logging
import yaml
import os
from .aws_secrets import get_secret, get_specific_secret
import jmespath

logger = logging.getLogger(__name__)


class Config(object):
    """
    Config object to facilitate parameter retrieval
    """
    profile_value = os.getenv('CONTEXT_ENV', 'local')
    profile = profile_value or 'local'
    yaml_config = None
    base_url = "http://sandbox.com:4000"

    @classmethod
    def set_config_path(cls, path):
        """
        Reads the appropriate YAML file at the given path into a class attribute.
        :param path: Directory used to look for YAML config file
        """
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
        """
        Retrieve configuration parameter using JMESPath key
        :param key: JMESPath string like `database.airflow_db.username`
        :return: Config value for given key
        :rtype: str
        """
        if not cls.yaml_config:
            raise RuntimeError("Config path must be set with `set_config_path(path)`")
        value = jmespath.search(key, cls.yaml_config)
        if value is not None and isinstance(value, str):
            if value.startswith('aws'):
                parts_of_key = value.split(":")
                if len(parts_of_key) == 2:
                    return get_secret(parts_of_key[1])
                elif len(parts_of_key) == 3:
                    return get_specific_secret(parts_of_key[2], parts_of_key[1])
            elif value.startswith("cache"):
                parts_of_key = value.split(":")
                secret = self.handle_secret_fetch_for_cached(parts_of_key[3], parts_of_key[1])
                if len(parts_of_key) == 4: 
                    return secret
                elif len(parts_of_key) == 5:
                    return secret.get(parts_of_key[4])
        elif value is None:
            key_parts = key.split(".")
            for i in range(1, len(key_parts)):
                k = ".".join(key_parts[:-i])
                v = cls.get(k)
                if v is not None and i < len(key_parts) - 1:
                    value = jmespath.search(".".join(key_parts[i + 1 :]), v)
                    break
            else:
                return None
        return value

    def handle_secret_fetch_for_cached(self, secret_key: str, ttl: object):
        secret_value = None
        try:
            response_from_cache = self.fetch_secret_value_from_cache(secret_key)

            if response_from_cache["is_cache_exists_and_not_expired"] and response_from_cache["secret_value"]:
                secret_value = response_from_cache["secret_value"]
            else:
                secret_value = get_secret(parts_of_key[2])
                response = self.write_secret_to_cache(secret_key=parts_of_key[2], secret_value=secret_value_to_write, ttl=ttl)

            if not secret_value:
                raise Exception("Invalid secret value received!")
        except:
            raise Exception("Error occured in fectching the secrets for cached.")
        return secret_value


    def fetch_secret_value_from_cache(self, secret_key: str):
        query_param = {"secret_key": secret_key}

        response = requests.get(url = self.base_url, params=query_param)

        secrets_value = json.loads(response["body"])["secret_value"]

        return secrets_value

    def write_secret_to_cache(self, secret_key: str, secret_value: str, ttl: object):
        payload = {"secret_key": secret_key, "secret_value": secret_value, "ttl": ttl}

        response = requests.post(url = self.base_url, data = payload)

        return response
