import re
from os.path import join, dirname, abspath
import logging
import yaml
import os

from yaml.reader import Reader

from .aws_secrets import get_secret, get_specific_secret
import jmespath

logger = logging.getLogger(__name__)

REGION = os.getenv(key='AWS_DEFAULT_REGION', default='us-west-2')


class Config(object):
    """
    Config object to facilitate parameter retrieval
    """
    profile_value = os.getenv('CONTEXT_ENV', 'local')
    profile = profile_value or 'local'
    yaml_config = None
    base_url = "http://localhost.com:4000"

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
                    # sometimes (especially when running on Docker?) yaml files get special characters such as
                    # \x00 added. This works around that bug by removing them.
                    full_dirty_yaml = stream.read()
                    full_cleaned_yaml = re.sub(Reader.NON_PRINTABLE, '', full_dirty_yaml)
                    cls.yaml_config = yaml.safe_load(full_cleaned_yaml)
                except yaml.YAMLError as err:
                    logger.exception(err)
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
                    return get_secret(parts_of_key[1], region_name=REGION)
                elif len(parts_of_key) == 3:
                    return get_specific_secret(parts_of_key[2], parts_of_key[1])
            elif value.startswith("cache"):
                parts_of_key = value.split(":")
                secret = cls.handle_secret_fetch_for_cached(parts_of_key[3], parts_of_key[1])
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

    @classmethod
    def handle_secret_fetch_for_cached(cls, secret_key: str, ttl: object):
        secret_value = None
        try:
            response_from_cache = cls.fetch_secret_value_from_cache(secret_key)

            if response_from_cache["is_cache_exists_and_not_expired"] and response_from_cache["secret_value"]:
                secret_value = response_from_cache["secret_value"]
                print("Fetched secret value from the cache server")
                logger.info("Fetched secret value from the cache server")
            else:
                secret_value = get_secret(parts_of_key[2])
                response = cls.write_secret_to_cache(secret_key=parts_of_key[2], secret_value=secret_value_to_write, ttl=ttl)
                print("Fetched the secret value from the AWS secrets instead of Cache!!!")
                logger.info("Fetched the secret value from the AWS secrets instead of Cache!!!")

            if not secret_value:
                raise Exception("Invalid secret value received!")
        except:
            raise Exception("Error occured in fectching the secrets for cached.")
        return secret_value


    @classmethod
    def fetch_secret_value_from_cache(cls, secret_key: str):
        query_param = {"secret_key": secret_key}

        response = requests.get(url = cls.base_url, params=query_param)

        secrets_value = json.loads(response["body"])["secret_value"]

        return secrets_value

    @classmethod
    def write_secret_to_cache(cls, secret_key: str, secret_value: str, ttl: str):
        if ttl[-1] == "m":
            ttl_obj["minutes"] = int(ttl[:-1]) 
        elif ttl[-1] == "s":
            ttl_obj["seconds"] = int(ttl[:-1])
        else:
            ttl_obj = None

        payload = {"secret_key": secret_key, "secret_value": secret_value, "ttl": ttl_obj}

        response = requests.post(url = cls.base_url, data = payload)

        return response
