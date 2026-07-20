# We import configparser to create and read user-editable configuration files.
# We can change application behavior without altering the main code
# the module will set the properties and
# configuration of the spark applicaation
# depending on which environment is actively running

import configparser
import os
from pyspark import SparkConf


def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}

    for key, val in config.items(env):
        conf[key] = val

    # Match exact variable names from your .env file
    if env.upper() == "LOCAL":
        api_key = os.environ.get("CONFLUENT_DEV_API_KEY", "")
        api_secret = os.environ.get("CONFLUENT_DEV_API_SECRET", "")
    else:
        # Defaults to QA / PROD keys
        api_key = os.environ.get("CONFLUENT_API_KEY", "")
        api_secret = os.environ.get("CONFLUENT_API_SECRET", "")

    # Dynamically inject credentials into jaas.config template
    if "kafka.sasl.jaas.config" in conf:
        conf["kafka.sasl.jaas.config"] = conf["kafka.sasl.jaas.config"].format(
            api_key, api_secret
        )

    return conf


def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for key, val in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
