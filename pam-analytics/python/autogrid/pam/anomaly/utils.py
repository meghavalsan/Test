"""Utilities module.

The :mod:`autogrid.pam.util` module contains helpful functions for using other
PAM modules.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import os
import numpy as np
import pandas as pd
from datetime import datetime
import yaml
from pytz import timezone

import autogrid.foundation.util.CacheUtil as cacheutil
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.scada_utils import scada_map, scada_db_map
from autogrid.pam.anomaly.ami_utils import ami_map, ami_db_map
from autogrid.pam.anomaly.tickets_utils import tickets_map, tickets_db_map
from autogrid.pam.anomaly.edna_utils import edna_map, edna_db_map
from autogrid.pam.anomaly.global_define import NS_SEPARATOR, SCADA_NS, EDNA_NS
from autogrid.pam.anomaly.global_define import TICKETS_NS, AMI_NS, SECONDS_IN_HOUR
from autogrid.pam.anomaly.global_define import REDIS_DB, MYSQL_DB

from warnings import filterwarnings
from sqlalchemy import exc as sa_exc


# A spark broadcast variable to store system value
SYSTEM_VAR = None


def __dropna_records(results_list):
    valid_results = []
    for v in results_list:
        if v is not None:
            v = v.dropna(how='any')
            if not v.empty:
                valid_results.append(v)
    return valid_results


def parallel_extract(system, tenant_id, data_type, spark_context, database=REDIS_DB):
    """Load and clean a single CSV file.

    Parameters
    ----------
    system: str
        System name - PAM

    tenant_id: int
        Tenant identification number

    data_type : str
        The type of data files that file_list points to. Available values are
        {'scada', 'ami', 'edna'}.

    spark_context : Spark_context
        Spark context to run Spark job.

    database : str
        The type of database to use (REDIS/MySQL) to get all data. Default is
        REDIS.

    Returns
    -------
    all_results : Pandas DataFrame
       A DataFrame of the anomalies extracted from the entire `file_list`.
    """
    __logger = Logger.get_logger('utils.parallel_extract')

    foundation_home = os.environ.get('FOUNDATION_HOME')
    if foundation_home is None:
        __logger.error('Environment variable FOUNDATION_HOME is not set.')
        raise KeyError('FOUNDATION_HOME')

    # A spark broadcast variable to store system value
    global SYSTEM_VAR
    SYSTEM_VAR = spark_context.broadcast(system)

    settings_yaml = os.path.join(foundation_home, 'settings.yml')
    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    anomaly_hrs = config_dict['anomaly_calculation_hrs']
    scada_cache_hrs = config_dict['scada_cache_hrs']
    ami_cache_hrs = config_dict['ami_cache_hrs']
    edna_cache_hrs = config_dict['edna_cache_hrs']
    tickets_cache_hrs = config_dict['tickets_cache_hrs']

    __logger.debug('Anomaly detect: anomaly_calculation_hrs = ' + str(anomaly_hrs))
    __logger.debug('Anomaly detect: scada_cache_hrs = ' + str(scada_cache_hrs))
    __logger.debug('Anomaly detect: ami_cache_hrs = ' + str(ami_cache_hrs))
    __logger.debug('Anomaly detect: edna_cache_hrs = ' + str(edna_cache_hrs))
    __logger.debug('Anomaly detect: tickets_cache_hrs = ' + str(tickets_cache_hrs))

    anomaly_secs = anomaly_hrs * SECONDS_IN_HOUR
    scada_cache_secs = scada_cache_hrs * SECONDS_IN_HOUR
    ami_cache_secs = ami_cache_hrs * SECONDS_IN_HOUR
    edna_cache_secs = edna_cache_hrs * SECONDS_IN_HOUR
    tickets_cache_secs = tickets_cache_hrs * SECONDS_IN_HOUR

    # Get tenant uid, which we will use to form redis cache keys
    tenant_uid = get_tenant_uid(system, tenant_id)
    if tenant_uid is None:
        raise ValueError("Tenanat UID not present for " + str(tenant_id))
    __logger.debug('Anomaly detect: Evaluating anomalies for tenant = ' + tenant_uid)

    if data_type == 'SCADA':
        scada_ns = tenant_uid + NS_SEPARATOR + SCADA_NS
        scada_key_pattern = '*'

        # Drop all keys from Redis which are older than configured cache hours
        curr_time_in_sec = long(datetime.utcnow().strftime("%s"))
        expire_time = curr_time_in_sec - anomaly_secs - scada_cache_secs - 1
        __logger.debug('Anomaly detect: SCADA: Expire all data older than ' +
                       str(expire_time) + ' seconds.')
        remove_expired_keys(scada_ns, scada_key_pattern, expire_time)

        __logger.debug('Anomaly detect: SCADA: Get list of all valid keys '
                       'from ' + database)
        if database == REDIS_DB:
            # Get all redis keys for SCADA using pattern
            scada_keys = get_keys(scada_ns, scada_key_pattern)
        elif database == MYSQL_DB:
            # Get all unique feeder_id's for SCADA
            scada_keys = get_db_keys(system, tenant_id, 'scada', 'feeder_id')
        else:
            __logger.error('Anomaly detect: Invalid database type %s. '
                           'It should be either REDIS or MySQL' % database)
            raise ValueError('Invalid database type')

        __logger.debug('Anomaly detect: SCADA: Parallelize all keys to '
                       'create multiple RDDs')
        # Create RDD of these keys with parallelize
        keys_rdd = spark_context.parallelize(scada_keys)
        if database == REDIS_DB:
            results_list = keys_rdd.mapPartitions(scada_map).collect()
        else:
            results_list = keys_rdd.mapPartitions(scada_db_map).collect()

        __logger.debug('Anomaly detect: SCADA: Drop all anomaly records with '
                       'null values.')
        all_results = None
        valid_results = __dropna_records(results_list)

        if not all(v is None for v in valid_results):
            all_results = pd.concat(valid_results)

    elif data_type == 'AMI':
        ami_ns = tenant_uid + NS_SEPARATOR + AMI_NS
        ami_key_pattern = '*'

        # Drop all keys from Redis which are older than configured cache hours
        curr_time_in_sec = long(datetime.utcnow().strftime("%s"))
        expire_time = curr_time_in_sec - anomaly_secs - ami_cache_secs - 1
        __logger.debug('Anomaly detect: AMI: Expire all data older than ' +
                       str(expire_time) + ' seconds.')
        remove_expired_keys(ami_ns, ami_key_pattern, expire_time)

        __logger.debug('Anomaly detect: AMI: Get list of all valid keys '
                       'from ' + database)
        if database == REDIS_DB:
            # Get all redis keys for AMI using pattern
            ami_keys = get_keys(ami_ns, ami_key_pattern)
        elif database == MYSQL_DB:
            # Get all unique feeder_id's for AMI
            ami_keys = get_db_keys(system, tenant_id, 'ami', 'feeder_id')
        else:
            __logger.error('Anomaly detect: Invalid database type %s. '
                           'It should be either REDIS or MySQL' % database)
            raise ValueError('Invalid database type')

        __logger.debug('Anomaly detect: AMI: Parallelize all keys to '
                       'create multiple RDDs')
        # Create RDD of these keys with parallelize
        keys_rdd = spark_context.parallelize(ami_keys)
        if database == REDIS_DB:
            results_list = keys_rdd.mapPartitions(ami_map).collect()
        else:
            results_list = keys_rdd.mapPartitions(ami_db_map).collect()

        __logger.debug('Anomaly detect: AMI: Drop all anomaly records '
                       'with null values.')
        all_results = None
        valid_results = __dropna_records(results_list)

        if not all(v is None for v in valid_results):
            all_results = pd.concat(valid_results)

    elif data_type == 'TICKETS':
        tickets_ns = tenant_uid + NS_SEPARATOR + TICKETS_NS
        tickets_key_pattern = '*'

        # Drop all keys from Redis which are older than configured cache hours
        curr_time_in_sec = long(datetime.utcnow().strftime("%s"))
        expire_time = curr_time_in_sec - anomaly_secs - tickets_cache_secs - 1
        __logger.debug('Anomaly detect: TICKETS: Expire all data older than ' +
                       str(expire_time) + ' seconds.')
        remove_expired_keys(tickets_ns, tickets_key_pattern, expire_time)

        __logger.debug('Anomaly detect: TICKETS: Get list of all valid keys '
                       'from ' + database)
        if database == REDIS_DB:
            # Get all redis keys for TICKETS using pattern
            tickets_keys = get_keys(tickets_ns, tickets_key_pattern)
        elif database == MYSQL_DB:
            # Get all unique feeder_id's for TICKETS
            tickets_keys = get_db_keys(system, tenant_id, 'tickets', 'feeder_id')
        else:
            __logger.error('Anomaly detect: Invalid database type %s. '
                           'It should be either REDIS or MySQL' % database)
            raise ValueError('Invalid database type')

        __logger.debug('Anomaly detect: TICKETS: Parallelize all keys to '
                       'create multiple RDDs')
        # Create RDD of these keys with parallelize
        keys_rdd = spark_context.parallelize(tickets_keys)
        if database == REDIS_DB:
            results_list = keys_rdd.mapPartitions(tickets_map).collect()
        else:
            results_list = keys_rdd.mapPartitions(tickets_db_map).collect()

        __logger.debug('Anomaly detect: TICKETS: Drop all anomaly records '
                       'with null values.')
        all_results = None
        valid_results = __dropna_records(results_list)

        if not all(v is None for v in valid_results):
            all_results = pd.concat(valid_results)

    elif data_type == 'EDNA':
        edna_ns = tenant_uid + NS_SEPARATOR + EDNA_NS
        edna_key_pattern = '*'

        # Drop all keys from Redis which are older than configured cache hours
        curr_time_in_sec = long(datetime.utcnow().strftime("%s"))
        expire_time = curr_time_in_sec - anomaly_secs - edna_cache_secs - 1
        __logger.debug('Anomaly detect: EDNA: Expire all data older than ' +
                       str(expire_time) + ' seconds.')
        remove_expired_keys(edna_ns, edna_key_pattern, expire_time)

        __logger.debug('Anomaly detect: EDNA: Get list of all valid keys '
                       'from ' + database)
        if database == REDIS_DB:
            # Get all redis keys for AMI using pattern
            edna_keys = get_keys(edna_ns, edna_key_pattern)
        elif database == MYSQL_DB:
            # Get all unique feeder_id's for EDNA
            edna_keys = get_db_keys(system, tenant_id, 'edna', 'feeder_id')
        else:
            __logger.error('Anomaly detect: Invalid database type %s. '
                           'It should be either REDIS or MySQL' % database)
            raise ValueError('Invalid database type')

        __logger.debug('Anomaly detect: EDNA: Parallelize all keys to '
                       'create multiple RDDs')
        # Create RDD of these keys with parallelize
        keys_rdd = spark_context.parallelize(edna_keys)

        if database == REDIS_DB:
            results_list = keys_rdd.mapPartitions(edna_map).collect()
        else:
            results_list = keys_rdd.mapPartitions(edna_db_map).collect()

        __logger.debug('Anomaly detect: EDNA: Drop all anomaly records '
                       'with null values.')
        all_results = None
        valid_results = __dropna_records(results_list)

        if not all(v is None for v in valid_results):
            all_results = pd.concat(valid_results)
    else:
        raise ValueError("Unknown data_type: %s." % data_type)

    return all_results


def get_keys(name_space, key_pattern):
    """Get list of keys from Redis.

    Parameters
    ----------
    name_space : str
        Name space of Redis key

    key_pattern : str
        Key pattern

    Returns
    -------
    keys_list : list
       list of qualified keys.
    """
    cu = cacheutil.CacheUtil()
    keys_list = cu.keys(name_space, key_pattern)
    return keys_list


def remove_expired_keys(name_space, key_pattern, time_in_sec):
    """Remove Redis zset values which are older than given time.

    Parameters
    ----------
    name_space : str
        Name space of Redis key

    key_pattern : str
        Key pattern

    time_in_sec : long
        Time till which keys needs to be remove

    Returns
    -------
    keys_list : list
       list of qualified keys.
    """
    cu = cacheutil.CacheUtil()

    # Get list of all keys and remove qualified values of each key
    keys = cu.keys(name_space, key_pattern)
    for key in keys:
        tokens = key.split(NS_SEPARATOR)
        feeder_id = tokens[-1]
        namespace = NS_SEPARATOR.join(tokens[:-1])
        cu.zremrangebyscore(namespace, feeder_id, '-inf', time_in_sec)


def get_tenant_uid(system, tenant_id):
    """Get Tenant UID from database.

    Parameters
    ----------
    system : str
        Database name

    tenant_id : int
        Unique tenant id

    Returns
    -------
    tenant_uid : str
       Unique ID string for given tenant.
    """
    __logger = Logger.get_logger('utils.get_tenant_uid')
    db_name = system
    rdbms_util = rdbmsUtil.RDBMSUtil()
    query = "SELECT uid from tenant where id=%s"
    res = rdbms_util.select(db_name, query, (tenant_id))
    if len(res) == 0:
        __logger.error('Anomaly detect: Error : Invalid tenant id : ' + str(tenant_id))
        return None
    return res[0][0]


def get_db_keys(system, tenant_id, table_name, column_name):
    """Get list of unique keys for given tenant from specified column and table.

    Parameters
    ----------
    system : str
        Database name

    tenant_id : int
        Unique tenant id

    table_name : str
        Database table name

    column_name : str
        Column name from given table

    Returns
    -------
    keys_list : list
       list of qualified keys and tenant_id tuple.
    """
    db_name = system
    rdbms_util = rdbmsUtil.RDBMSUtil()
    query = "SELECT DISTINCT " + column_name + " FROM " + table_name + " WHERE tenant_id=%s"
    rows = rdbms_util.select(db_name, query, (tenant_id))
    keys_list = []
    for row in rows:
        keys_list.append((tenant_id, row[0]))
    return keys_list


def save_anomalies_to_db(system, tenant_id, anomaly_df):
    """Save anomaly data in MySQL database.

    Parameters
    ----------
    system : str
        Database name
    tenant_id : int
        Unique tenant id
    anomaly_df : dataframe
        Input data frame that needs to be saved in database.
    """
    __logger = Logger.get_logger('utils.parallel_extract')
    rdbms_util = rdbmsUtil.RDBMSUtil()

    filterwarnings('ignore', category=sa_exc.SAWarning)
    if anomaly_df is not None:
        anomaly_db_col_names = ['anomaly', 'deviceId', 'devicePh', 'deviceType',
                                'feeder', 'signal_id', 'timestamp_utc',
                                'tenant_id', 'isProcessed', 'processedTime_utc']
        anomaly_df['Time'] = anomaly_df['Time'].apply(lambda x: np.datetime64(x))

        # Add additional columns to dataframe
        anomaly_df = anomaly_df.assign(tenant_id=tenant_id)
        anomaly_df = anomaly_df.assign(isProcessed=0)
        anomaly_df = anomaly_df.assign(processedTime_utc=None)

        # Rename dataframe columns as per db Schema
        anomaly_df.rename(columns={'Anomaly': 'anomaly',
                                   'DeviceId': 'deviceId',
                                   'DevicePh': 'devicePh',
                                   'DeviceType': 'deviceType',
                                   'Feeder': 'feeder',
                                   'Signal': 'signal_id',
                                   'Time': 'timestamp_utc'}, inplace=True)

        anomalies_to_insert = pd.DataFrame(columns=anomaly_db_col_names)

        i = 0
        for index, row in anomaly_df.iterrows():    # pylint: disable=W0612
            data_sel_qry = (row['tenant_id'], row['feeder'], row['deviceId'],
                            row['devicePh'], row['deviceType'], row['signal_id'],
                            row['timestamp_utc'])
            sel_qry = "SELECT feeder from anomaly where tenant_id=%s AND " \
                      "feeder=%s AND  deviceId=%s AND devicePh=%s AND " \
                      "deviceType=%s AND signal_id=%s " \
                      "AND timestamp_utc=%s"
            try:
                ret = rdbms_util.select(system, sel_qry, data_sel_qry)
                if not ret:
                    anomalies_to_insert.loc[i] = row
                    i += 1
            except Exception:
                __logger.error('Anomaly detect: Error while quering '
                               'anomalies data from database.')
                raise

        try:
            if not anomalies_to_insert.empty:
                rdbms_util.insert(system, 'anomaly',
                                  anomalies_to_insert.to_dict('records'))
                __logger.debug('Anomaly detect: Saved anomalies in database.')
        except Exception:
            __logger.error('Anomaly detect: Error while saving anomalies '
                           'data to database.')
            raise


def get_timezone(system, tenant_id):
    """Return time zone of given tenant_id.

    Parameters
    ----------
    system : str
        Database name.

    tenant_id : int
        Unique id for tenant.

    Returns
    -------
    tenant_timezone : str
        timezone of input tenant_id.
    """
    __logger = Logger.get_logger("utils.get_timezone")
    try:
        rdbms_util = rdbmsUtil.RDBMSUtil()
        query = "select timezone from tenant where id=%s;"
        rows = rdbms_util.select(system, query, (tenant_id))
        tenant_timezone = rows[0]['timezone']
        return timezone(tenant_timezone)
    except Exception:
        __logger.error("Anomaly detect: Error while getting tenant time_zone "
                       "from MYSQL.")
        raise


def get_unprocessed_anomaly_count(system, tenant_id):
    """Return count of unprocessed anomalies from anomaly table.

    Parameters
    ----------
    system : str
        Database name.

    tenant_id : int
        Unique id for tenant.

    Returns
    -------
    count : int
        Unprocessed anomaly count.
    """
    __logger = Logger.get_logger("utils.get_unprocessed_anomaly_count")
    try:
        rdbms_util = rdbmsUtil.RDBMSUtil()
        query = "select count(*) as anomaly_count from anomaly where " \
                "tenant_id=%s and isProcessed=0;"
        rows = rdbms_util.select(system, query, (tenant_id))
        count = rows[0]['anomaly_count']
        return count
    except Exception:
        __logger.error("Anomaly detect: Error while getting unprocessed "
                       "anomaly count from MYSQL.")
        raise
