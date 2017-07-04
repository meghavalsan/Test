"""AMI Utilities module.

The :mod:`autogrid.pam.ami_utils` module contains ami functions for spark job.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import json
import yaml
import pandas as pd
from datetime import timedelta
from pandas.io.json import json_normalize
from autogrid.pam.anomaly.anomaly import AmiAnomalies
import autogrid.foundation.util.CacheUtil as cache_util
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import VALUE_COUNT, NS_SEPARATOR, UTC, PAM


def __process_amidata(feeder, ami_df_list, start_time=None, end_time=None,
                      customer_df=None):
    """Process ami data for given feeder and calculates anomalies.

    Parameters
    ----------
    feeder : str
        The feeder ID number.

    ami_df_list : list
        List of data frames containing ami data.

    start_time : datetime
        Start time of data to detect anomaly

    end_time : datetime
        End time of data to detect anomaly

    customer_df : dataframe
        Dataframe containing customer data

    Returns
    -------
    anomalies : list
        List of data frames containing ami anomalies.
    """
    out_res_list = []
    ami_df = []
    __logger = Logger.get_logger('ami_utils.__process_amidata')
    for df in ami_df_list:
        # Rename columns in df
        df.rename(columns={'event_id': 'mtr_evnt_id',
                           'meter_id': 'ami_dvc_name',
                           'timestamp_utc': 'mtr_evnt_tmstmp',
                           'feeder_id': 'fdr_num'}, inplace=True)
        df = df[df.ami_dvc_name != 'ami_dvc_name']
        df.fdr_num = df.fdr_num.astype(str)
        df.mtr_evnt_id = df.mtr_evnt_id.astype(int).astype(str)
        df.mtr_evnt_tmstmp = pd.to_datetime(df.mtr_evnt_tmstmp)

        if start_time:
            df = df.loc[df['mtr_evnt_tmstmp'] >= start_time]
        if end_time:
            df = df.loc[df['mtr_evnt_tmstmp'] < end_time]
        df = df.dropna(how='any')
        if not df.empty:
            ami_df.append(df)

    if ami_df:
        ami_dfs = pd.concat(ami_df)
        try:
            customers = customer_df.loc[feeder].customers
        except KeyError:
            customers = 0
        __logger.debug("Anomaly detect: Customer count for feeder " +
                       str(feeder) + ":" + str(customers))
        result = None
        if customers > 100:
            anomalies = 'default'
            result = AmiAnomalies(feeder, anomalies).extract(ami_dfs,
                                                             customers,
                                                             start_time,
                                                             end_time).to_df()
        out_res_list.append(result)
    return out_res_list


def ami_map(keys):
    """Ami RDD MAP function.

    This scans all data related to each key from Redis and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for ami data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of ami anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('ami_utils.ami_map')

    settings_yaml = 'settings.yml'

    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    anomaly_hrs = config_dict['anomaly_calculation_hrs']
    cust_data_df = read_customer_data()
    cu = cache_util.CacheUtil()
    out_res_list = []
    __logger.info("Anomaly detect: Starting ami anomaly calculation from Redis ")
    for key in keys:
        tokens = key.split(NS_SEPARATOR)
        feeder_id = tokens[-1]
        namespace = NS_SEPARATOR.join(tokens[:-1])
        cursor = 0
        ami_data_list = list()
        __logger.debug('Anomaly detect: Scan Redis for ami keys with ' +
                       namespace + ':' + feeder_id)
        cursor, amidatalist_next = cu.zscan(namespace=namespace, key=str(feeder_id),
                                            cursor=cursor, count=VALUE_COUNT)
        if amidatalist_next:
            ami_data_list = amidatalist_next
        while cursor != 0:
            cursor, amidatalist_next = cu.zscan(namespace=namespace, key=str(feeder_id),
                                                cursor=cursor, count=VALUE_COUNT)
            if amidatalist_next:
                ami_data_list = ami_data_list + amidatalist_next

        __logger.debug('Anomaly detect: Converting all JSON data to dataframe ...')
        ami_json_list = []
        for ami_data in ami_data_list:
            j = json.loads(ami_data[0])
            ami_json_list.append(j)

        ami_df = []
        df = json_normalize(ami_json_list, 'time_series', ['feeder_id'])
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)
        end_time = df.timestamp_utc.max()
        ami_df.append(df)

        start_time = end_time - timedelta(hours=anomaly_hrs)
        __logger.debug('Anomaly detect: Process ami data for anomaly evaluation for'
                       ' \n\tFeeder_id=' + feeder_id +
                       '\n\tStarttime=' + str(start_time) +
                       '\n\tEndtime=' + str(end_time))
        results = __process_amidata(feeder_id, ami_df, start_time, end_time,
                                    cust_data_df)
        out_res_list.extend(results)
    __logger.info("Anomaly detect: Completed ami anomaly calculation from Redis ")
    return out_res_list


def ami_db_map(keys):
    """Ami RDD MAP function.

    This scans all data related to each key from MySQL and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for ami data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of ami anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('ami_utils.ami_db_map')

    cust_data_df = read_customer_data()
    out_res_list = []
    rdbms_util = rdbmsUtil.RDBMSUtil()
    __logger.info("Anomaly detect: Starting ami anomaly calculation from MySql ")
    for key in keys:
        tenant_id = str(key[0])
        feeder_id = str(key[1])
        __logger.debug('Anomaly detect: Query ami table for feeder=' +
                       feeder_id + ' tenant=' + tenant_id)

        # Query all data and evaluate anomalies from it.
        qry_columns = ['event_id', 'meter_id', 'timestamp_utc', 'feeder_id']
        columns = ', '.join(qry_columns)
        qry = "SELECT " + columns + " FROM ami WHERE feeder_id=%s AND tenant_id=%s"
        rows = rdbms_util.select(system, qry, (feeder_id, tenant_id))

        __logger.debug('Anomaly detect: Converting all query result data to dataframe..')
        df = pd.DataFrame(rows, columns=qry_columns)
        if df.shape[0] > 0:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)

            ami_df = []
            ami_df.append(df)
            start_time = None
            end_time = None
            __logger.debug('Anomaly detect: Process ami data for anomaly '
                           'evaluation for Feeder_id=' + feeder_id)
            results = __process_amidata(feeder_id, ami_df, start_time, end_time,
                                        cust_data_df)
            for res in results:
                if res is not None and not res.empty:
                    out_res_list.append(res)
    __logger.info("Anomaly detect: Completed ami anomaly calculation from MySql ")
    return out_res_list


def read_customer_data():
    """Read customer data from feeder_metadata table.

    Returns
    -------
    sql_df : dataframe
        dataframe containing feeder and number of customers
    """
    rdbms_util = rdbmsUtil.RDBMSUtil()
    columns_list = ["feeder_id", "customers"]
    column_string = ",".join(columns_list)
    query = "select %s from feeder_metadata" % column_string
    result = rdbms_util.select(PAM, query)
    sql_df = pd.DataFrame.from_records(result, columns=columns_list)
    sql_df.feeder_id = sql_df.feeder_id.astype(str)
    sql_df = sql_df.set_index('feeder_id', drop=False).sort_index()
    return sql_df
