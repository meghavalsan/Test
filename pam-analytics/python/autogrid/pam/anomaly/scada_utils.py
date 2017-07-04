"""SCADA Utilities module.

The :mod:`autogrid.pam.scada_utils` module contains scada functions for spark
job.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import json
import yaml
import pandas as pd
from datetime import timedelta
from pandas.io.json import json_normalize
from autogrid.pam.anomaly.anomaly import ScadaAnomalies
import autogrid.foundation.util.CacheUtil as cache_util
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import VALUE_COUNT, NS_SEPARATOR, UTC, PAM


def __process_scadadata(feeder_id, scada_df_list, start_time=None, end_time=None):
    """Process scada data for given feeder and calculates anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    scada_df_list : list
        List of data frames containing scada data.

    start_time : datetime
        Start time of data to detect anomaly

    end_time : datetime
        End time of data to detect anomaly

    Returns
    -------
    anomalies : list
        List of data frames containing scada anomalies.
    """
    out_res_list = []
    scada_df = []
    for df in scada_df_list:
        # Rename columns in df
        df.rename(columns={'timestamp_utc': 'localTime',
                           'value': 'OBSERV_DATA',
                           'feeder_id': 'feederNumber'}, inplace=True)
        df.feederNumber = df.feederNumber.astype(str)
        df.OBSERV_DATA = df.OBSERV_DATA.astype(str)
        df = df.loc[df.feederNumber == feeder_id]

        if start_time:
            df = df.loc[df['localTime'] >= start_time]
        if end_time:
            df = df.loc[df['localTime'] < end_time]

        df = df.dropna(how='any')
        if not df.empty:
            scada_df.append(df)

    if scada_df:
        scada_dfs = pd.concat(scada_df)
        anomalies = 'default'
        result = ScadaAnomalies(feeder_id, anomalies).extract(scada_dfs, start_time,
                                                              end_time).to_df()
        out_res_list.append(result)
    return out_res_list


def scada_map(keys):
    """Scada RDD MAP function.

    This scans all data related to each key from Redis and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for scada data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of scada anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('scada_utils.scada_map')

    settings_yaml = 'settings.yml'

    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    anomaly_hrs = config_dict['anomaly_calculation_hrs']

    cu = cache_util.CacheUtil()
    out_res_list = []
    __logger.info("Anomaly detect: Starting scada anomaly calculation from Redis ")
    for key in keys:
        tokens = key.split(NS_SEPARATOR)
        feeder_id = tokens[-1]
        namespace = NS_SEPARATOR.join(tokens[:-1])
        cursor = 0
        scada_data_list = list()
        __logger.debug('Anomaly detect: Scan Redis for scada keys with ' +
                       namespace + ':' + feeder_id)
        cursor, scadadatalist_next = cu.zscan(namespace=namespace, key=str(feeder_id),
                                              cursor=cursor, count=VALUE_COUNT)
        if scadadatalist_next:
            scada_data_list = scadadatalist_next

        while cursor != 0:
            cursor, scadadatalist_next = cu.zscan(namespace=namespace,
                                                  key=str(feeder_id),
                                                  cursor=cursor,
                                                  count=VALUE_COUNT)
            if scadadatalist_next:
                scada_data_list = scada_data_list + scadadatalist_next

        __logger.debug('Anomaly detect: Converting all JSON data to dataframe ...')
        scada_json_list = []
        for scada_data in scada_data_list:
            j = json.loads(scada_data[0])
            scada_json_list.append(j)

        scada_df = []
        df = json_normalize(scada_json_list, 'time_series', ['feeder_id'])
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)
        end_time = df.timestamp_utc.max()
        scada_df.append(df)

        start_time = end_time - timedelta(hours=anomaly_hrs)
        __logger.debug('Anomaly detect: Process scada data for anomaly evaluation for'
                       ' \n\tFeeder_id=' + feeder_id +
                       '\n\tStarttime=' + str(start_time) +
                       '\n\tEndtime=' + str(end_time))
        results = __process_scadadata(feeder_id, scada_df, start_time, end_time)
        out_res_list.extend(results)
    __logger.info("Anomaly detect: Completed scada anomaly calculation from Redis ")
    return out_res_list


def scada_db_map(keys):
    """Scada RDD MAP function.

    This scans all data related to each key from MySQL and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for scada data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of scada anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('scada_utils.scada_db_map')

    out_res_list = []
    rdbms_util = rdbmsUtil.RDBMSUtil()
    __logger.info("Anomaly detect: Starting scada anomaly calculation from MySql ")
    for key in keys:
        tenant_id = str(key[0])
        feeder_id = str(key[1])
        __logger.debug('Anomaly detect: Query scada table for feeder=' +
                       feeder_id + ' tenant=' + tenant_id)

        # Query all data and evaluate anomalies from it.
        qry_columns = ['timestamp_utc', 'value', 'feeder_id']
        columns = ', '.join(qry_columns)
        qry = "SELECT " + columns + " FROM scada WHERE feeder_id=%s AND tenant_id=%s"
        rows = rdbms_util.select(system, qry, (feeder_id, tenant_id))

        __logger.debug('Anomaly detect: Converting all query result data to dataframe..')
        df = pd.DataFrame(rows, columns=qry_columns)
        if df.shape[0] > 0:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)

            scada_df = []
            scada_df.append(df)
            start_time = None
            end_time = None
            __logger.debug('Anomaly detect: Process scada data for anomaly '
                           'evaluation for Feeder_id=' + feeder_id)
            results = __process_scadadata(feeder_id, scada_df, start_time, end_time)
            for res in results:
                if res is not None and not res.empty:
                    out_res_list.append(res)
    __logger.info("Anomaly detect: Completed scada anomaly calculation from MySql ")
    return out_res_list
