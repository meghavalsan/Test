"""EDNA Utilities module.

The :mod:`autogrid.pam.edna_utils` module contains edna functions for
spark job.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import json
import os
import yaml
import pandas as pd
from datetime import timedelta
from pandas.io.json import json_normalize
from autogrid.pam.anomaly.anomaly import EdnaAnomalies
import autogrid.foundation.util.CacheUtil as cacheutil
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import VALUE_COUNT, NS_SEPARATOR, UTC, PAM


def __process_edna_data(feeder_id, edna_df_list, start_time=None, end_time=None):
    """Process edna data for given feeder and calculates anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    edna_df_list : list
        List of data frames containing edna data.

    start_time : datetime
        Start time of data to detect anomaly

    end_time : datetime
        End time of data to detect anomaly

    Returns
    -------
    anomalies : list
        List of data frames containing edna anomalies.
    """
    out_res_list = []
    edna_df = []
    for df in edna_df_list:
        # Rename columns in df
        df.rename(columns={'status': 'Status',
                           'timestamp_utc': 'Time',
                           'value': 'Value',
                           'value_string': 'ValueString',
                           'feeder_id': 'feederId',
                           'extended_id': 'ExtendedId'}, inplace=True)
        df[['ValueString']] = df[['ValueString']].astype(str)

        edna_codes = df.ExtendedId.unique()
        edna_codes = pd.Series(edna_codes)
        edna_codes = edna_codes[-edna_codes.str.contains(r'Bad point')]
        edna_codes = edna_codes[edna_codes.str.contains(feeder_id)]
        edna_codes = edna_codes[((edna_codes.str.contains(r'\.PF\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.THD_')) &
                                 (edna_codes.str.contains(r'urrent'))) |
                                (((edna_codes.str.contains(r'\.MVAR')) |
                                  (edna_codes.str.contains(r'\.MVR\.'))) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.V\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.I\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.MW')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.FCI\.')) &
                                 ((edna_codes.str.contains(r'\.FAULT')) |
                                  (edna_codes.str.contains(r'\.I_FAULT')))) |
                                ((edna_codes.str.contains(r'\.AFS\.')) &
                                 ((edna_codes.str.contains(r'\.ALARM')) |
                                  (edna_codes.str.contains(r'\.GROUND')) |
                                  (edna_codes.str.contains(r'\.I_FAULT'))))]
        edna_codes = set(edna_codes.values)
        df = df[df.ExtendedId.isin(edna_codes)]

        df = df.dropna(how='any')
        if not df.empty:
            edna_df.append(df)

    if edna_df:
        edna_dfs = pd.concat(edna_df, ignore_index=True)
        anomalies = 'default'
        result = EdnaAnomalies(feeder_id, anomalies).extract(edna_dfs, start_time,
                                                             end_time).to_df()
        out_res_list.append(result)
    return out_res_list


def edna_map(keys):
    """Edna RDD MAP function.

    This scans all data related to each key from Redis and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for edna data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of edna anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('edna_utils.edna_map')

    settings_yaml = 'settings.yml'

    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    anomaly_hrs = config_dict['anomaly_calculation_hrs']

    cu = cacheutil.CacheUtil()
    out_res_list = []
    __logger.info("Anomaly detect: Starting edna anomaly calculation from Redis ")
    for key in keys:
        tokens = key.split(NS_SEPARATOR)
        feeder_id = tokens[-1]
        namespace = NS_SEPARATOR.join(tokens[:-1])
        cursor = 0
        edna_data_list = list()
        __logger.debug('Anomaly detect: Scan Redis for edna keys with ' +
                       namespace + ':' + feeder_id)
        cursor, ednadatalist_next = cu.zscan(namespace=namespace, key=str(feeder_id),
                                             cursor=cursor, count=VALUE_COUNT)
        if ednadatalist_next:
            edna_data_list = ednadatalist_next
        while cursor != 0:
            cursor, ednadatalist_next = cu.zscan(namespace=namespace,
                                                 key=feeder_id,
                                                 cursor=cursor,
                                                 count=VALUE_COUNT)
            if ednadatalist_next:
                edna_data_list = edna_data_list + ednadatalist_next

        __logger.debug('Anomaly detect: Converting all JSON data to dataframe ...')
        edna_json_list = []
        for edna_data in edna_data_list:
            j = json.loads(edna_data[0])
            edna_json_list.append(j)

        edna_df = []
        df = json_normalize(edna_json_list, 'time_series', ['extended_id', 'feeder_id'])
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)
        end_time = df.timestamp_utc.max()
        edna_df.append(df)

        start_time = end_time - timedelta(hours=anomaly_hrs)
        __logger.debug('Anomaly detect: Process edna data for anomaly evaluation for '
                       '\n\t Feeder_id=' + feeder_id +
                       '\n\tStarttime=' + str(start_time) +
                       '\n\tEndtime=' + str(end_time))
        results = __process_edna_data(feeder_id, edna_df, start_time, end_time)
        out_res_list.extend(results)
    __logger.info("Anomaly detect: Completed edna anomaly calculation from Redis ")
    return out_res_list


def edna_db_map(keys):
    """Edna RDD MAP function.

    This scans all data related to each key from MySQL and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for edna data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of edna anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('edna_utils.edna_db_map')

    settings_yaml = os.path.join('settings.yml')

    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    burn_in_hrs = config_dict['mysql_burn_in_hrs']
    evaluation_hrs = config_dict['mysql_evaluation_hrs']

    out_res_list = []
    rdbms_util = rdbmsUtil.RDBMSUtil()
    __logger.info("Anomaly detect: Starting edna anomaly calculation from MySql ")
    for key in keys:
        tenant_id = str(key[0])
        feeder_id = str(key[1])
        __logger.debug('Anomaly detect: Query edna table for feeder=' +
                       feeder_id + ' tenant=' + tenant_id)

        # Now query all data from DB.
        qry_columns = ['status', 'timestamp_utc', 'value', 'value_string', 'feeder_id',
                       'extended_id']
        columns = ', '.join(qry_columns)
        qry = "SELECT " + columns + " FROM edna WHERE feeder_id=%s AND tenant_id=%s"
        rows = rdbms_util.select(system, qry, (feeder_id, tenant_id))

        __logger.debug('Anomaly detect: Converting all query result data to dataframe..')
        df = pd.DataFrame(rows, columns=qry_columns)
        if df.shape[0] > 0:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            df.timestamp_utc = df.timestamp_utc.dt.tz_localize(UTC)

            min_date = df.timestamp_utc.min()
            max_date = df.timestamp_utc.max()

            start_date = min_date
            end_date = min_date + timedelta(hours=burn_in_hrs) \
                + timedelta(hours=evaluation_hrs)
            if end_date > max_date:
                end_date = max_date
            # Now loop through to evaluate anomalies for configured burn-in and
            # evaluation time window.
            while end_date <= max_date:
                # Filter data within start and end date range and pass it to
                # __process_edna_data to evaluate anomalies
                mask = (df['timestamp_utc'] >= start_date) & \
                       (df['timestamp_utc'] <= end_date)
                edna_df = []
                edna_df.append(df.loc[mask])

                # Set anomaly calculation start_time as end_date - evaluation_hours
                start_time = end_date - timedelta(hours=evaluation_hrs)
                end_time = end_date
                __logger.debug('Anomaly detect: Process edna data for anomaly evaluation for '
                               '\n\tFeeder_id=' + feeder_id +
                               '\n\tStarttime=' + str(start_time) +
                               '\n\tEndtime=' + str(end_time))
                results = __process_edna_data(feeder_id, edna_df, start_time, end_time)
                for res in results:
                    if res is not None and not res.empty:
                        out_res_list.append(res)

                if end_date >= max_date:
                    # Means we have already evaluated anomalies for last window
                    break

                start_date = start_date + timedelta(hours=evaluation_hrs)
                end_date = start_date + timedelta(hours=burn_in_hrs)\
                    + timedelta(hours=evaluation_hrs)
                if end_date > max_date:
                    end_date = max_date
    __logger.info("Anomaly detect: Completed edna anomaly calculation from MySql ")
    return out_res_list
