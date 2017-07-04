"""TICKETS Utilities module.

The :mod:`autogrid.pam.tickets_utils` module contains tickets functions for
spark job.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import json
import yaml
import pandas as pd
from datetime import timedelta
from pandas.io.json import json_normalize
from autogrid.pam.anomaly.anomaly import TicketAnomalies
import autogrid.foundation.util.CacheUtil as cache_util
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import NS_SEPARATOR, VALUE_COUNT, UTC, PAM


def __process_tickets_data(feeder_id, tickets_df_list, start_time=None, end_time=None):
    """Process tickets data for given feeder and calculates anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    tickets_df_list : list
        List of data frames containing tickets data.

    start_time : datetime
        Start time of data to detect anomaly

    end_time : datetime
        End time of data to detect anomaly

    Returns
    -------
    anomalies : list
        List of data frames containing tickets anomalies.
    """
    out_res_list = []
    tickets_df = []
    for df in tickets_df_list:
        required_ticket_cols = ['DW_TCKT_KEY', 'FDR_NUM', 'POWEROFF', 'POWERRESTORE',
                                'IRPT_TYPE_CODE', 'RPR_ACTN_TYPE']
        # Rename columns in df
        df.rename(columns={'cmi': 'CMI',
                           'dw_ticket_id': 'DW_TCKT_KEY',
                           'irpt_cause_code': 'IRPT_CAUS_CODE',
                           'irpt_type_code': 'IRPT_TYPE_CODE',
                           'power_off': 'POWEROFF',
                           'power_restore': 'POWERRESTORE',
                           'repair_action_description': 'RPR_ACTN_DS',
                           'repair_action_type': 'RPR_ACTN_TYPE',
                           'support_code': 'SUPT_CODE',
                           'trbl_ticket_id': 'TRBL_TCKT_NUM',
                           'feeder_id': 'FDR_NUM'}, inplace=True)
        df = df[required_ticket_cols]
        df = df[pd.notnull(df['FDR_NUM'])]
        df.FDR_NUM = df.FDR_NUM.astype(int).astype(str)
        df.RPR_ACTN_TYPE.fillna('NA', inplace=True)

        if start_time:
            df = df.loc[df['POWEROFF'] >= start_time]
        if end_time:
            df = df.loc[df['POWEROFF'] < end_time]

        df = df.dropna(how='any')
        if not df.empty:
            tickets_df.append(df)

    if tickets_df:
        tickets_dfs = pd.concat(tickets_df)
        anomalies = 'default'
        ticket_result = TicketAnomalies(feeder_id, anomalies)
        ticket_result = ticket_result.extract(tickets_dfs, start_time, end_time).to_df()
        ticket_result.dropna(how='any')
        if not ticket_result.empty:
            out_res_list.append(ticket_result)

    return out_res_list


def tickets_map(keys):
    """RDD MAP function for Tickets.

    This scans all data related to each key from Redis and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for tickets data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of tickets anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('tikets_utils.tickets_map')

    settings_yaml = 'settings.yml'

    config_dict = yaml.load(open(settings_yaml, 'r'))[system]
    anomaly_hrs = config_dict['anomaly_calculation_hrs']

    cu = cache_util.CacheUtil()
    out_res_list = []
    __logger.info("Anomaly detect: Starting tickets anomaly calculation from Redis ")
    for key in keys:
        tokens = key.split(NS_SEPARATOR)
        feeder_id = tokens[-1]
        namespace = NS_SEPARATOR.join(tokens[:-1])
        cursor = 0
        tickets_data_list = list()
        __logger.debug('Anomaly detect: Scan Redis for tickets keys with ' +
                       namespace + ':' + feeder_id)
        cursor, ticketsdatalist_next = cu.zscan(namespace=namespace, key=str(feeder_id),
                                                cursor=cursor, count=VALUE_COUNT)
        if ticketsdatalist_next:
            tickets_data_list = ticketsdatalist_next
        while cursor != 0:
            cursor, ticketsdatalist_next = cu.zscan(namespace=namespace,
                                                    key=str(feeder_id),
                                                    cursor=cursor,
                                                    count=VALUE_COUNT)
            if ticketsdatalist_next:
                tickets_data_list = tickets_data_list + ticketsdatalist_next

        __logger.debug('Anomaly detect: Converting all JSON data to dataframe ...')
        tickets_json_list = []
        for tickets_data in tickets_data_list:
            j = json.loads(tickets_data[0])
            tickets_json_list.append(j)

        tickets_df = []
        df = json_normalize(tickets_json_list, 'time_series', ['feeder_id'])
        df['power_off'] = pd.to_datetime(df['power_off'])
        df['power_restore'] = pd.to_datetime(df['power_restore'])
        df.power_off = df.power_off.dt.tz_localize(UTC)
        df.power_restore = df.power_restore.dt.tz_localize(UTC)
        end_time = df.power_off.max()
        tickets_df.append(df)

        start_time = end_time - timedelta(hours=anomaly_hrs)
        __logger.debug('Anomaly detect: Process tickets data for anomaly evaluation for'
                       ' \n\tFeeder_id=' + feeder_id +
                       '\n\tStarttime=' + str(start_time) +
                       '\n\tEndtime=' + str(end_time))
        results = __process_tickets_data(feeder_id, tickets_df, start_time, end_time)
        for res in results:
            if not res.empty:
                out_res_list.append(res)
    __logger.info("Anomaly detect: Completed tickets anomaly calculation from Redis ")
    return out_res_list


def tickets_db_map(keys):
    """RDD MAP function for Tickets.

    This scans all data related to each key from MySQL and process to calculate
    anomalies.

    Parameters
    ----------
    keys : list
        List of redis keys for tickets data.

    Returns
    -------
    anomalies : RDD
        A Spark RDD containing list of data frames of tickets anomalies.
    """
    system = PAM

    __logger = Logger.get_logger('tikets_utils.tickets_db_map')

    out_res_list = []
    rdbms_util = rdbmsUtil.RDBMSUtil()
    __logger.info("Anomaly detect: Starting tickets anomaly calculation from MySql ")
    for key in keys:
        tenant_id = str(key[0])
        feeder_id = str(key[1])
        __logger.debug('Anomaly detect: Query tickets table for feeder=' +
                       feeder_id + ' tenant=' + tenant_id)

        # Query all data and evaluate anomalies from it.
        qry_columns = ['cmi', 'dw_ticket_id', 'irpt_cause_code',
                       'irpt_type_code', 'power_off', 'power_restore',
                       'repair_action_description', 'repair_action_type',
                       'support_code', 'trbl_ticket_id', 'feeder_id']
        columns = ', '.join(qry_columns)
        qry = "SELECT " + columns + " FROM tickets WHERE feeder_id=%s AND tenant_id=%s"
        rows = rdbms_util.select(system, qry, (feeder_id, tenant_id))

        __logger.debug('Anomaly detect: Converting all query result data to dataframe..')
        df = pd.DataFrame(rows, columns=qry_columns)
        if df.shape[0] > 0:
            df['power_off'] = pd.to_datetime(df['power_off'])
            df['power_restore'] = pd.to_datetime(df['power_restore'])
            df.power_off = df.power_off.dt.tz_localize(UTC)
            df.power_restore = df.power_restore.dt.tz_localize(UTC)

            tickets_df = []
            tickets_df.append(df)
            start_time = None
            end_time = None
            __logger.debug('Anomaly detect: Process tickets data for anomaly '
                           'evaluation for Feeder_id=' + feeder_id)
            results = __process_tickets_data(feeder_id, tickets_df, start_time, end_time)
            for res in results:
                if res is not None and not res.empty:
                    out_res_list.append(res)
    __logger.info("Anomaly detect: Completed tickets anomaly calculation from MySql ")
    return out_res_list
