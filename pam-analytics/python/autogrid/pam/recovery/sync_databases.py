"""This job is to ensure that MySQL and Redis Data is consistent.

If the data is not consistent changes should be made to Redis and MySQL to
ensure that there is consistency between the two.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import os
import sys
import yaml
import json
import MySQLdb
import datetime
import pandas as pd
from autogrid.pam.anomaly.global_define import NS_SEPARATOR, SCADA_NS, EDNA_NS
from autogrid.pam.anomaly.global_define import TICKETS_NS, AMI_NS, VALUE_COUNT
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil
from autogrid.foundation.util.Logger import Logger


class SyncDatabases(object):

    """Keep Redis and MySql in Sync.

    Parameters
    ----------
    tenant_id : int
        unique id of tenant.

    system : str
        system name
    """

    def __init__(self, tenant_id, system='PAM'):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.cache_util = cacheUtil.CacheUtil()
        self.system = system
        self.tenant_id = int(tenant_id)
        self.home = os.environ['PAM_ANALYTICS_HOME']
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')
        settings_dict = yaml.load(open(self.settings_path, 'r'))[self.system]
        self.anomaly_calculation_hrs = settings_dict['anomaly_calculation_hrs']
        self.scada_cache_hrs = settings_dict['scada_cache_hrs']
        self.ami_cache_hrs = settings_dict['ami_cache_hrs']
        self.edna_cache_hrs = settings_dict['edna_cache_hrs']
        self.tickets_cache_hrs = settings_dict['tickets_cache_hrs']
        self.sql_database = system
        self.redis_date_format = '%Y-%m-%dT%H:%M:%SZ'
        self.date_format = "%Y-%m-%d %H:%M:%S"
        tenant_query = "select uid from tenant where id=%s;"
        self.tenant_uid = None
        try:
            result = self.rdbms_util.select(self.sql_database, tenant_query,
                                            (self.tenant_id))
            self.tenant_uid = result[0]['uid']
        except MySQLdb.Error:
            self.__logger.error("Sync db: Error in executing MYSQL query")
            raise

    def get_data(self):
        """Get data from Redis and MySql.

        Call compare methods for all kinds of data('scada', 'ami', 'edna',
         'tickets') for last 48+8 hrs(configurable in settings.xml).
        """
        datasets = ['scada', 'ami', 'edna', 'tickets']
        for dataset in datasets:
            self.__logger.debug("Sync db: Running get %s data..." % dataset)
            redis_keys = None
            redis_data = []
            mysql_data = None
            time_frame = None
            if dataset == 'scada':
                time_frame = self.anomaly_calculation_hrs + self.scada_cache_hrs
                new_keypattern = self.tenant_uid + NS_SEPARATOR + SCADA_NS
            if dataset == 'ami':
                time_frame = self.anomaly_calculation_hrs + self.ami_cache_hrs
                new_keypattern = self.tenant_uid + NS_SEPARATOR + AMI_NS
            if dataset == 'edna':
                time_frame = self.anomaly_calculation_hrs + self.edna_cache_hrs
                new_keypattern = self.tenant_uid + NS_SEPARATOR + EDNA_NS
            if dataset == 'tickets':
                time_frame = self.anomaly_calculation_hrs + self.tickets_cache_hrs
                new_keypattern = self.tenant_uid + NS_SEPARATOR + TICKETS_NS
            current_date = datetime.datetime.utcnow()
            previous_date = current_date - datetime.timedelta(hours=time_frame)
            current_date = int(current_date.strftime('%s'))
            previous_date = int(previous_date.strftime('%s'))
            # Get the keys for scada
            try:
                redis_keys = self.cache_util.keys(new_keypattern, '*')
            except Exception:
                self.__logger.error('Sync db: Error in getting redis keys')
                raise
            # Now replicate through keys to get values
            try:
                for redis_key in redis_keys:
                    cursor = 0
                    new_key = redis_key.split(":")[-1]
                    cursor, vals = self.cache_util.zscan(namespace=new_keypattern,
                                                         key=new_key, cursor=cursor,
                                                         count=VALUE_COUNT)
                    if len(vals) > 0:
                        for eachdata in vals:
                            if int(eachdata[1]) >= previous_date and int(
                                    eachdata[1]) <= current_date:
                                redis_data.append(eachdata)
                    while cursor > 0:
                        cursor, vals = self.cache_util.zscan(namespace=new_keypattern,
                                                             key=new_key, cursor=cursor,
                                                             count=VALUE_COUNT)
                        if len(vals) > 0:
                            for eachdata in vals:
                                if int(eachdata[1]) >= previous_date and int(
                                        eachdata[1]) <= current_date:
                                    redis_data.append(eachdata)
            except Exception:
                self.__logger.error('Sync db: Error in getting redis values')
                raise
            # Now get values from MySql
            query = "SELECT * FROM " + dataset + " where score >=%s AND score<=%s;"
            try:
                mysql_data = self.rdbms_util.select(self.sql_database, query,
                                                    (previous_date, current_date))
            except MySQLdb.Error:
                self.__logger.error("Sync db: Error in executing MYSQL query")
                raise
            if dataset == 'scada':
                self.compare_scada(redis_data, mysql_data)
            if dataset == 'ami':
                self.compare_ami(redis_data, mysql_data)
            if dataset == 'edna':
                self.compare_edna(redis_data, mysql_data)
            if dataset == 'tickets':
                self.compare_tickets(redis_data, mysql_data)

    def compare_scada(self, redis_data, mysql_data):
        """Compare scada data in MySql and Redis and synchronise.

        Parameters
        ----------
        redis_data : list
            redis data list containing data from redis
        mysql_data : list
            mysql data list containing data from mysql
        """
        self.__logger.debug("Sync db: Running compare scada data...")
        redis_data_set = set()
        mysql_data_set = set()
        try:
            # Add mysql data to set for comparision
            for items in mysql_data:
                item = list(items)
                feeder_id = int(item[0])
                value = item[1]
                timestamp_utc = item[2].strftime(self.date_format)
                mysql_score = item[3]
                mysql_row = (
                    feeder_id, value, timestamp_utc, mysql_score,
                    self.tenant_id)
                mysql_data_set.add(mysql_row)

            # Add Redis data to set for comparision
            for data in redis_data:
                redis_data_json = json.loads(data[0])
                feeder_id = int(str(redis_data_json['feeder_id']))
                timeseries = redis_data_json['time_series']
                for items in timeseries:
                    timestamp_utc = items['timestamp_utc']
                    timestamp_utc = datetime.datetime. \
                        strptime(timestamp_utc, self.redis_date_format). \
                        strftime(self.date_format)
                    value = str(items['value'])
                    redis_row = (
                        feeder_id, value, timestamp_utc, data[1],
                        self.tenant_id)
                    redis_data_set.add(redis_row)
            # Data not exists in MYSQL
            redis_mysql_diff = redis_data_set - mysql_data_set
            if len(redis_mysql_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(redis_mysql_diff)) +
                                    " records are missing in MYSQL")
                self.__logger.debug("Sync db: Inserting missing data in MYSQL")
                for row in redis_mysql_diff:
                    mysql_query = "insert into %s values%s" % ('scada', row)
                    self.rdbms_util.execute(self.sql_database, mysql_query)

            # Data not exists in redis
            mysql_redis_diff = mysql_data_set - redis_data_set
            if len(mysql_redis_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(mysql_redis_diff)) +
                                    " records are missing in Redis")
                self.__logger.debug("Sync db: Inserting missing data in Redis")
                for row in mysql_redis_diff:
                    redis_key = 'FPL:feeders:scada'
                    diff_dict = {}
                    timeseries_jsnobj = {}
                    timeseries_list = []
                    diff_dict['feeder_id'] = str(row[0])
                    timeseries_jsnobj['timestamp_utc'] = datetime.datetime. \
                        strptime(row[2], self.date_format). \
                        strftime(self.redis_date_format)
                    timeseries_jsnobj['value'] = row[1]
                    timeseries_list.append(timeseries_jsnobj)
                    diff_dict['time_series'] = timeseries_list
                    redis_data_json_str = json.dumps(diff_dict)
                    redis_score = row[3]
                    value_dict = dict()
                    value_dict[redis_score] = redis_data_json_str
                    self.cache_util.batch_zadd(redis_key, row[0], value_dict)
                self.__logger.debug('Sync db: Scada Data synchronised successfully!!')

            if len(redis_mysql_diff) == 0 and len(mysql_redis_diff) == 0:
                self.__logger.debug('Sync db: Scada Data is in sync!!')
        except Exception:
            self.__logger.error('Sync db: Error while comparing Scada data in '
                                'MYSQL and Redis')
            raise

    def compare_ami(self, redis_data, mysql_data):
        """Compare ami data in MySql and Redis and synchronise.

        Parameters
        ----------
        redis_data : list
            redis data list containing data from redis
        mysql_data : list
            mysql data list containing data from mysql
        """
        self.__logger.debug("Sync db: Running compare ami data...")
        redis_data_set = set()
        mysql_data_set = set()
        try:
            # Add mysql data to set for comparision
            for items in mysql_data:
                item = list(items)
                feeder_id = int(item[0])
                meter_id = item[1]
                event_id = item[2]
                timestamp_utc = item[3].strftime(self.date_format)
                mysql_score = item[4]
                mysql_row = (
                    feeder_id, meter_id, event_id, timestamp_utc, mysql_score,
                    self.tenant_id)
                mysql_data_set.add(mysql_row)
            # Add Redis data to set for comparision
            for data in redis_data:
                redis_data_json = json.loads(data[0])
                feeder_id = int(str(redis_data_json['feeder_id']))
                timeseries = redis_data_json['time_series']
                for items in timeseries:
                    timestamp_utc = items['timestamp_utc']
                    timestamp_utc = datetime.datetime. \
                        strptime(timestamp_utc, self.redis_date_format). \
                        strftime(self.date_format)
                    meter_id = str(items['meter_id'])
                    event_id = str(items['event_id'])
                    redis_row = (
                        feeder_id, meter_id, event_id, timestamp_utc, data[1],
                        self.tenant_id)
                    redis_data_set.add(redis_row)
            # Data not exists in MYSQL
            redis_mysql_diff = redis_data_set - mysql_data_set
            if len(redis_mysql_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(redis_mysql_diff)) +
                                    " records are missing in MYSQL")
                self.__logger.debug("Sync db: Inserting missing data in MYSQL")
                for row in redis_mysql_diff:
                    mysql_query = "insert into %s values%s" % ('ami', row)
                    self.rdbms_util.execute(self.sql_database, mysql_query)
            # Data not exists in redis
            mysql_redis_diff = mysql_data_set - redis_data_set
            if len(mysql_redis_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(mysql_redis_diff)) +
                                    " records are missing in REDIS")
                self.__logger.debug("Sync db: Inserting missing data in Redis")
                for row in mysql_redis_diff:
                    redis_key = 'FPL:feeders:ami'
                    diff_dict = {}
                    timeseries_jsnobj = {}
                    timeseries_list = []
                    diff_dict['feeder_id'] = str(row[0])
                    timeseries_jsnobj['timestamp_utc'] = datetime.datetime. \
                        strptime(row[3], self.date_format). \
                        strftime(self.redis_date_format)
                    timeseries_jsnobj['meter_id'] = row[1]
                    timeseries_jsnobj['event_id'] = row[2]
                    redis_score = row[4]
                    timeseries_list.append(timeseries_jsnobj)
                    diff_dict['time_series'] = timeseries_list
                    redis_data_json_str = json.dumps(diff_dict)
                    value_dict = dict()
                    value_dict[redis_score] = redis_data_json_str
                    self.cache_util.batch_zadd(redis_key, row[0], value_dict)
                self.__logger.debug('Sync db: Ami Data synchronised successfully!!')

            if len(redis_mysql_diff) == 0 and len(mysql_redis_diff) == 0:
                self.__logger.debug('Sync db: Ami Data is in sync!!')
        except Exception:
            self.__logger.error('Sync db: Error while comparing AMI data in '
                                'MYSQL and Redis')
            raise

    def compare_edna(self, redis_data, mysql_data):
        """Compare edna data in MySql and Redis and synchronise.

        Parameters
        ----------
        redis_data : list
            redis data list containing data from redis
        mysql_data : list
            mysql data list containing data from mysql
        """
        self.__logger.debug("Sync db: Running compare edna data...")
        redis_data_list = []
        try:
            # Add mysql data to dataframe for comparision
            mysql_data_df = pd.DataFrame.from_records(mysql_data,
                                                      columns=['feeder_id',
                                                               'extended_id',
                                                               'timestamp_utc',
                                                               'value',
                                                               'value_string',
                                                               'status',
                                                               'score',
                                                               'tenant_id'])

            # Add Redis data to dataframe for comparision
            for data in redis_data:
                redis_data_json = json.loads(data[0])
                feeder_id = int(str(redis_data_json['feeder_id']))
                extended_id = str(redis_data_json['extended_id'])
                timeseries = redis_data_json['time_series']
                for items in timeseries:
                    timestamp_utc = items['timestamp_utc']
                    timestamp_utc = datetime.datetime. \
                        strptime(timestamp_utc, self.redis_date_format). \
                        strftime(self.date_format)
                    value = float(items['value'])
                    value_string = str(items['value_string'])
                    status = str(items['status'])
                    redis_row = (
                        feeder_id, extended_id, timestamp_utc, value,
                        value_string,
                        status,
                        data[1], self.tenant_id)
                    redis_data_list.append(redis_row)
            redis_data_df = pd.DataFrame.from_records(redis_data_list,
                                                      columns=['feeder_id',
                                                               'extended_id',
                                                               'timestamp_utc',
                                                               'value',
                                                               'value_string',
                                                               'status',
                                                               'score',
                                                               'tenant_id'])
            redis_data_df['tenant_id'] = redis_data_df['tenant_id'].astype(int)
            redis_data_df['timestamp_utc'] = pd.\
                to_datetime(redis_data_df['timestamp_utc'])
            mysql_data_df_copy = mysql_data_df.iloc[:, [0, 1, 2, 4, 5, 6, 7]]
            redis_data_df_copy = redis_data_df.iloc[:, [0, 1, 2, 4, 5, 6, 7]]
            mysql_data_df_copy.sort_values(['feeder_id',
                                            'timestamp_utc',
                                            'value_string', 'status',
                                            'score', 'tenant_id', 'extended_id'],
                                           ascending=[True, True, True, True, True,
                                                      True, True],
                                           inplace=True)
            mysql_data_df_copy.reset_index(drop=True, inplace=True)
            redis_data_df_copy.sort_values(['feeder_id',
                                            'timestamp_utc',
                                            'value_string', 'status',
                                            'score', 'tenant_id', 'extended_id'],
                                           ascending=[True, True, True, True, True,
                                                      True, True],
                                           inplace=True)
            redis_data_df_copy.reset_index(drop=True, inplace=True)

            # Data not exists in MYSQL
            redis_mysql_diff = redis_data_df_copy[
                ~redis_data_df_copy.isin(mysql_data_df_copy).all(1)]
            redis_mysql_diff_value = pd.merge(redis_mysql_diff, redis_data_df,
                                              how='left',
                                              on=['feeder_id', 'extended_id',
                                                  'timestamp_utc',
                                                  'value_string', 'status',
                                                  'score', 'tenant_id'])
            if redis_mysql_diff_value.shape[0] > 0:
                self.__logger.debug("Sync db: " + str(redis_mysql_diff_value.shape[0]) +
                                    " records are missing in MYSQL")
                self.__logger.debug("Sync db: Inserting missing data in MYSQL")
                self.rdbms_util.insert(self.sql_database, 'edna',
                                       redis_mysql_diff_value.to_dict('records'))

            # Data not exists in redis
            mysql_redis_diff = mysql_data_df_copy[
                ~mysql_data_df_copy.isin(redis_data_df_copy).all(1)]
            mysql_redis_diff_value = pd.merge(mysql_redis_diff, mysql_data_df,
                                              how='left',
                                              on=['feeder_id', 'extended_id',
                                                  'timestamp_utc',
                                                  'value_string', 'status',
                                                  'score', 'tenant_id'])
            if mysql_redis_diff_value.shape[0] > 0:
                mysql_redis_diff_value['score'] = mysql_redis_diff_value[
                    'score'].astype(int)
                self.__logger.debug("Sync db: " + str(mysql_redis_diff_value.shape[0]) +
                                    " records are missing in REDIS")
                self.__logger.debug("Sync db: Inserting missing data in Redis")
                for index, row in mysql_redis_diff_value.iterrows():
                    redis_key = 'FPL:feeders:edna'
                    diff_dict = {}
                    timeseries_jsnobj = {}
                    timeseries_list = []
                    diff_dict['feeder_id'] = str(row['feeder_id'])
                    diff_dict['extended_id'] = str(row['extended_id'])
                    timeseries_jsnobj['timestamp_utc'] = row[
                        'timestamp_utc'].strftime(self.redis_date_format)
                    timeseries_jsnobj['value'] = float(row['value'])
                    timeseries_jsnobj['value_string'] = str(row['value_string'])
                    timeseries_jsnobj['status'] = str(row['status'])
                    timeseries_list.append(timeseries_jsnobj)
                    diff_dict['time_series'] = timeseries_list
                    redis_data_json_str = json.dumps(diff_dict)
                    redis_score = row['score']
                    value_dict = dict()
                    value_dict[redis_score] = redis_data_json_str
                    self.cache_util.batch_zadd(redis_key, row[0], value_dict)
                self.__logger.debug('Sync db: EDNA Data synchronised successfully!!')

            if len(redis_mysql_diff) == 0 and len(mysql_redis_diff) == 0:
                self.__logger.debug('Sync db: EDNA Data is in sync!!')
        except Exception:
            self.__logger.error('Sync db: Error while comparing EDNA data in '
                                'MYSQL and Redis')
            raise

    def compare_tickets(self, redis_data, mysql_data):
        """Compare tickets data in MySql and Redis and synchronise.

        Parameters
        ----------
        redis_data : list
            redis data list containing data from redis
        mysql_data : list
            mysql data list containing data from mysql
        """
        self.__logger.debug("Sync db: Running compare tickets data...")
        redis_data_set = set()
        mysql_data_set = set()
        try:
            # Add mysql data to set for comparision
            for items in mysql_data:
                item = list(items)
                feeder_id = int(item[0])
                dw_ticket_id = item[1]
                trouble_ticket_id = item[2]
                interruption_typecode = item[3]
                interruption_causecode = item[4]
                support_code = item[5]
                cmi = float(item[6])
                poweroff_utc = item[7].strftime(self.date_format)
                power_restore_utc = item[8].strftime(self.date_format)
                repair_action_type = str(item[9])
                repair_action_description = str(item[10])
                mysql_score = item[11]
                mysql_row = (
                    feeder_id, dw_ticket_id, trouble_ticket_id,
                    interruption_typecode,
                    interruption_causecode,
                    support_code, cmi, poweroff_utc, power_restore_utc,
                    repair_action_type,
                    repair_action_description, mysql_score, self.tenant_id)

                mysql_data_set.add(mysql_row)

            # Add Redis data to set for comparision
            for data in redis_data:
                redis_data_json = json.loads(data[0])
                feeder_id = int(str(redis_data_json['feeder_id']))
                timeseries = redis_data_json['time_series']
                for items in timeseries:
                    dw_ticket_id = str(items['dw_ticket_id'])
                    trbl_ticket_id = str(items['trbl_ticket_id'])
                    irpt_type_code = str(items['irpt_type_code'])
                    irpt_cause_code = str(items['irpt_cause_code'])
                    support_code = str(items['support_code'])
                    cmi = float(items['cmi'])
                    power_off = datetime.datetime. \
                        strptime(items['power_off'], self.redis_date_format). \
                        strftime(self.date_format)
                    power_restore = datetime.datetime. \
                        strptime(items['power_restore'],
                                 self.redis_date_format). \
                        strftime(self.date_format)
                    repair_action_type = str(items['repair_action_type'])
                    repair_action_description = str(items['repair_action_description'])
                    redis_row = (
                        feeder_id, dw_ticket_id, trbl_ticket_id,
                        irpt_type_code,
                        irpt_cause_code, support_code, cmi,
                        power_off, power_restore, repair_action_type,
                        repair_action_description, data[1], self.tenant_id)
                    redis_data_set.add(redis_row)
            # Data not exists in MYSQL
            redis_mysql_diff = redis_data_set - mysql_data_set
            if len(redis_mysql_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(redis_mysql_diff)) +
                                    " records are missing in MYSQL")
                self.__logger.debug("Sync db: Inserting missing data in MYSQL")
                for row in redis_mysql_diff:
                    mysql_query = "insert into %s values %s" % ('tickets', row)
                    self.rdbms_util.execute(self.sql_database, mysql_query)
            # Data not exists in redis
            mysql_redis_diff = mysql_data_set - redis_data_set
            if len(mysql_redis_diff) > 0:
                self.__logger.debug("Sync db: " + str(len(mysql_redis_diff)) +
                                    " records are missing in Redis")
                self.__logger.debug("Sync db: Inserting missing data in Redis")
                for row in mysql_redis_diff:
                    redis_key = 'FPL:feeders:tickets'
                    diff_dict = {}
                    timeseries_jsnobj = {}
                    timeseries_list = []
                    diff_dict['feeder_id'] = str(row[0])
                    timeseries_jsnobj['dw_ticket_id'] = str(row[1])
                    timeseries_jsnobj['trbl_ticket_id'] = str(row[2])
                    timeseries_jsnobj['irpt_type_code'] = str(row[3])
                    timeseries_jsnobj['irpt_cause_code'] = str(row[4])
                    timeseries_jsnobj['support_code'] = str(row[5])
                    timeseries_jsnobj['cmi'] = str(row[6])
                    timeseries_jsnobj['power_off'] = datetime.datetime. \
                        strptime(row[7], self.date_format). \
                        strftime(self.redis_date_format)
                    timeseries_jsnobj['power_restore'] = datetime.datetime. \
                        strptime(row[8], self.date_format). \
                        strftime(self.redis_date_format)
                    timeseries_jsnobj['repair_action_type'] = str(row[9])
                    timeseries_jsnobj['repair_action_description'] = str(row[10])
                    timeseries_list.append(timeseries_jsnobj)
                    diff_dict['time_series'] = timeseries_list
                    redis_data_json_str = json.dumps(diff_dict)
                    redis_score = row[11]
                    value_dict = dict()
                    value_dict[redis_score] = redis_data_json_str
                    self.cache_util.batch_zadd(redis_key, row[0], value_dict)
                self.__logger.debug('Sync db: Tickets Data synchronised successfully!!')

            if len(redis_mysql_diff) == 0 and len(mysql_redis_diff) == 0:
                self.__logger.debug('Sync db: Tickets Data is in sync!!')
        except Exception:
            self.__logger.error('Sync db: Error while comparing Tickets data '
                                'in MYSQL and Redis')
            raise


if __name__ == '__main__':
    TENANT_ID = sys.argv[1]
    SYNCDATABASE = SyncDatabases(TENANT_ID)
    SYNCDATABASE.get_data()
