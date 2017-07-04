# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import unittest
import os
import yaml
import json
import pandas as pd
from datetime import datetime, timedelta
from autogrid.pam.anomaly import utils
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc

ISOFORMAT = '%Y-%m-%dT%H:%M:%SZ'


class AnomalySparkJobTest(unittest.TestCase):
    def setUp(self):
        self.system = 'PAM_TEST'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        # self.settings_path = os.path.join(self.foundation_home, 'settings.yml')

        # Define database schema and create tenant record
        self.rdbms_path = os.path.join(self.foundation_home, 'rdbms.yml')
        rdbms_dict = yaml.load(open(self.rdbms_path, 'r'))[self.system]
        self.host = rdbms_dict['host']
        self.username = rdbms_dict['username']
        self.password = rdbms_dict['password']
        sql_file_path = os.path.join(self.home, 'scripts/define_schema.sql')
        os.system('mysql -h %s -u%s -p%s < %s %s' % (self.host, self.username,
                                                     self.password, sql_file_path,
                                                     self.system))

        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.cache_util = cacheUtil.CacheUtil()

        self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)
        if self.tenant_uid is None:
            tenant_query = "insert into tenant values(1,'FPL','FPL',NOW(),NOW(),'UTC');"
            self.rdbms_util.execute(self.system, tenant_query)
            self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)

        # Define key expiry time hours in hours and seconds
        self.expiry_time_hrs = 48
        self.expiry_time_secs = self.expiry_time_hrs * 60 * 60

        # Populate anomaly dataframe
        self._populate_anomaly_df()
        # Populate Redis test data
        self._populate_redis()
        # Populate MySQL data for Scada
        self._populate_mysql_scada()

    def _populate_anomaly_df(self):
        anomaly_col = ['anomaly', 'deviceId', 'devicePh', 'deviceType', 'feeder',
                       'signal_id', 'Time']
        cur_time = datetime.utcnow()
        data = [['LATERAL_OUTAGES', '-', '-', 'TICKETS', '100232', '10381870', cur_time],
                ['LATERAL_OUTAGES1', '-', '-', 'TICKETS', '100232', '10381870', cur_time],
                ['LATERAL_OUTAGES2', '-', '-', 'TICKETS', '100233', '10381870', cur_time],
                ['LATERAL_OUTAGES3', '-', '-', 'TICKETS', '100233', '10381870', cur_time],
                ['LATERAL_OUTAGES4', '-', '-', 'TICKETS', '100235', '10381870', cur_time]]

        # Create some anomalies and save it
        self.anomaly_df = pd.DataFrame(data=data, columns=anomaly_col)

    def _populate_redis(self):
        self.name_space = self.tenant_uid + ':' + 'test'
        for feeder  in range(10001, 10011, 1):
            scada_ts_list = list()
            start_time = datetime.utcnow()
            if (feeder in (10004, 10005, 10006)):
                start_time = start_time - timedelta(hours=(self.expiry_time_hrs + 1))
            strtime = start_time.strftime(ISOFORMAT)
            score = long(start_time.strftime("%s"))
            for val in range(321, 325, 1):
                scada_ts_dict = dict()
                scada_ts_dict['value'] = 'Value_' + str(val)
                scada_ts_dict['timestamp_utc'] = strtime
                scada_ts_list.append(scada_ts_dict)

            scada_data_dict = dict()
            scada_data_dict['feeder_id'] = feeder
            scada_data_dict['time_series'] = scada_ts_list

            value_dict = dict()
            value_dict[score] = json.dumps(scada_data_dict)
            self.cache_util.batch_zadd(self.name_space, feeder, value_dict)

    def _populate_mysql_scada(self):
        filterwarnings('ignore', category=sa_exc.SAWarning)

        scada_cols = ['feeder_id', 'value', 'timestamp_utc', 'score', 'tenant_id']
        data = []
        for feeder  in range(10001, 10011, 1):
            obsrv_data = 'Observer_data_' + str(feeder)
            cur_time = datetime.utcnow()
            score = long(datetime.utcnow().strftime("%s"))
            row = []
            row.append(feeder)
            row.append(obsrv_data)
            row.append(cur_time)
            row.append(score)
            row.append(self.tenant_id)
            data.append(row)

        # Add more data for some of same feeders
        for feeder  in range(10001, 10011, 3):
            obsrv_data = 'Observer_data_' + str(feeder)
            cur_time = datetime.utcnow()
            score = long(datetime.utcnow().strftime("%s"))
            row = []
            row.append(feeder)
            row.append(obsrv_data)
            row.append(cur_time)
            row.append(score)
            row.append(self.tenant_id)
            data.append(row)

        df = pd.DataFrame(data=data, columns=scada_cols)
        self.rdbms_util.insert(self.system, 'scada', df.to_dict('records'))

    def test_t1_save_anomalies_to_db(self):
        # Initially truncate anomaly table
        qry = 'TRUNCATE TABLE anomaly;'
        self.rdbms_util.execute(self.system, qry)
        utils.save_anomalies_to_db(self.system, self.tenant_id, self.anomaly_df)

        qry = 'SELECT count(*) FROM anomaly;'
        row = self.rdbms_util.select(self.system, qry)
        self.assertEqual(5, row[0][0])

    def test_t2_no_duplicate_anomalies(self):
        # Initially truncate anomaly table
        qry = 'TRUNCATE TABLE anomaly;'
        self.rdbms_util.execute(self.system, qry)

        # Populate data in anomaly table
        utils.save_anomalies_to_db(self.system, self.tenant_id, self.anomaly_df)

        qry = 'SELECT count(*) FROM anomaly;'
        row = self.rdbms_util.select(self.system, qry)
        cnt_before = row[0][0]

        # Try to insert same data again
        utils.save_anomalies_to_db(self.system, self.tenant_id, self.anomaly_df)

        row = self.rdbms_util.select(self.system, qry)
        cnt_after = row[0][0]
        self.assertEqual(cnt_before, cnt_after)

    def test_t3_get_keys(self):
        keys = utils.get_keys(self.name_space, '*')
        self.assertEqual(10, len(keys))

    def test_t4_remove_expired_keys(self):
        curr_time_in_sec = long(datetime.utcnow().strftime("%s"))
        exp_time = curr_time_in_sec - self.expiry_time_secs - 1
        utils.remove_expired_keys(self.name_space, '*', exp_time)
        keys = utils.get_keys(self.name_space, '*')
        self.assertEqual(7, len(keys))

    def test_t5_remove_expired_keys(self):
        keys = utils.get_db_keys(self.system, self.tenant_id, 'scada', 'feeder_id')
        self.assertEqual(10, len(keys))

    def tearDown(self):
        self.rdbms_util.execute(self.system, 'drop table scada;')
        self.rdbms_util.execute(self.system, 'drop table ami;')
        self.rdbms_util.execute(self.system, 'drop table edna;')
        self.rdbms_util.execute(self.system, 'drop table tickets;')
        self.rdbms_util.execute(self.system, 'drop table signatures;')
        self.rdbms_util.execute(self.system, 'drop table anomaly;')
        self.rdbms_util.execute(self.system, 'drop table feeder_metadata;')
        self.rdbms_util.execute(self.system, 'drop table alerts;')
        self.rdbms_util.execute(self.system, 'drop table models;')
        self.rdbms_util.execute(self.system, 'drop table tenant;')
        self.cache_util.flush()
