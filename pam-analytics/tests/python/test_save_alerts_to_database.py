# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

import unittest
import os
import yaml
import pandas as pd
from autogrid.pam.anomaly import utils
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.pam.alert.save_alerts_to_database import LoadAlertsToDatabase


class AlertGeneratorWrapperTest(unittest.TestCase):
    def setUp(self):
        self.system = 'PAM_TEST'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']

        # Define database schema and create tenant record
        self.rdbms_path = os.path.join(self.foundation_home, 'rdbms.yml')
        rdbms_dict = yaml.load(open(self.rdbms_path, 'r'))[self.system]
        self.host = rdbms_dict['host']
        self.username = rdbms_dict['username']
        self.password = rdbms_dict['password']
        sql_file_path = os.path.join(self.home, 'scripts/define_schema.sql')
        os.system('mysql -h %s -u%s -p%s < %s %s' % (self.host, self.username,
                                                     self.password,
                                                     sql_file_path,
                                                     self.system))

        self.rdbms_util = rdbmsUtil.RDBMSUtil()

        self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)
        if self.tenant_uid is None:
            tenant_query = "insert into tenant values(1,'FPL','FPL',NOW(),NOW(),'UTC');"
            self.rdbms_util.execute(self.system, tenant_query)
            self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)

        self.alert_dict = {"feeder_id": "811263", "alert_type": "Yellow",
                           "alert_sent_utc": "2016-01-25T23:29:23Z",
                           "model_version": "1.0",
                           "alert_time_utc": "2016-01-16T15:13:00Z",
                           "anomalies": [{
                               "Signal":
                                   "EUREKA FEEDER 3W133_F11263 BKR OPEN-CLOSED-OPEN",
                               "DeviceType": "FEEDER", "DeviceId": "3W133_F11263",
                               "Time": "2016-01-16T10:12:00-05:00",
                               "Anomaly": "BKR_CLOSE", "DevicePh": "-"}]}

        self.save_alerts_to_db = LoadAlertsToDatabase(self.system,
                                                      self.tenant_id)

    def test_t1_insert_raw_json_to_db(self):
        self.save_alerts_to_db.insert_raw_json_to_db(self.alert_dict)
        qry = 'SELECT count(*) FROM alerts;'
        row = self.rdbms_util.select(self.system, qry)
        expected_row_count = row[0][0]
        self.assertEqual(1, expected_row_count)

    def test_t2_update_alert_column(self):
        self.save_alerts_to_db.insert_raw_json_to_db(self.alert_dict)

        self.save_alerts_to_db.update_alert_column('published_to_kafka',
                                                   True, self.alert_dict)
        self.save_alerts_to_db.update_alert_column('sent_email',
                                                   True, self.alert_dict)

        qry = 'SELECT count(*) FROM alerts where published_to_kafka=True ' \
              'AND sent_email=True;'
        row = self.rdbms_util.select(self.system, qry)
        print row
        expected_row_count = row[0][0]

        self.assertEqual(1, expected_row_count)

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
