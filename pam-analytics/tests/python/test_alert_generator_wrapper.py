# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

import unittest
import os
import yaml
import pandas as pd
import datetime
from autogrid.pam.anomaly import utils
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.pam.alert.alert_generator_wrapper import AlertGeneratorWrapper
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc


class AlertGeneratorWrapperTest(unittest.TestCase):
    def setUp(self):
        self.system = 'PAM_TEST'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')
        self.settings_dict = yaml.load(open(self.settings_path, 'r'))[
            self.system]

        # Define database schema and create tenant record
        self.rdbms_path = os.path.join(self.foundation_home, 'rdbms.yml')
        rdbms_dict = yaml.load(open(self.rdbms_path, 'r'))[self.system]
        self.host = rdbms_dict['host']
        self.username = rdbms_dict['username']
        self.password = rdbms_dict['password']
        self.processed_window_hrs = self.settings_dict[
            'processed_anomalies_window_hrs']
        self.signatures_calculation_hrs = self.settings_dict[
            'signatures_calculation_hrs']
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

        self.alert_yml = os.path.join(self.foundation_home, 'pam_1_0_alert.yaml')
        self.anomaly_yml = os.path.join(self.foundation_home, 'pam_1_0_anomaly_map.yaml')
        self.dataset_yml = os.path.join(self.foundation_home, 'pam_1_0_dataset.yaml')

        self.alert_generator = AlertGeneratorWrapper(self.system, None,
                                                     self.dataset_yml,
                                                     self.anomaly_yml,
                                                     self.alert_yml,
                                                     'PAM_1.0',
                                                     self.tenant_id)

    def populate_anomaly(self):
        anomaly_col = ['anomaly', 'deviceId', 'devicePh', 'deviceType',
                       'feeder', 'signal_id', 'timestamp_utc',
                       'processedTime_utc', 'isProcessed', 'tenant_id']
        cur_time = datetime.datetime.utcnow()
        old_time = cur_time - datetime.timedelta(
            hours=(self.processed_window_hrs + 2))
        in_window_time = cur_time - datetime.timedelta(
            hours=(self.processed_window_hrs - 5))

        data = [['LATERAL_OUTAGES', '-', '-', 'TICKETS', '100232', '10381870',
                 cur_time, cur_time, 0, self.tenant_id],
                ['LATERAL_OUTAGES1', '-', '-', 'TICKETS', '100232', '10381870',
                 cur_time, cur_time, 0, self.tenant_id],
                ['LATERAL_OUTAGES2', '-', '-', 'TICKETS', '100233', '10381870',
                 cur_time, cur_time, 0, self.tenant_id],
                ['LATERAL_OUTAGES3', '-', '-', 'TICKETS', '100233', '10381870',
                 cur_time, cur_time, 0, self.tenant_id],
                ['LATERAL_OUTAGES4', '-', '-', 'TICKETS', '100235', '10381870',
                 cur_time, cur_time, 0, self.tenant_id],
                ['LATERAL_OUTAGES', '-', '-', 'AMI', '100232', '10381870',
                 old_time, cur_time, 1, self.tenant_id],
                ['LATERAL_OUTAGES1', '-', '-', 'AMI', '100232', '10381870',
                 in_window_time, cur_time, 1, self.tenant_id],
                ['LATERAL_OUTAGES2', '-', '-', 'AMI', '100233', '10381870',
                 old_time, cur_time, 1, self.tenant_id],
                ['LATERAL_OUTAGES3', '-', '-', 'AMI', '100233', '10381870',
                 in_window_time, cur_time, 1, self.tenant_id]]

        filterwarnings('ignore', category=sa_exc.SAWarning)
        # Create some anomalies and save it
        self.anomaly_df = pd.DataFrame(data=data, columns=anomaly_col)
        self.rdbms_util.insert(self.system, 'anomaly',
                               self.anomaly_df.to_dict('records'))

    def populate_signatures(self):
        signatures_col = ['feeder_id', 'timestamp_utc', 'model_id',
                          'probability', 'tier', 'alert', 'processedTime_utc',
                          'tenant_id', 'sent']
        cur_time = datetime.datetime.utcnow()
        signatures_old_time = cur_time - datetime.timedelta(
            hours=(self.signatures_calculation_hrs + 2))
        signatures_in_window_time = cur_time - datetime.timedelta(
            hours=(self.signatures_calculation_hrs - 5))

        data = [
            [701836, signatures_old_time, 'PAM 1.0', 0.4607, 0.795, 'None',
             cur_time, self.tenant_id, False],
            [701838, signatures_in_window_time, 'PAM 1.0', 0.4607, 0.795,
             'Orange', cur_time, self.tenant_id, False],
            [701839, signatures_in_window_time, 'PAM 1.0', 0.4607, 0.795,
             'Red', cur_time, self.tenant_id, False],
            [701837, signatures_old_time, 'PAM 1.0', 0.4607, 0.795, 'None',
             cur_time, self.tenant_id, False]]

        filterwarnings('ignore', category=sa_exc.SAWarning)
        # Create some anomalies and save it
        self.signatures_df = pd.DataFrame(data=data, columns=signatures_col)
        self.rdbms_util.insert(self.system, 'signatures',
                               self.signatures_df.to_dict('records'))


    def test_t1_get_unprocessed_data(self):
        self.populate_anomaly()
        self.alert_generator.get_unprocessed_data()
        expected_row_count = self.alert_generator.unprocessed_df.shape[0]
        self.assertEqual(5, expected_row_count)

    def test_t2_processed_data(self):
        self.populate_anomaly()
        self.alert_generator.get_unprocessed_data()
        self.alert_generator.get_processed_data()
        expected_row_count = self.alert_generator.processed_df.shape[0]
        self.assertEqual(2, expected_row_count)

    def test_t3_get_old_alerts_data(self):
        self.populate_signatures()
        self.alert_generator.get_old_alerts_data()
        expected_row_count = self.alert_generator.old_alerts_df.shape[0]
        self.assertEqual(2, expected_row_count)

    def test_t4_get_unprocessed_data_with_empyt_table(self):
        self.alert_generator.get_unprocessed_data()
        self.alert_generator.get_processed_data()
        expected_row_count = self.alert_generator.unprocessed_df.shape[0]
        self.assertEqual(0, expected_row_count)

    def test_t5_get_processed_data_with_empyt_table(self):
        self.alert_generator.get_unprocessed_data()
        self.alert_generator.get_processed_data()
        expected_row_count = self.alert_generator.processed_df.shape[0]
        self.assertEqual(0, expected_row_count)

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
