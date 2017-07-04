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
from autogrid.pam.utils.weekly_report_generator import WeeklyReportGenerator
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc

ISOFORMAT = '%Y-%m-%dT%H:%M:%SZ'


class WeeklyReportGeneratorTest(unittest.TestCase):
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
                                                     self.password, sql_file_path,
                                                     self.system))

        self.rdbms_util = rdbmsUtil.RDBMSUtil()

        self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)
        if self.tenant_uid is None:
            tenant_query = "insert into tenant values(1,'FPL','FPL',NOW(),NOW(),'UTC');"
            self.rdbms_util.execute(self.system, tenant_query)
            self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)

        self.wrg = WeeklyReportGenerator(self.system, self.tenant_id)

    def _populate_mysql_alerts(self):
        filterwarnings('ignore', category=sa_exc.SAWarning)

        alerts_cols = ['feeder_id', 'alert_type', 'alert_sent_utc',
                       'model_version', 'alert_time_utc', 'raw_json',
                       'published_to_kafka', 'sent_email', 'tenant_id']

        anomalies_json = '[{"Signal": "BKR CLOSED", "DeviceType": ' \
                         '"FEEDER", "DeviceId": "266_FX123", "Time": ' \
                         '"2015-08-26T17:35:00Z", "Anomaly": "BKR_CLOSE", ' \
                         '"DevicePh": "-"}, {"Signal": "BKR CLOSED", "DeviceType":' \
                         ' "FEEDER", "DeviceId": "266_F123", "Time": ' \
                         '"2015-08-26T17:39:00Z", "Anomaly": "BKR_CLOSE", ' \
                         '"DevicePh": "-"}]'
        anomalies_dict = json.loads(anomalies_json)
        print anomalies_dict
        data = []
        for feeder  in range(10001, 10011, 1):
            raw_json_list = dict()
            alert_type = 'Orange'
            alert_sent_utc = (datetime.utcnow() - timedelta(days=8)).strftime(ISOFORMAT)
            model_version = '1.0'
            alert_time_utc = (datetime.utcnow() - timedelta(days=8)).strftime(ISOFORMAT)

            raw_json_list['feeder_id'] = feeder
            raw_json_list['alert_type'] = alert_type
            raw_json_list['alert_sent_utc'] = alert_sent_utc
            raw_json_list['model_version'] = model_version
            raw_json_list['alert_time_utc'] = alert_time_utc
            raw_json_list['anomalies'] = anomalies_dict

            raw_json = json.dumps(raw_json_list)
            published_to_kafka = 0
            sent_email = 0

            row = []
            row.append(feeder)
            row.append(alert_type)
            row.append(alert_sent_utc)
            row.append(model_version)
            row.append(alert_time_utc)
            row.append(raw_json)
            row.append(published_to_kafka)
            row.append(sent_email)
            row.append(self.tenant_id)

            data.append(row)

        df = pd.DataFrame(data=data, columns=alerts_cols)
        self.rdbms_util.insert(self.system, 'alerts', df.to_dict('records'))

    def _populate_mysql_tickets(self):
        filterwarnings('ignore', category=sa_exc.SAWarning)
        tickets_columns = ['cmi', 'dw_ticket_id', 'irpt_cause_code',
                           'irpt_type_code', 'power_off', 'power_restore',
                           'repair_action_description', 'repair_action_type',
                           'support_code', 'trbl_ticket_id', 'feeder_id']
        data = []
        for feeder  in range(10001, 10011, 1):
            cmi = feeder * 3.1
            dw_ticket_id = 'Ticket_' + str(feeder)
            irpt_cause_code = 'Cause_code_' + str(feeder)
            irpt_type_code = 'Type_code_' + str(feeder)
            power_off = (datetime.utcnow() - timedelta(days=8)).strftime(ISOFORMAT)
            power_restore = (datetime.utcnow() - timedelta(days=8)).strftime(ISOFORMAT)
            repair_action_desc = 'repair_action_description_' + str(feeder)
            repair_action_type = 'repair_action_type_' + str(feeder)
            support_code = 'support_code_' + str(feeder)
            trbl_ticket_id = 'trbl_ticket_id_' + str(feeder)

            row = []
            row.append(cmi)
            row.append(dw_ticket_id)
            row.append(irpt_cause_code)
            row.append(irpt_type_code)
            row.append(power_off)
            row.append(power_restore)
            row.append(repair_action_desc)
            row.append(repair_action_type)
            row.append(support_code)
            row.append(trbl_ticket_id)
            row.append(feeder)

            data.append(row)

        df = pd.DataFrame(data=data, columns=tickets_columns)
        self.rdbms_util.insert(self.system, 'tickets', df.to_dict('records'))

    def test_t1_select_alerts(self):
        self._populate_mysql_alerts()
        alerts_json = self.wrg.get_alerts_data()
        self.assertEqual(10, len(alerts_json))

    def test_t1_select_tickets(self):
        self._populate_mysql_tickets()
        tickets_df = self.wrg.get_tickets_data()
        self.assertEqual(10, tickets_df.shape[0])

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
