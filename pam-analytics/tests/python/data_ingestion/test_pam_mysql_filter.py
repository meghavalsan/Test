# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Saaransh Gulati' <saaransh.gulati@auto-grid.com>

import unittest
import os
import yaml

import json
from autogrid.pam.data_ingestion.pam_mysql_filter import PamMySQLFilter
from autogrid.pam.anomaly import utils
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil


class PamMySQLFilterTestCase(unittest.TestCase):

    def setUp(self):
        self.system = 'PAM_TEST'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')

        # Define database schema and create tenant record
        self.rdbms_path = os.path.join(self.foundation_home, 'rdbms.yml')
        rdbms_dict = yaml.load(open(self.rdbms_path, 'r'))[self.system]
        self.host = rdbms_dict['host']
        self.port = str(rdbms_dict['port'])
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

        self.my_sql_filter = PamMySQLFilter('PAM_TEST', self.tenant_uid)

    def _get_total_rows(self, database_name, table_name):
        qry = 'SELECT count(*) FROM %s;' %(table_name)
        row = self.rdbms_util.select(database_name, qry)
        return row[0][0]

    def test_my_sql_call_edna(self):
        json_data ={}
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_15T02_00_811263_EDNA.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'edna')
        self.assertEqual(62, expected_row_count)

    def test_my_sql_call_ami(self):
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16T10_00_811263_AMI.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'ami')
        self.assertEqual(2176, expected_row_count)

    def test_my_sql_call_scada(self):
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16T10_00_811263_SCADA.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'scada')
        self.assertEqual(19, expected_row_count)

    def test_my_sql_call_tickets(self):
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16_TICKETS.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'tickets')
        self.assertEqual(379, expected_row_count)

    def test_my_sql_call_feeder_metadata(self):
        with open(os.path.join(self.home, "tests/data/JSON_data/feeder_metadata.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'feeder_metadata')
        self.assertEqual(2898, expected_row_count)

    def test_my_sql_call_feeder_metadata_overwrite(self):
        with open(os.path.join(self.home, "tests/data/JSON_data/feeder_metadata.json")) as json_file:
            json_data = json.load(json_file)

        self.my_sql_filter(json_data)
        # To test existing entries are deleted before new insertion.
        self.my_sql_filter(json_data)
        expected_row_count = self._get_total_rows(self.system, 'feeder_metadata')
        self.assertEqual(2898, expected_row_count)

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

if __name__ == '__main__':
    unittest.main()