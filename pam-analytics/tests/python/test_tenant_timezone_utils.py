# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

import unittest
import os
import yaml
from pytz import timezone
from autogrid.pam.anomaly import utils
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil


class TenantUtilsTest(unittest.TestCase):
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

    def test_t1_get_timezone(self):
        expected_timezone = utils.get_timezone(self.system, self.tenant_id)
        self.assertEqual(timezone('UTC'), expected_timezone)

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
