# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Saaransh Gulati' <saaransh.gulati@auto-grid.com>

import unittest
import os
import yaml
from autogrid.pam.anomaly import utils
import json
from autogrid.pam.data_ingestion.pam_redis_filter import PamRedisFilter
import autogrid.foundation.util.CacheUtil as cacheUtil
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil


class PamRedisFilterTestCase(unittest.TestCase):

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
        self.redis_filter = PamRedisFilter(self.tenant_uid)
        self.cache_util = cacheUtil.CacheUtil()

    def test_pam_redis_call_edna(self):
        json_data ={}
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_15T02_00_811263_EDNA.json")) as json_file:
            json_data = json.load(json_file)

        self.redis_filter(json_data)
        expected_keys_count = len(utils.get_keys('FPL:feeders:edna', '*'))
        self.assertEqual(1, expected_keys_count)

    def test_pam_redis_call_ami(self):
        json_data ={}
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16T10_00_811263_AMI.json")) as json_file:
            json_data = json.load(json_file)

        self.redis_filter(json_data)
        expected_keys_count = len(utils.get_keys('FPL:feeders:ami', '*'))
        self.assertEqual(1, expected_keys_count)

    def test_pam_redis_call_scada(self):
        json_data ={}
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16T10_00_811263_SCADA.json")) as json_file:
            json_data = json.load(json_file)

        self.redis_filter(json_data)
        expected_keys_count = len(utils.get_keys('FPL:feeders:scada', '*'))
        self.assertEqual(1, expected_keys_count)

    def test_pam_redis_call_tickets(self):
        json_data ={}
        with open(os.path.join(self.home, "tests/data/JSON_data/2016_01_16_TICKETS.json")) as json_file:
            json_data = json.load(json_file)

        self.redis_filter(json_data)
        expected_keys_count = len(utils.get_keys('FPL:feeders:tickets', '*'))
        self.assertEqual(186, expected_keys_count)

    def tearDown(self):
        self.cache_util.flush()
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