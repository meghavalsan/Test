# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import unittest
import os
import json
import yaml
import redis
from autogrid.pam.recovery.sync_databases import SyncDatabases
from scripts.load_csv_to_redis import _load_csv, _process_tickets_files
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil


class SyncDatabasesTestCase(unittest.TestCase):
    def setUp(self):
        self.system = 'PAM_TEST'
        tenant_uid = 'FPL'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')
        settings_dict = yaml.load(open(self.settings_path, 'r'))[self.system]
        self.anomaly_calculation_hrs = settings_dict['anomaly_calculation_hrs']
        self.scada_cache_hrs = settings_dict['scada_cache_hrs']
        self.ami_cache_hrs = settings_dict['ami_cache_hrs']
        self.edna_cache_hrs = settings_dict['edna_cache_hrs']
        self.tickets_cache_hrs = settings_dict['tickets_cache_hrs']
        self.cache_util = cacheUtil.CacheUtil()
        self.rdbms_util = rdbmsUtil.RDBMSUtil()

        # TODO: This code needs to be removed after adding dbsize function in cacheutil

        self.foundation_path = os.path.join(self.foundation_home, 'cache.yml')
        redis_dict = yaml.load(open(self.foundation_path, 'r'))
        self.redis_host = redis_dict['host']
        self.redis_port = redis_dict['port']
        self.redis_db = redis_dict['db']
        self.rpool = redis.StrictRedis(host=self.redis_host,
                                       port=self.redis_port,
                                       db=self.redis_db)

        # Insert Sample csv data into MYSQL
        self.rdbms_path = os.path.join(self.foundation_home, 'rdbms.yml')
        rdbms_dict = yaml.load(open(self.rdbms_path, 'r'))[self.system]
        self.host = rdbms_dict['host']
        self.username = rdbms_dict['username']
        self.password = rdbms_dict['password']
        sql_file_path = os.path.join(self.home, 'scripts/define_schema.sql')
        os.system('mysql -h %s -u%s -p%s < %s %s' % (
            self.host, self.username, self.password, sql_file_path,
            self.system))
        tenant_query = "insert into tenant values(1,'FPL','FPL',NOW(),NOW(),'UTC');"
        self.rdbms_util.execute(self.system, tenant_query)

        # Insert Sample CSV Data into Redis
        base_dir = os.path.join(self.home, "tests/data/Redis_data")
        dirs = [d for d in os.listdir(base_dir) if
                os.path.isdir(os.path.join(base_dir, d))]
        for d in dirs:
            _load_csv(self.system, self.tenant_id, tenant_uid,
                      os.path.join(base_dir, d), 'both')

        # Tickets files are stored directly in basedir,
        # so get list of it and then process.
        files = [d for d in os.listdir(base_dir)
                 if
                 os.path.isfile(os.path.join(base_dir, d)) and 'TICKETS' in d]
        os.chdir(base_dir)
        _process_tickets_files(self.system, self.tenant_id, tenant_uid, files,
                               'both')

    def test_t1_get_data(self):
        # Remove some records from MySql
        scada_remove = "delete from scada where feeder_id=701837;"
        tickets_remove = "delete from tickets where feeder_id=804532;"
        self.rdbms_util.execute(self.system, scada_remove)
        self.rdbms_util.execute(self.system, tickets_remove)

        self.syndatabase = SyncDatabases(self.tenant_id, self.system)
        try:
            self.syndatabase.get_data()
            scada_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from scada;')[0][
                    'count(*)']
            ami_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from ami;')[0][
                    'count(*)']
            edna_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from edna;')[0][
                    'count(*)']
            tickets_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from tickets;')[0][
                    'count(*)']
            sum_of_count = scada_count + ami_count + edna_count + tickets_count
            redis_count = self.rpool.dbsize()
            status = 1

            if sum_of_count == 17 and redis_count == 7:
                status = 0
            self.assertEqual(0, status)

        except RuntimeError:
            self.assertTrue(False, 'Syncdatabase test case failed')

    def test_t2_get_data(self):
        # Remove some records from Redis
        self.cache_util.flush()

        self.syndatabase = SyncDatabases(self.tenant_id, self.system)
        try:
            self.syndatabase.get_data()
            scada_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from scada;')[0][
                    'count(*)']
            ami_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from ami;')[0][
                    'count(*)']
            edna_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from edna;')[0][
                    'count(*)']
            tickets_count = \
                self.rdbms_util.select(self.system,
                                       'select count(*) from tickets;')[0][
                    'count(*)']
            sum_of_count = scada_count + ami_count + edna_count + tickets_count
            redis_count = self.rpool.dbsize()
            status = 1
            if sum_of_count == 17 and redis_count == 7:
                status = 0
            self.assertEqual(0, status)

        except RuntimeError:
            self.assertTrue(False, 'Syncdatabase test case failed')

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


if __name__ == '__main__':
    unittest.main()
