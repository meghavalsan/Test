# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import unittest
import os
import yaml
import json
import shutil
from datetime import datetime, timedelta
from autogrid.pam.data_ingestion.pam_mysql_filter import PamMySQLFilter
from autogrid.pam.data_ingestion.pam_redis_filter import PamRedisFilter
from autogrid.pam.scripts.check_n_update_data import CheckAndUpdateData
from autogrid.pam.anomaly import utils
from autogrid.pam.anomaly.global_define import SECONDS_IN_HOUR
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil


class PamMySQLFilterTestCase(unittest.TestCase):

    def setUp(self):
        self.system = 'PAM_TEST'
        self.tenant_id = 1
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.data_ingest_check_hrs = 3

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
        self.cache_util = cacheUtil.CacheUtil()

        self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)
        if self.tenant_uid is None:
            tenant_query = "insert into tenant values(1, 'FPL', 'FPL', NOW()," \
                           "NOW(), 'UTC');"
            self.rdbms_util.execute(self.system, tenant_query)
            self.tenant_uid = utils.get_tenant_uid(self.system, self.tenant_id)

        self.my_sql_filter = PamMySQLFilter(self.system, self.tenant_uid)
        self.redis_filter = PamRedisFilter(self.tenant_uid)

        self.input_dir = os.path.join(self.home, 'tests/data/template_json_data')
        self.output_dir = os.path.join(self.home, 'tests/data/temp_' +
                                       str(datetime.utcnow()))

        self.outfile = os.path.join(self.output_dir, 'ingest_monit.log')
        self.chk_n_update = CheckAndUpdateData(self.system, self.tenant_uid,
                                               self.data_ingest_check_hrs,
                                               self.outfile)

        self.ISOFORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def generate_tmp_files(self, delta_hours):
        current_time = datetime.utcnow()
        previous_hr = current_time - timedelta(hours=delta_hours)
        previous_hr = previous_hr.strftime(self.ISOFORMAT)
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        for file_in in os.listdir(self.input_dir):
            in_file_path = os.path.join(self.input_dir, file_in)
            out_file_path = os.path.join(self.output_dir, file_in)
            with open(in_file_path) as json_file:
                json_data = json.load(json_file)
                json_data['data'][0]['time_series'][0]['timestamp_utc'] = previous_hr
                json_data['message_time_utc'] = previous_hr
                json_data['start_time_utc'] = previous_hr
                json_data['end_time_utc'] = previous_hr

            jsonFile = open(out_file_path, "w")
            jsonFile.write(json.dumps(json_data))
            jsonFile.close()

    def test_mysql_latest_time(self):
        # Generate myql data with  (data_ingest_check_hrs + 1) hrs old timestamp
        cur_time = datetime.utcnow()
        self.generate_tmp_files(self.data_ingest_check_hrs + 1)
        with open(os.path.join(self.output_dir, "edna.json")) as json_file:
            json_data = json.load(json_file)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "ami.json")) as json_file:
            json_data = json.load(json_file)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "scada.json")) as json_file:
            json_data = json.load(json_file)
            self.my_sql_filter(json_data)

        latest_mysql_timestamp = self.chk_n_update.get_mysql_latest_time()
        mysql_hrs = int((cur_time - latest_mysql_timestamp).total_seconds() / \
                    SECONDS_IN_HOUR)

        self.assertEqual(self.data_ingest_check_hrs + 1, mysql_hrs)


    def test_redis_latest_time(self):
        # Generate myql data with  (data_ingest_check_hrs + 1) hrs old timestamp
        cur_time = datetime.utcnow()
        self.generate_tmp_files(self.data_ingest_check_hrs + 1)
        with open(os.path.join(self.output_dir, "edna.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
        with open(os.path.join(self.output_dir, "ami.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
        with open(os.path.join(self.output_dir, "scada.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)

        latest_redis_timestamp = self.chk_n_update.get_redis_latest_time()
        redis_hrs = int((cur_time - latest_redis_timestamp).total_seconds() / \
                    SECONDS_IN_HOUR)

        self.assertEqual(self.data_ingest_check_hrs + 1, redis_hrs)

    def test_chk_update_log_file_1(self):
        # Generate myql data with  (data_ingest_check_hrs - 2) hrs old timestamp
        self.generate_tmp_files(self.data_ingest_check_hrs - 2)
        with open(os.path.join(self.output_dir, "edna.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "ami.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "scada.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)

        self.chk_n_update.run()
        self.assertTrue(os.path.isfile(self.outfile))

    def test_chk_update_log_file_2(self):
        # Generate myql data with  (data_ingest_check_hrs + 2) hrs old timestamp
        self.generate_tmp_files(self.data_ingest_check_hrs + 2)
        with open(os.path.join(self.output_dir, "edna.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "ami.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)
        with open(os.path.join(self.output_dir, "scada.json")) as json_file:
            json_data = json.load(json_file)
            self.redis_filter(json_data)
            self.my_sql_filter(json_data)

        self.chk_n_update.run()
        self.assertFalse(os.path.isfile(self.outfile))

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
        shutil.rmtree(self.output_dir)
