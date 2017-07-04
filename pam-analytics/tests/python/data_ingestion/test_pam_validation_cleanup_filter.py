# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import unittest
import os
import json
from autogrid.pam.data_ingestion.pam_validation_cleanup_filter import PamValidationCleanupFilter


class PamValidationCleanupFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.home = os.environ['PAM_ANALYTICS_HOME']
        os.environ['FOUNDATION_HOME'] = os.path.join(self.home, 'tests/config')
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.cleanup_filter = PamValidationCleanupFilter('FPL')

    def test_t1_validate_scada(self):
        scada_ts_columns = ['timestamp_utc', 'value']
        with open(os.path.join(self.home + "/tests/data/validation_data/test_data/scada.json")) as json_file:
            json_data = json.load(json_file)
        validated_json = self.cleanup_filter._validate_json('scada', json_data, scada_ts_columns)

        with open(os.path.join(self.home + "/tests/data/validation_data/expected_data/scada.json")) as json_file:
            expected_json_data = json.load(json_file)
        equal_flag = sorted(validated_json.items()) == sorted(expected_json_data.items())
        self.assertEqual(True, equal_flag)

    def test_t2_validate_ami(self):
        ami_ts_columns = ['event_id', 'timestamp_utc', 'meter_id']
        with open(os.path.join(self.home + "/tests/data/validation_data/test_data/ami.json")) as json_file:
            json_data = json.load(json_file)
        validated_json = self.cleanup_filter._validate_json('ami', json_data, ami_ts_columns)

        with open(os.path.join(self.home + "/tests/data/validation_data/expected_data/ami.json")) as json_file:
            expected_json_data = json.load(json_file)
        equal_flag = sorted(validated_json.items()) == sorted(expected_json_data.items())
        self.assertEqual(True, equal_flag)

    def test_t3_validate_edna(self):
        edna_ts_columns = ['status', 'timestamp_utc', 'value_string', 'value']
        with open(os.path.join(self.home + "/tests/data/validation_data/test_data/edna.json")) as json_file:
            json_data = json.load(json_file)
        validated_json = self.cleanup_filter._validate_json('edna', json_data, edna_ts_columns)

        with open(os.path.join(self.home + "/tests/data/validation_data/expected_data/edna.json")) as json_file:
            expected_json_data = json.load(json_file)
        equal_flag = sorted(validated_json.items()) == sorted(expected_json_data.items())
        self.assertEqual(True, equal_flag)

    def test_t4_validate_tickets(self):
        tickets_ts_columns = ['trbl_ticket_id', 'power_restore', 'cmi',
                           'power_off', 'repair_action_type', 'irpt_type_code',
                           'support_code', 'dw_ticket_id', 'irpt_cause_code',
                           'repair_action_description']
        with open(os.path.join(self.home + "/tests/data/validation_data/test_data/tickets.json")) as json_file:
            json_data = json.load(json_file)
        validated_json = self.cleanup_filter._validate_json('tickets', json_data, tickets_ts_columns)

        with open(os.path.join(self.home + "/tests/data/validation_data/expected_data/tickets.json")) as json_file:
            expected_json_data = json.load(json_file)
        equal_flag = sorted(validated_json.items()) == sorted(expected_json_data.items())
        self.assertEqual(True, equal_flag)

    def test_t5_validate_feeder_metadata(self):
        feeder_columns = ["feeder_id", "industrial", "capbank", "county", "1ph_ocr",
                             "type", "hardening", "area", "fdr_ug", "cemm35_feeder",
                             "lat_ug", "afs", "pole_count", "customers", "relay",
                             "commercial", "fci", "substation", "lat_oh", "afs_scheme",
                             "3ph_ocr", "region", "install_date", "4n_feeder", "fdr_oh",
                             "kv", "residential"]
        with open(os.path.join(self.home + "/tests/data/validation_data/test_data/feeder_metadata.json")) as json_file:
            json_data = json.load(json_file)
        validated_json = self.cleanup_filter._validate_json('feeder_metadata', json_data, feeder_columns)

        with open(os.path.join(self.home + "/tests/data/validation_data/expected_data/feeder_metadata.json")) as json_file:
            expected_json_data = json.load(json_file)
        equal_flag = sorted(validated_json.items()) == sorted(expected_json_data.items())
        self.assertEqual(True, equal_flag)


if __name__ == '__main__':
    unittest.main()