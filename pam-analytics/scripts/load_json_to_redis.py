"""Load Json data to Redis.

Load EDNA, SCADA, AMI and TICKETS Josn data to Redis.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import os
import sys
import json
import zipfile
import fnmatch
import shutil
from autogrid.pam.anomaly import utils
from autogrid.pam.data_ingestion.pam_redis_filter import PamRedisFilter
from autogrid.pam.data_ingestion.pam_mysql_filter import PamMySQLFilter
from autogrid.pam.anomaly.global_define import PAM
from autogrid.foundation.util.Logger import Logger


class LoadJsonDataWrapper(object):

    """Load Json data to Redis.

    Parameters
    ----------
    tenant_uid : str
        tenant_uid
    input_dir : str
        base directory of data
    """

    def __init__(self, tenant_uid, input_dir, insert_database):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.system = PAM
        self.tenant_uid = tenant_uid
        self.input_dir = input_dir
        self.insert_database = insert_database
        self.redis_filter = PamRedisFilter(self.tenant_uid)
        self.mysql_filter = PamMySQLFilter(PAM, self.tenant_uid)
        self.extracted_files_dir = 'extracted_zip_data'

        try:
            os.mkdir(self.extracted_files_dir)
        except Exception:
            self.__logger.debug("Error while creating temporary directory.")

    def load_json_data(self):
        """load json data to Redis."""
        for base_dir, dirs, files in os.walk(self.extracted_files_dir, topdown='true'):
            for in_file in files:
                in_file_path = os.path.join(base_dir, in_file)
                self.__logger.debug('Processing file ' + in_file_path)
                try:
                    with open(in_file_path) as json_file:
                        json_data = json.load(json_file)
                except ValueError:
                    self.__logger.debug("error in parsing json file" + in_file_path)
                json_data['start_time_utc'] = json_data['message_time_utc']
                json_data['end_time_utc'] = json_data['message_time_utc']
                if "SCADA" in json_data['message_type'].upper():
                    json_data['message_type'] = "SCADA"
                if "AMI" in json_data['message_type'].upper():
                    json_data['message_type'] = "AMI"
                if "EDNA" in json_data['message_type'].upper():
                    json_data['message_type'] = "EDNA"
                if "TICKET" in json_data['message_type'].upper():
                    json_data['message_type'] = "TICKETS"
                self.redis_filter(json_data)
        self.__logger.debug("Loaded json data successfully into Redis.")

    def load_json_data_to_mysql(self):
        """load json data to MySQL."""
        for base_dir, dirs, files in os.walk(self.extracted_files_dir, topdown='true'):
            for in_file in files:
                in_file_path = os.path.join(base_dir, in_file)
                self.__logger.debug('Processing file ' + in_file_path)
                try:
                    with open(in_file_path) as json_file:
                        json_data = json.load(json_file)
                except ValueError:
                    self.__logger.debug("error in parsing json file" + in_file_path)
                json_data['start_time_utc'] = json_data['message_time_utc']
                json_data['end_time_utc'] = json_data['message_time_utc']
                if "SCADA" in json_data['message_type'].upper():
                    json_data['message_type'] = "SCADA"
                if "AMI" in json_data['message_type'].upper():
                    json_data['message_type'] = "AMI"
                if "EDNA" in json_data['message_type'].upper():
                    json_data['message_type'] = "EDNA"
                if "TICKET" in json_data['message_type'].upper():
                    json_data['message_type'] = "TICKETS"
                self.mysql_filter(json_data)
        self.__logger.debug("Loaded json data successfully into MySQL.")

    def unzip_files(self, base_path):
        """Unzip zip files present in input directory.

        Parameters
        ----------
        base_path : str
            Input directory path which contains zip files.
        """
        pattern = '*.zip'
        for base_dir, dirs, files in os.walk(base_path):
            for filename in fnmatch.filter(files, pattern):
                self.__logger.debug("Extracting Zip file: " +
                                    str(os.path.join(base_dir, filename)))
                zipfile.ZipFile(os.path.join(base_dir, filename)).extractall(
                    os.path.join(self.extracted_files_dir,
                                 os.path.splitext(filename)[0]))

    def remove_temp_dir(self):
        """Remove temporary directory."""
        try:
            shutil.rmtree(self.extracted_files_dir)
        except Exception:
            self.__logger.debug("Error while deleting temporary directory : " +
                                self.extracted_files_dir)

    def run(self):
        """Run LoadJsonDataWrapper and loads json data in redis."""
        self.unzip_files(self.input_dir)

        if self.insert_database.lower() == 'redis' or self.insert_database.lower() \
                == 'both':
            self.load_json_data()
        if self.insert_database.lower() == 'mysql' or self.insert_database.lower() \
                == 'both':
            self.load_json_data_to_mysql()

        self.remove_temp_dir()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'Usage: ' + sys.argv[0] + ' <tenant_id> <input_data_base_dir> ' \
                                        '<insert_database>'
        exit(-1)

    TENANT_ID = sys.argv[1]
    FILE_DIR_PATH = sys.argv[2]

    if len(sys.argv) > 3:
        INSERT_DATABASE = sys.argv[3]
    else:
        INSERT_DATABASE = 'both'

    TENANT_UID = utils.get_tenant_uid(PAM, TENANT_ID)

    if TENANT_UID is None:
        print "Tenant uod can not be None. Exiting..."
        exit(-1)

    if not os.path.isdir(FILE_DIR_PATH):
        print 'Error: Provided argument is not a valid directory'
        print 'Provide input data directory path.'
        exit(-1)

    if INSERT_DATABASE.lower() not in ['mysql', 'redis', 'both']:
        print "Database should be Mysql or redis or both."
        exit(-1)

    WRAPPER = LoadJsonDataWrapper(TENANT_UID, FILE_DIR_PATH, INSERT_DATABASE)
    WRAPPER.run()
