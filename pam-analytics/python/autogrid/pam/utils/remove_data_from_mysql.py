"""Removes data from MYSQL.

The script should removes all data prior to configurable time
from AMI, EDNA, SCADA tables.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import os
import yaml
import sys
import datetime
import MySQLdb
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import PAM


class RemoveDataFromMysql(object):

    """Removes data from Mysql.

    Parameters
    ----------
    tenant_id : int
        unique id of tenant.

    system : str
        system name
    """

    def __init__(self, tenant_id, system=PAM):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.__logger.debug("Remove data: Running remove data from Mysql...")
        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.system = system
        self.tenant_id = int(tenant_id)
        self.foundation_home = os.environ['FOUNDATION_HOME']
        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')
        settings_dict = yaml.load(open(self.settings_path, 'r'))[self.system]
        self.scada_mysql_expiry_days = settings_dict['scada_mysql_expiry_days']
        self.ami_mysql_expiry_days = settings_dict['ami_mysql_expiry_days']
        self.edna_mysql_expiry_days = settings_dict['edna_mysql_expiry_days']
        self.date_format = "%Y-%m-%d %H:%M:%S"
        self.sql_database = system

    def remove_data(self):
        """Remove data from Mysql.

        Removes all data prior to configurable time
        from AMI, EDNA, SCADA tables.
        """
        current_date = datetime.datetime.utcnow()
        scada_expiry_date = (current_date - datetime.
                             timedelta(days=self.scada_mysql_expiry_days)).\
            strftime(self.date_format)
        scada_query = "delete from scada where timestamp_utc < %s "
        self.__logger.debug("Remove data: Deleting data from scada table...")
        try:
            self.rdbms_util.execute(self.sql_database, scada_query, (scada_expiry_date))
            self.__logger.debug("Remove data: Successfully deleted data "
                                "from scada table...")
        except MySQLdb.Error:
            self.__logger.error("Remove data: Error while deleting data "
                                "from scada table")
            raise

        ami_expiry_date = (current_date - datetime.
                           timedelta(days=self.ami_mysql_expiry_days)).\
            strftime(self.date_format)
        ami_query = "delete from ami where timestamp_utc < %s "
        self.__logger.debug("Remove data: Deleting data from ami table...")
        try:
            self.rdbms_util.execute(self.sql_database, ami_query, (ami_expiry_date))
            self.__logger.debug("Remove data: Successfully deleted data "
                                "from ami table...")
        except MySQLdb.Error:
            self.__logger.error("Remove data: Error while deleting data "
                                "from ami table")
            raise

        edna_expiry_date = (current_date - datetime.
                            timedelta(days=self.edna_mysql_expiry_days)).\
            strftime(self.date_format)
        edna_query = "delete from edna where timestamp_utc < %s "
        self.__logger.debug("Remove data: Deleting data from edna table...")
        try:
            self.rdbms_util.execute(self.sql_database, edna_query, (edna_expiry_date))
            self.__logger.debug("Remove data: Successfully deleted data "
                                "from edna table...")
        except MySQLdb.Error:
            self.__logger.error("Remove data: Error while deleting data "
                                "from edna table")
            raise

if __name__ == '__main__':
    TENANT_ID = sys.argv[1]
    REMOVE_DATA = RemoveDataFromMysql(TENANT_ID)
    REMOVE_DATA.remove_data()
