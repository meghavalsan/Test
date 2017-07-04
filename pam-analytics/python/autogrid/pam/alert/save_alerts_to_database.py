"""LoadAlertsToDatabase Module.

This class saves the alerts's json in database..
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

import json
from datetime import datetime

import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger


class LoadAlertsToDatabase(object):

    """Class to load alert's in json format to database.

    Parameters
    ----------
    system : str
        System name - PAM

    tenant_id : int
        unique id of tenant.
    """

    def __init__(self, system, tenant_id):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.system = system
        self.tenant_id = int(tenant_id)

    def insert_raw_json_to_db(self, alert_json_dict):
        """Insert alert into alert table in database.

        Parameters
        ----------
        alert_json_dict : dict
            Alert object as json dict.
        """
        feeder_id = alert_json_dict["feeder_id"]
        alert_type = alert_json_dict["alert_type"]
        alert_sent_utc = datetime.strptime(alert_json_dict["alert_sent_utc"],
                                           '%Y-%m-%dT%H:%M:%SZ')
        model_version = alert_json_dict["model_version"]
        alert_time_utc = datetime.strptime(alert_json_dict["alert_time_utc"],
                                           '%Y-%m-%dT%H:%M:%SZ')
        raw_json = json.dumps(alert_json_dict)
        published_to_kafka = False
        sent_email = False
        tenant_id = self.tenant_id

        try:
            query_cols = ['feeder_id', 'alert_type', 'alert_sent_utc',
                          'model_version', 'alert_time_utc', 'raw_json',
                          'published_to_kafka', 'sent_email', 'tenant_id']

            query = "INSERT into alerts(" + ",".join(query_cols) \
                    + ") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.rdbms_util.execute(self.system, query, (feeder_id,
                                                         alert_type, alert_sent_utc,
                                                         model_version, alert_time_utc,
                                                         raw_json, published_to_kafka,
                                                         sent_email, tenant_id))
            self.__logger.debug("Alert gen: Inserted alert in database.")
        except Exception:
            self.__logger.error("Alert gen: Error while inserting data in "
                                "alert table in database.")
            raise

    def update_alert_column(self, col_name, col_value, alert_json_dict):
        """Update the column (published_to_kafka or sent_email).

        Update value as True or False based on success or failure of kafka or
        sent_mail functionality.

        Parameters
        ----------
        col_name : str
            column name of alert table that needs to be updated.
            (published_to_kafka or sent_email)

        col_value : bool
            True or False

        alert_json_dict : dict
            Alert object as json dict.
        """
        feeder_id = alert_json_dict["feeder_id"]
        alert_type = alert_json_dict["alert_type"]
        alert_sent_utc = datetime.strptime(alert_json_dict["alert_sent_utc"],
                                           '%Y-%m-%dT%H:%M:%SZ')
        model_version = alert_json_dict["model_version"]
        alert_time_utc = datetime.strptime(alert_json_dict["alert_time_utc"],
                                           '%Y-%m-%dT%H:%M:%SZ')
        tenant_id = self.tenant_id
        try:
            query = "UPDATE alerts SET " + col_name + "=%s where tenant_id=%s AND " \
                    "feeder_id=%s AND alert_type=%s AND alert_sent_utc=%s AND " \
                    "model_version=%s AND alert_time_utc=%s"

            self.rdbms_util.execute(self.system, query, (col_value, tenant_id,
                                                         feeder_id, alert_type,
                                                         alert_sent_utc, model_version,
                                                         alert_time_utc))
            self.__logger.debug("Alert gen: Updated " +
                                str(col_name) + " = " + str(col_value))
        except Exception:
            self.__logger.error("Alert gen: Error while updating %s " +
                                str(col_name) + " in alert table.")
