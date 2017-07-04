"""Validation filter.

Ensures that all the input is valid, particularly the timestamps
are valid and in our required iso8601 format
Ensures that only those timestamps are present in the returned JSON that
lie between message_start_time_utc and message_end_time_utc (with the
exception of AMI which may legitimately have earlier data).
Returns a clean JSON, that will be subsequently used for inserting into
MySQL and Redis
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

from yukon.filters.base_filter import BaseFilter
import iso8601 as isodate
from autogrid.foundation.util.Logger import Logger
from datetime import timedelta
import traceback
import os
import yaml


class PamValidationCleanupFilter(BaseFilter):

    """Save incoming data to mysql.

    These parameters are configured in sample_pipeline.yml.

    Parameters
    ----------
    url: str
        Database url
            eg. url = 'mysql://root:root@localhost:3306'

    database : str
        Name of database
            eg. database ='PAM'

    tenant_uid : str
        Tenant UID
    """

    def __init__(self, tenant_uid):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.tenant_uid = tenant_uid
        # This is hard-coded to 1 at the moment because we are still not sure
        # if we can pass tenant_uid while initializing or not
        self.tenant_id = 1
        self.date_format = "%Y-%m-%d %H:%M:%S"
        # Get settings.yml cache size
        foundation_home = os.environ.get('FOUNDATION_HOME')
        if foundation_home is None:
            self.__logger.error('Environment variable FOUNDATION_HOME is not set.')
            raise KeyError('FOUNDATION_HOME')
        settings_yaml = os.path.join(foundation_home, 'settings.yml')
        config_dict = yaml.load(open(settings_yaml, 'r'))['PAM']
        self.ami_cache_hrs = config_dict['ami_cache_hrs']

    def __call__(self, message):
        """Make JSON from incoming message and call _validate_json method.

        Parameters
        ----------
        message : instance of autogrid.foundation.messaging.message class
            Incoming message data for AMI, SCADA, EDNA, Tickets, Feeder
            as per design document
        Returns
        -------
        valid_json : dict
            Valid json after removing incorrect data
        """
        try:
            data = {
                'message_id': message.message_id,
                'message_type': message.message_type,
                'message_time_utc': message.message_time_utc,
                'data': message.data,
                'start_time_utc': message.start_time_utc,
                'end_time_utc': message.end_time_utc
            }
        except ValueError:
            self.__logger.error("Data ingest: One of the mandatory column is missing..")
            self.__logger.error(traceback.format_exc())
            raise
        except Exception:
            self.__logger.error("Data ingest: Error while parsing message..")
            self.__logger.error(traceback.format_exc())

        if data['message_type'].lower() not in ['edna', 'ami', 'scada', 'tickets',
                                                'feeder_metadata']:
            self.__logger.error("Data ingest: message_type field does not "
                                "have valid values.")
            self.__logger.error(traceback.format_exc())
            return {}

        scada_ts_columns = ['timestamp_utc', 'value']
        edna_ts_columns = ['status', 'timestamp_utc', 'value_string', 'value']
        ami_ts_columns = ['event_id', 'timestamp_utc', 'meter_id']
        tickets_ts_columns = ['trbl_ticket_id', 'power_restore', 'cmi',
                              'power_off', 'repair_action_type', 'irpt_type_code',
                              'support_code', 'dw_ticket_id', 'irpt_cause_code',
                              'repair_action_description']
        feeder_columns = ["feeder_id", "industrial", "capbank", "county", "1ph_ocr",
                          "type", "hardening", "area", "fdr_ug", "cemm35_feeder",
                          "lat_ug", "afs", "pole_count", "customers", "relay",
                          "commercial", "fci", "substation", "lat_oh", "afs_scheme",
                          "3ph_ocr", "region", "install_date", "4n_feeder", "fdr_oh",
                          "kv", "residential"]
        valid_json = {}

        self.__logger.debug("Data ingest: Validating " +
                            data['message_type'] + " file type")
        data_type = data['message_type'].lower()
        if data_type == 'edna':
            valid_json = self._validate_json(data_type, data, edna_ts_columns)
        elif data_type == 'ami':
            valid_json = self._validate_json(data_type, data, ami_ts_columns)
        elif data_type == 'scada':
            valid_json = self._validate_json(data_type, data, scada_ts_columns)
        elif data_type == 'tickets':
            valid_json = self._validate_json(data_type, data, tickets_ts_columns)
        elif data_type == 'feeder_metadata':
            valid_json = self._validate_json(data_type, data, feeder_columns)
        return valid_json

    ###################
    # PRIVATE METHODS #
    ###################

    def _validate_json(self, data_type, data, expected_coulmns):
        """Validate JSON.

        Parameters
        ----------
        data : dict
            Incoming JSON data for AMI, SCADA, EDNA, Tickets, Feeder as per
            design document
        expected_coulmns : list
            Expected list of columns for incoming json

        Returns
        -------
        valid_json : dict
            Valid json after removing incorrect data
        """
        start_time = isodate.parse_date(data['start_time_utc'])
        end_time = isodate.parse_date(data['end_time_utc'])
        ami_min_time = start_time - timedelta(hours=self.ami_cache_hrs)
        remove_feeder = []
        # Iterate over incoming dictionary
        for key, value in data.items():
            if key == 'data':
                # Iterate over data part of dictionary
                for val_data in value:
                    # If feeder is empty add it to remove_feeder list
                    feeder_id = val_data['feeder_id']
                    if len(feeder_id) == 0:
                        remove_feeder.append(val_data)
                    elif data_type == 'feeder_metadata':
                        columns = val_data.keys()
                        columns = [str(item) for item in columns]
                        valid_columns = self.\
                            _validate_columns(feeder_id,
                                              expected_coulmns, columns)
                        if not valid_columns:
                            remove_feeder.append(val_data)
                    elif data_type in ['edna', 'ami', 'scada', 'tickets']:
                        for new_key, new_value in val_data.items():
                            remove_timeseries = []
                            # Iterate over time_series part and check
                            # 1. If expected columns are present
                            # 2. If time_utc/power_off are in ISO8601
                            #       date format
                            # 3. If either condition doesnt met then
                            #       mark valid_columns as False
                            if new_key == 'time_series':
                                for val_time_series in new_value:
                                    columns = val_time_series.keys()
                                    columns = [str(item) for item in columns]
                                    valid_columns = self.\
                                        _validate_columns(feeder_id,
                                                          expected_coulmns, columns)
                                    if valid_columns:
                                        if 'timestamp_utc' in columns:
                                            timestamp_column = \
                                                val_time_series['timestamp_utc']
                                        elif 'power_off' in columns:
                                            timestamp_column = \
                                                val_time_series['power_off']
                                        else:
                                            timestamp_column = None
                                        if timestamp_column:
                                            try:
                                                time_utc = isodate.\
                                                    parse_date(timestamp_column)
                                                if not ((time_utc >= start_time) and
                                                        (time_utc <= end_time)):
                                                    valid_columns = False
                                                if (data_type == 'ami' and
                                                        time_utc > ami_min_time):
                                                    valid_columns = True
                                            except isodate.ParseError:
                                                valid_columns = False
                                    # If column flag is False then add json
                                    # to remove_timeseries list
                                    if not valid_columns:
                                        remove_timeseries.append(val_time_series)
                                # Remove data from dict if it is present
                                # in remove_timeseries
                                for ts_item in remove_timeseries:
                                    new_value.remove(ts_item)
                                    self.__logger.debug("Data ingest: Removing json "
                                                        "because one of "
                                                        "the TIME_SERIES columns are "
                                                        "missing or TIMESTAMP_UTC column "
                                                        "is outside START_TIME_UTC "
                                                        "and END_TIME_UTC for FEEDER %s "
                                                        "and MESSAGE_TYPE %s..."
                                                        % (feeder_id, data_type))
                                    self.__logger.debug("Data ingest: Removed josn is :" +
                                                        str(ts_item))
                                if len(new_value) == 0:
                                    remove_feeder.append(val_data)
        # Remove data from incoming dict if it is present in remove_feeder list
        for key, value in data.items():
            if key == 'data':
                for val_data in list(value):
                    if val_data in remove_feeder:
                        value.remove(val_data)
                        self.__logger.debug("Data ingest: Removing json as "
                                            "FEEDER/TIME_SERIES is "
                                            "empty for MESSAGE_TYPE %s" % data_type)
                        self.__logger.debug("Data ingest: removed json is :" +
                                            str(val_data))
        return data

    def _validate_columns(self, feeder_id, expected_cols, actual_cols):
        """Validate if all expected columns are present.

        Parameters
        ----------
        expected_cols: list
            Expected list of columns for incoming json
        actual_cols: list
            actual column list which are present in data

        Returns
        -------
        missing_flag: boolean
            flag value representing if expected columns are present in
            actual columns
        """
        missing_cols = []
        missing_flag = False
        for cols in expected_cols:
            if cols not in actual_cols:
                missing_flag = True
                missing_cols.append(cols)

        if missing_flag:
            self.__logger.error("Data ingest: %s column is missing for "
                                "FEEDER %s " % (missing_cols, feeder_id))
        return not missing_flag
