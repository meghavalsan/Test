"""AlertGeneratorWrapper Module.

This is a wrapper class to run autogrid.pam.alert.alert.AlertGenerator class.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

import yaml
import os
import MySQLdb
import pandas as pd
import datetime
from autogrid.pam.anomaly.global_define import UTC
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.alert.alert import AlertGenerator
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc


class AlertGeneratorWrapper(object):

    """Wrapper class to run autogrid.pam.alert.alert.AlertGenerator class.

    Parameters
    ----------
    system : str
        System name - PAM

    clf : RandomForestClassifier
        RandomForestClassifier instance that includes model.

    tenant_id : int
        unique id of tenant.
    """

    def __init__(self, system, clf, tenant_id, model_id, dataset_config_file,
                 anomaly_map_file, alert_config_file, last_model):

        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.system = system
        self.tenant_id = int(tenant_id)
        self.model_id = model_id
        self.last_model = last_model
        self.foundation_home = os.environ.get('FOUNDATION_HOME')

        if self.foundation_home is None:
            self.__logger.error("FOUNDATION_HOME is not set in environment")
            raise KeyError('FOUNDATION_HOME')

        self.settings_path = os.path.join(self.foundation_home, 'settings.yml')
        settings_dict = yaml.load(open(self.settings_path, 'r'))[self.system]
        self.processed_anomalies_window_hrs = \
            settings_dict['processed_anomalies_window_hrs']

        self.dataset_config = yaml.load(open(dataset_config_file, 'rb'))
        self.anomaly_map = yaml.load(open(anomaly_map_file, 'rb'))
        self.alert_config = yaml.load(open(alert_config_file, 'rb'))

        self.feeder_df = None
        self.clf = clf
        self.start_time = None
        self.end_time = None
        self.unprocessed_df = None
        self.processed_df = None
        self.old_alerts_df = None
        self.unprocessed_max_time = None
        self.unprocessed_min_time = None

    def get_feeder_metadata(self):
        """Get feeder metadata function.

        Read feeder metadata from feeder_metadata table.

        Returns
        -------
        feeder_df : dataframe
            A feeder data frame containing cross sectional data.
        """
        try:
            self.__logger.debug('Alert gen: Getting feeder metadata from database..')
            return_cols = ['feeder_id', '1ph_ocr', '3ph_ocr', '4n_feeder',
                           'afs', 'afs_scheme', 'area', 'capbank',
                           'cemm35_feeder', 'commercial', 'county',
                           'customers', 'fci', 'fdr_oh', 'fdr_ug', 'hardening',
                           'industrial', 'install_date', 'kv', 'lat_oh',
                           'lat_ug', 'pole_count', 'region', 'relay',
                           'residential', 'substation', 'type', 'has_afs',
                           'has_afs_ocr', 'is_dade', 'has_industrial',
                           'length', 'pct_ug', 'oh_fdr', 'ug_fdr', 'hybrid']
            query = "SELECT %s FROM feeder_metadata;" % (','.join(return_cols))
            feeder_data = self.rdbms_util.select(self.system, query)
            feeder_columns = ['FEEDER', '1PH_OCR', '3PH_OCR', '4N+_FEEDER',
                              'AFS', 'AFS_SCHEME', 'AREA', 'CAPBANK',
                              'CEMM35_FEEDER', 'COMMERCIAL', 'COUNTY',
                              'CUSTOMERS', 'FI', 'FDR_OH', 'FDR_UG',
                              'HARDENING', 'INDUSTRIAL', 'INSTALL_DATE',
                              'KV', 'LAT_OH', 'LAT_UG', 'POLE_COUNT', 'REGION',
                              'RELAY', 'RESIDENTIAL', 'SUBSTATION', 'TYPE',
                              'HAS_AFS', 'HAS_AFS_OCR', 'IS_DADE',
                              'HAS_INDUSTRIAL', 'LENGTH', 'PCT_UG', 'OH_FDR',
                              'UG_FDR', 'HYBRID']

            feeder_df = pd.DataFrame(feeder_data, columns=feeder_columns)
            feeder_df.set_index('FEEDER', drop=False, inplace=True)
            feeder_df['HARDENING'] = pd.to_datetime(feeder_df['HARDENING'])
            feeder_df['HARDENING'] = feeder_df['HARDENING'].dt.tz_localize(UTC)
            feeder_df.HARDENING.fillna(pd.Timestamp('1982-07-18T00:00:00Z'),
                                       inplace=True)
            feeder_df['INSTALL_DATE'] = pd.to_datetime(feeder_df['INSTALL_DATE'])
            feeder_df['INSTALL_DATE'] = feeder_df['INSTALL_DATE'].dt.tz_localize(UTC)
            feeder_df['CEMM35_FEEDER'] = feeder_df['CEMM35_FEEDER'].astype(bool)
            feeder_df['4N+_FEEDER'] = feeder_df['4N+_FEEDER'].astype(bool)
            return feeder_df
        except Exception:
            self.__logger.error("Alert gen: Error while reading feeder "
                                "metadata from database.")
            raise

    def map_anomaly_names(self, df):

        df.loc[(df.Anomaly == 'CURRENT_LIMIT') &
               (df.DeviceType == 'FDRHD'), 'Anomaly'] = 'FDRHD_CURRENT_LIMIT'
        df.loc[(df.Anomaly == 'CURRENT_LIMIT') &
               (df.DeviceType == 'FEEDER'), 'Anomaly'] = 'FEEDER_CURRENT_LIMIT'
        df.loc[(df.Anomaly == 'CURRENT_LIMIT') &
               (df.DeviceType == 'INTELI'), 'Anomaly'] = 'INTELI_CURRENT_LIMIT'
        df.loc[(df.Anomaly == 'FAULT_CURRENT') &
               (df.DeviceType == 'INTELI'), 'Anomaly'] = 'INTELI_FAULT_CURRENT'
        df.loc[(df.Anomaly == 'HIGH_VOLTAGE') &
               (df.DeviceType == 'FDRHD'), 'Anomaly'] = 'FDRHD_HIGH_VOLTAGE'
        df.loc[(df.Anomaly == 'HIGH_VOLTAGE') &
               (df.DeviceType == 'FEEDER'), 'Anomaly'] = 'FEEDER_HIGH_VOLTAGE'
        df.loc[(df.Anomaly == 'HIGH_VOLTAGE') &
               (df.DeviceType == 'INTELI'), 'Anomaly'] = 'INTELI_HIGH_VOLTAGE'
        df.loc[(df.Anomaly == 'TEMP_FAULT_CURRENT') &
               (df.DeviceType == 'INTELI'), 'Anomaly'] = 'INTELI_TEMP_FAULT_CURRENT'
        df.loc[(df.Anomaly == 'VOLTAGE_DROP') &
               (df.DeviceType == 'FDRHD'), 'Anomaly'] = 'FDRHD_VOLTAGE_DROP'
        df.loc[(df.Anomaly == 'VOLTAGE_DROP') &
               (df.DeviceType == 'FEEDER'), 'Anomaly'] = 'FEEDER_VOLTAGE_DROP'
        df.loc[(df.Anomaly == 'VOLTAGE_DROP') &
               (df.DeviceType == 'INTELI'), 'Anomaly'] = 'INTELI_VOLTAGE_DROP'

        return df

    def get_unprocessed_data(self):
        """Read unprocessed data from anomaly table."""
        try:
            self.__logger.debug('Alert gen: Getting unprocessed data from database...')
            # drop the columns that we don't require in alert processing
            columns_to_drop = ['processedTime_utc', 'isProcessed', 'tenant_id']

            query = "SELECT * FROM anomaly WHERE tenant_id=%s and isProcessed=0;"
            unprocessed_data = self.rdbms_util.select(self.system, query,
                                                      (self.tenant_id))
            unprocessed_df_columns = ['Anomaly', 'DeviceId', 'DevicePh',
                                      'DeviceType', 'Feeder', 'Signal',
                                      'Time', 'processedTime_utc',
                                      'isProcessed', 'tenant_id']
            self.unprocessed_df = pd.DataFrame(unprocessed_data,
                                               columns=unprocessed_df_columns)

            # drop the columns from data frame that we don't require
            # in alert processing.
            self.unprocessed_df.drop(columns_to_drop, axis=1, inplace=True)

            self.unprocessed_df = self.map_anomaly_names(self.unprocessed_df)

            self.unprocessed_df['Time'] = pd.to_datetime(self.unprocessed_df['Time'])
            # max_time and min_time is used to calculate the range of
            # processed anomalies.
            self.unprocessed_max_time = self.unprocessed_df['Time'].max()
            self.unprocessed_min_time = self.unprocessed_df['Time'].min()

            self.unprocessed_df['Time'] = self.unprocessed_df.\
                Time.dt.tz_localize(UTC)

            self.start_time = self.unprocessed_df['Time'].min()
            self.end_time = self.unprocessed_df['Time'].max()
        except MySQLdb.Error:
            self.__logger.error("Alert gen: Error while getting unprocessed "
                                "data from MYSQL.")
            raise

    def get_processed_data(self):
        """Read processed data from anomaly table."""
        try:
            self.__logger.debug('Alert gen: Getting processed data from database...')
            # drop the columns that we don't require in alert processing
            columns_to_drop = ['processedTime_utc', 'isProcessed', 'tenant_id']

            processed_max_time = self.unprocessed_max_time + datetime.\
                timedelta(hours=self.processed_anomalies_window_hrs)
            processed_min_time = self.unprocessed_min_time - datetime.\
                timedelta(hours=self.processed_anomalies_window_hrs)
            query = "SELECT * FROM anomaly WHERE tenant_id=%s and isProcessed=1 " \
                    "and timestamp_utc >= %s and timestamp_utc <= %s;"
            processed_data = self.rdbms_util.select(
                self.system, query, (self.tenant_id, processed_min_time,
                                     processed_max_time))
            processed_df_columns = ['Anomaly', 'DeviceId', 'DevicePh',
                                    'DeviceType', 'Feeder', 'Signal',
                                    'Time', 'processedTime_utc',
                                    'isProcessed', 'tenant_id']
            self.processed_df = pd.DataFrame(processed_data,
                                             columns=processed_df_columns)
            # drop the columns from data frame that we don't require
            # in alert processing.
            self.processed_df.drop(columns_to_drop, axis=1, inplace=True)

            self.processed_df = self.map_anomaly_names(self.processed_df)

            self.processed_df['Time'] = pd.to_datetime(self.processed_df['Time'])
            self.processed_df['Time'] = self.processed_df.\
                Time.dt.tz_localize(UTC)
        except MySQLdb.Error:
            self.__logger.error("Alert gen: Error while getting unprocessed "
                                "data from MYSQL.")
            raise

    def get_old_alerts_data(self):
        """Read data from signatures table."""
        try:
            self.__logger.debug('Alert gen: Getting old alerts data from database...')
            # drop the columns that we don't require in alert processing
            columns_to_drop = ['processedTime_utc', 'tenant_id', 'sent']

            self.old_alerts_df = pd.DataFrame(self.old_alerts_df)

            range_query = "SELECT * FROM signatures WHERE tenant_id=%s and " \
                          "Alert != 'None';"
            old_alerts_data = self.rdbms_util.select(
                self.system, range_query, self.tenant_id)
            old_alerts_columns = ['FEEDER', 'TIMESTAMP', 'model_id', 'PROB',
                                  'TIER', 'ALERT', 'processedTime_utc',
                                  'tenant_id', 'sent']
            self.old_alerts_df = pd.DataFrame(old_alerts_data,
                                              columns=old_alerts_columns)
            # drop the columns from data frame that we don't require
            # in alert processing.
            self.old_alerts_df.drop(columns_to_drop, axis=1, inplace=True)

            self.old_alerts_df['TIMESTAMP'] = pd.\
                to_datetime(self.old_alerts_df['TIMESTAMP'])
            self.old_alerts_df['TIMESTAMP'] = self.old_alerts_df.\
                TIMESTAMP.dt.tz_localize(UTC)
        except MySQLdb.Error:
            self.__logger.error("Alert gen: Error while getting unprocessed "
                                "data from MYSQL.")
            raise

    def update_anomaly_processed_time(self):
        """Method to update processedTime_utc of anomaly table."""
        try:
            current_time = datetime.datetime.utcnow()
            query = "UPDATE anomaly SET processedTime_utc=%s, isProcessed=1 " \
                    "WHERE tenant_id=%s and isProcessed=0;"
            self.rdbms_util.execute(self.system, query, (current_time, self.tenant_id))
            self.__logger.debug("Alert gen: Updated processedTime_utc = " +
                                str(current_time) + " and isProcessed = 1")
        except Exception:
            self.__logger.error("Alert gen: Error while updating "
                                "processedTime_utc column of anomaly table.")
            raise

    def save_preds_df_to_db(self, pred_df):
        """Method to save preds dataframe of alert.py to database.

        Parameters
        ----------
        alert_pred_df : dataframe
            A DataFrame with the Feeder ID and timestamp associated with each
            signature in ``X`` as well as the probability of failure and the
            alert level. It is in same row order as ``X``.
        """
        filterwarnings('ignore', category=sa_exc.SAWarning)
        try:
            # Add additional columns to dataframe
            pred_df['model_id'] = self.model_id #'PAM 1.0'
            pred_df['processedTime_utc'] = datetime.datetime.utcnow()
            pred_df['tenant_id'] = self.tenant_id
            pred_df['sent'] = False

            pred_df = pred_df.rename(columns={'FEEDER': 'feeder_id',
                                              'TIMESTAMP': 'timestamp_utc',
                                              'PROB': 'probability',
                                              'ALERT': 'alert',
                                              'TIER': 'tier'})

            self.rdbms_util.insert(self.system, 'signatures',
                                   pred_df.to_dict('records'))
            self.__logger.debug('Alert gen: Saved preds in database.')
        except Exception:
            self.__logger.error("Alert gen: Error while saving preds table in database.")
            raise

    def run_alerts(self):
        """Method to run alert generator class's generate_alerts method.

        Returns
        -------
        alert_list : list
            list of alert objects. Can be serialized to JSON for sending
            to the customer.
        """
        self.feeder_df = self.get_feeder_metadata()
        self.get_unprocessed_data()
        self.get_processed_data()
        self.get_old_alerts_data()

        alert_generator = AlertGenerator(clf=self.clf,
                                         dataset_config=self.dataset_config,
                                         anomaly_map=self.anomaly_map,
                                         alert_config=self.alert_config,
                                         model_id=self.model_id,
                                         feeder_df=self.feeder_df,
                                         start_time=self.start_time,
                                         end_time=self.end_time)

        self.__logger.debug('Calling generate_alerts method of AlertGenerator class...')
        alert_list = alert_generator.generate_alerts(unprocessed=self.unprocessed_df,
                                                     processed=self.processed_df,
                                                     old_alerts=self.old_alerts_df)
        pred_df = alert_generator.preds
        if not pred_df.empty:
            self.save_preds_df_to_db(pred_df)
        else:
            self.__logger.debug('Alert gen: Nothing to save in signatures '
                                'table as pred_df dataframe is empty.')
        if self.last_model:
            self.update_anomaly_processed_time()
        self.__logger.debug('Alert gen: AlertGeneratorWrapper completed successfully...')
        return alert_list
