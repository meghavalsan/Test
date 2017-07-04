"""batch_alerts module.

The :mod:`batch_alerts` utilizes anomaly.py and alert.py modules to
generate batch alerts (typically in a daily fashion). This module is meant
to use on raw data pulled directly from the live system's mySQL table in a
csv format.

"""


# Copyright (c) 2011-2017 AutoGrid Systems
# Author(s): 'Eric Hsieh' eric.hsieh@auto-grid.com>


# I. libraries:
from autogrid.pam.anomaly import anomaly
from autogrid.pam.alert import alert

from sklearn.externals.joblib import Parallel, delayed, cpu_count
from datetime import datetime, timedelta
from pytz import timezone, UTC
from random import shuffle
import cPickle as pickle
import pandas as pd
import yaml
EASTERN = timezone('US/Eastern')


# II.functions:
def _parallel_anomalies(raw_df, fdr_col, feeders, data_type, meta_df,anomslist):
    '''
    PURPOSE: generate anomalies from raw data (use with parrelel job)
    INPUT:
        - raw_df (
          df of a specific data type ex. edna; contains info for all fdrs)
        - fdr_col (str; name of the fdr column)
        - feeders (list of a parrelel process group)
        - data_type (str; the type of data)
        - meta_df (df; meta info)
    OUTPUT:
        - group_anoms_df (df of anoms of a particular parallel group)
    '''
    # i. generate anomalies:
    group_anoms_df_lst = []
    loc_raw_df = raw_df.loc[raw_df[fdr_col].isin(feeders)].copy()
    # reindex df:
    loc_raw_df = loc_raw_df.reset_index()
    loc_raw_df.drop(['index'], axis=1, inplace=True)
    for fdr in loc_raw_df[fdr_col].unique():
        fdr_raw_df =\
            loc_raw_df.loc[loc_raw_df[fdr_col] == fdr]
        # different process per data type:
        if data_type == 'scada':
            loc_anoms = anomaly.ScadaAnomalies(feeder_id=fdr, anomalies=anomslist)
            loc_anoms = loc_anoms.extract(fdr_raw_df)
        elif data_type == 'ami':
            loc_anoms = anomaly.AmiAnomalies(feeder_id=fdr, anomalies=anomslist)
            if fdr in meta_df.index:
                customers = meta_df.loc[fdr].CUSTOMERS
                loc_anoms = loc_anoms.extract(fdr_raw_df, customers)
            else:
                print 'ERROR! fdr {} does not have meta data info!'.format(fdr)
                continue
        elif data_type == 'edna':
            loc_anoms = anomaly.EdnaAnomalies(feeder_id=fdr, anomalies=anomslist)
            loc_anoms = loc_anoms.extract(fdr_raw_df)
        else:
            print 'ERROR! {} data type is not supported!'.format(data_type)
            return
        # record result:
        loc_anoms_df = loc_anoms.to_df()
        if not loc_anoms_df.empty:
            group_anoms_df_lst.append(loc_anoms_df)
        else:
            continue
    # concat all dfs into master result:
    group_anoms_df = pd.concat(group_anoms_df_lst)

    return group_anoms_df


def _get_fdr_groups(n, df, fdr_col):
    '''
    PURPOSE:
        - generates fdr groups for parrelel processing
    INPUT:
        - n (int; number of groups)
        - df (df; raw data or anomalies)
        - fdr_col (str; name of the fdr column)
    OUTPUT:
        - fdr_groups (dict; keys: groups; values: fdrs)
    '''
    fdr_groups = dict.fromkeys(range(n))
    for group in fdr_groups:
        fdr_groups[group] = []
    for i, f in enumerate(df[fdr_col].value_counts().index.values):
        group = (i % (n * 2)) - n
        if group < 0:
            group = abs(group) - 1
        fdr_groups[group].append(f)
    for group in fdr_groups:
        shuffle(fdr_groups[group])

    return fdr_groups


def _generates_alert(
        fdr, anoms,
        clf, dataset_config, anomaly_map,
        alert_config, model_id, xsect):
    '''
    PURPOSE: generate alert for one fdr
    INPUT:
        - fdr : str
          fdr number
        - anoms: df (anomalies)
        - clf: sklearn (classifier)
        - ataset_config, anomaly_map, alert_config (configs)
        - model_id (str; model_id)
        - xsect (df; meta data)
    OUTPUT:
        - alerter (object)
          processed alerter class of a particular feeder
    '''
    alerter = alert.AlertGenerator(
            clf, dataset_config,
            anomaly_map, alert_config,
            model_id=model_id, feeder_df=xsect
        )
    alerter.generate_alerts(
            unprocessed=anoms.loc[anoms['Feeder'] == fdr],
            processed=None, old_alerts=None
        )
    return alerter


def _parallel_alerts(
        anoms, feeders,
        clf, dataset_config, anomaly_map,
        alert_config, model_id, xsect):
    '''
    PURPOSE: generate batch alerts (use with parrelel job)
    INPUT:
        - anoms: df (anomalies)
        - clf: sklearn (classifier)
        - ataset_config, anomaly_map, alert_config (configs)
        - model_id (str; model_id)
        - xsect (df; meta data)
    OUTPUT:
        - alerters_dict (dict of alerters)
    '''
    # i. generate alerts:
    loc_results_dict = {}
    loc_anom_df =\
        anoms.loc[anoms['Feeder'].isin(feeders)].copy()
    for fdr in anoms['Feeder'].unique():
        loc_results_dict[fdr] =\
            _generates_alert(
                    fdr, anoms,
                    clf, dataset_config, anomaly_map,
                    alert_config, model_id, xsect
                )
    return loc_results_dict


# III. classes:
class BatchAlerts(object):
    '''
    Class to hold batch alerts.

    Parameters
    ----------
    n_jobs: int
        - number of parrelel jobs
    start_date: pd.datetime (make sure put in the date form with date())
        - start date of the predictions/alerts.
    end_date : pd.datetime (make sure put in the date form with date())
        - end date of the predictions/alerts.
    raw_data_pwd: str
        - path to the folder that holds all the raw data
    meta_pwd: str
        - path to the folder that holds the meta data

    Attributes
    ----------
    raw_data_dict: dict
        - dict that holds dfs for raw data such as edan, ami, and scada
          (key: data types; values: dfs of respective datatypes)
    anoms_df: pd.DataFrame
        - df of all anomalies
    alerts_dict: dict
        - dict that holds alerts
          (key: fdr_number; values: df of alerts)
    xsect: pd.df
        - meta data for all the feeders
    time_cols_dict: dict
        - stores the name to the time col of
          different data types
          (keys: data type; values: name)
    fdr_cols_dict: dict
        - store the name of the fdr col of
          different data type
          (keys: data type; values: name)
    clf: sklearn classifier
    dataset_config, anomaly_map, alert_config: yaml
        - configurations
    data_min_date = pd.datetime
        - the min date that needs to be fetch to make desired calculations.
    Methods:
    ----------
    #1. load_data: ingests data
    #2. generates_anomalies: generate anomalies with parrelel processes
    #4. generates_alerts: generates alerts
    #5. _generates_alert: generate single alert
    #6. _load_csv: load csv files
    '''
    def __init__(
            self, n_jobs,
            start_date, end_date,
            raw_data_pwd, meta_pwd, anomslist):
        '''
        PURPOSE: init class
        '''
        self.n_jobs = n_jobs
        self.start_date = start_date
        self.end_date = end_date
        # pwds:
        self.raw_data_pwd = raw_data_pwd
        self.meta_pwd = meta_pwd
        # grab min data dates:
        self.data_min_date =\
            (self.start_date - timedelta(days=1))
        # time_cols_dict:
        self.time_cols_dict =\
            {'scada': 'localTime', 'ami': 'mtr_evnt_tmstmp', 'edna': 'Time'}
        # fdr_cols_dict:
        self.fdr_cols_dict =\
            {'scada': 'feederNumber', 'ami': 'fdr_num', 'edna': 'feeder_id'}
        # raw data dict:
        self.raw_data_dict = {'scada': [], 'ami': [], 'edna': []}
        # alert dict to hold all resulted alerts:
        self.alerts_dict = dict()
        self.anomslist=anomslist

    def load_data(self):
        '''
        PURPOSE: load data
        '''
        # i. load meta data:
        self.xsect =\
            pd.read_pickle(self.meta_pwd+'feeder_metadata.pkl')
        # ii. load files:
        for date in pd.date_range(start=self.data_min_date, end=self.end_date):
            date_name = str(date)[:10].replace('-', '_')
            for data_type in self.fdr_cols_dict.keys():
                loc_file_name = '{}_{}.csv'.format(data_type, date_name)
                try:
                    loc_df =\
                        self._load_csv(
                                self.raw_data_pwd, loc_file_name, data_type
                            )
                    self.raw_data_dict[data_type].append(loc_df)
                except:
                    print 'ERROR! Can not complete calculations!'
                    print 'Missing {} data for date {}'.format(
                            data_type, loc_file_name
                        )
                    return
        # iii. collapse lists into master dfs:
        for data_type in self.fdr_cols_dict.keys():
            self.raw_data_dict[data_type] =\
                pd.concat(self.raw_data_dict[data_type])
            # make sure no data out of range:
            t_col = self.time_cols_dict[data_type]
            next_date = (self.end_date + timedelta(days=1))
            self.raw_data_dict[data_type] = self.raw_data_dict[data_type].loc[
                (self.raw_data_dict[data_type][t_col] >= self.data_min_date) &
                (self.raw_data_dict[data_type][t_col] < next_date)]

    def generates_anomalies(self):
        '''
        PURPOSE: generates anomalies using _parallel_anomalies
        '''
        master_results_lst = []
        for data_type in ['scada', 'ami', 'edna']:
            loc_raw_df = self.raw_data_dict[data_type]
            print 'Launching', self.n_jobs, 'jobs for {} data!'.format(
                data_type)
            loc_fdr_groups =\
                _get_fdr_groups(
                    self.n_jobs, loc_raw_df,
                    self.fdr_cols_dict[data_type]
                )
            loc_results_lst = Parallel(n_jobs=self.n_jobs, verbose=1)(
                delayed(_parallel_anomalies)(loc_raw_df,
                                             self.fdr_cols_dict[data_type],
                                             loc_fdr_groups[i],
                                             data_type,
                                             self.xsect,self.anomslist)
                for i in range(self.n_jobs))
            master_results_lst.append(pd.concat(loc_results_lst))
        # store master results:
        self.anoms_df = pd.concat(master_results_lst)

    def generates_alerts(
            self, model_id,
            model_pwd, dataset_config_pwd,
            anomaly_map_pwd, alert_config_pwd):
        '''
        PURPOSE: generate alerts from anomalies

        Parameters
        ----------
            model_id: str
                - model id (ex. 3.0)
            model_pwd: str
                - path, including file name, to pkl model
            dataset_config_pwd, anomaly_map_pwd, alert_config_pwd: str
                - path, including file name, to yaml
        Attributes
        ----------
        dataset_config, anomaly_map, alert_config:
            - congif yamls
        '''
        # load parameters:
        self.model_id = model_id
        self.model_pwd = model_pwd
        self.dataset_config_pwd = dataset_config_pwd
        self.anomaly_map_pwd = anomaly_map_pwd
        self.alert_config_pwd = alert_config_pwd
        # load yaml files (for respective congfigurations):
        self.dataset_config, self.anomaly_map, self.alert_config =\
            yaml.load(open(self.dataset_config_pwd, 'rb')),\
            yaml.load(open(self.anomaly_map_pwd, 'rb')),\
            yaml.load(open(self.alert_config_pwd, 'rb'))
        # load clf:
        self.clf = pickle.load(open(self.model_pwd, 'rb'))
        # generate groups:
        loc_fdr_groups =\
            _get_fdr_groups(self.n_jobs, self.anoms_df, 'Feeder')
        all_results_lst = Parallel(n_jobs=self.n_jobs, verbose=1)(
            delayed(_parallel_alerts)(self.anoms_df,
                                      loc_fdr_groups[i],
                                      self.clf, self.dataset_config,
                                      self.anomaly_map, self.alert_config,
                                      self.model_id, self.xsect)
            for i in range(self.n_jobs))
        # combine results:
        for alerters_group_dict in all_results_lst:
            self.alerts_dict.update(alerters_group_dict)

    def _load_csv(self, pwd, filename, data_type):
        '''
        PURPOSE:
            - read and process csv file.
        INPUT:
            - pwd : str
              The path to the CSV file to be loaded.
            - filename : str
              Name of the CSV file
            - data_type : str
              The type of dataset to load.
              One of {'scada', 'ami', 'edna'}.
        OUTPUT:
            - df: pd.DataFrame
              time-series raw data
        '''
        # capture the cases where data type doesn't exist:
        if data_type not in {'scada', 'ami', 'edna'}:
            print '{} data not supported!'.format(data_type)
            return
        # load data
        df = pd.read_csv(
                self.raw_data_pwd+filename, parse_dates=['timestamp_utc'],
                error_bad_lines=False)
        # drop unneeded cols:
        df.drop(['score', 'tenant_id'], axis=1, inplace=True)
        # rename columns to match tha pam-analytics package:
        if data_type == 'scada':
            df.columns = ['feederNumber', 'OBSERV_DATA', 'localTime']
        elif data_type == 'ami':
            df.columns =\
                ['fdr_num', 'ami_dvc_name', 'mtr_evnt_id', 'mtr_evnt_tmstmp']
            # grab the desired event id and type:
            df.mtr_evnt_id = df.mtr_evnt_id.astype(str)
            df = df[df.mtr_evnt_id.isin({'12007', '12024'})]
            # grab meter starts with G=GE, with L=L&G
            df = df[df.ami_dvc_name.str.startswith('G')]
        elif data_type == 'edna':
            df.columns =\
                ['feeder_id', 'ExtendedId', 'Time',
                 'Value', 'ValueString', 'Status']
            df.ValueString = df.ValueString.astype(str)
            edna_codes = pd.Series(df.ExtendedId.unique())
            edna_codes = edna_codes[-edna_codes.str.contains(r'Bad point')]
            edna_codes = edna_codes[((edna_codes.str.contains(r'\.PF\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.THD_')) &
                                 (edna_codes.str.contains(r'urrent'))) |
                                (((edna_codes.str.contains(r'\.MVAR')) |
                                  (edna_codes.str.contains(r'\.MVR\.'))) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.V\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.I\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.MW')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 -(edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.FCI\.')) &
                                 ((edna_codes.str.contains(r'\.FAULT')) |
                                  (edna_codes.str.contains(r'\.I_FAULT')))) |
                                ((edna_codes.str.contains(r'\.AFS\.')) &
                                 ((edna_codes.str.contains(r'\.ALARM')) |
                                  (edna_codes.str.contains(r'\.GROUND')) |
                                  (edna_codes.str.contains(r'\.I_FAULT'))))]
            edna_codes = set(edna_codes.values)
            df = df[df.ExtendedId.isin(edna_codes)]
        # format df:
        df[self.fdr_cols_dict[data_type]] =\
            df[self.fdr_cols_dict[data_type]].astype(str)
        df[self.time_cols_dict[data_type]] =\
            df[self.time_cols_dict[data_type]].dt.tz_localize(UTC)
        # return results:
        return df


# IV. main code
if __name__ == '__main__':
    pass
