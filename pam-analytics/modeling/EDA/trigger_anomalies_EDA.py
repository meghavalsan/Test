'''
'trigger_anomalies_EDA.py'

This EDA py file explores the signature data frame based on trigger anomalies.
The master signature files used in this file were generated by turning all
anomaly into a trigger type without any anomaly grouping. The trigger anomaly
of each row was recorded respectively as 'TRIGGER_ANOMALY' in the y data frame.

The ultimate purpose of this EDA work is to identify the golden trigger rules
that captures the most outage cases depending on the definitions of an
'outage cases'

The outputs of this py file are series of graphs (ex. bar graphs).

Other notes:
    - Data source:
        - bulk (2012 to 2015)
        - monthly (2015 to Feb. 2017)
'''
# Copyright (c) 2011-2017 AutoGrid Systems
# Author(s): 'Eric Hsieh' <eric.hsieh@auto-grid.com>

# I.Libraries:
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
plt.style.use('ggplot')


# II. Functions:
# function #1:
def calc_out_perc(y, perc_results_dict):
    '''
    PURPOSE:
        - calculates the percentage of outage to none outage
         signatures for given anomaly
    INPUT:
        - y (df; signatures with trigger anoms recorded)
        - perc_results_dict (key: set of tuples; each tuple (len 2)
          records the lower and upper bound of outage definition
          ; bound unit = hours; values: lists)
    OUTPUT:
        - perc_results_dict (dict; hold percentage results)
        - anom_results_dict (dict; hold the order of anomaly based on perc)
    '''
    # 1. declare variables:
    total_rows, anom_results_dict, trig_names_lst =\
        float(y.shape[0]), perc_results_dict.copy(), []
    # 2. calculates percentages:
    for trigger_name in y.TRIGGER_ANOMALY.unique():
        loc_df = y.loc[y['TRIGGER_ANOMALY'] == trigger_name]
        trig_rows = float(loc_df.shape[0])
        # append trigger_name so we have the right order for later calc:
        trig_names_lst.append(trigger_name)
        # calculate %  of different thresholds definitions:
        for bounds_tuple in perc_results_dict:
            if 'all' in bounds_tuple:
                perc_results_dict[bounds_tuple].append(
                    (trig_rows*100/total_rows))
            elif 'null' in bounds_tuple:
                perc_results_dict[bounds_tuple].append(
                    ((100*loc_df['OUTAGE'].isnull().sum())/trig_rows))
            else:
                perc_results_dict[bounds_tuple].append(
                        (
                            100*((loc_df['OUTAGE'] >= bounds_tuple[0]) &
                                 (loc_df['OUTAGE'] <= bounds_tuple[1])).sum()
                        )/trig_rows
                )
    # 3.record respective orders of anomaly lst based on each perc lst:
    trig_names_lst = np.array(trig_names_lst)
    for key in perc_results_dict.keys():
        loc_lst = np.array(perc_results_dict[key])
        loc_sort_arr = loc_lst.argsort()

        loc_sorted_anoms, loc_sorted_lst =\
            trig_names_lst[loc_sort_arr], loc_lst[loc_sort_arr]
        # record results:
        perc_results_dict[key] = loc_sorted_lst
        anom_results_dict[key] = loc_sorted_anoms

    return perc_results_dict, anom_results_dict


# function #2:
def get_trigger_info(y, perc_results_dict):
    '''
    PURPOSE:
        - find basic stats/info about signatures of respective
        trigger anomalies.
    INPUT:
        - y (df of target;
            includes trigger anomaly (col: 'TRIGGER_ANOMALY');
            hours to next outage (col: 'OUTAGE'))
         - perc_results_dict (key: set of tuples; each tuple (len 2)
           records the lower and upper bound of outage definition
           ; bound unit = hours; values: lists)
    OUTPUT:
        - 1. graph % of trigger anomaly in the df (y)
        - 2. graph % of signatures with outages (at different defintions)
             and no outages for sub trigger anomalies df
    '''
    # 1. set variables:
    y_pos = np.arange(len(y['TRIGGER_ANOMALY'].unique()))
    # 2. calculate respective percentage lst:
    perc_result_dict, anom_result_dict = calc_out_perc(y, perc_results_dict)
    # 3. graph respective graphs:
    for key in perc_result_dict:
        # a. set tiles:
        if type(key) == str:
            title =\
                '% of {}'.format(key)
        else:
            title =\
                '% of signatures with outage {}(unit=hours)'.format(key) +\
                'over respective trigger-anomaly induced signatures'
        plt.figure(figsize=(20, 10))
        fig = plt.figure(figsize=(20, 10))
        ax = fig.add_subplot(111)
        plt.subplot(111)
        plt.bar(y_pos, perc_result_dict[key], align='center', alpha=0.5)
        plt.xticks(y_pos, anom_result_dict[key], rotation='vertical')
        plt.ylabel(title)
        plt.title(title)
        plt.savefig(title+'.png', bbox_inches='tight')
        plt.show()


# function #3:
def load_data(pwd):
    '''
    PURPOSE:
        - load master signature files. scripts for generating the
          master signature files can be found here:
          # /Users/erichsieh/Desktop/conda_envs/pam_modeling_e/modeling/
          # files/sig_gen_scripts with file master_sig_gen.py
    INPUT:
        - pwd (path to the files)
    OUTPUT:
        - y (df)
    '''
    # read csvs:
    X = pd.read_csv(pwd + 'master_sig_dataset_X_pre_formating_hardening.csv')
    y = pd.read_csv(pwd + 'master_sig_dataset_y_pre_formating_hardening.csv')
    # formating:
    X.drop(['Unnamed: 0'], axis=1, inplace=True)
    y.drop(['Unnamed: 0'], axis=1, inplace=True)
    # unit conversions:
    for col in X.columns:
        X[col] = X[col].astype(int)
    # formating y:
    # y formating:
    y['FEEDER'] = y['FEEDER'].apply(lambda x: str(x))
    y['OUTAGE'] = y['OUTAGE'].astype(float)
    y['TICKET'] = y['TICKET'].astype(float)
    y['TIMESTAMP'] = y['TIMESTAMP'].apply(
        lambda x: pd.to_datetime(x, utc=True))
    y['TARGETS'] = y['TARGETS'].astype(float)
    y['TARGET_TICKET'] = y['TARGET_TICKET'].astype(float)

    return y

# III. main code (run EDA code):
if __name__ == '__main__':
    # i. load data:
    print 'loading data...'
    pwd =\
        '/Users/erichsieh/Desktop/conda_envs/pam_modeling_e/' + \
        'modeling/files/data_sets/master_signatures/'
    y = load_data(pwd)
    # ii. declare dict that contains outage definitions etc.:
    print 'setting input variables...'
    input_dict =\
        {
            'trigger-anomaly induced sigs over all sigs': [],
            'null sigs over respective trigger-anomaly induced sigs': [],
            (-2.0, 0.0): [], (-1.0, 0.0): [],
            (0.0, 0.5): [], (0.5, 12): [],
            (0.5, 24): [], (0.5, 336): [],
            (0.5, 720): []
        }
    # iii. calls function for analysis:
    print 'running analysis...'
    get_trigger_info(y, input_dict)
