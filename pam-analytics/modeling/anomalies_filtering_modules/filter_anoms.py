'''
Anomalies data filter module (ONLY FOR eDNA anomalies from Trevor's anomaly.py)

The 'filter_anoms.py', based on filter.py,
Special Edition: meant to use on Trevor's anomalies (produced by anomaly.py)
module screens out 'noisy data' with below mechanisms:

    -Method #1:
        only takes one reading per anomaly on a given device
        per given time gap
        (ex. 1 hour)

NOTED:
    -These methods are more of an 'art' rather than a 'science'.
        This is implemented as a patchy solution to the data quality problem.
    -This module should only be used on eDNA data.
'''
# Copyright (c) 2011-2017 AutoGrid Systems
# Author(s): 'Eric Hsieh' <eric.hsieh@auto-grid.com>

# I.Libraries:
from collections import defaultdict
from pytz import timezone, UTC
from datetime import timedelta
import cPickle as pickle
from glob import glob
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

plt.style.use('ggplot')
EASTERN = timezone('US/Eastern')


# II.Functions:
# function#1:
def remove_dup_timedelta(fdr_df, timedelta):
    '''
    PURPOSE:
        -only takes one reading per anoamly on a device and phase
         per given time gap (timedelta)
    INPUT:
        -fdr_df(df; df of the same feeder, same device and phase)
        -timedelta(pd timedelta)
    OUTPUT:
        -df (df with duplicates remove)
    '''
    fdr_df.sort(columns=['Time'], inplace=True)
    dt_delta = fdr_df['Time'].diff() > timedelta
    dt_delta.iloc[0] = True

    return fdr_df.loc[dt_delta]

# function#3:
def plot_count_vs_time(df, anom_name, size, device, status):
    '''
    PURPOSE:
        -plot anomaly/DNP point daily readings (across all feeders) through out
         time
        -plot, monthly, the unique # of feeders sending signals through out
         time
    INPUT:
        -df (df; the original or screened data frame, must be matching the
         anomaly rules one is testing)
        -anom_name (str; name of the anomly/pointname)
        -size (tuple of two int)
        -device (str)
        -statius (str)
    OUTPUT:
        -N/A
    '''
    # i.set up data frames + variable names:
    loc_df = df[df['Anomaly'] == anom_name]

    title =\
        '{}; Device: {}; Anomaly Name: {}'.format(status, device, anom_name)
    # ii.aggregate daily count:
    daily_loc_count =\
        loc_df.groupby(
            ['Date']).agg(['count'])['Anomaly']['count'].reset_index()
    daily_loc_count.sort(['Date'], ascending=True, inplace=True)
    # iii.count monthly unique feeder sending having DNP point:
    monthly_point =\
        loc_df.groupby(
            ['Month', 'Feeder']).agg(
                ['count'])['Anomaly']['count'].reset_index()
    monthly_fdr_count =\
        monthly_point.groupby(
            ['Month']).agg(['count'])['Feeder']['count'].reset_index()
    monthly_fdr_count['count'] = monthly_fdr_count['count']
    monthly_fdr_count.sort(columns=['Month'], inplace=True)
    # iv.signal_per_fdr*100 rate calculation:
    daily_point = loc_df.groupby(
        ['Date', 'Feeder']).agg(['count'])['Anomaly']['count'].reset_index()
    daily_fdr_count = daily_point.groupby(
        ['Date']).agg(['count'])['Feeder']['count'].reset_index()

    merged_df = daily_loc_count.merge(
        daily_fdr_count, left_on='Date',
        right_on='Date', how='left', suffixes=['_signal', '_unique_fdr'])
    merged_df['count_signal'] =\
        merged_df.count_signal.astype(dtype=float)
    merged_df['count_unique_fdr'] =\
        merged_df.count_unique_fdr.astype(dtype=float)
    merged_df['signal_per_fdr'] =\
        100*(merged_df['count_signal']/merged_df['count_unique_fdr'])
    merged_df.sort(columns=['Date'], inplace=True)
    # v.basic stats overview:
    print '____stats on daily count___:'
    print 'Earliest: {}'.format(daily_loc_count['Date'].min())
    print 'Latest: {}'.format(daily_loc_count['Date'].max())
    print daily_loc_count['count'].describe()
    # vi.ploting:
    plt.figure(figsize=size)
    fig = plt.figure(figsize=size)
    ax = fig.add_subplot(111)
    plt.subplot(111)
    plt.plot(
        daily_loc_count['Date'], daily_loc_count['count'],
        marker='o', linestyle='--', alpha=1, color='r',
        label='signal per day across all fdrs (unit=count)')
    plt.plot(
        monthly_fdr_count['Month'], monthly_fdr_count['count'],
        marker='o', linestyle='--', alpha=1, color='g',
        label='unique feeder count per month (unit=count)')
    #plt.plot(
    #    merged_df['Date'], merged_df['signal_per_fdr'],
    #    marker='o', linestyle='--', alpha=0.1, color='b',
    #    label='signal per fdr (unit=rate*100)')
    plt.legend()
    ax.set_xlabel('Time')
    ax.set_ylabel('Count or Rate')
    plt.title(title)
    plt.savefig(title, bbox_inches='tight')
    plt.show()


# III.Classes:
# class#1:
class Filter(object):
    '''
    Mother Class for all Filter methods.
    This is the base-class for all Filter methods
    use the derived class for specific Filter logic.

    Parameters
    ----------
    df: pd.DataFrame.
        Pandas dataframe of anomalies. The columns of the df should be:
            -'Anomaly':
                str, anomaly name. derived from anomlay.py/anomaly.go
            -'DeviceId':
                str, DeviceId of the physical unique device that the
                data point is collected from.
            -'DevicePh':
                str, the phase that the device resides on
            -'DeviceType':
                str, the device type. since this is only mean for eDNA.
                It cant only be AFS, FCI, or Phasers
            -'Feeder':
                unicode, the feeder id
            -'Signal':
                str, detailed description of the signal
            -'Time':
                datetime64, in UTC, time when the signal is collected

    Attributes
    ----------
    anom_name: str.
        -name of the anomaly
    device: str.
        -name of the DeviceType (Phaser, AFS, and FCI)
    size: tuple of int
        -size of the image

    Methods:
    ----------
    #1. __init__:
        -input pandas dataframe and format the df
    #2. pre_filter_info:
        -shows the data analysis before filtering
    '''
    def __init__(self, df, anom_name, device):
        '''
        OVERVIEW:
            -read in pd dataframe, the anomaly name, adn the device
        '''
        # i.load data:
        self.df = df
        self.anom_name = anom_name
        self.size = (20, 10)
        self.device = device
        # ii.produce
        self.df['Date'] = self.df.Time.dt.date
        # iii. grab month and date:
        self.df['Month'] = self.df.Time.dt.strftime('%Y-%m')
        # iv. convert 'Month' to datetime object:
        self.df['Month'] = pd.to_datetime(self.df.Month)
        # v.replace df with desired conditioned sub df:
        self.df = self.df.loc[self.df.Anomaly == self.anom_name]

    def pre_filter_info(self):
        '''
        OVERVIEW:
            -run basic analytics + daily graph for data pre-filtering.
        '''
        plot_count_vs_time(
            self.df, self.anom_name, self.size, self.device,
            'Pre-duplicate-removal')

# class#2:
class Method1_filter(Filter):
    '''
    Sub-class of Filter class. This uses method#1 to Filter:
    only takes one reading per DNP point name per given time gap (ex. '1 hour')

    Parameters
    ----------
    gap: str
        desired time gap to use for filtering.
        ex. pd.Timedelta('1 hour') denotes one hour

    Attributes
    ----------
    result: pd.DataFrame
        resulting filtered dataframes using above filtering method.
        columns: everything original df has plus columns 'Date' and 'Month'

    Methods:
    ----------
    #1. filter:
        filter dataframe + updated attribute result
    #2. post_filter_info:
        display analytics of filtered results
    '''
    def __init__(self, df, anom_name, device, gap):
        '''
        OVERVIEW:
        '''
        super(Method1_filter, self).__init__(
            df, anom_name, device)
        self.gap = pd.Timedelta(gap)

    def filter(self):
        '''
        OVERVIEW:
            filter dataframe + updated attribute result
        '''
        first_run = True
        unique_fdrs = self.df.Feeder.unique()

        for fdr in unique_fdrs:
            loc_fdr_df = self.df.loc[self.df.Feeder == fdr]
            unique_phs = loc_fdr_df.DevicePh.unique()
            unique_ids = loc_fdr_df.DeviceId.unique()

            for ph in unique_phs:
                for ID in unique_ids:
                    device_loc_df =\
                        loc_fdr_df.loc[
                            (loc_fdr_df.DevicePh==ph)&
                            (loc_fdr_df.DeviceId==ID)]
                    if device_loc_df.empty:
                        continue
                    else:
                        device_result =\
                            remove_dup_timedelta(device_loc_df, self.gap)
                    if first_run:
                        self.result = device_result
                        first_run = False
                    else:
                        self.result =\
                            pd.concat([self.result, device_result])

    def post_filter_info(self):
        '''
        OVERVIEW:
            display analytics of filtered results
        '''
        pre_row_count, post_row_count =\
            float(self.df.shape[0]), float(self.result.shape[0])
        print '****Method#1:'
        print 'df originally has {} rows.'.format(pre_row_count)
        print 'df after method#1 has {} rows.'.format(post_row_count)
        print '{}% decrease.'.format(
            ((pre_row_count-post_row_count)/pre_row_count)*100)

        # plot:
        plot_count_vs_time(
            self.result, self.anom_name, self.size,
            self.device, 'duplicate-removal-method#1')

if __name__ == '__main__':
    print 'This is the filter_anom_test.py module!'
