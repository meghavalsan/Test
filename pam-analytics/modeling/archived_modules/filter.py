'''
Raw data Filter module.

The 'Filter.py' module screens out 'noisy data' with below mechanisms:
    -Method #1:
        only takes one reading per DNP point name per given time gap
        (ex. 1 hour)
    -Method #2:
        collapse identical magnitude reading (through out time) into one
        reading
    -Method #3:
        Method #1 + #2

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
        -only takes one reading per DNP point name per given time gap
         (timedelta)
    INPUT:
        -fdr_df(df; df of the same feeder)
        -timedelta(pd timedelta)
    OUTPUT:
        -df (df with duplicates remove)
    '''
    fdr_df.sort(columns=['Time'], inplace=True)
    dt_delta = fdr_df['Time'].diff() > timedelta
    dt_delta.iloc[0] = True

    return fdr_df.loc[dt_delta]


# function#2:
def remove_dup_alltime(fdr_df):
    '''
    PURPOSE:
        -collapse identical magnitude reading (through out time) into one
         reading
        -only good for magnitude reading
    INPUT:
        -fdr_df(df; df of the same feeder)
    OUTPUT:
        -df (df with duplicates remove)
    '''
    fdr_df.sort(columns=['Time'], inplace=True)
    fdr_df.drop_duplicates(subset=['Value'], keep='first', inplace=True)
    return fdr_df


# function#3:
def plot_count_vs_time(df, point_name, size, device, status, reading):
    '''
    PURPOSE:
        -plot anomaly/DNP point daily readings (across all feeders) through out
         time
        -plot, monthly, the unique # of feeders sending signals through out
         time
    INPUT:
        -df (df; the screened data frame, must be matching the anomaly rules
         one is testing)
        -point_name (str; name of the anomly/pointname)
        -size (tuple of two int)
        -device (str)
        -statius (str)
        -reading (str)
    OUTPUT:
        -N/A
    '''
    # i.set up data frames + variable names:
    loc_df = df[df['PointName'] == point_name]

    title = '{}; Device: {}; DNP PointName: {}; Rading: {};' + \
        ' timeseries graphs'.format(status, device, point_name, reading)
    # ii.aggregate daily count:
    daily_loc_count =\
        loc_df.groupby(
            ['Date']).agg(['count'])['PointName']['count'].reset_index()
    daily_loc_count.sort(['Date'], ascending=True, inplace=True)
    # iii.count monthly unique feeder sending having DNP point:
    monthly_point =\
        loc_df.groupby(
            ['Month', 'Feeder']).agg(
                ['count'])['PointName']['count'].reset_index()
    monthly_fdr_count =\
        monthly_point.groupby(
            ['Month']).agg(['count'])['Feeder']['count'].reset_index()
    monthly_fdr_count['count'] = monthly_fdr_count['count']
    monthly_fdr_count.sort(columns=['Month'], inplace=True)
    # iv.signal_per_fdr*100 rate calculation:
    daily_point = loc_df.groupby(
        ['Date', 'Feeder']).agg(['count'])['PointName']['count'].reset_index()
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
    plt.plot(
        merged_df['Date'], merged_df['signal_per_fdr'],
        marker='o', linestyle='--', alpha=0.1, color='b',
        label='signal per fdr (unit=rate*100)')
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
    filepath: str.
        path to the file of raw data (file should be csv)

    Attributes
    ----------
    df: pd.Dataframe.
        Pandas dataframe of raw data. The columns of the df should be:
            -'ExtendedId': str
                Contains the DNP point name and detail info of the Feeder
            -'Time': str
                Time of the signal.
            -'Value': str
                Value reading
            -'ValueString': str
                Value reading. If it is a boolean class, it will be different
                 from 'Value'; else, identical.
            -'Status': str
            -'PointName': str
                DNP PointName. Derived from 'ExtendedId'. Will only show up
                after applying method 'read()'
            -'Feeder': str
                Feeder ID. Derived from 'ExtendedId'. Will only show up
                after applying method 'read()'
            -'Date': pd.datetime object
                Date of the reading. Derived from 'ExtendedId'. Will only show
                up after applying method 'read()'
            -'Month' pd.datetime object
                Date + Year of the reading. Derived from 'ExtendedId'.
                Will only show up after applying method 'read()'
    point_name: str.
        desired DNP PointName (ex. ALARM)
    size: tuple (len 2) of int
        desired size of images (ex. (20,10))
    device: str
        device name (ex. AFS or FCI)
    reading: str
        desired ValueString (ex input: 'ValueString = ALARM')

    Methods:
    ----------
    #1. read:
        -load file (pandas pkl file)
    #2. pre_filter_info:
        -shows the data analysis before filtering
    '''
    def __init__(self, filepath, point_name, device, reading):
        '''
        OVERVIEW:
            -read in csv file name as filepath
        '''
        self.filepath = filepath
        self.point_name = point_name
        self.size = (20, 10)
        self.device = device
        self.reading = reading

    def read(self):
        '''
        OVERVIEW:
            -load file (python pandas df object) into pd data frame
            -format/transform dataframe
        '''
        # i. load data:
        self.df = pd.read_pickle(self.filepath)
        # ii.find DNP point names:
        self.df['PointName'] =\
            self.df.ExtendedId.str.extract(r'\w+\.\w+\.\w+\.\w+\.(\w+)')
        # iii.find fdr number:
        self.df['Feeder'] =\
            self.df.ExtendedId.str.extract(r'\w+\.(\w+)\.')
        # iv. add date:
        self.df['Date'] = self.df.Time.dt.date
        # v. grab month and date:
        self.df['Month'] = self.df.Time.dt.strftime('%Y-%m')
        # vi. convert 'Month' to datetime object:
        self.df['Month'] = pd.to_datetime(self.df.Month)
        # vii.convert Value to float:
        self.df['Value'] =\
            self.df.Value.convert_objects(convert_numeric=True)
        # viii.replace df with desired conditioned sub df:
        self.df = self.df.loc[self.df.PointName == self.point_name]
        # iX:if magnitude data filter out above 800:
        if self.point_name == 'I_FAULT':
            self.df = self.df[self.df['Value'] >= 800]
        elif (self.point_name == 'ALARM') or (self.point_name == 'GROUND'):
            self.df =\
                self.df[self.df['ValueString'] == 'ALARM']
        else:
            self.df =\
                self.df[self.df.ValueString != 'NORMAL']

    def pre_filter_info(self):
        '''
        OVERVIEW:
            -run basic analytics + daily graph for data pre-filtering.
        '''
        plot_count_vs_time(
            self.df, self.point_name, self.size, self.device,
            'Pre-duplicate-removal', self.reading)


# class#2:
class Method1_filter(Filter):
    '''
    Sub-class of Filter class. This uses method#1 to Filter:
    only takes one reading per DNP point name per given time gap (ex. '1 hour')

    Parameters
    ----------
    gap: str
        desired time gap to use for filtering.

    Attributes
    ----------
    result: pd.DataFrame
        resulting filtered dataframes using above filtering method.

    Methods:
    ----------
    #1. filter:
        filter dataframe + updated attribute result
    #2. post_filter_info:
        display analytics of filtered results
    '''
    def __init__(self, filepath, point_name, device, reading, gap):
        '''
        OVERVIEW:
        '''
        super(Method1_filter, self).__init__(
            filepath, point_name, device, reading)
        self.gap = pd.Timedelta(gap)

    def filter(self):
        '''
        OVERVIEW:
            filter dataframe + updated attribute result
        '''
        self.result = pd.DataFrame(columns=self.df.columns)
        unique_fdrs = self.df.Feeder.unique()
        for fdr in unique_fdrs:
            loc_fdr_df = self.df.loc[self.df.Feeder == fdr]
            loc_result_df = remove_dup_timedelta(loc_fdr_df, self.gap)
            self.result = pd.concat([self.result, loc_result_df])

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
            self.result, self.point_name, self.size,
            self.device, 'duplicate-removal-method#1', self.reading)


# class#3:
class Method2_filter(Filter):
    '''
    Sub-class of Filter class. This uses method#2 to filter:
    collapse identical magnitude reading (through out time) into one reading

    Parameters
    ----------

    Attributes
    ----------
    result: pd.DataFrame
        resulting filtered dataframes using above filtering method.

    Methods:
    ----------
    #1. filter:
        filter dataframe + updated attribute result
    #2. post_filter_info:
        display analytics of filtered results
    '''
    def __init__(self, filepath, point_name, device, reading):
        '''
        OVERVIEW:
        '''
        super(Method2_filter, self).__init__(
            filepath, point_name, device, reading)

    def filter(self):
        '''
        OVERVIEW:
            filter dataframe + updated attribute result
        '''
        self.result = pd.DataFrame(columns=self.df.columns)
        unique_fdrs = self.df.Feeder.unique()

        for fdr in unique_fdrs:
            loc_fdr_df = self.df.loc[self.df.Feeder == fdr]
            loc_result_df = remove_dup_alltime(self.result)
            self.result = pd.concat([self.result, loc_result_df])

    def post_filter_info(self):
        '''
        OVERVIEW:
            display analytics of filtered results
        '''
        pre_row_count, post_row_count =\
            float(self.df.shape[0]), float(self.result.shape[0])
        print '****Method#2:'
        print 'df originally has {} rows.'.format(pre_row_count)
        print 'df after method#1 has {} rows.'.format(post_row_count)
        print '{}% decrease.'.format(
            ((pre_row_count-post_row_count)/pre_row_count)*100)
        # plot:
        plot_count_vs_time(
            self.result, self.point_name, self.size,
            self.device, 'duplicate-removal-method#2', self.reading)


# class#4:
class Method3_filter(Filter):
    '''
    Sub-class of Filter class. This uses method#3 to filter:
    method#1 + method#2

    Parameters
    ----------
    gap: str
        desired time gap to use for filtering.

    Attributes
    ----------
    result: pd.DataFrame
        resulting filtered dataframes using above filtering method.

    Methods:
    ----------
    #1. filter:
        filter dataframe + updated attribute result
    #2. post_filter_info:
        display analytics of filtered results
    '''
    def __init__(self, filepath, point_name, device, reading, gap):
        '''
        OVERVIEW:
        '''
        super(Method2_filter, self).__init__(
            filepath, point_name, device, reading)
        self.gap = pd.Timedelta(gap)

    def filter(self):
        '''
        OVERVIEW:
            filter dataframe + updated attribute result
        '''
        self.result = pd.DataFrame(columns=self.df.columns)
        unique_fdrs = self.df.Feeder.unique()
        for fdr in unique_fdrs:
            loc_fdr_df = self.df.loc[self.df.Feeder == fdr]
            loc_result_df = remove_dup_timedelta(loc_fdr_df, self.gap)
            loc_result_df = remove_dup_alltime(loc_result_df)
            self.result = pd.concat([self.result, loc_result_df])

    def post_filter_info(self):
        '''
        OVERVIEW:
            display analytics of filtered results
        '''
        pre_row_count, post_row_count =\
            float(self.df.shape[0]), float(self.result.shape[0])
        print '****Method#3:'
        print 'df originally has {} rows.'.format(pre_row_count)
        print 'df after method#1 has {} rows.'.format(post_row_count)
        print '{}% decrease.'.format(
            ((pre_row_count-post_row_count)/pre_row_count)*100)

        # plot:
        plot_count_vs_time(
            self.result, self.point_name, self.size,
            self.device, 'duplicate-removal-method#3', self.reading)


if __name__ == '__main__':
    print 'This is the filter.py module!'
