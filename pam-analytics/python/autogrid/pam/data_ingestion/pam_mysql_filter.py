"""Data Filter Module for MySQL.

This is class to save incoming data to MySQL.
"""
# Copyright (c) 2011-2015 AutoGrid Systems

from yukon.filters.base_filter import BaseFilter
from autogrid.foundation.util.RDBMSUtil import RDBMSUtil
import time
from pandas.io.json import json_normalize
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc


class PamMySQLFilter(BaseFilter):

    """Save incoming data to mysql.

    These parameters are configured is sample_pipeline.yml

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

    def __init__(self, database, tenant_uid):
        self.db_pool = RDBMSUtil()
        self.db = database
        self.tenant_uid = tenant_uid
        # This is hard-coded to 1 at the moment because we are still not sure
        # if we can pass tenant_uid while initializing or not
        self.tenant_id = 1

    def __call__(self, data):
        """Callable method inserts data to database.

        Parameters
        ----------
        data : dict
            Incoming JSON data passed automatically by sample_pipeline.yml
        """
        filterwarnings('ignore', category=sa_exc.SAWarning)
        if data['message_type'].lower() == 'edna':
            df = json_normalize(data['data'],
                                'time_series',
                                ['feeder_id', 'extended_id'])
        elif data['message_type'].lower() == 'feeder_metadata':
            df = json_normalize(data['data'])

            df['has_afs'] = (df.afs > 0).astype(int)
            afs_cols = ['afs', '3ph_ocr', '1ph_ocr']
            df['has_afs_ocr'] = (df[afs_cols].sum(1) > 0).astype(int)
            df['is_dade'] = (df.county == 'Miami-Dade').astype(int)
            df['has_industrial'] = (df.industrial > 0).astype(int)

            df['length'] = (df.fdr_oh + df.fdr_ug).astype(float)
            df['pct_ug'] = df.fdr_ug.div(df.length)
            df['pct_ug'] = (df.pct_ug > 0.5).astype(int)
            df['oh_fdr'] = df.fdr_oh.div(df.length)
            df['oh_fdr'] = (df.oh_fdr > 0.8).astype(int)
            df['ug_fdr'] = df.fdr_ug.div(df.length)
            df['ug_fdr'] = (df.ug_fdr > 0.8).astype(int)
            df['hybrid'] = df.oh_fdr + df.ug_fdr
            df['hybrid'] = (df.hybrid < 0.5).astype(int)
        else:
            df = json_normalize(data['data'], 'time_series', ['feeder_id'])

        if data['message_type'].lower() != 'feeder_metadata':
            df['score'] = self._get_score(data['start_time_utc'])
            df['tenant_id'] = self.tenant_id

        if 'timestamp_utc' in df:
            df['timestamp_utc'] = df['timestamp_utc'].str.replace('T', ' ').\
                str.replace('Z', '')
        if 'power_off' in df:
            df['power_off'] = df['power_off'].str.replace('T', ' ').str.replace('Z', '')

        if 'power_restore' in df:
            df['power_restore'] = df['power_restore'].str.replace('T', ' ').\
                str.replace('Z', '')

        if data['message_type'].lower() == 'feeder_metadata':
            columns = df.columns.values
            record_tuples = [tuple(x) for x in df.values]
            self.db_pool.replace(application_name=self.db,
                                 table_name=data['message_type'].lower(),
                                 columns=columns, data_list=record_tuples)
        else:
            record_list = df.to_dict('records')
            self.db_pool.insert(application_name=self.db,
                                table_name=data['message_type'].lower(),
                                data=record_list)

    ###################
    # PRIVATE METHODS #
    ###################

    def _get_score(self, start_time):
        isoformat = '%Y-%m-%dT%H:%M:%SZ'
        score_value = int(time.mktime(time.strptime(start_time, isoformat)))
        return score_value
