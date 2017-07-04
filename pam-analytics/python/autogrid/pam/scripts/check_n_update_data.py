"""Chack and update Json data to Redis and MySQL.

Load EDNA, SCADA and AMI Josn data to Redis and MySQL.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import sys
import os
from datetime import datetime, timedelta
from autogrid.pam.anomaly import utils
from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly.global_define import NS_SEPARATOR, SCADA_NS, EDNA_NS
from autogrid.pam.anomaly.global_define import AMI_NS, VALUE_COUNT, SECONDS_IN_HOUR
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil


class CheckAndUpdateData(object):

    """Check and update Json data to Redis and MySQL.

    Parameters
    ----------
    system : str
        database name
    tenant_uid : str
        tenant_uid
    data_ingest_check_hrs : float
        number of hours to check for lagging data ingestion
    file_name : str
        output file name
    """

    def __init__(self, system, tenant_uid, data_ingest_check_hrs, file_name):
        self.__logger = Logger.get_logger(self.__class__.__name__)
        self.system = system
        self.tenant_uid = tenant_uid
        self.data_ingest_check_hrs = data_ingest_check_hrs
        self.fname = file_name

        self.foundation_home = os.environ.get('FOUNDATION_HOME')

        if self.foundation_home is None:
            self.__logger.error("FOUNDATION_HOME is not set in environment")
            raise KeyError('FOUNDATION_HOME')

        self.cache_util = cacheUtil.CacheUtil()
        self.rdbms_util = rdbmsUtil.RDBMSUtil()

    def get_max_score(self, keypattern):
        """Get max score of EDNA, SCADA and AMI from Redis.

        Parameters
        ----------
        keypattern : str
            Redis key pattern

        Returns
        -------
        score : int
            Maximum score for given key pattern.
        """
        redis_data = []
        score_list = []
        try:
            redis_keys = self.cache_util.keys(keypattern, '*')
        except Exception:
            raise
        # Now replicate through keys to get values
        try:
            for redis_key in redis_keys:
                cursor = 0
                new_key = redis_key.split(":")[-1]
                cursor, vals = self.cache_util.zscan(namespace=keypattern,
                                                     key=new_key,
                                                     cursor=cursor,
                                                     count=VALUE_COUNT)
                if len(vals) > 0:
                    for eachdata in vals:
                        redis_data.append(eachdata)
                while cursor > 0:
                    cursor, vals = self.cache_util.zscan(namespace=keypattern,
                                                         key=new_key,
                                                         cursor=cursor,
                                                         count=VALUE_COUNT)
                    if len(vals) > 0:
                        for eachdata in vals:
                            redis_data.append(eachdata)
        except Exception:
            raise Exception("Lag checker: Error while reading data from Redis")

        for eachdata in redis_data:
            score_list.append(eachdata[1])

        return max(score_list)

    def get_redis_latest_time(self):
        """Get latest timestamp_utc from EDNA, SCADA and AMI.

        Returns
        -------
            min_latest_time : datetime
        """
        # Set default time as current time - check hours.
        # This is the min time diff we are looking before ingesting the data.
        default_time = datetime.utcnow() - \
            timedelta(hours=self.data_ingest_check_hrs + 1)

        datasets = ['scada', 'ami', 'edna']
        max_score_list = []

        for dataset in datasets:
            if dataset == 'scada':
                ns = SCADA_NS
            elif dataset == 'ami':
                ns = AMI_NS
            elif dataset == 'edna':
                ns = EDNA_NS
            else:
                ns = ''
            try:
                keypattern = self.tenant_uid + NS_SEPARATOR + ns
                max_score_list.append(self.get_max_score(keypattern))
            except Exception:
                self.__logger.info('Lag checker: Error while reading ' + ns +
                                   ' data from redis. Using default latest date.')

        if len(max_score_list) > 0:
            min_score = min(max_score_list)
            min_latest_time = datetime.fromtimestamp(int(min_score))
        else:
            min_latest_time = default_time

        return min_latest_time

    def get_mysql_latest_time(self):
        """Get latest timestamp_utc from EDNA, SCADA and AMI.

        Returns
        -------
            min_latest_time : datetime
        """
        # Set default time as current time - check hours.
        # This is the min time diff we are looking before ingesting the data.
        default_time = datetime.utcnow() - \
            timedelta(hours=self.data_ingest_check_hrs + 1)

        qry = 'select max(timestamp_utc) from '

        rows = self.rdbms_util.select(self.system, qry + 'edna;')
        if len(rows) > 0:
            edna_date = rows[0][0]
        else:
            edna_date = default_time

        rows = self.rdbms_util.select(self.system, qry + 'ami;')
        if len(rows) > 0:
            ami_date = rows[0][0]
        else:
            ami_date = default_time

        rows = self.rdbms_util.select(self.system, qry + 'scada;')
        if len(rows) > 0:
            scada_date = rows[0][0]
        else:
            scada_date = default_time

        min_latest_time = min([edna_date, ami_date, scada_date])

        return min_latest_time

    def run(self):
        """Check if data is updated recently (not more than n hour old).

        Note, this n hour is configurable.
        """
        cur_time = datetime.utcnow()
        latest_mysql_timestamp = self.get_mysql_latest_time()
        latest_redis_timestamp = self.get_redis_latest_time()

        self.__logger.debug('Lag checker: Min latest data timestamp in MySQL: ' +
                            str(latest_mysql_timestamp))
        self.__logger.debug('Lag checker: Min latest data timestamp in Redis: ' +
                            str(latest_redis_timestamp))

        mysql_hrs = (cur_time - latest_mysql_timestamp).total_seconds() / SECONDS_IN_HOUR
        redis_hrs = (cur_time - latest_redis_timestamp).total_seconds() / SECONDS_IN_HOUR

        if mysql_hrs < self.data_ingest_check_hrs \
                or redis_hrs < self.data_ingest_check_hrs:
            self.__logger.info('Lag checker: PAM data is recently updated. '
                               'MySQL lag hours: %.2f, Redis lag hours: %.2f' %
                               (mysql_hrs, redis_hrs))
            fl = open(self.fname, 'w')
            fl.write(str(cur_time))
            fl.close()
        else:
            self.__logger.info('Lag checker: Last PAM data update is stale. '
                               'MySQL lag hours: %.2f, Redis lag hours: %.2f' %
                               (mysql_hrs, redis_hrs))

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print 'Usage: ' + sys.argv[0] + '<system> <tenant_id> <lag hours> <output_file>'
        exit(-1)
    SYSTEM = sys.argv[1]
    TENANT_ID = sys.argv[2]
    LAG_HRS = float(sys.argv[3])
    OUT_FILE = sys.argv[4]

    TENANT_UID = utils.get_tenant_uid(SYSTEM, TENANT_ID)

    if TENANT_UID is None:
        exit(-1)

    CHKNUPDT = CheckAndUpdateData(system=SYSTEM, tenant_uid=TENANT_UID,
                                  data_ingest_check_hrs=LAG_HRS,
                                  file_name=OUT_FILE)
    CHKNUPDT.run()
