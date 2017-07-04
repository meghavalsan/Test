"""Load CSV data to Redis.

Converts EDNA, SCADA, AMI and TICKETS CSV data to JSON format and
loads these josn data to Redis.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

import pandas as pd
import os
from glob import glob
import json
import sys
import yaml
import time
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
import autogrid.foundation.util.CacheUtil as cacheUtil
from pytz import timezone
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc

SCADA_NS = 'feeders:scada'
AMI_NS = 'feeders:ami'
EDNA_NS = 'feeders:edna'
TICKETS_NS = 'feeders:tickets'
NS_SEPARATOR = ':'

EASTERN = timezone('US/Eastern')
ISOFORMAT = '%Y-%m-%dT%H:%M:%SZ'


def _process_scada_files(system, tenant_id, tenant_uid, files_lst,
                         insert_database):
    cache_util = cacheUtil.CacheUtil()
    rdbms_util = rdbmsUtil.RDBMSUtil()
    filterwarnings('ignore', category=sa_exc.SAWarning)
    for fname in files_lst:
        print '\t', fname
        df = pd.read_csv(fname, parse_dates=[2])
        df.localTime = df.localTime.dt.tz_localize(EASTERN) \
            .dt.tz_convert(None).dt.strftime(ISOFORMAT)
        scada_fdr_lst = pd.unique(df.feederNumber)
        for feeder in scada_fdr_lst:
            scada_ts_list = list()
            # Filter dataframe for feeder
            scada_fltr_df = df[df.feederNumber == feeder]
            start_time = scada_fltr_df['localTime'].min()
            # end_time = scada_fltr_df['localTime'].max()
            score = _get_score(start_time)
            if insert_database == 'mysql' or insert_database == 'both':
                scada_mysql_df = scada_fltr_df.copy()
                scada_mysql_df['score'] = score
                scada_mysql_df['tenant_id'] = tenant_id
                scada_mysql_df = scada_mysql_df.\
                    rename(columns={'feederNumber': 'feeder_id',
                                    'localTime': 'timestamp_utc',
                                    'OBSERV_DATA': 'value'})
                scada_mysql_df = scada_mysql_df.astype(object)\
                    .where(pd.notnull(scada_mysql_df), 'nan')
                rdbms_util.insert(system, 'scada',
                                  scada_mysql_df.to_dict('records'))
            if insert_database == 'redis' or insert_database == 'both':
                for index, row in scada_fltr_df.iterrows():  # pylint: disable=W0612
                    scada_ts_dict = dict()
                    scada_ts_dict['value'] = row['OBSERV_DATA']
                    scada_ts_dict['timestamp_utc'] = row['localTime']
                    scada_ts_list.append(scada_ts_dict)
                scada_data_dict = dict()
                scada_data_dict['feeder_id'] = feeder
                scada_data_dict['time_series'] = scada_ts_list

                value_dict = dict()
                value_dict[score] = json.dumps(scada_data_dict)
                cache_util.batch_zadd(tenant_uid + NS_SEPARATOR + SCADA_NS,
                                      feeder,
                                      value_dict)


def _process_ami_files(system, tenant_id, tenant_uid, files_lst,
                       insert_database):
    cache_util = cacheUtil.CacheUtil()
    rdbms_util = rdbmsUtil.RDBMSUtil()
    filterwarnings('ignore', category=sa_exc.SAWarning)
    for fname in files_lst:
        print '\t', fname
        df = pd.read_csv(fname)
        df.mtr_evnt_tmstmp = pd.to_datetime(df.mtr_evnt_tmstmp)
        df.mtr_evnt_tmstmp = df.mtr_evnt_tmstmp.dt.tz_localize(EASTERN) \
            .dt.tz_convert(None).dt.strftime(ISOFORMAT)
        ami_fdr_lst = pd.unique(df.fdr_num)
        for feeder in ami_fdr_lst:
            ami_ts_list = list()
            # Filter dataframe for feeder
            ami_fltr_df = df[df.fdr_num == feeder]
            start_time = ami_fltr_df['mtr_evnt_tmstmp'].min()
            # end_time = ami_fltr_df['mtr_evnt_tmstmp'].max()
            score = _get_score(start_time)
            if insert_database == 'mysql' or insert_database == 'both':
                ami_mysql_df = ami_fltr_df.copy()
                ami_mysql_df['score'] = score
                ami_mysql_df['tenant_id'] = tenant_id
                ami_mysql_df = ami_mysql_df.\
                    rename(columns={'fdr_num': 'feeder_id',
                                    'ami_dvc_name': 'meter_id',
                                    'mtr_evnt_id': 'event_id',
                                    'mtr_evnt_tmstmp': 'timestamp_utc'})
                ami_mysql_df = ami_mysql_df[
                    ['feeder_id', 'meter_id', 'event_id', 'timestamp_utc',
                     'score',
                     'tenant_id']]
                ami_mysql_df = ami_mysql_df.astype(object).\
                    where(pd.notnull(ami_mysql_df), 'nan')
                rdbms_util.insert(system, 'ami',
                                  ami_mysql_df.to_dict('records'))
            if insert_database == 'redis' or insert_database == 'both':
                for index, row in ami_fltr_df.iterrows():  # pylint: disable=W0612
                    ami_ts_dict = dict()
                    ami_ts_dict['event_id'] = row['mtr_evnt_id']
                    ami_ts_dict['timestamp_utc'] = row['mtr_evnt_tmstmp']
                    ami_ts_dict['meter_id'] = row['ami_dvc_name']
                    ami_ts_list.append(ami_ts_dict)
                ami_data_dict = dict()
                ami_data_dict['feeder_id'] = feeder
                ami_data_dict['time_series'] = ami_ts_list

                value_dict = dict()
                value_dict[score] = json.dumps(ami_data_dict)
                cache_util.batch_zadd(tenant_uid + NS_SEPARATOR + AMI_NS,
                                      feeder,
                                      value_dict)


def _process_edna_files(system, tenant_id, tenant_uid, files_lst,
                        insert_database):
    cache_util = cacheUtil.CacheUtil()
    rdbms_util = rdbmsUtil.RDBMSUtil()
    filterwarnings('ignore', category=sa_exc.SAWarning)
    for fname in files_lst:
        print '\t', fname
        value_dict = dict()
        df = pd.read_csv(fname, header=0, names=['Extended Id', 'Time',
                                                 'Value', 'ValueString', 'Status'],
                         dtype={'ValueString': str})
        df.columns = [col.replace(' ', '') for col in df.columns]
        df.Time = pd.to_datetime(df.Time)
        df.Time = df.Time.dt.tz_localize(EASTERN) \
            .dt.tz_convert(None).dt.strftime(ISOFORMAT)
        feeder = fname.split('.')[0]
        extended_ids = pd.unique(df.ExtendedId)
        for extended_id in extended_ids:
            edna_ts_list = list()
            # Filter dataframe for feeder
            edna_fltr_df = df[df.ExtendedId == extended_id]
            start_time = edna_fltr_df['Time'].min()
            # end_time = edna_fltr_df['Time'].max()
            score = _get_score(start_time)
            if insert_database == 'mysql' or insert_database == 'both':
                edna_mysql_df = edna_fltr_df.copy()
                edna_mysql_df['score'] = score
                edna_mysql_df['tenant_id'] = tenant_id
                edna_mysql_df['feeder_id'] = feeder
                edna_mysql_df = edna_mysql_df.\
                    rename(columns={'ExtendedId': 'extended_id',
                                    'Time': 'timestamp_utc',
                                    'ValueString': 'value_string'})
                edna_mysql_df = edna_mysql_df.astype(object).\
                    where(pd.notnull(edna_mysql_df), 'nan')
                rdbms_util.insert(system, 'edna',
                                  edna_mysql_df.to_dict('records'))

            if insert_database == 'redis' or insert_database == 'both':
                for index, row in edna_fltr_df.iterrows():  # pylint: disable=W0612
                    edna_ts_dict = dict()
                    edna_ts_dict['status'] = row['Status']
                    edna_ts_dict['timestamp_utc'] = row['Time']
                    edna_ts_dict['value_string'] = str(row['ValueString'])
                    edna_ts_dict['value'] = row['Value']
                    edna_ts_list.append(edna_ts_dict)
                edna_data_dict = dict()
                edna_data_dict['feeder_id'] = feeder
                edna_data_dict['time_series'] = edna_ts_list
                edna_data_dict['extended_id'] = extended_id

                value_dict[score] = json.dumps(edna_data_dict)

                cache_util.batch_zadd(tenant_uid + NS_SEPARATOR + EDNA_NS,
                                      feeder,
                                      value_dict)


def _process_tickets_files(system, tenant_id, tenant_uid, files_lst,
                           insert_database):
    cache_util = cacheUtil.CacheUtil()
    rdbms_util = rdbmsUtil.RDBMSUtil()
    filterwarnings('ignore', category=sa_exc.SAWarning)
    for fname in files_lst:
        print '\t', fname
        df = pd.read_csv(fname)
        df.POWEROFF = pd.to_datetime(df.POWEROFF)
        df.POWERRESTORE = pd.to_datetime(df.POWERRESTORE)
        df.POWEROFF = df.POWEROFF.dt.tz_localize(EASTERN) \
            .dt.tz_convert(None).dt.strftime(ISOFORMAT)
        df.POWERRESTORE = df.POWERRESTORE.dt.tz_localize(EASTERN) \
            .dt.tz_convert(None).dt.strftime(ISOFORMAT)
        tkt_fdr_lst = pd.unique(df.FDR_NUM)
        for feeder in tkt_fdr_lst:
            tkt_ts_list = list()
            # Filter dataframe for feeder
            tkt_fltr_df = df[df.FDR_NUM == feeder]
            start_time = tkt_fltr_df['POWEROFF'].min()
            # end_time = tkt_fltr_df['POWEROFF'].max()
            score = _get_score(start_time)
            if insert_database == 'mysql' or insert_database == 'both':
                tickets_mysql_df = tkt_fltr_df.copy()
                tickets_mysql_df['score'] = score
                tickets_mysql_df['tenant_id'] = tenant_id
                tickets_mysql_df = tickets_mysql_df. \
                    rename(columns={'FDR_NUM': 'feeder_id',
                                    'DW_TCKT_KEY': 'dw_ticket_id',
                                    'TRBL_TCKT_NUM': 'trbl_ticket_id',
                                    'IRPT_TYPE_CODE': 'irpt_type_code',
                                    'IRPT_CAUS_CODE': 'irpt_cause_code',
                                    'SUPT_CODE': 'support_code',
                                    'POWEROFF': 'power_off',
                                    'POWERRESTORE': 'power_restore',
                                    'RPR_ACTN_TYPE': 'repair_action_type',
                                    'RPR_ACTN_DS': 'repair_action_description'})
                tickets_mysql_df = tickets_mysql_df[
                    ['feeder_id', 'dw_ticket_id', 'trbl_ticket_id',
                     'irpt_type_code', 'irpt_cause_code',
                     'support_code', 'power_off',
                     'power_restore', 'repair_action_type', 'CMI',
                     'repair_action_description', 'score', 'tenant_id']]
                tickets_mysql_df = tickets_mysql_df.astype(object).\
                    where(pd.notnull(tickets_mysql_df), 'nan')
                rdbms_util.insert(system, 'tickets',
                                  tickets_mysql_df.to_dict('records'))

            if insert_database == 'redis' or insert_database == 'both':
                for index, row in tkt_fltr_df.iterrows():  # pylint: disable=W0612
                    tkt_ts_dict = dict()
                    tkt_ts_dict['dw_ticket_id'] = row['DW_TCKT_KEY']
                    tkt_ts_dict['trbl_ticket_id'] = row['TRBL_TCKT_NUM']
                    tkt_ts_dict['irpt_type_code'] = row['IRPT_TYPE_CODE']
                    tkt_ts_dict['irpt_cause_code'] = row['IRPT_CAUS_CODE']
                    tkt_ts_dict['support_code'] = row['SUPT_CODE']
                    tkt_ts_dict['cmi'] = row['CMI']
                    tkt_ts_dict['power_off'] = row['POWEROFF']
                    tkt_ts_dict['power_restore'] = row['POWERRESTORE']
                    tkt_ts_dict['repair_action_type'] = row['RPR_ACTN_TYPE']
                    tkt_ts_dict['repair_action_description'] = row[
                        'RPR_ACTN_DS']
                    tkt_ts_list.append(tkt_ts_dict)
                tkt_data_dict = dict()
                tkt_data_dict['feeder_id'] = feeder
                tkt_data_dict['time_series'] = tkt_ts_list

                value_dict = dict()
                value_dict[score] = json.dumps(tkt_data_dict)
                cache_util.batch_zadd(tenant_uid + NS_SEPARATOR + TICKETS_NS,
                                      feeder,
                                      value_dict)


def _load_csv(system, tenant_id, tenant_uid, source_dir, insert_database):
    os.chdir(source_dir)
    print 'Processing directory', source_dir
    # Find new files
    files_lst = sorted(glob('*'))
    if 'SCADA' in source_dir:
        print 'Data type = SCADA'
        _process_scada_files(system, tenant_id, tenant_uid, files_lst,
                             insert_database)
    elif 'AMI' in source_dir:
        print 'Data type = AMI'
        _process_ami_files(system, tenant_id, tenant_uid, files_lst,
                           insert_database)
    elif 'EDNA' in source_dir:
        print 'Data type = EDNA'
        _process_edna_files(system, tenant_id, tenant_uid, files_lst,
                            insert_database)
    else:
        print 'Error: Unknown datatype .... please check'
        exit(-1)


def _get_tenant_uid(tenant_id):
    db_name = 'PAM'
    settings_path = os.path.join(os.environ["FOUNDATION_HOME"], 'rdbms.yml')
    try:
        redisprop = yaml.load(open(settings_path, 'r'))[db_name]
        mysql_url = redisprop['url']
    except Exception:
        print 'Error: MySQL DB configuration not set ... '
        exit(-1)

    print 'using MySQL connection url =', mysql_url

    rdbms_util = rdbmsUtil.RDBMSUtil()
    query = "SELECT uid from tenant where id=%s"
    res = rdbms_util.select(db_name, query, (tenant_id))
    if len(res) == 0:
        print 'Error : Invalid tenanat id : ', tenant_id
        return None
    return res[0][0]


def _get_score(start_time):
    score_value = int(time.mktime(time.strptime(start_time, ISOFORMAT)))
    # datetime.datetime.strptime(start_time,ISOFORMAT).strftime('%s')
    return score_value


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print 'Usage: ' + sys.argv[0] + \
              '<system_name> <tenant_id> <input_data_base_dir> [insert_database]'
        exit(-1)
    SYSTEM = sys.argv[1]
    TENANT_ID = sys.argv[2]
    BASE_DIR = sys.argv[3]

    if len(sys.argv) > 4:
        INSERT_DATABASE = sys.argv[4]
    else:
        INSERT_DATABASE = 'both'

    TENANT_UID = _get_tenant_uid(TENANT_ID)

    if TENANT_UID is None:
        exit(-1)

    if not os.path.isdir(BASE_DIR):
        print 'Error: Provided argument is not a valid directory'
        print '       Provide input data directory path.'
        exit(-1)

    DIRS = [d for d in os.listdir(BASE_DIR) if
            os.path.isdir(os.path.join(BASE_DIR, d))]
    for d in DIRS:
        _load_csv(SYSTEM, TENANT_ID, TENANT_UID, os.path.join(BASE_DIR, d),
                  INSERT_DATABASE)

    print 'Data type = TICKETS'
    # Tickets files are stored directly in basedir,
    # so get list of it and then process.
    FILES = [d for d in os.listdir(BASE_DIR)
             if os.path.isfile(os.path.join(BASE_DIR, d)) and 'TICKETS' in d]
    os.chdir(BASE_DIR)
    _process_tickets_files(SYSTEM, TENANT_ID, TENANT_UID, FILES,
                           INSERT_DATABASE)
