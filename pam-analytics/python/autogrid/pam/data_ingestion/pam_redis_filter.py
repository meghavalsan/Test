"""Data Filter Module for Redis.

This is class to save incoming data to Redis.
"""
# Copyright (c) 2011-2015 AutoGrid Systems

import time
import json
from yukon.filters.base_filter import BaseFilter
from autogrid.foundation.util.CacheUtil import CacheUtil
from autogrid.pam.anomaly.global_define import NS_SEPARATOR, EDNA_NS, SCADA_NS,\
    TICKETS_NS, AMI_NS


class PamRedisFilter(BaseFilter):

    """Save incoming data to redis.

    Parameters
    ----------
    tenant_uid: TenantUID
        Tenant UID
    """

    REQUIRED_PARAMS = ['message_id', 'message_type', 'message_time_utc', 'data']

    def __init__(self, tenant_uid):
        self.tenant_uid = tenant_uid
        self.cache = CacheUtil()

    def __call__(self, data):
        """Callable method inserts data to database.

        Parameters
        ----------
        data : dict
            Incoming JSON data passed automatically by sample_pipeline.yml
        """
        feeder_data = data['data']
        message_type = data['message_type']
        score = self._get_score(data['start_time_utc'])
        redis_ns = None
        if message_type.upper() == 'SCADA':
            redis_ns = SCADA_NS
        elif message_type.upper() == 'AMI':
            redis_ns = AMI_NS
        elif message_type.upper() == 'EDNA':
            redis_ns = EDNA_NS
        elif message_type.upper() == 'TICKETS':
            redis_ns = TICKETS_NS

        if redis_ns is not None:
            for feeder_datum in feeder_data:
                feeder_id = feeder_datum['feeder_id']
                value_dict = dict()
                value_dict[score] = json.dumps(feeder_datum)
                self.cache.batch_zadd(self.tenant_uid + NS_SEPARATOR + redis_ns,
                                      feeder_id, value_dict)

    def _get_score(self, start_time):
        isoformat = '%Y-%m-%dT%H:%M:%SZ'
        score_value = int(time.mktime(time.strptime(start_time, isoformat)))
        return score_value
