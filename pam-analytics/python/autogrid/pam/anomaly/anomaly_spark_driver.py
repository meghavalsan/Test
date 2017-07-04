"""PamAnomalySparkDriver Module.

Creates the spark job and process the tickets, edna, scada and ami data types
and store the result in database.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>

from autogrid.foundation.util.Logger import Logger
from autogrid.pam.anomaly import utils
from autogrid.foundation.jobserver.jobs import SparkJob
from autogrid.pam.anomaly.global_define import REDIS_DB, PAM


class PamAnomalySparkDriver(SparkJob):

    """Class to run anaomaly processing.

    Parameters
    ----------
    spark_util : SparkUtil
        SparkUtil instance to run spark job.

    job_uid : String
        Unique ID for SparkJob.

    *args : list
        Arguments that get passed to the Job's 'run' method.

    **kwargs : dict
        Keyword arguments that get passed to the Job's 'run' method.
    """

    def __init__(self, spark_util=None, job_uid=None, *args, **kwargs):

        self.__logger = Logger.get_logger(self.__class__.__name__)

        # Tenant_ID
        self.tenant_id = None
        self.system = None
        self.database = None

        SparkJob.__init__(self, spark_util, job_uid, args, kwargs)

    def run(self, *args, **kwargs):  # pylint: disable=W0613
        """Run method for spark job.

        Parameters
        ----------
        *args : list
            Arguments that is used by spark job like tenant_id.

        **kwargs : dict
            Keyword arguments is used by spark job.
        """
        self.system = args[0][0]
        self.tenant_id = args[0][1]
        self.database = args[0][2]

        if self.tenant_id is None:
            self.__logger.error('Anomaly detect: Argument tenant_id is missing.')
            raise Exception('Argument tenant_id is missing.')

        if self.system is None:
            self.system = PAM

        if self.database is None:
            self.database = REDIS_DB

        ticket_results = utils.parallel_extract(system=self.system,
                                                tenant_id=self.tenant_id,
                                                data_type='TICKETS',
                                                spark_context=self.spark_util.sc,
                                                database=self.database)

        self.__logger.debug('Anomaly detect: Saving TICKETS anomalies to MySQL DB')
        utils.save_anomalies_to_db(self.system, self.tenant_id, ticket_results)

        edna_results = utils.parallel_extract(system=self.system,
                                              tenant_id=self.tenant_id,
                                              data_type='EDNA',
                                              spark_context=self.spark_util.sc,
                                              database=self.database)
        self.__logger.debug('Anomaly detect: Saving EDNA anomalies to MySQL DB')
        utils.save_anomalies_to_db(self.system, self.tenant_id, edna_results)

        scada_results = utils.parallel_extract(system=self.system,
                                               tenant_id=self.tenant_id,
                                               data_type='SCADA',
                                               spark_context=self.spark_util.sc,
                                               database=self.database)
        self.__logger.debug('Anomaly detect: Saving SCADA anomalies to MySQL DB')
        utils.save_anomalies_to_db(self.system, self.tenant_id, scada_results)

        ami_results = utils.parallel_extract(system=self.system,
                                             tenant_id=self.tenant_id,
                                             data_type='AMI',
                                             spark_context=self.spark_util.sc,
                                             database=self.database)
        self.__logger.debug('Anomaly detect: Saving AMI anomalies to MySQL DB')
        utils.save_anomalies_to_db(self.system, self.tenant_id, ami_results)
