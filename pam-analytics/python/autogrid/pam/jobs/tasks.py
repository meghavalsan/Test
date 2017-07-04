#!/usr/bin/env python
"""Celery Tasks.

Task module executes following PAM tasks.
- PamAnomalySparkDriver
- SyncDatabases
"""
# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Percy Link' <percy.link@auto-grid.com>
from __future__ import absolute_import

import datetime as dt
import traceback
import uuid
import cPickle as pickle
import yaml
import os
import json
from pytz import timezone
from django.core.cache import cache

from autogrid.pam.jobs.celerydriver import APP
from autogrid.foundation.jobserver.tasks import submit_job
from autogrid.foundation.util.Logger import Logger
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.pam.anomaly.global_define import REDIS_DB, PAM
from autogrid.pam.recovery.sync_databases import SyncDatabases
from autogrid.pam.alert.alert_generator_wrapper import AlertGeneratorWrapper
from autogrid.foundation.messaging.kafkaproducer import KafkaProducer
from autogrid.pam.utils.alert_report import send_alert
from autogrid.pam.alert.save_alerts_to_database import LoadAlertsToDatabase
from autogrid.pam.utils.remove_data_from_mysql import RemoveDataFromMysql
from autogrid.pam.utils.weekly_report_generator import WeeklyReportGenerator
from autogrid.pam.anomaly.utils import get_unprocessed_anomaly_count
from autogrid.pam.dao.models import Models

SYSTEM_PAM = PAM
FOUNDATION_HOME = os.environ.get('FOUNDATION_HOME')
if FOUNDATION_HOME is None:
    raise KeyError('FOUNDATION_HOME:FOUNDATION_HOME is not set in environment.')

SETTINGS_PATH = os.path.join(FOUNDATION_HOME, 'settings.yml')
SETTINGS_DICT = yaml.load(open(SETTINGS_PATH, 'r'))[SYSTEM_PAM]
KAFKA_SETTINGS_PATH = os.path.join(FOUNDATION_HOME, 'kafka.yml')
KAFKA_SETTINGS_DICT = yaml.load(open(KAFKA_SETTINGS_PATH, 'r'))
DATABASE_FOR_SPARK = SETTINGS_DICT['DATABASE_FOR_SPARK']
STATUS_OK = 200


# Function to get all model files from given directory
def __get_model_files(source_dir):
    files = os.listdir(source_dir)
    pkl_file = None
    dataset_file = None
    anomaly_file = None
    alert_file = None
    email_file = None
    for f in files:
        if '.pkl' in f:
            pkl_file = os.path.join(source_dir, f)
        elif 'dataset.yaml' in f:
            dataset_file = os.path.join(source_dir, f)
        elif 'anomaly_map.yaml' in f:
            anomaly_file = os.path.join(source_dir, f)
        elif 'alert.yaml' in f:
            alert_file = os.path.join(source_dir, f)
        elif 'email.yaml' in f:
            email_file = os.path.join(source_dir, f)
    files_dict = {'model_pkl': pkl_file,
                  'dataset_yml': dataset_file,
                  'anomaly_yml': anomaly_file,
                  'alert_yml': alert_file,
                  'email_file': email_file}
    return files_dict


# Function to load all models pickle files
def __load_models():
    __logger = Logger.get_logger("Celery_Task_Preload")
    tenant_models_dict = {}
    rdbms_util = rdbmsUtil.RDBMSUtil()
    models = Models(SYSTEM_PAM)
    query = "SELECT id FROM tenant;"
    rows = rdbms_util.select(SYSTEM_PAM, query)
    for tenant in rows:
        out_models_list = []
        models_list = models.get_all_models(tenant['id'])
        for model in models_list:
            model_dict = {}
            if not os.path.isdir(model['path']):
                __logger.error('Alert gen: Model directory not found. {0} '.format
                               (model['path']))
                continue
            __logger.info('Alert gen: Checking model files for model {0} tenant '
                          '{1}'.format(model['model_id'], tenant['id']))
            files = __get_model_files(model['path'])
            if files['model_pkl'] is None:
                __logger.error('Alert gen: Model pkl file not found for model {0} tenant '
                               '{1}'.format(model['model_id'], tenant['id']))
                continue
            elif files['dataset_yml'] is None:
                __logger.error('Alert gen: Dataset yml file not found for model {0} '
                               'tenant {1}'.format(model['model_id'],
                                                   tenant['id']))
                continue
            elif files['anomaly_yml'] is None:
                __logger.error('Alert gen: Anomaly yml file not found for model {0} '
                               'tenant {1}'.format(model['model_id'],
                                                   tenant['id']))
                continue
            elif files['alert_yml'] is None:
                __logger.error('Alert gen: Alert yml file not found for model {0} tenant'
                               ' {1}'.format(model['model_id'], tenant['id']))
                continue
            elif files['email_file'] is None:
                __logger.info('Alert gen: Email yml file not found for model '
                              '{0} tenant {1} - falling back on settings.yml '
                              'configuration'.format(model['model_id'], tenant['id']))

            __logger.info('Alert gen: Loading model files for model {0} tenant {1}'.format
                          (model['model_id'], tenant['id']))
            model_dict['model_id'] = model['model_id']
            model_dict['model_version'] = model['model_version']
            model_dict['dataset_yml'] = files['dataset_yml']
            model_dict['anomaly_yml'] = files['anomaly_yml']
            model_dict['alert_yml'] = files['alert_yml']
            if files['email_file'] is None:
                model_dict['custom_emails'] = None
            else:
                model_dict['custom_emails'] = yaml.load(open(files['email_file'],
                                                             'r'))['to_email']
            model_dict['clf'] = pickle.load(open(files['model_pkl'], 'rb'))

            out_models_list.append(model_dict)

        tenant_models_dict[tenant['id']] = out_models_list

    return tenant_models_dict

# Load all required model pickle files here once and use it in tasks.
TENANT_MODELS_DICT = __load_models()


@APP.task
def task_evaluation(use_celery=True, database=REDIS_DB):
    """Task to run PamAnomalySparkDriver job.

    Parameters
    ----------
    use_celery : bool
        Boolean flag tells whether to use celery while running a task

    database : str
        String defining the database type. Valid values ('REDIS' or 'MySQL')
    """
    __logger = Logger.get_logger("Celery_Task")
    __logger.info("Executing spark job PamAnomalySparkDriver at {}"
                  .format(dt.datetime.now()))

    kafka_topic = SETTINGS_DICT['kafka_outgoing_topic']

    # api key and mail information to send alert mails.
    to_email = SETTINGS_DICT['to_email']
    if type(to_email) is not list:
        __logger.error("Alert gen: to_emails should be a list of receipients, found %s"
                       "Please check settings.yml." % type(to_email))
        raise ValueError("settings.yml : to_emails should be a list of "
                         "recipients, found %s" % type(to_email))
    from_email = SETTINGS_DICT['from_email']
    email_method = SETTINGS_DICT['email_method']
    if email_method == 'sendgrid':
        api_key = SETTINGS_DICT['api_key']
        email_server = None
        email_port = None
    elif email_method == 'smarthost':
        api_key = None
        email_server = SETTINGS_DICT['email_server']
        email_port = SETTINGS_DICT['email_port']
    else:
        api_key = None
        email_server = None
        email_port = None

    if 'lock_expire_time_in_seconds' in SETTINGS_DICT:
        lock_expire_time = SETTINGS_DICT['lock_expire_time_in_seconds']
    else:
        lock_expire_time = 60 * 30  # 30 minutes
    lock_id = 'PAM-evaluation-task-lock'
    acquire_lock = lambda: cache.add(lock_id, 'true', lock_expire_time)
    release_lock = lambda: cache.delete(lock_id)

    if acquire_lock():
        try:
            rdbms_util = rdbmsUtil.RDBMSUtil()
            query = "select id, timezone from tenant;"
            rows = rdbms_util.select(SYSTEM_PAM, query)
            for record in rows:
                tenant_id = record['id']
                tenant_tz = timezone(record['timezone'])
                __logger.info("Anomaly detect: Processing PamAnomalySparkDriver"
                              " for tenant_id " + str(tenant_id))
                # Generate a unique ID that gets passed to the SparkJob.
                # This is the ID with which metadata and results of the
                # PamAnomalySparkDriver job are stored.
                job_uid = SYSTEM_PAM + '_' + str(uuid.uuid1())

                # Submit PamAnomalySparkDriver job as a SparkJob to the job server.
                async = submit_job(
                        'autogrid.pam.anomaly.anomaly_spark_driver.PamAnomalySparkDriver',
                        job_args=(SYSTEM_PAM, str(tenant_id), DATABASE_FOR_SPARK),
                        job_uid=job_uid,
                        use_celery=use_celery)

                # Wait for job to finish.
                spark_job_result = async.get()

                __logger.info("Anomaly detect: Spark job status: " +
                              str(spark_job_result))

                anomaly_count = get_unprocessed_anomaly_count(SYSTEM_PAM,
                                                              tenant_id)

                if anomaly_count == 0:
                    __logger.info("Anomaly detect: No anomalies deteacted. "
                                  "Skipping alert generation and signature "
                                  "processing.")
                    return

                __logger.info("Anomaly detect: Anomaly count = " +
                              str(anomaly_count))

                # Process alert generator for all models in DB for given tenant
                if tenant_id not in TENANT_MODELS_DICT:
                    __logger.error('Alert gen: Cannot process Alert Generator '
                                   'as no models are defined in DB for tenant '
                                   '{0}'.format(tenant_id))
                else:
                    for model_num, model in enumerate(TENANT_MODELS_DICT[tenant_id]):
                        __logger.info("Alert gen: Processing AlertGeneratorWrapper for "
                                      "model: {0} tenant: {1} at {2}".format
                                      (model['model_id'], tenant_id,
                                       dt.datetime.now()))
                        # This will only mark data as processed for final model
                        last_model = model_num == len(TENANT_MODELS_DICT[tenant_id]) - 1
                        alert_gen_wrap = \
                            AlertGeneratorWrapper(system=SYSTEM_PAM,
                                                  clf=model['clf'],
                                                  tenant_id=tenant_id,
                                                  model_id=model['model_version'],
                                                  last_model=last_model,
                                                  dataset_config_file=model['dataset_yml'],
                                                  anomaly_map_file=model['anomaly_yml'],
                                                  alert_config_file=model['alert_yml'])
                        alert_list = alert_gen_wrap.run_alerts()
                        # alert_list will be used to send to customer.

                        alerts_to_db = LoadAlertsToDatabase(SYSTEM_PAM, tenant_id)
                        for alert in alert_list:
                            alert_dict = alert.to_dict()
                            alerts_to_db.insert_raw_json_to_db(alert_dict)
                            try:
                                kafka = KafkaProducer(props={'url': '' +
                                        str(KAFKA_SETTINGS_DICT['host']) + ':' +
                                        str(KAFKA_SETTINGS_DICT['port'])})
                                __logger.info("Alert gen: Sending alerts to Kafka...")
                                code, message = kafka.publish(topic=kafka_topic,
                                                              message=str(json.dumps(alert_dict)))
                                if code == 1:
                                    __logger.info("Alert gen: Alerts published to Kafka")
                                    alerts_to_db.update_alert_column(col_name='published_to_kafka',
                                                                     col_value=True,
                                                                     alert_json_dict=alert_dict)
                                else:
                                    __logger.error("Alert gen: Error while sending alerts to kafka")
                                    __logger.error(message)
                            except Exception:
                                __logger.error("Alert gen: Error while sending alerts to kafka")
                                __logger.error(traceback.format_exc())

                            result = None
                            try:
                                __logger.info("Alert gen: Sending alert emails at {}"
                                              .format(dt.datetime.now()))
                                if model['custom_emails'] is None:
                                    emails_to_use = to_email
                                else:
                                    emails_to_use = model['custom_emails']
                                result = send_alert(tenant_tz=tenant_tz,
                                                    alert=alert,
                                                    to_email=emails_to_use,
                                                    from_email=from_email,
                                                    method=email_method,
                                                    api_key=api_key,
                                                    server=email_server,
                                                    port=email_port)
                                __logger.info("Alert gen: Email sent status... " + str(result))
                                if result[0] is STATUS_OK:
                                    alerts_to_db.update_alert_column(col_name='sent_email',
                                                                     col_value=True,
                                                                     alert_json_dict=alert_dict)
                                else:
                                    __logger.error("Alert gen: Error while sending email alert : " +
                                                   str(result[0]) + "\n" + str(result[1]))
                            except Exception:
                                __logger.error("Alert gen: Error while sending alert emails. "
                                               "Please check mail status: ", result)
                                __logger.error(traceback.format_exc())

            __logger.info("Completed spark job PamAnomalySparkDriver at {}"
                          .format(dt.datetime.now()))
        except Exception:
            __logger.error("Error while running celery task")
            __logger.error(traceback.format_exc())

        finally:
            release_lock()
    else:
        __logger.info('PAM anomaly extraction and processing task is already '
                      'running. Please try after some time.')


@APP.task
def task_sync_databases():
    """Task to run sync_databases job."""
    __logger = Logger.get_logger("Sync_DB_Celery_Task")
    __logger.info("Sync db: Executing task sync databases at {}"
                  .format(dt.datetime.now()))
    try:
        rdbms_util = rdbmsUtil.RDBMSUtil()
        query = "select id from tenant;"
        rows = rdbms_util.select(SYSTEM_PAM, query)
        for record in rows:
            tenant_id = record['id']
            __logger.info("Sync db: Executing task sync databases for "
                          "tenant_id " + str(tenant_id))
            sync_database = SyncDatabases(tenant_id)
            sync_database.get_data()
        __logger.info("Sync db: Completed task sync databases at {}"
                      .format(dt.datetime.now()))
    except Exception:
        __logger.error("Sync db: Error while running Sync databases")
        __logger.error(traceback.format_exc())


@APP.task
def task_remove_data_from_mysql():
    """Task to run remove_data_from_mysql job."""
    __logger = Logger.get_logger("Remove_Mysql_Data_Celery_Task")
    __logger.info("Remove data: Executing remove data from mysql at {}"
                  .format(dt.datetime.now()))
    try:
        rdbms_util = rdbmsUtil.RDBMSUtil()
        query = "select id from tenant;"
        rows = rdbms_util.select(SYSTEM_PAM, query)
        for record in rows:
            tenant_id = record['id']
            __logger.info("Remove data: Executing remove data from mysql for "
                          "tenant_id " + str(tenant_id))
            data_mysql = RemoveDataFromMysql(tenant_id)
            data_mysql.remove_data()
        __logger.info("Remove data: Completed remove data from mysql at {}"
                      .format(dt.datetime.now()))
    except Exception:
        __logger.error("Remove data: Error while running remove data from mysql")
        __logger.error(traceback.format_exc())


@APP.task()
def task_weekly_report():
    """Task to run weekly reprots."""
    __logger = Logger.get_logger("weekly_report_task")
    __logger.info("Weekly report: Executing weekly report at {}"
                  .format(dt.datetime.now()))
    try:
        rdbms_util = rdbmsUtil.RDBMSUtil()
        query = "select id from tenant;"
        rows = rdbms_util.select(SYSTEM_PAM, query)
        for record in rows:
            tenant_id = record['id']
            __logger.info("Weekly report: Executing weekly report for "
                          "tenant_id " + str(tenant_id))
            wrg = WeeklyReportGenerator(SYSTEM_PAM, tenant_id)
            # api key and mail information to send alert mails.
            to_email_dict = {}
            for model in TENANT_MODELS_DICT[tenant_id]:
                if model['custom_emails'] is None:
                    to_email_dict[model['model_version']] = SETTINGS_DICT['to_email']
                else:
                    to_email_dict[model['model_version']] = model['custom_emails']
            from_email = SETTINGS_DICT['from_email']
            email_method = SETTINGS_DICT['email_method']
            if email_method == 'sendgrid':
                api_key = SETTINGS_DICT['api_key']
                email_server = None
                email_port = None
            elif email_method == 'smarthost':
                api_key = None
                email_server = SETTINGS_DICT['email_server']
                email_port = SETTINGS_DICT['email_port']
            else:
                api_key = None
                email_server = None
                email_port = None
            wrg.run(to_email_dict=to_email_dict,
                    from_email=from_email,
                    method=email_method,
                    api_key=api_key,
                    server=email_server,
                    port=email_port)
        __logger.info("Weekly report: Completed weekly report at {}"
                      .format(dt.datetime.now()))
    except Exception:
        __logger.error("Weekly report: Error while running weekly report")
        __logger.error(traceback.format_exc())
