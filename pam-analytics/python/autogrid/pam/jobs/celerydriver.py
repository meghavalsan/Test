#!/usr/bin/env python
"""Celery driver.

Celery driver for PAM tasks.
"""
# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Percy Link' <percy.link@auto-grid.com>
from __future__ import absolute_import

from celery import Celery
from autogrid.foundation.messaging import celeryconfig


APP = Celery('pam_worker')
celeryconfig.CELERY_IMPORTS = 'autogrid.pam.jobs.tasks'
APP.config_from_object(celeryconfig)


if __name__ == "__main__":

    APP.start(['celery', '-A', 'autogrid.pam.jobs.celerydriver', 'worker',
               '--loglevel=DEBUG'])
