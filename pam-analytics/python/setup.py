# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Trevor Stephens' <trevor.stephens@auto-grid.com>

import os
from setuptools import setup, find_packages

current_dir = os.path.dirname(os.path.realpath(__file__))
version = open(os.path.join(current_dir, 'VERSION')).readline().strip()

setup(
    name='pam-analytics',
    version=version,
    packages=find_packages(exclude=[
        'test',
        'test.*',
        '*.test',
        '*.test.*',
        'local',
        'local.*',
    ]),
    # For installing dependencies using Ansible.
    data_files=[('ag_config/pam-analytics', ['requirements.txt'])],
    url='http://www.auto-grid.com',
    license='Copyright (c) 2011-2015 AutoGrid Systems',
    author='AutoGrid Systems',
    author_email='info@auto-grid.com',
    description='Predictive Asset Maintenance libraries code'
)
