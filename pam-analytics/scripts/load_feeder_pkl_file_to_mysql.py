"""LoadFeederPickleFile Module.

This class loads the cross sectional data(feeder metadata) to mysql.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vidhi Chopra' <vidhi.chopra@auto-grid.com>


import pandas as pd
import os
import sys
from autogrid.pam.anomaly.global_define import PAM
import autogrid.foundation.util.RDBMSUtil as rdbmsUtil
from autogrid.foundation.util.Logger import Logger
from warnings import filterwarnings
from sqlalchemy import exc as sa_exc


class LoadFeederPickleFile(object):

    """Class to load feeder metadata to feeder_metadata table in mysql.

    Parameters
    ----------
    feeder_pickle_file : str
        Fully specified path of feeder metadata pickle file.
    """

    def __init__(self, feeder_pickle_file):

        self.__logger = Logger.get_logger(self.__class__.__name__)

        if not os.path.exists(feeder_pickle_file):
            self.__logger.error(feeder_pickle_file + " file not found")
            raise Exception(feeder_pickle_file + " file not found.")

        self.feeder_pickle_file_path = feeder_pickle_file
        self.rdbms_util = rdbmsUtil.RDBMSUtil()
        self.system = PAM

    def save_feeder_meta_data(self):
        """Import feeder metadata to feeder_metadata table in mysql."""
        filterwarnings('ignore', category=sa_exc.SAWarning)
        feeder_df = pd.read_pickle(self.feeder_pickle_file_path)
        feeder_df = feeder_df.rename(columns={'FEEDER': 'feeder_id',
                                              '4N+_FEEDER': '4n_feeder', 'FI': 'fci'})
        feeder_df = feeder_df.astype(object).where(pd.notnull(feeder_df), None)
        feeder_df_data = feeder_df.to_dict('records')

        try:
            self.rdbms_util.insert(self.system, 'feeder_metadata', feeder_df_data)
            self.__logger.debug('Saved feeder metadata in database.')
        except Exception:
            self.__logger.error('Error while saving feeder metadata to database.')
            raise


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: ' + sys.argv[0] + ' <feeder_pickle_file_path>'
        exit(-1)
    FEEDER_PICKLE_FILE_PATH = sys.argv[1]

    LOAD_PICKLE_FILE = LoadFeederPickleFile(FEEDER_PICKLE_FILE_PATH)
    LOAD_PICKLE_FILE.save_feeder_meta_data()
