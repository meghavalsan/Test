'''
'test_live_train_match.py' holds integration tests to ensure that the
results of live mode and training mode for pam are consistant with each other.
These simple tests check if the generated respective anomalies, signatures,
and predictions (probability based) between live mode and training mode are
identical to one another.
'''
# Copyright (c) 2011-2017 AutoGrid Systems
# Author(s): 'Eric Hsieh' <eric.hsieh@auto-grid.com>
from pandas.util.testing import assert_frame_equal
import pandas as pd
import unittest


def get_data(file_live, file_train):
    live_df = pd.read_pickle(file_live)
    train_df = pd.read_pickle(file_train)
    return live_df, train_df


def compare_dfs(df1, df2):
    '''
    PURPOSE: check if two dfs are the same
    '''
    try:
        assert_frame_equal(df1, df2)
        return True
    except:
        return False


class LiveTrainMatchTest(unittest.TestCase):
    def test_live_train_anomalies_match(self):
        live_df, train_df = get_data(
                'data/sample_model_3_0_live_anoms_jan_2017',
                'data/sample_model_3_0_train_anoms_jan_2017'
            )
        answer = compare_dfs(live_df, train_df)
        self.assertEqual(answer, True)

    def test_live_train_signatures_match(self):
        live_df, train_df = get_data(
                'data/sample_model_3_0_live_sigs_jan_2017',
                'data/sample_model_3_0_train_sigs_jan_2017'
            )
        answer = compare_dfs(live_df, train_df)
        self.assertEqual(answer, True)

    def test_live_train_predictions_match(self):
        live_df, train_df = get_data(
                'data/sample_model_3_0_live_preds_jan_2017',
                'data/sample_model_3_0_train_preds_jan_2017'
            )
        answer = compare_dfs(live_df, train_df)
        self.assertEqual(answer, True)


if __name__ == '__main__':
    unittest.main()
