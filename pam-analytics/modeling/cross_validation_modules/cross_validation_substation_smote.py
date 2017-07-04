"""cross_validation_ substation_smote.py.

The :mod: the module does cross-validation on substations.

This is a special edition of the cross_validation module.
It builds on the original CV module written by Trevor stephens.

Method#1:
    1. Bucket Substations into 4 buckets
        - Bucket one has the substations with lower feeders freqeuncy
        - Bucket three has the substations with higher feeders frequency
        - Each bucket contains around ~700 feeders equally
    2. Above buckets are then splitted into K equal segments respectively.
    3. Feeders, based on substations, are sorted into folds with above info.
    4. Rows of X, y, and wieghts are then bucketed based on the above info.
Method#2 (experimential):
    1. SMOTE for class imbalance
    2. result = 50 percent positive and 50 percent negative class by using
       a KNN based method to create more minority class
Method#3 (experimential):
    1. drop rows where they have negative outage hours
       in 'OUTAGE' during training
    2. drop rows where they have near misses outage hours
       in 'OUTAGE' during traing
"""

# Copyright (c) 2011-2017 AutoGrid Systems
# Author(s):
#'Trevor Stephens' <trevor.stephens@auto-grid.com>
#'Eric Hsieh' <eric.hsieh@auto-grid.com>

from imblearn.over_sampling import SMOTE
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import random

import matplotlib.pyplot as plt
plt.style.use('ggplot')

class PAMCrossValidator_Subst(object):
    """Performs randomized cross validation for time series.

    Parameters
    ----------
    min_train_hrs : float (default = 0.5)
        The lower-bound "warning time" for an outage to count the signature as
        a positive case during training.

    max_train_hrs : float (default = 336.0)
        The upper-bound "warning time" for an outage to count the signature as
        a positive case during training.

    min_test_hrs : float (default = 0.5)
        The lower-bound "warning time" for an outage to count the signature as
        a positive case during evaluation.

    max_test_hrs : float (defaut = 336.0)
        The upper-bound "warning time" for an outage to count the signature as
        a positive case during evaluation.

    num_folds : integer, (default=3)
        The number of folds to perform cross validation on.

    blackout_days : integer, optional (default=1)
        The number of days at the beginning of a month to throw away. This stops
        "leakage" between adjacent months.

    train_filter : function or None, optional (default=None)
        An optional filter to subset the rows of X and y to use in training.
        This function should take X and y as inputs and return a boolean vector.

    target_col : str, optional (default='OUTAGE')
        The name of the column used for determining training "wins".

    win_col : str, optional (default='OUTAGE')
        The name of the column used for determining testing "wins".

    nearmiss_col : str, optional (default='OUTAGE')
        The name of the column used for determining "near misses".

    random_state : integer or None, optional (default=None)
        The random seed to use for randomly splitting the dataset.
    """

    def __init__(
                 self,
                 min_train_hrs=0.5, max_train_hrs=336.0,
                 min_test_hrs=0.5, max_test_hrs=336.0,
                 num_folds=3, blackout_days=1, test_begin=None, test_end=None,
                 train_filter=None, target_col='OUTAGE', win_col='OUTAGE',
                 nearmiss_col='OUTAGE', random_state=None):

        self.min_train_hrs = min_train_hrs
        self.max_train_hrs = max_train_hrs
        self.min_test_hrs = min_test_hrs
        self.max_test_hrs = max_test_hrs
        self.num_folds = num_folds
        self.blackout_days = blackout_days
        self.test_begin = test_begin
        self.test_end = test_end
        self.train_filter = train_filter
        self.target_col = target_col
        self.win_col = win_col
        self.nearmiss_col = nearmiss_col
        self.random_state = random_state

    def _plot(self, y_test, fold):
        # Time for the infamous backwards plot!
        opto = {'Prob': [], 'Precision': [], 'Alerts': [], 'Wins': [],
                'NearMiss': []}
        for p in np.arange(0.5, 1., 0.001):
            oopto = y_test.loc[y_test.PROBA > p]
            if not oopto.empty:
                oopto = oopto.groupby(by=['FEEDER_DATE']).OUTCOME.max()
                wins = oopto.loc[oopto == 1.].shape[0]
                nearmiss = oopto.loc[oopto == 0.5].shape[0]
                alerts = oopto.shape[0] - nearmiss
                opto['Prob'].append(p)
                opto['Precision'].append(100. * wins / max(alerts, 1.))
                opto['Alerts'].append(alerts)
                opto['Wins'].append(wins)
                opto['NearMiss'].append(nearmiss)
        opto = pd.DataFrame(opto)

        plt.figure(figsize=(16, 5))
        plt.plot(opto.Prob, opto.Precision, 'r-', label='% Precision')
        plt.plot(opto.Prob, opto.Alerts, 'g-', label='# Alerts')
        plt.plot(opto.Prob, opto.Wins, 'b-', label='# Wins')
        plt.plot(opto.Prob, opto.NearMiss, 'c-', label='# Near Misses')
        plt.hlines(np.arange(20, 100, 20), 0.5, 1.0, '0.7')
        plt.hlines(np.arange(10, 100, 20), 0.5, 1.0, '0.85')
        plt.ylim(0, 200)
        plt.xlim(0.0, 1.)
        plt.legend(title='Fold: %s' % fold, shadow=True, fancybox=True)
        ax = plt.gca()
        ax.invert_xaxis()
        plt.show()
        plt.close()

    def fit(self, clf, X, y, meta_df, sample_weight=None):
        """Fit the model over each fold.

        Parameters
        ----------
        clf : Classifier
            The pre-initialized classifier to train.
            Must have fit and predict_proba methods.

        X : np.array
            The features to fit.

        y : np.arrayfvcb
            The target vector.

        meta_df: meta data of the feeders

        sample_weight : np.array or None, optional (default=None)
            The sample weight to use when fitting.
        """
        if self.random_state is None:
            random_state = np.random.mtrand._rand
        else:
            random_state = np.random.RandomState(self.random_state)

        # Create folds based on substations:

        # i. bucket substations:
        # grab count of feeders is in a substation:
        sub_fdrs_count =\
            meta_df.groupby(['SUBSTATION']).agg(
                ['count'])['FEEDER']['count'].reset_index()
        sub_fdrs_count.sort(columns=['count'], inplace=True)
        sub_fdrs_count = sub_fdrs_count.reset_index()
        # checking the frequency of the bins (if bin=4):
        #values = list(sub_fdrs_count.index)
        #freqs = list(sub_fdrs_count['count'])
        # grab bins:
        b_0 = sub_fdrs_count.loc[range(0, 224+1)]['SUBSTATION'].values
        b_1 = sub_fdrs_count.loc[range(224+1, 342+1)]['SUBSTATION'].values
        b_2 = sub_fdrs_count.loc[range(342+1, 432+1)]['SUBSTATION'].values
        b_3 = sub_fdrs_count.loc[range(432+1, 500+1)]['SUBSTATION'].values

        # ii. split the substations based on folds (k):
        k = self.num_folds
        # iii. lst of substation bins and to hold fold results:
        b_lst, k_lst = [b_0, b_1, b_2, b_3], []
        # iv.split each bin into k folds and combine them: final fold bin = k:
        for b in b_lst:
            random.shuffle(b)
            loc_result = np.array_split(b, k)
            k_lst.append(loc_result)
        # v.combine substation into respective k_fold:
        subt_fold_dict = defaultdict()
        for Bin in k_lst:
            for k, arr in enumerate(Bin):
                if k in subt_fold_dict:
                    subt_fold_dict[k] =\
                        np.concatenate([subt_fold_dict[k], arr], axis=0)
                else:
                    subt_fold_dict[k] = arr
        # vi. create folds with fdrs using the above dictionary:
        fdr_fold_dict = defaultdict()
        for k in subt_fold_dict:
            fdr_fold_dict[k] =\
            meta_df[meta_df['SUBSTATION'].isin(subt_fold_dict[k])].FEEDER.values
        # vii.create folds with data indices using the above dictionary:
        idx_fold_dict = defaultdict()
        for k in fdr_fold_dict:
            idx_fold_dict[k] =\
                np.array(y[y['FEEDER'].isin(fdr_fold_dict[k])].index)
        # viii.print out sizes of each folds:
        total, fold_size = 0, []
        for arr in idx_fold_dict.values():
            fold_size.append(len(arr))
            total += len(arr)
        fold_size = np.array(fold_size)
        print 'Summary: fold sizes are (%): '
        print np.apply_along_axis(\
            lambda x: 100*(x/float(total)), 0, fold_size)
        # ix.formating:
        folds = pd.Series(index=y.index)
        for fold in idx_fold_dict:
            folds.loc[idx_fold_dict[fold]] = fold
        # x:
        self.folds = folds

        # Now begin the cross validation
        results = []

        for fold in range(self.num_folds):
            # Split train and test sets up
            y_train = y.loc[(folds != -1) & (folds != fold)]
            X_train = X.loc[(folds != -1) & (folds != fold)]
            if sample_weight is not None:
                sw_train = sample_weight.loc[(folds != -1) & (folds != fold)]
            else:
                sw_train = None
            if self.train_filter is not None:
                train_filter = self.train_filter(X=X_train, y=y_train)
                y_train = y_train.loc[train_filter]
                X_train = X_train.loc[train_filter]
                if sw_train is not None:
                    sw_train = sw_train.loc[train_filter]
            y_test = y.loc[folds == fold].copy()
            y_test['FOLD'] = fold
            X_test = X.loc[folds == fold]
            if hasattr(sw_train, 'values'):
                sw_train = sw_train.values

            # Generate target vector:

            # for y_target/X_train/sw_train drop all rows that is negative
            # outage hours or within in the near_miss range for training:
            print 'Original target y shape : {}'.format(y_train.shape)
            print 'Original training X shape {}'.format(X_train.shape)
            y_target =\
                y_train[~(y_train[self.target_col] < self.min_train_hrs)]
            X_train_input = X_train.loc[y_target.index]

            if sw_train is not None:
                sw_train_input = sw_train.loc[y_target.index]
            else:
                sw_train_input = sw_train

            y_target = ((y_target[self.target_col] <= self.max_train_hrs) &
                        (y_target[self.target_col] >= self.min_train_hrs)).astype(int)
            print 'After filtering out negative and near misses outages:'
            print 'Filterd target y shape: {}'.format(y_target.shape)
            print 'FIltered training X shape {}'.format(X_train_input.shape)
            # SMOTE for class imbalance:
            sm = SMOTE(random_state=42)
            X_train_res, y_target_res =\
                sm.fit_sample(X_train_input.values, y_target.values)
            print ('Resampled dataset shape {}'.format(Counter(y_target_res)))
            # Train the model & make predictions (added extra lines by EH for ROC Curve):
            clf.fit(X=X_train_res, y=y_target_res, sample_weight=sw_train_input)

            y_test['PROBA'] = clf.predict_proba(X=X_test)[:, 1]
            y_test['y_score'] = clf.predict(X_test)

            y_test['FEEDER_DATE'] = y_test.FEEDER + '_' + y_test.TIMESTAMP.dt.date.astype(str)
            y_test['OUTCOME'] = 0.
            y_test.loc[(y_test[self.win_col] <= self.max_test_hrs) &
                       (y_test[self.win_col] >= self.min_test_hrs),
                       'OUTCOME'] = 1.
            y_test.loc[y_test[self.nearmiss_col] < self.min_test_hrs, 'OUTCOME'] = 0.5

            results.append(y_test)

            if self.test_begin is not None:
                # End loop for single explicit hold-out set case
                self._plot(y_test=y_test, fold='Hold-Out')
                break

            self._plot(y_test=y_test, fold=fold)

        self.results = pd.concat(results)

        return self
