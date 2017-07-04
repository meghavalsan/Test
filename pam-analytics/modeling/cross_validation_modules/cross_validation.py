"""Cross-validation module.

The :mod:`cross_validation` module does cross-validation on time series data.
"""

# Copyright (c) 2011-2016 AutoGrid Systems
# Author(s): 'Trevor Stephens' <trevor.stephens@auto-grid.com>


import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


class PAMCrossValidator(object):
    """Performs randomized cross validation for time series.

    Parameters
    ----------
    min_train_hrs : float
        The lower-bound "warning time" for an outage to count the signature as
        a positive case during training.

    max_train_hrs : float
        The upper-bound "warning time" for an outage to count the signature as
        a positive case during training.

    min_test_hrs : float
        The lower-bound "warning time" for an outage to count the signature as
        a positive case during evaluation.

    max_test_hrs : float
        The upper-bound "warning time" for an outage to count the signature as
        a positive case during evaluation.

    num_folds : integer, (default=3)
        The number of folds to perform cross validation on. Folds are
        constructed such that each month in the dataset is randomly assigned to
        a fold. So long as the number of years in the dataset is at least as
        large as the number of folds, each fold will carry at least one of every
        month.

    test_begin : date or None, optional (default=None)
        Instead of randomized months as used when setting `folds`, an explicit
        held-out set can be specified instead. Set the beginning of the test set
        with this parameter (inclusive). Data prior to this date will be used
        for training (with the blackout days removed).

    test_end : date or None, optional (default=None)
        The end of the test set (inclusive). If not specified, the latest data
        will be used.

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

    def __init__(self, min_train_hrs, max_train_hrs, min_test_hrs, max_test_hrs,
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
        opto = {'Prob': [], 'Accuracy': [], 'Alerts': [], 'Wins': [],
                'NearMiss': []}
        for p in np.arange(0.5, 1., 0.001):
            oopto = y_test.loc[y_test.PROBA > p]
            if not oopto.empty:
                oopto = oopto.groupby(by=['FEEDER_DATE']).OUTCOME.max()
                wins = oopto.loc[oopto == 1.].shape[0]
                nearmiss = oopto.loc[oopto == 0.5].shape[0]
                alerts = oopto.shape[0] - nearmiss
                opto['Prob'].append(p)
                opto['Accuracy'].append(100. * wins / max(alerts, 1.))
                opto['Alerts'].append(alerts)
                opto['Wins'].append(wins)
                opto['NearMiss'].append(nearmiss)
        opto = pd.DataFrame(opto)

        plt.figure(figsize=(16, 5))
        plt.plot(opto.Prob, opto.Accuracy, 'r-', label='% Accuracy')
        plt.plot(opto.Prob, opto.Alerts, 'g-', label='# Alerts')
        plt.plot(opto.Prob, opto.Wins, 'b-', label='# Wins')
        plt.plot(opto.Prob, opto.NearMiss, 'c-', label='# Near Misses')
        plt.hlines(np.arange(20, 100, 20), 0.5, 1.0, '0.7')
        plt.hlines(np.arange(10, 100, 20), 0.5, 1.0, '0.85')
        plt.ylim(0, 101)
        plt.xlim(0.5, 1.)
        plt.legend(title='Fold: %s' % fold, shadow=True, fancybox=True)
        ax = plt.gca()
        ax.invert_xaxis()
        plt.show()
        plt.close()

    def fit(self, clf, X, y, sample_weight=None):
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

        sample_weight : np.array or None, optional (default=None)
            The sample weight to use when fitting.
        """
        if self.random_state is None:
            random_state = np.random.mtrand._rand
        else:
            random_state = np.random.RandomState(self.random_state)

        # Create a dataframe spanning all dates in y.
        date_range = pd.DataFrame(pd.date_range(start=y.TIMESTAMP.min(),
                                                end=y.TIMESTAMP.max(),
                                                normalize=True))
        date_range.columns = ['TIMESTAMP']
        date_range['DATE'] = date_range.TIMESTAMP.dt.date
        date_range['YEAR'] = date_range.TIMESTAMP.dt.year
        date_range['MONTH'] = date_range.TIMESTAMP.dt.month
        date_range['DAY'] = date_range.TIMESTAMP.dt.day
        date_range['YEAR_MONTH'] = date_range.YEAR.astype(
            str) + date_range.MONTH.astype(str)
        date_range['FOLD'] = -1

        if self.test_begin is None:
            # Randomly assign each month to a fold
            num_years = date_range.YEAR.nunique()
            num_repeats = num_years / self.num_folds
            num_extra = num_years % self.num_folds
            for month in date_range.MONTH.unique():
                folds = np.repeat(range(self.num_folds), num_repeats)
                folds = list(random_state.permutation(folds))
                folds = folds + list(random_state.randint(low=0,
                                                          high=self.num_folds,
                                                          size=num_extra))
                for i, year in enumerate(date_range.YEAR.unique()):
                    date_range.loc[(date_range.YEAR == year) &
                                   (date_range.MONTH == month), 'FOLD'] = folds[i]

            # Blackout early days in the month to avoid leakage
            for day in range(self.blackout_days):
                date_range.loc[date_range.DAY == day + 1, 'FOLD'] = -1
        else:
            # 0 is the test set, 1 is the train set
            date_range.loc[date_range.DATE >= self.test_begin, 'FOLD'] = 0
            if self.test_end is not None:
                date_range.loc[date_range.DATE >= self.test_end, 'FOLD'] = -1
            date_range.loc[date_range.DATE < self.test_begin - pd.Timedelta(
                self.blackout_days, 'D'), 'FOLD'] = 1

        # Assign each sample in y based on it's date
        folds = date_range.T.to_dict()
        folds = {folds[k]['DATE']: folds[k]['FOLD'] for k in folds}
        folds = y.TIMESTAMP.dt.date.map(folds)
        self.folds = folds

        # Print fold summary
        summary_format = '%5s |%3s%3s%3s%3s%3s%3s%3s%3s%3s%3s%3s%3s |%5s%8s'
        print summary_format % ('Fold', 'J', 'F', 'M', 'A', 'M', 'J', 'J',
                                'A', 'S', 'O', 'N', 'D', 'Days', 'Samples')
        for fold in range(self.num_folds):
            printer = [fold]
            for month in range(12):
                printer.append(date_range.loc[(date_range.MONTH == month + 1) &
                                              (date_range.FOLD == fold),
                                              'YEAR_MONTH'].nunique())
            printer.append(date_range.loc[date_range.FOLD == fold,
                                          'DATE'].nunique())
            percentage = 100. * folds.loc[folds == fold].shape[0] / float(folds.shape[0])
            percentage = '%.1f%%' % percentage
            printer.append(percentage)
            print summary_format % tuple(printer)
            if self.test_begin is not None and fold == 1:
                break

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

            # Generate target vector
            y_target = ((y_train[self.target_col] <= self.max_train_hrs) &
                        (y_train[self.target_col] >= self.min_train_hrs)).astype(int)

            # Train the model & make predictions
            clf.fit(X=X_train.values, y=y_target.values, sample_weight=sw_train)
            y_test['PROBA'] = clf.predict_proba(X=X_test)[:, 1]
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
