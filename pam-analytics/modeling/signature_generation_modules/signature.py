"""Signature module.

The :mod:`signature` module generates signatures based on anomalies generated
by the :mod:`autogrid.pam.anomaly` module.
"""

# Copyright (c) 2011-2016 AutoGrid Systems
# Author(s): 'Trevor Stephens' <trevor.stephens@auto-grid.com>


import numpy as np
import pandas as pd
from sklearn.externals.joblib import Parallel, delayed, cpu_count
from random import shuffle
from pytz import UTC
from copy import deepcopy
import cPickle as pickle


HOUR = pd.Timedelta(1, 'h')


def very_deep_copy(self):
    return pd.DataFrame(data=deepcopy(self.values.copy()),
                        columns=deepcopy(self.columns.copy()))


pd.DataFrame.very_deep_copy = very_deep_copy


def _triple_threat(anomalies):
    """Private function to generate the triple_threat rule.

    Parameters
    ----------
    anomalies : pd.DataFrame
        DataFrame containing one row per anomaly. Same format as the output
        of the anomaly module.
    """
    # Note: Must be a cleaned dataset
    triple_threat = ['PF_SPIKES', 'THD_SPIKES', 'ZERO_CURRENT',
                     'ZERO_POWER', 'ZERO_VOLTAGE']
    triple_threat = anomalies.loc[anomalies.Anomaly.isin(triple_threat)]
    cols = ['Anomaly', 'Feeder', 'Time']
    triple_threat = triple_threat.drop_duplicates(cols)
    cols = ['Feeder', 'Time']
    triple_threat = triple_threat.groupby(cols)['Anomaly'].count()
    triple_threat = triple_threat[triple_threat > 2]
    triple_threat = triple_threat.reset_index()[cols]

    return triple_threat


def _parallel_transform(anomalies, tickets, dataset_config, feeder_df, feeders):
    # Get dataset configurations
    col_order = [c['name'] for c in dataset_config]
    triggers = [c['lookup'] for c in dataset_config if
                c['type'] == 'trigger']
    durations = [c['name'] for c in dataset_config if
                 c['type'] == 'duration']
    constants = [c['name'] for c in dataset_config if
                 c['type'] == 'constant']

    y = {'FEEDER': [], 'TIMESTAMP': [], 'TRIGGER_ANOMALY':[]}
    if tickets is not None:
        y['OUTAGE'] = []
        y['TICKET'] = []
    X = {c: [] for c in col_order}

    if not anomalies.empty:
        for feeder in feeders:

            f_anoms = anomalies.loc[anomalies.Feeder == feeder]

            if tickets is not None:
                f_tickets = tickets.loc[tickets.FDR_NUM == feeder]

            # Only create new rows for "triggers":

            # create an array of trigger anomalies of this feeder:
            f_rows = f_anoms.loc[f_anoms.Anomaly.isin(triggers)]
            # limit this array to only trigger anomaly and time:
            f_rows = f_rows[['Anomaly','Time']]
            # loop through the trigger anomaly name and the time:
            for loc_trig_anom, t in f_rows.itertuples(index=False):
                y['TRIGGER_ANOMALY'].append(loc_trig_anom)
                y['FEEDER'].append(feeder)
                y['TIMESTAMP'].append(t)
                if tickets is not None:
                    if f_tickets.empty:
                        delta = np.nan
                        ticket_no = np.nan
                    elif f_tickets.POWEROFF.max() > t - HOUR:
                        delta = f_tickets.loc[f_tickets.POWEROFF > t - HOUR]
                        ticket_no = delta.loc[delta.POWEROFF == delta.POWEROFF.min(),
                                              'DW_TCKT_KEY'].values[0]
                        delta = delta.POWEROFF.min() - t
                        delta = delta.total_seconds() / (60. * 60.)
                    else:
                        delta = np.nan
                        ticket_no = np.nan
                    y['OUTAGE'].append(delta)
                    y['TICKET'].append(ticket_no)

                for col in dataset_config:

                    # These variables' utility was found to be
                    # questionable, and so are manually set to all possible
                    # combinations of their boolean values. We set to zero
                    # for now, and manually override at prediction time.
                    deprecated = {'FDR_GEO_0', 'FDR_GEO_1', 'FDR_GEO_2',
                                  'IS_DADE', 'SIG_OUTLIER'}

                    if col['type'] in {'trigger', 'flag', 'background',
                                       'cluster', 'sequence'}:
                        # These require lookups on the anomaly table
                        c_name = col['lookup']
                        min_lag = t - pd.Timedelta(hours=col['min_lag'])
                        max_lag = t - pd.Timedelta(hours=col['max_lag'])

                        c_anoms = f_anoms.loc[(f_anoms.Anomaly == c_name) &
                                              (f_anoms.Time <= min_lag) &
                                              (f_anoms.Time > max_lag)]
                        if c_anoms.empty:
                            X[col['name']].append(0)
                            continue
                        if col['type'] in {'trigger', 'background'}:
                            X[col['name']].append(c_anoms.shape[0])
                        elif col['type'] == 'cluster':
                            clusters = (c_anoms.Time.diff() > HOUR).sum() + 1
                            X[col['name']].append(clusters)
                        elif col['type'] == 'sequence':
                            X[col['name']].append((t - c_anoms.Time).dt.days.tolist())
                        else:
                            X[col['name']].append(1)

                    elif col['type'] == 'special':
                        # Special variables with named logic
                        if col['name'] in deprecated:
                            X[col['name']].append(0)
                        elif 'TRIPLE_THREAT' in col['name']:
                            c_anoms = _triple_threat(f_anoms)
                            min_lag = t - pd.Timedelta(hours=col['min_lag'])
                            max_lag = t - pd.Timedelta(hours=col['max_lag'])
                            c_anoms = c_anoms.loc[(c_anoms.Time <= min_lag) &
                                                  (c_anoms.Time > max_lag)]
                            X[col['name']].append(c_anoms.shape[0])
                        else:
                            raise ValueError('Unknown special column %s.' %
                                             col['name'])

                    elif col['type'] in {'duration', 'constant'}:
                        # These are more efficient to map once dataset is
                        # built
                        X[col['name']].append(0)

                    else:
                        raise ValueError('Unknown column type %s.' %
                                         col['type'])

    X = pd.DataFrame(X)
    X = X[col_order]
    y = pd.DataFrame(y)

    for col in constants:
        const_mapper = {f: feeder_df.loc[f, col] for f in feeder_df.index}
        X[col] = y.FEEDER.map(const_mapper)

    ns_to_years = 1e9 * 60 * 60 * 24 * 365.

    for col in durations:
        # Calculate the number of years since an event
        const_mapper = {f: feeder_df.loc[f, col] for f in feeder_df.index}
        X[col] = y.FEEDER.map(const_mapper)
        if not X.empty and not y.empty:
            X[col] = y.TIMESTAMP - X[col]
            X[col] = X[col].map(lambda t: np.floor(int(t) / ns_to_years))

    return X, y


def _parallel_target(X, y, tickets, feeders, max_lookahead, max_lookback,
                     outage_name, ticket_name):
    # Subset datasets for this chunk
    X = X.loc[y.FEEDER.isin(feeders)].copy()
    y = y.loc[y.FEEDER.isin(feeders)].copy()
    tickets = tickets.loc[tickets.FDR_NUM.isin(feeders)].copy()

    for ticket in tickets.DW_TCKT_KEY.unique():

        poweroff = tickets.loc[tickets.DW_TCKT_KEY == ticket, 'POWEROFF'].tolist()[0]
        feeder = tickets.loc[tickets.DW_TCKT_KEY == ticket, 'FDR_NUM'].values[0]

        min_time = poweroff - max_lookahead * HOUR
        max_time = poweroff + max_lookback * HOUR

        target_times = y.loc[(y.TIMESTAMP > min_time) &
                             (y.TIMESTAMP < max_time) &
                             (y.FEEDER == feeder), 'TIMESTAMP'].tolist()

        for t in target_times:
            delta = (poweroff - t).total_seconds() / (60. * 60.)
            if (y.loc[(y.TIMESTAMP == t) &
                      (y.FEEDER == feeder), outage_name] < delta).any():
                continue
            y.loc[(y.TIMESTAMP == t) &
                  (y.FEEDER == feeder), outage_name] = delta
            y.loc[(y.TIMESTAMP == t) &
                  (y.FEEDER == feeder), ticket_name] = ticket

    return X, y


class SignatureTransformer(object):

    """Generates signatures from anomalies.

    Parameters
    ----------
    dataset_config : dict
        The configuration of the dataset. For example, please see
        ``pam_1_0_dataset.yml``.

    anomaly_map : dict
        The mapping of anomaly names from the :mod:`autogrid.pam.anomaly` module
        to those listed in `dataset_config`. Used to combine similar anomalies
        that come from different sources. For example, please see
        ``pam_1_0_anomaly_map.yml``.

    feeder_df : Pandas DataFrame
        A DataFrame with an index column of feeder IDs, and a column named
        "CUSTOMERS" with a count of the number of customers on the feeder. Also
        columns for each "constant" or "duration" variables required.

    n_jobs : int, optional (default=1)
        The number of cores to parallelize over.

    Attributes
    ----------
    feeder_ignore : set
        Feeder IDs that will be ignored based on having either zero length or
        zero customers.

    X : Pandas DataFrame
        A DataFrame with the anomaly signatures.

    y : Pandas DataFrame
        A DataFrame with the Feeder ID and timestamp associated with each
        signature in ``X``. It is in same row order as ``X``.
    """

    def __init__(self, dataset_config, anomaly_map, feeder_df, n_jobs=1):

        self.dataset_config = dataset_config
        self.anomaly_map = anomaly_map
        self.feeder_df = feeder_df
        if n_jobs < 0:
            self.n_jobs = max(cpu_count() + 1 + n_jobs, 1)
        elif n_jobs == 0:
            raise ValueError('Parameter n_jobs == 0 has no meaning.')
        else:
            self.n_jobs = n_jobs
        low_cust = feeder_df.loc[feeder_df.CUSTOMERS < 100].index.values
        zero_len = feeder_df.loc[(feeder_df.FDR_OH == 0) &
                                 (feeder_df.FDR_UG == 0)].index.values
        self.feeder_ignore = set(list(low_cust) + list(zero_len))

    def _clean_data(self, anomalies):
        """Private function to clean up anomalies dataset.

        Parameters
        ----------
        anomalies : pd.DataFrame
            DataFrame containing one row per anomaly. Same format as the output
            of the anomaly module.
        """
        anomalies.Time = [t.replace(second=0, microsecond=0) for t in anomalies.Time]

        # Remove zero-length or low-customer feeders
        anomalies = anomalies.loc[-anomalies.Feeder.isin(self.feeder_ignore)]
        # Remove feeders without available meta-data
        good_feeders = self.feeder_df.index.values
        anomalies = anomalies.loc[anomalies.Feeder.isin(good_feeders)]

        # Rename anomalies for use with configuration file
        good_anomalies = self.anomaly_map.keys()
        anomalies = anomalies.loc[anomalies.Anomaly.isin(good_anomalies)]
        anomalies.Anomaly = anomalies.Anomaly.map(self.anomaly_map)

        # Aggregate faults if required
        if [c['name'] for c in self.dataset_config if 'PH_FAULT' in c['name']]:
            faults = anomalies.loc[anomalies.Anomaly.str.contains('FAULT')].copy()
            faults = faults.very_deep_copy()
            faults['Key'] = faults.Feeder + faults.Time.dt.strftime('.%Y-%m-%dT%H:%M.') + faults.Anomaly
            faults.Key = faults.Key.str.replace('TEMP_', '')
            groupby = faults.loc[faults.DevicePh.isin(['A', 'B', 'C'])].groupby('Key')
            faults = groupby.Time.unique().map(lambda x: x[0]).dt.tz_localize(UTC).to_frame()
            faults['Anomaly'] = groupby.DevicePh.nunique().astype(str) + '_PH_' + groupby.Anomaly.unique()
            faults.Anomaly = faults.Anomaly.map(lambda x: x[0].replace('TEMP_', ''))
            faults['DeviceId'] = groupby.DeviceId.unique().map(lambda x: ', '.join(x))
            faults['DevicePh'] = groupby.DevicePh.unique().map(lambda x: ', '.join(x))
            faults['DeviceType'] = groupby.DeviceType.unique().map(lambda x: ', '.join(x))
            faults['Feeder'] = groupby.Feeder.unique().map(lambda x: ', '.join(x))
            faults['Signal'] = groupby.Signal.unique().map(lambda x: ', '.join(x))
            faults = faults.reset_index(drop=True)

            g_faults = anomalies.loc[(anomalies.Anomaly.str.contains('FAULT')) &
                                     (anomalies.DevicePh == 'G')].copy()
            g_faults = g_faults.very_deep_copy()
            g_faults.Anomaly = 'G_PH_' + g_faults.Anomaly.str.replace('TEMP_', '')
            g_faults.Time = pd.to_datetime(g_faults.Time)

            self.faults, self.g_faults = faults, g_faults
        else:
            faults, g_faults = pd.DataFrame(), pd.DataFrame()

        # Drop duplicates where required
        drop_dupes = [c['lookup'] for c in self.dataset_config if
                      'keep_all' in c and c['keep_all'] is False]
        drop_dupes = anomalies.loc[anomalies.Anomaly.isin(drop_dupes)]
        drop_dupes = drop_dupes.drop_duplicates(['Anomaly', 'Feeder', 'Time'])
        keep_dupes = [c['lookup'] for c in self.dataset_config if
                      'keep_all' in c and c['keep_all'] is True]
        keep_dupes = anomalies.loc[anomalies.Anomaly.isin(keep_dupes)]

        anomalies = []
        if not drop_dupes.empty:
            anomalies.append(drop_dupes)
        if not keep_dupes.empty:
            anomalies.append(keep_dupes)
        if not faults.empty:
            anomalies.append(faults)
        if not g_faults.empty:
            anomalies.append(g_faults)
        if anomalies:
            anomalies = pd.concat(anomalies, ignore_index=True)
        else:
            anomalies = pd.DataFrame()

        #anomalies = anomalies.dropna()

        return anomalies

    def transform(self, anomalies, tickets=None):
        """Compute signatures for the specified feeder.

        Parameters
        ----------
        anomalies : pd.DataFrame
            DataFrame containing one row per anomaly. Same format as the output
            of the anomaly module. These are the new, previously unprocessed
            anomalies.

        tickets : pd.DataFrame or None, optional (default=None)
            DataFrame containing ticket data. This should be restricted to only
            the tickets of interest, ie. any cleaning should be done prior to
            running this method.
        """
        anomalies = self._clean_data(anomalies.copy())
        self.anomalies = anomalies

        feeder_groups = dict.fromkeys(range(self.n_jobs))
        for group in feeder_groups:
            feeder_groups[group] = []
        for i, f in enumerate(anomalies.Feeder.value_counts().index.values):
            group = (i % (self.n_jobs * 2)) - self.n_jobs
            if group < 0:
                group = abs(group) - 1
            feeder_groups[group].append(f)
        for group in feeder_groups:
            shuffle(feeder_groups[group])

        # Kick off parallel jobs
        print 'Launching', self.n_jobs, 'parallel jobs!'
        all_results = Parallel(n_jobs=self.n_jobs, verbose=1)(
            delayed(_parallel_transform)(anomalies,
                                         tickets,
                                         self.dataset_config,
                                         self.feeder_df,
                                         feeder_groups[i])
            for i in range(self.n_jobs))

        self.X = pd.concat([X[0] for X in all_results if not X[0].empty],
                           ignore_index=True)
        self.y = pd.concat([y[1] for y in all_results if not y[1].empty],
                           ignore_index=True)

        return self

    def add_target(self, tickets, max_lookahead, max_lookback,
                   outage_name='OUTAGE', ticket_name='TICKET'):
        """Compute new targets for pre-transformed signatures.

        Parameters
        ----------
        tickets : pd.DataFrame or None, optional (default=None)
            DataFrame containing ticket data. This should be restricted to only
            the tickets of interest, ie. any cleaning should be done prior to
            running this method.

        max_lookahead : float
            The maximum value that could appear in the OUTAGE column. Smaller
            values will run faster.

        max_lookback : float
            The maximum negative value that could appear in the OUTAGE column.
            This represents the time after the outage and can be used to screen
            out observations related to restoration activities.

        outage_name : str (default='OUTAGE')
            The name of the column containing the time to outage values. The
            default will overwrite any previous values.

        ticket_name : str (default='TICKET')
            The name of the column containing the ticket ID values. The default
            will overwrite any previous values.
        """
        if not hasattr(self, 'y'):
            raise ValueError('Run `transform` before adding new targets.')

        self.y[outage_name] = np.nan
        self.y[ticket_name] = np.nan

        feeder_groups = dict.fromkeys(range(self.n_jobs))
        for group in feeder_groups:
            feeder_groups[group] = []
        for i, f in enumerate(tickets.FDR_NUM.value_counts().index.values):
            group = (i % (self.n_jobs * 2)) - self.n_jobs
            if group < 0:
                group = abs(group) - 1
            feeder_groups[group].append(f)
        for group in feeder_groups:
            shuffle(feeder_groups[group])

        notix_feeders = self.y.loc[-self.y.FEEDER.isin(tickets.FDR_NUM.unique()),
                                   'FEEDER'].unique()
        Xs, ys = [], []
        if len(notix_feeders) > 0:
            Xs.append(self.X.loc[self.y.FEEDER.isin(notix_feeders)].copy())
            ys.append(self.y.loc[self.y.FEEDER.isin(notix_feeders)].copy())

        # Kick off parallel jobs
        print 'Launching', self.n_jobs, 'parallel jobs!'
        all_results = Parallel(n_jobs=self.n_jobs, verbose=1)(
            delayed(_parallel_target)(self.X,
                                      self.y,
                                      tickets,
                                      feeder_groups[i],
                                      max_lookahead,
                                      max_lookback,
                                      outage_name,
                                      ticket_name)
            for i in range(self.n_jobs))

        for X, y in all_results:
            if not y.empty:
                Xs.append(X)
                ys.append(y)

        self.X = pd.concat(Xs, ignore_index=True)
        self.y = pd.concat(ys, ignore_index=True)

        return self
