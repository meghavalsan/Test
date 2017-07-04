"""Anomaly extraction module.

The :mod:`autogrid.pam.anomaly` module implements anomaly extraction for
time-series data.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Trevor Stephens' <trevor.stephens@auto-grid.com>


import pandas as pd
import numpy as np
import re


DAY = pd.Timedelta(days=1)


class _AnomalyBlob(object):

    """A collection of anomalies.

    This is the base-class for all anomalies and should not be accessed
    directly, use the derived classes for specific anomaly logic.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    anomalies : list, 'all', or 'default', optional (default='default')
        The anomalies to extract from ``df``. If 'default', all default
        anomalies will be extracted. If 'all', all available anomalies will be
        extracted.

    Attributes
    ----------
    anomaly_names : list
        The names of the anomalies.

    anomaly_times : list
        The timestamps of the anomalies extracted from ``df``.

    source_signal : list
        The signals from which the ``anomalies`` were parsed.
    """

    def __init__(self, feeder_id, anomalies='default'):
        self.feeder_id = feeder_id
        if anomalies == 'default':
            self.anomalies = self.default_anomalies
        elif anomalies == 'all':
            self.anomalies = self.all_anomalies
        else:
            self.anomalies = anomalies
        # Allocate member variables to hold anomalies
        self.anomaly_names = []
        self.anomaly_times = []
        self.source_signal = []
        self.device_type = []
        self.device_id = []
        self.device_ph = []

    @staticmethod
    def _check_df(df, expected_cols):
        """Check data structure of ``df`` is valid.

        Parameters
        ----------
        df : Pandas DataFrame
            The time-series data.

        expected_cols : list
            The expected columns for ``df``.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("`df` must be a Pandas DataFrame")
        missing = [col for col in expected_cols if col not in df.columns]
        if missing:
            raise ValueError("%s missing from `df.columns`" % missing)

    def _check_anomalies(self):
        """Check requested anomalies are valid."""
        extra = [a for a in self.anomalies if a not in self.all_anomalies]
        if extra:
            raise ValueError("unknown anomaly: %s" % extra)

    def to_df(self):
        """Convert ``_AnomalyBlob`` into a Pandas DataFrame."""
        try:
            df = {'Feeder': [self.feeder_id] * len(self),
                  'Anomaly': self.anomaly_names,
                  'Time': self.anomaly_times,
                  'Signal': self.source_signal,
                  'DeviceType': self.device_type,
                  'DeviceId': self.device_id,
                  'DevicePh': self.device_ph}
        except AttributeError:
            raise ValueError("Run `extract` before `to_df`.")
        return pd.DataFrame(df)

    def __len__(self):
        return len(self.anomaly_times)

    @property
    def all_anomalies(self):
        """A list of all possible anomalies that can be extracted."""
        return None

    @property
    def default_anomalies(self):
        """A list of the default anomalies that will be extracted."""
        return None


class ScadaAnomalies(_AnomalyBlob):

    """A collection of anomalies.

    This is used to extract all SCADA-based anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    anomalies : list, 'all', or 'default', optional (default='default')
        The anomalies to extract from ``df``. If 'default', all default
        anomalies will be extracted. If 'all', all available anomalies will be
        extracted.

    Attributes
    ----------
    anomaly_names : list
        The names of the anomalies.

    anomaly_times : list
        The timestamps of the anomalies extracted from ``df``.

    source_signal : list
        The signals from which the ``anomalies`` were parsed.
    """

    def _store(self, anoms, label):
        """Helper function to add new anomalies to object.

        Parameters
        ----------
        anoms : Pandas DataFrame
            Condensed DataFrame containing one row per anomaly.

        label : str or list
            The name of the anomaly. If str, the anomaly name will be repeated.
            If list, the list will be used.
        """
        if isinstance(label, str):
            anomaly_names = [label] * anoms.shape[0]
        else:
            anomaly_names = list(label)
        self.anomaly_names.extend(anomaly_names)
        self.anomaly_times.extend(anoms.localTime.tolist())
        self.source_signal.extend(list(anoms.OBSERV_DATA.values))
        self.device_type.extend(list(anoms.devType.values))
        self.device_id.extend(list(anoms.devId.values))
        self.device_ph.extend(list(anoms.devPh.values))

    def extract(self, df, start_time=None, end_time=None):
        """Begin anomaly extraction on ``df``.

        Parameters
        ----------
        df : Pandas DataFrame
            The time-series data.

        start_time : datetime.datetime or None, optional (default=None)
            The time to begin extracting anomalies. If None, the entire
            time-series will be used.

        end_time : datetime.datetime or None, optional (default=None)
            The time to stop extracting anomalies. If None, the entire
            time-series will be used.
        """
        scada_cols = ['feederNumber', 'OBSERV_DATA', 'localTime']
        self._check_df(df, scada_cols)
        #self._check_anomalies()

        if start_time is not None:
            df = df.loc[df.localTime >= start_time]
        if end_time is not None:
            df = df.loc[df.localTime <= end_time]
        df = df.loc[df.feederNumber == self.feeder_id]
        df = df.drop_duplicates(subset=scada_cols)

        def get_ith(x, i):
            """Get the ``i``th space delimited section of alarm string ``x``."""
            try:
                parsed = x.split(' ')[i]
            except IndexError:
                return '-'
            return parsed

        df['devType'] = df.OBSERV_DATA.map(lambda x: get_ith(x, 1))
        df['devId'] = df.OBSERV_DATA.map(lambda x: get_ith(x, 2))
        df['devPh'] = df.OBSERV_DATA.map(lambda x: get_ith(x, 3))

        if set(self.anomalies) & {'BKR_OPEN', 'BKR_CLOSE',
                                  'BKR_FAIL_TO_OPR', 'FC_NO_BO'}:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains("FEED")) &
                            (df.OBSERV_DATA.str.contains("BKR")) &
                            (-df.OBSERV_DATA.str.contains("Composite")) &
                            (-df.OBSERV_DATA.str.contains("STATUS")) &
                            (-df.OBSERV_DATA.str.contains("DEFINITION")) &
                            (-df.OBSERV_DATA.str.contains("CTRL")) &
                            (-df.OBSERV_DATA.str.contains("OVERRIDDEN")) &
                            (-df.OBSERV_DATA.str.contains("has experienced")) &
                            (-df.OBSERV_DATA.str.contains("Comments:")) &
                            (-df.OBSERV_DATA.str.contains("ISD POINT")) &
                            (-df.OBSERV_DATA.str.contains("operation"))].copy()

            def breaker_parser(x):
                """Get the open/close combinations from the alarm string."""
                try:
                    parsed = x.split(' ')[4]
                except IndexError:
                    return 'UNKNOWN'
                return parsed.replace('D', '').replace('-', '_')

            df_sub['Anomaly'] = df_sub.OBSERV_DATA.map(breaker_parser)
            df_sub['devPh'] = '-'

            fc_bo = []

            for s in df_sub.Anomaly.unique():
                df_sub2 = df_sub.loc[df_sub.Anomaly == s]
                if 'OPEN' in s:
                    self._store(df_sub2, 'BKR_OPEN')
                    fc_bo.extend(df_sub2.localTime.tolist())
                if 'CLOSE' in s:
                    self._store(df_sub2, 'BKR_CLOSE')
                if s == 'OPEN_CLOSE_OPEN':
                    self._store(df_sub2, 'BKR_OPEN')
                if s == 'CLOSE_OPEN_CLOSE':
                    self._store(df_sub2, 'BKR_CLOSE')
                if s == 'FAIL_TO_OPR':
                    self._store(df_sub2, 'BKR_FAIL_TO_OPR')

        if 'FAULT_ALARM' in self.anomalies:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains(" FAULT ")) &
                            (df.OBSERV_DATA.str.contains(" ALARM")) &
                            (-df.OBSERV_DATA.str.contains(" ANALOG ")) &
                            (-df.OBSERV_DATA.str.contains(" STATUS "))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[:1])

            self._store(df_sub, 'FAULT_ALARM')

        if set(self.anomalies) & {'FAULT_CURRENT', 'TEMP_FAULT_CURRENT',
                                  'FC_NO_BO'}:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains("LIM-HIGH"))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[1:2] if x != "FAMP" else "-")

            def fault_parser(x):
                """Get the magnitude of the fault from the alarm string."""
                try:
                    parsed = x.split(' ')[5]
                except IndexError:
                    return 0.
                try:
                    parsed = float(parsed)
                except ValueError:
                    return 0.
                return parsed

            df_sub['Size'] = df_sub.OBSERV_DATA.map(fault_parser)
            df_sub = df_sub.loc[df_sub.Size > 1.]
            df_sub['Anomaly'] = 'FAULT_CURRENT'
            # Temp fault current is defined by being less than 900 amps
            df_sub.loc[df_sub.Size < 900, 'Anomaly'] = 'TEMP_FAULT_CURRENT'

            self._store(df_sub, df_sub.Anomaly.values)
            faults = df_sub.loc[df_sub.Size >= 900].localTime.tolist()

        if 'FC_NO_BO' in self.anomalies:

            fc_bo = np.array(fc_bo)
            fc_no_bo = []
            for f in faults:
                fc_bo2 = fc_bo[(fc_bo > (f - pd.Timedelta(minutes=1))) &
                               (fc_bo < (f + pd.Timedelta(minutes=2)))]
                if fc_bo2.shape[0] == 0:
                    fc_no_bo.append(f)

            self.anomaly_names.extend(['FC_NO_BO'] * len(fc_no_bo))
            self.anomaly_times.extend(fc_no_bo)
            self.source_signal.extend(['-'] * len(fc_no_bo))
            self.device_type.extend(['-'] * len(fc_no_bo))
            self.device_id.extend(['-'] * len(fc_no_bo))
            self.device_ph.extend(['-'] * len(fc_no_bo))

        if 'CURRENT_LIMIT' in self.anomalies:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains("AMP LIM-1 HIGH"))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[0])

            self._store(df_sub, 'CURRENT_LIMIT')

        if set(self.anomalies) & {'FDRHD_DE_ENERGIZED', 'FDRHD_ENERGIZED'}:

            for s in ['ENERGIZED', 'DE-ENERGIZED']:

                df_sub = df.loc[(df.OBSERV_DATA.str.contains(" FDRHD ")) &
                                (df.OBSERV_DATA.str.contains('ENGZ ' + s))].copy()

                df_sub['devPh'] = '-'

                self._store(df_sub, 'FDRHD_' + s.replace('-', '_'))

        if 'HIGH_VOLTAGE' in self.anomalies:

            df_sub = df.loc[((df.OBSERV_DATA.str.contains("VLT LIM")) |
                             (df.OBSERV_DATA.str.contains("VT LIM"))) &
                            (df.OBSERV_DATA.str.contains("HIGH")) &
                            (-df.OBSERV_DATA.str.contains(" LOW ")) &
                            (-df.OBSERV_DATA.str.contains("LIMIT"))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[1:2] if x[0] == "L" else x[0])

            def voltage_parser(x):
                """Get the magnitude of the voltage from the alarm string."""
                try:
                    parsed = x.split(' ')[6]
                except IndexError:
                    return 0.
                try:
                    parsed = float(parsed)
                except ValueError:
                    return 0.
                return parsed

            df_sub['PARSED'] = df_sub.OBSERV_DATA.map(voltage_parser)
            df_sub = df_sub.loc[(df_sub.PARSED < 1000.) &
                                (df_sub.PARSED >= 130.)]

            self._store(df_sub, 'HIGH_VOLTAGE')

        if 'INTELI_PH_ALARM' in self.anomalies:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains(" INTELI ")) &
                            (df.OBSERV_DATA.str.contains('PH ALARM'))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[0])

            self._store(df_sub, 'INTELI_PH_ALARM')

        if set(self.anomalies) & {'INTELI_OPS_DSW_CLOSE', 'INTELI_OPS_DSW_OPEN'}:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains(" INTELI ")) &
                            (df.OBSERV_DATA.str.contains("DSW")) &
                            (-df.OBSERV_DATA.str.contains("MAINT")) &
                            (-df.OBSERV_DATA.str.contains("CTRL")) &
                            (-df.OBSERV_DATA.str.contains("DEFINITION")) &
                            (-df.OBSERV_DATA.str.contains("STATUS")) &
                            (-df.OBSERV_DATA.str.contains("ABLED")) &
                            (-df.OBSERV_DATA.str.contains("INHIBITED"))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[-1] if len(x) == 4 else "-")

            for s in ['OPEN', 'CLOSE']:

                df_sub2 = df_sub.loc[df_sub.OBSERV_DATA.str.contains(s)].copy()

                self._store(df_sub2, 'INTELI_OPS_DSW_' + s)

        if 'REGULATOR_BLOCK' in self.anomalies:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains(" FDRHD ")) &
                            (df.OBSERV_DATA.str.contains(" REGU ")) &
                            (df.OBSERV_DATA.str.contains("BLOCK")) &
                            (-df.OBSERV_DATA.str.contains(" NORMAL")) &
                            (-df.OBSERV_DATA.str.contains(" STATUS ")) &
                            (-df.OBSERV_DATA.str.contains(" CTRL "))].copy()

            df_sub['devPh'] = '-'

            self._store(df_sub, 'REGULATOR_BLOCK')

        if set(self.anomalies) & {'RELAY_ALARM', 'RELAY_TRIP'}:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains(" RELAY ")) &
                            (-df.OBSERV_DATA.str.contains("NORMAL")) &
                            (-df.OBSERV_DATA.str.contains("STATUS"))]
            for s in ['ALARM', 'TRIP']:
                df_sub2 = df_sub.loc[df_sub.OBSERV_DATA.str.contains(s)]

                df_sub2['devPh'] = '-'

                self._store(df_sub, 'RELAY_' + s)

        if 'VOLTAGE_DROP' in self.anomalies:

            df_sub = df.loc[(df.OBSERV_DATA.str.contains("FORBDN"))].copy()

            df_sub.devPh = df_sub.devPh.map(lambda x: x[1:2] if x[0] == "L" else x[0])

            self._store(df_sub, 'VOLTAGE_DROP')

        return self

    @property
    def all_anomalies(self):
        """A list of all possible anomalies that can be extracted."""
        return {'BKR_CLOSE', 'BKR_FAIL_TO_OPR', 'BKR_OPEN', 'CURRENT_LIMIT',
                'FAULT_ALARM', 'FAULT_CURRENT', 'FC_NO_BO',
                'FDRHD_DE_ENERGIZED', 'FDRHD_ENERGIZED', 'HIGH_VOLTAGE',
                'INTELI_PH_ALARM', 'INTELI_OPS_DSW_CLOSE',
                'INTELI_OPS_DSW_OPEN', 'REGULATOR_BLOCK', 'RELAY_ALARM',
                'RELAY_TRIP', 'TEMP_FAULT_CURRENT', 'VOLTAGE_DROP'}

    @property
    def default_anomalies(self):
        """A list of the default anomalies that will be extracted."""
        return {'BKR_CLOSE', 'BKR_FAIL_TO_OPR', 'BKR_OPEN', 'CURRENT_LIMIT',
                'FAULT_ALARM', 'FAULT_CURRENT', 'FC_NO_BO',
                'FDRHD_DE_ENERGIZED', 'FDRHD_ENERGIZED', 'HIGH_VOLTAGE',
                'INTELI_PH_ALARM', 'INTELI_OPS_DSW_CLOSE',
                'INTELI_OPS_DSW_OPEN', 'REGULATOR_BLOCK', 'RELAY_ALARM',
                'RELAY_TRIP', 'TEMP_FAULT_CURRENT', 'VOLTAGE_DROP'}


class TicketAnomalies(_AnomalyBlob):

    """A collection of anomalies.

    This is used to extract all ticket-based anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    anomalies : list, 'all', or 'default', optional (default='default')
        The anomalies to extract from ``df``. If 'default', all default
        anomalies will be extracted. If 'all', all available anomalies will be
        extracted.

    Attributes
    ----------
    anomaly_names : list
        The names of the anomalies.

    anomaly_times : list
        The timestamps of the anomalies extracted from ``df``.

    source_signal : list
        The signals from which the ``anomalies`` were parsed.
    """

    def extract(self, df, start_time=None, end_time=None):
        """Begin anomaly extraction on ``df``.

        Parameters
        ----------
        df : Pandas DataFrame
            The time-series data.

        start_time : datetime.datetime or None, optional (default=None)
            The time to begin extracting anomalies. If None, the entire
            time-series will be used.

        end_time : datetime.datetime or None, optional (default=None)
            The time to stop extracting anomalies. If None, the entire
            time-series will be used.
        """
        ticket_cols = ['DW_TCKT_KEY', 'FDR_NUM', 'POWEROFF', 'POWERRESTORE',
                       'IRPT_TYPE_CODE', 'RPR_ACTN_TYPE']
        self._check_df(df, ticket_cols)
        #self._check_anomalies()

        df = df.loc[df.FDR_NUM == self.feeder_id]
        df = df.loc[df.IRPT_TYPE_CODE.isin(['OCR', 'LAT', 'FDR'])]

        if 'RE_FUSE_ONLY' in self.anomalies:

            refuse = df.DW_TCKT_KEY.value_counts()
            refuse = refuse[refuse == 1].index.values
            refuse = df.loc[(df.DW_TCKT_KEY.isin(refuse)) &
                            (df.RPR_ACTN_TYPE.str.contains('Refuse'))]
            refuse = refuse[['DW_TCKT_KEY', 'POWERRESTORE']]
            refuse = refuse.sort_values(by='DW_TCKT_KEY').drop_duplicates()

            if start_time is not None:
                refuse = refuse.loc[refuse.POWERRESTORE >= start_time]
            if end_time is not None:
                refuse = refuse.loc[refuse.POWERRESTORE <= end_time]

            self.anomaly_names.extend(['RE_FUSE_ONLY'] * refuse.shape[0])
            self.anomaly_times.extend(list(refuse.POWERRESTORE.tolist()))
            self.source_signal.extend(list(refuse.DW_TCKT_KEY.values.astype(str)))
            self.device_type.extend(['TICKETS'] * refuse.shape[0])
            self.device_id.extend(['-'] * refuse.shape[0])
            self.device_ph.extend(['-'] * refuse.shape[0])

        if 'LATERAL_OUTAGES' in self.anomalies:

            lats = df.loc[(df.IRPT_TYPE_CODE.isin(['OCR', 'LAT']))]
            lats = lats[['DW_TCKT_KEY', 'POWEROFF']]
            lats = lats.sort_values(by='DW_TCKT_KEY').drop_duplicates()

            if start_time is not None:
                lats = lats.loc[lats.POWEROFF >= start_time]
            if end_time is not None:
                lats = lats.loc[lats.POWEROFF <= end_time]

            self.anomaly_names.extend(['LATERAL_OUTAGES'] * lats.shape[0])
            self.anomaly_times.extend(list(lats.POWEROFF.tolist()))
            self.source_signal.extend(list(lats.DW_TCKT_KEY.values.astype(str)))
            self.device_type.extend(['TICKETS'] * lats.shape[0])
            self.device_id.extend(['-'] * lats.shape[0])
            self.device_ph.extend(['-'] * lats.shape[0])

        return self

    @property
    def all_anomalies(self):
        """A list of all possible anomalies that can be extracted."""
        return {'RE_FUSE_ONLY', 'LATERAL_OUTAGES'}

    @property
    def default_anomalies(self):
        """A list of the default anomalies that will be extracted."""
        return {'RE_FUSE_ONLY', 'LATERAL_OUTAGES'}


class AmiAnomalies(_AnomalyBlob):

    """A collection of anomalies.

    This is used to extract all AMI-based anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    anomalies : list, 'all', or 'default', optional (default='default')
        The anomalies to extract from ``df``. If 'default', all default
        anomalies will be extracted. If 'all', all available anomalies will be
        extracted.

    Attributes
    ----------
    anomaly_names : list
        The names of the anomalies.

    anomaly_times : list
        The timestamps of the anomalies extracted from ``df``.

    source_signal : list
        The signals from which the ``anomalies`` were parsed.
    """

    def extract(self, df, customers, start_time=None, end_time=None):
        """Begin anomaly extraction on ``df``.

        Parameters
        ----------
        df : Pandas DataFrame
            The time-series data.

        customers : int
            The number of customers on the feeder.

        start_time : datetime.datetime or None, optional (default=None)
            The time to begin extracting anomalies. If None, the entire
            time-series will be used.

        end_time : datetime.datetime or None, optional (default=None)
            The time to stop extracting anomalies. If None, the entire
            time-series will be used.
        """
        ami_cols = ['fdr_num', 'ami_dvc_name', 'mtr_evnt_tmstmp', 'mtr_evnt_id']
        self._check_df(df, ami_cols)
        #self._check_anomalies()

        df = df.loc[df.fdr_num == self.feeder_id]

        if 'LG_PD_10' in self.anomalies:

            ami_sent = {}
            lg_pd_10 = []
            signals = []

            if start_time is not None:
                df = df.loc[df.mtr_evnt_tmstmp >= start_time]
            if end_time is not None:
                df = df.loc[df.mtr_evnt_tmstmp <= end_time]
            df = df.drop_duplicates(subset=ami_cols)

            # Last Gasp (12007), NIC Power Down (12024)
            ami = df.loc[df.mtr_evnt_id.isin({'12007', '12024'})].copy()
            # Meter starts with 'G'=GE, with 'L'=L&G
            ami = ami.loc[ami.ami_dvc_name.str.startswith('G')]

            def strip_seconds(t):
                """Reduce granularity of timestamps to 1 minute."""
                return t.replace(second=0).replace(microsecond=0)

            ami = ami.set_index('mtr_evnt_tmstmp')
            ami.index = ami.index.map(strip_seconds)

            for t in ami.index.unique():
                meters = ami[ami.index == t].ami_dvc_name.unique()
                if len(meters) > 1:
                    ami_sent[t] = list(meters)

            gasps = np.array(sorted(ami_sent.keys()))
            for t in gasps:
                nearby_gasps = gasps[(gasps >= t) &
                                     (gasps <= t + pd.Timedelta(minutes=5))]
                gasp_meters = []
                for t2 in nearby_gasps:
                    gasp_meters.extend(ami_sent[t2])
                gasp_count = len(np.unique(gasp_meters))
                gasp_pct = gasp_count / float(customers)

                if nearby_gasps.max() not in lg_pd_10 and gasp_pct > 0.1:
                    lg_pd_10.append(nearby_gasps.max())
                    signals.append('LAST GASPS / POWER DOWNS AT %.1f%% OF FEEDER '
                                   'CUSTOMERS (%d METERS)' % (100 * gasp_pct,
                                                              gasp_count))

            lg_pd_10 = np.array(lg_pd_10)
            signals = np.array(signals)
            if start_time is not None:
                signals = signals[lg_pd_10 >= start_time]
                lg_pd_10 = lg_pd_10[lg_pd_10 >= start_time]
            if end_time is not None:
                signals = signals[lg_pd_10 <= end_time]
                lg_pd_10 = lg_pd_10[lg_pd_10 <= end_time]

            self.anomaly_names.extend(['LG_PD_10'] * len(lg_pd_10))
            self.anomaly_times.extend(list(lg_pd_10))
            self.source_signal.extend(list(signals))
            self.device_type.extend(['AMI'] * len(lg_pd_10))
            self.device_id.extend(['-'] * len(lg_pd_10))
            self.device_ph.extend(['-'] * len(lg_pd_10))

        if 'LG_PD_10_V2' in self.anomalies:

            if start_time is not None:
                df = df.loc[df.mtr_evnt_tmstmp >= start_time]
            if end_time is not None:
                df = df.loc[df.mtr_evnt_tmstmp <= end_time]

            # Last Gasp (12007), NIC Power Down (12024)
            df = df.loc[df.mtr_evnt_id.isin({'12007', '12024'})].copy()
            # Meter starts with 'G'=GE, with 'L'=L&G
            df = df.loc[df.ami_dvc_name.str.startswith('G')]

            def strip_seconds(t):
                """Reduce granularity of timestamps to 1 minute."""
                return t.replace(second=0).replace(microsecond=0)

            if not df.empty:
                df.mtr_evnt_tmstmp = [strip_seconds(t) for t in df.mtr_evnt_tmstmp]

                df = df.groupby(by='mtr_evnt_tmstmp').ami_dvc_name.nunique()
                df = df.loc[df > customers * 0.1]

                signals = 'LAST GAPS / POWER DOWNS AT %.1f%% OF FEEDER ' \
                          'CUSTOMERS (%d METERS)'
                signals = [signals % (100. * i / customers, i) for i in df.values]

                self.anomaly_names.extend(['LG_PD_10_V2'] * df.shape[0])
                self.anomaly_times.extend(df.index.tolist())
                self.source_signal.extend(signals)
                self.device_type.extend(['AMI'] * df.shape[0])
                self.device_id.extend(['-'] * df.shape[0])
                self.device_ph.extend(['-'] * df.shape[0])

        return self

    @property
    def all_anomalies(self):
        """A list of all possible anomalies that can be extracted."""
        return {'LG_PD_10', 'LG_PD_10_V2'}

    @property
    def default_anomalies(self):
        """A list of the default anomalies that will be extracted."""
        return {'LG_PD_10_V2'}


class EdnaAnomalies(_AnomalyBlob):

    """A collection of anomalies.

    This is used to extract all eDNA-based anomalies.

    Parameters
    ----------
    feeder_id : str
        The feeder ID number.

    anomalies : list, 'all', or 'default', optional (default='default')
        The anomalies to extract from ``df``. If 'default', all default
        anomalies will be extracted. If 'all', all available anomalies will be
        extracted.

    Attributes
    ----------
    anomaly_names : list
        The names of the anomalies.

    anomaly_times : list
        The timestamps of the anomalies extracted from ``df``.

    source_signal : list
        The signals from which the ``anomalies`` were parsed.
    """

    @staticmethod
    def _get_window(df, hours):
        """Helper function to get the rolling window for a time-series.

        Parameters
        ----------
        df : Pandas DataFrame
            DataFrame containing one time-series.

        hours : int or float
            The number of hours for which the rolling window should apply.

        Returns
        -------
        window : int
            The number of samples to approximate a rolling-window of ``hours``
            length. Returns -1 for any df's with too small a sample size.
        """
        resolution = df.Time.max() - df.Time.min()
        resolution = resolution.total_seconds() / (60. * 60.)
        if resolution < 1:
            return -1
        if df.shape[0] < 24:
            return -1
        resolution = df.shape[0] / resolution
        return int(np.ceil(resolution * float(hours)))

    def _store(self, anoms, label, start_time=None, end_time=None):
        """Helper function to add new anomalies to object.

        Parameters
        ----------
        anoms : Pandas DataFrame
            Condensed DataFrame containing one row per anomaly.

        label : str, list or None
            The name of the anomaly. If str, the anomaly name will be repeated.
            If list, the list will be used. If None, the "Anomaly" column of
            ``df`` will be used.

        start_time : datetime.datetime or None, optional (default=None)
            The time to begin extracting anomalies. If None, the entire
            time-series will be used.

        end_time : datetime.datetime or None, optional (default=None)
            The time to stop extracting anomalies. If None, the entire
            time-series will be used.
        """
        if start_time is not None:
            anoms = anoms.loc[anoms.Time >= start_time]
        if end_time is not None:
            anoms = anoms.loc[anoms.Time < end_time]
        if isinstance(label, str):
            anomaly_names = [label] * anoms.shape[0]
        elif label is None:
            anomaly_names = list(anoms.Anomaly.values)
        else:
            anomaly_names = list(label)

        def get_device_type(id_str):
            """Get the device type from the eDNA point name."""
            if '.FCI.' in id_str:
                return 'FCI'
            elif '.AFS.' in id_str:
                return 'AFS'
            elif '.FDR' in id_str and 'BKR.' not in id_str:
                return 'PHASER'
            else:
                return 'UNKNOWN'

        def get_device_id(id_str):
            """Get the device ID from the eDNA point name."""
            if '.FCI.' in id_str or '.AFS.' in id_str:
                return id_str.split('.')[3]
            elif '.FDR' in id_str and 'BKR.' not in id_str:
                try:
                    return id_str.split('.')[2].split('_')[1]
                except IndexError:
                    return 'UNKNOWN'
            else:
                return 'UNKNOWN'

        def get_phase(id_str):
            """Get the device phase from the eDNA point name."""
            if '_PH' in id_str:
                return id_str[-4:-3]
            else:
                return '-'

        self.anomaly_names.extend(anomaly_names)
        self.anomaly_times.extend(anoms.Time.tolist())
        self.source_signal.extend(list(anoms.ExtendedId.values))
        device_type = anoms.ExtendedId.map(get_device_type).values
        self.device_type.extend(device_type)
        device_id = anoms.ExtendedId.map(get_device_id).values
        self.device_id.extend(device_id)
        device_ph = anoms.ExtendedId.map(get_phase).values
        self.device_ph.extend(device_ph)

    def extract(self, df, start_time=None, end_time=None):
        """Begin anomaly extraction on ``df``.

        Parameters
        ----------
        df : Pandas DataFrame
            The time-series data.

        start_time : datetime.datetime or None, optional (default=None)
            The time to begin extracting anomalies. If None, the entire
            time-series will be used.

        end_time : datetime.datetime or None, optional (default=None)
            The time to stop extracting anomalies. If None, the entire
            time-series will be used.
        """
        edna_cols = ['ExtendedId', 'Value', 'ValueString', 'Time', 'Status']
        self._check_df(df, edna_cols)
        #self._check_anomalies()

        def get_fdr(id_str):
            """Get the feeder ID from the eDNA point name."""
            fdr_pattern = re.compile(r'[\._][0-9]{6}[\._]')
            try:
                fdr = fdr_pattern.findall(id_str)[0][1:7]
            except IndexError:
                fdr = 'NULL'
            return fdr

        points = pd.DataFrame({'Point': df.ExtendedId.unique()})
        points = points[-points.Point.str.contains('Bad point')]
        points['Feeder'] = points.Point.map(get_fdr)
        points = points[points.Feeder == self.feeder_id].Point

        df = df.loc[df.ExtendedId.isin(set(points.values))]
        df = df.drop_duplicates(subset=edna_cols)

        if 'FCI_FAULT_ALARM' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.FCI\.')) &
                                    (points.str.contains(r'\.FAULT'))]
            df_sub = df.loc[(df.ExtendedId.isin(points_sub)) &
                            (df.ValueString != 'NORMAL')]

            self._store(df_sub, 'FCI_FAULT_ALARM', start_time, end_time)

        if set(self.anomalies) & {'FCI_I_FAULT_FULL', 'FCI_I_FAULT_TEMP'}:

            points_sub = points.loc[(points.str.contains(r'\.FCI\.')) &
                                    (points.str.contains(r'\.I_FAULT'))]
            df_sub = df.loc[(df.ExtendedId.isin(points_sub)) &
                            (df.Value >= 600.)].copy()
            df_sub['Anomaly'] = 'FCI_I_FAULT_FULL'
            df_sub.loc[df.Value < 900., 'Anomaly'] = 'FCI_I_FAULT_TEMP'

            self._store(df_sub, None, start_time, end_time)

        if 'AFS_ALARM_ALARM' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.AFS\.')) &
                                    (points.str.contains(r'\.ALARM'))]
            df_sub = df.loc[(df.ExtendedId.isin(points_sub)) &
                            (df.ValueString == 'ALARM')]

            self._store(df_sub, 'AFS_ALARM_ALARM', start_time, end_time)

        if 'AFS_GROUND_ALARM' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.AFS\.')) &
                                    (points.str.contains(r'\.GROUND'))]
            df_sub = df.loc[(df.ExtendedId.isin(points_sub)) &
                            (df.ValueString == 'ALARM')]

            self._store(df_sub, 'AFS_GROUND_ALARM', start_time, end_time)

        if set(self.anomalies) & {'AFS_I_FAULT_FULL', 'AFS_I_FAULT_TEMP'}:

            points_sub = points.loc[(points.str.contains(r'\.AFS\.')) &
                                    (points.str.contains(r'\.I_FAULT'))]
            df_sub = df.loc[(df.ExtendedId.isin(points_sub)) &
                            (df.Value >= 600.)].copy()
            df_sub['Anomaly'] = 'AFS_I_FAULT_FULL'
            df_sub.loc[df.Value < 900., 'Anomaly'] = 'AFS_I_FAULT_TEMP'

            self._store(df_sub, None, start_time, end_time)

        # Okay to remove SET/NOT-SET rows now, those just matter for AFSs/FCIs
        df = df.loc[df.Status == 'OK']

        if 'ZERO_CURRENT_V3' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.I\.')) &
                                    (points.str.contains(r'_PH')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')
                window = self._get_window(df_sub2, 24)
                if window == -1:
                    continue
                anoms = df_sub2[(df_sub2.Value < 1) &
                                (df_sub2.Value > -0.5) &
                                (pd.rolling_quantile(df_sub2.Value,
                                                     window,
                                                     0.01) > 10)].Time.tolist()
                anoms = np.array(anoms)

                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_CURRENT_V3', start_time, end_time)

        if 'ZERO_CURRENT_V4' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.I\.')) &
                                    (points.str.contains(r'_PH')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')

                df_sub2['LowValue'] = (df_sub2.Value < 1) & (df_sub2.Value > -0.5)
                df_sub2['OkayValue'] = df_sub2.Value >= 1
                df_sub2['OkayValue'] = df_sub2.OkayValue.shift()
                df_sub2.OkayValue = df_sub2.OkayValue.fillna(False)
                df_sub2['ZeroValue'] = df_sub2.LowValue & df_sub2.OkayValue
                anoms = df_sub2[df_sub2.ZeroValue].Time.tolist()
                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.OkayValue) &
                                 (df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_CURRENT_V4', start_time, end_time)

        if 'PF_SPIKES_V3' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.PF\.')) &
                                    (points.str.contains(r'_PH')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')
                window = self._get_window(df_sub2, 24)
                if window == -1:
                    continue
                anoms = df_sub2[(df_sub2.Value.abs() < 0.75) &
                                (pd.rolling_quantile(df_sub2.Value.abs(),
                                                     window,
                                                     0.01) > 0.8)].Time.tolist()
                anoms = np.array(anoms)
                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'PF_SPIKES_V3', start_time, end_time)

        if 'ZERO_POWER_V3' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.MW')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')
                window = self._get_window(df_sub2, 24)
                if window == -1:
                    continue
                anoms = df_sub2[(df_sub2.Value < 0.1) &
                                (df_sub2.Value > -0.5) &
                                (pd.rolling_quantile(df_sub2.Value,
                                                     window,
                                                     0.01) > 0.5)].Time.tolist()
                anoms = np.array(anoms)

                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_POWER_V3', start_time, end_time)

        if 'ZERO_POWER_V4' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.MW')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')

                df_sub2['LowValue'] = (df_sub2.Value < 0.1) & (df_sub2.Value > -0.5)
                df_sub2['OkayValue'] = df_sub2.Value >= 0.1
                df_sub2['OkayValue'] = df_sub2.OkayValue.shift()
                df_sub2.OkayValue = df_sub2.OkayValue.fillna(False)
                df_sub2['ZeroValue'] = df_sub2.LowValue & df_sub2.OkayValue
                anoms = df_sub2[df_sub2.ZeroValue].Time.tolist()
                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.OkayValue) &
                                 (df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_POWER_V4', start_time, end_time)

        if 'THD_SPIKES_V3' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.THD_')) &
                                    (points.str.contains(r'urrent'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')
                window = self._get_window(df_sub2, 24)
                if window == -1:
                    continue
                df_sub2['roll'] = pd.rolling_mean(df_sub2.Value, window)
                df_sub2['stdev'] = pd.rolling_std(df_sub2.Value, window)
                df_sub2['threshold'] = df_sub2.roll + (7 * df_sub2.stdev)
                df_sub2.threshold = df_sub2.threshold.shift()
                anoms = df_sub2.loc[df_sub2.threshold < df_sub2.Value]
                anoms = [e for e in anoms.Time.tolist() if
                         df_sub2[(df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'THD_SPIKES_V3', start_time, end_time)

        if 'ZERO_VOLTAGE_V3' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.V\.')) &
                                    (points.str.contains(r'_PH')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')
                window = self._get_window(df_sub2, 24)
                if window == -1:
                    continue
                anoms = df_sub2[(df_sub2.Value < 1) &
                                (df_sub2.Value > -0.5) &
                                (pd.rolling_quantile(df_sub2.Value,
                                                     window,
                                                     0.01) > 90)].Time.tolist()
                anoms = np.array(anoms)

                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_VOLTAGE_V3', start_time, end_time)

        if 'ZERO_VOLTAGE_V4' in self.anomalies:

            points_sub = points.loc[(points.str.contains(r'\.V\.')) &
                                    (points.str.contains(r'_PH')) &
                                    (points.str.contains(r'\.FDR\.')) &
                                    (-points.str.contains(r'BKR\.'))]
            df_sub = df.loc[df.ExtendedId.isin(points_sub)]

            for point_id in df_sub.ExtendedId.unique():

                df_sub2 = df_sub.loc[df_sub.ExtendedId == point_id]
                df_sub2 = df_sub2.sort_values(by='Time')

                df_sub2['LowValue'] = (df_sub2.Value < 1) & (df_sub2.Value > -0.5)
                df_sub2['OkayValue'] = df_sub2.Value >= 1
                df_sub2['OkayValue'] = df_sub2.OkayValue.shift()
                df_sub2.OkayValue = df_sub2.OkayValue.fillna(False)
                df_sub2['ZeroValue'] = df_sub2.LowValue & df_sub2.OkayValue
                anoms = df_sub2[df_sub2.ZeroValue].Time.tolist()
                anoms = [e for e in anoms if
                         df_sub2[(df_sub2.OkayValue) &
                                 (df_sub2.Time <= e) &
                                 (df_sub2.Time > (e - DAY))].shape[0] > 24]
                df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]

                self._store(df_sub2, 'ZERO_VOLTAGE_V4', start_time, end_time)

        return self

    @property
    def all_anomalies(self):
        """A list of all possible anomalies that can be extracted."""
        return {'PF_SPIKES_V3', 'THD_SPIKES_V3', 'ZERO_CURRENT_V3',
                'ZERO_CURRENT_V4', 'ZERO_POWER_V3', 'ZERO_POWER_V4',
                'ZERO_VOLTAGE_V3', 'ZERO_VOLTAGE_V4', 'FCI_FAULT_ALARM',
                'FCI_I_FAULT_FULL', 'FCI_I_FAULT_TEMP', 'AFS_ALARM_ALARM',
                'AFS_GROUND_ALARM', 'AFS_I_FAULT_FULL', 'AFS_I_FAULT_TEMP'}

    @property
    def default_anomalies(self):
        """A list of the default anomalies that will be extracted."""
        return {'PF_SPIKES_V3', 'THD_SPIKES_V3', 'ZERO_CURRENT_V4',
                'ZERO_POWER_V4', 'ZERO_VOLTAGE_V4', 'FCI_FAULT_ALARM',
                'FCI_I_FAULT_FULL', 'FCI_I_FAULT_TEMP', 'AFS_ALARM_ALARM',
                'AFS_GROUND_ALARM', 'AFS_I_FAULT_FULL', 'AFS_I_FAULT_TEMP'}
