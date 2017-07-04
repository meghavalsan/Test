# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Trevor Stephens' <trevor.stephens@auto-grid.com>

import unittest
import pandas as pd
from ..anomaly import ScadaAnomalies, EdnaAnomalies, AmiAnomalies, TicketAnomalies


valid_scada = {'feederNumber': ['123456', '123456'],
               'OBSERV_DATA': ['MOTOROLA FEEDER 3W82_F4062 BKR OPEN',
                               'MOTOROLA FEEDER 3W82_F4062 BKR CLOSED'],
               'localTime': [pd.Timestamp('2016-04-05T11:00:05Z'),
                             pd.Timestamp('2016-04-05T11:00:15Z')]}
valid_scada = pd.DataFrame(valid_scada)

valid_ami = {'fdr_num': ['123456', '123456'],
             'ami_dvc_name': ['G1', 'G2'],
             'mtr_evnt_id': ['12007', '12007'],
             'mtr_evnt_tmstmp': [pd.Timestamp('2016-04-05T11:00:05Z'),
                                 pd.Timestamp('2016-04-05T11:00:15Z')]}
valid_ami = pd.DataFrame(valid_ami)

valid_tix = {'DW_TCKT_KEY': ['13', '12'],
             'FDR_NUM': ['123456', '123456'],
             'POWEROFF': [pd.Timestamp('2016-04-05T11:00:05Z'),
                          pd.Timestamp('2016-04-06T11:00:15Z')],
             'POWERRESTORE': [pd.Timestamp('2016-04-05T12:00:05Z'),
                              pd.Timestamp('2016-04-06T12:00:15Z')],
             'IRPT_TYPE_CODE': ['LAT', 'FDR'],
             'RPR_ACTN_TYPE': ['', 'Refuse Transformer']}
valid_tix = pd.DataFrame(valid_tix)

timestamps = ['2016-04-05T%s:00:05' % t for t in range(24)]
timestamps += ['2016-04-05T23:30:05']
timestamps += ['2016-04-06T%s:00:05' % t for t in range(24)]
valid_edna = {'ExtendedId': ['DODGER.FDR.123456_2W100.THD_current'] * 49,
              'Value': [1., 2., 1.5] * 15 + [300., 1, 1., 1.],
              'ValueString': ['-'] * 50,
              'Time': timestamps,
              'Status': ['OK'] * 50}
valid_edna['ExtendedId'].append('DODGER.123456.FCI.177C.I_FAULT.C_PH')
valid_edna['Value'].append(900)
valid_edna['Time'].append('2016-04-05T23:30:05Z')
valid_edna = pd.DataFrame(valid_edna)
valid_edna.Time = pd.to_datetime(valid_edna.Time)
valid_edna.Time = valid_edna.Time.dt.tz_localize('UTC')


class TestAnomaly(unittest.TestCase):

    def test_invalid_df(self):

        with self.assertRaises(ValueError):
            anoms = ScadaAnomalies('123456', 'default')
            anoms.extract(df=pd.DataFrame())

        with self.assertRaises(ValueError):
            anoms = AmiAnomalies('123456', 'default')
            anoms.extract(df=pd.DataFrame(), customers=100)

        with self.assertRaises(ValueError):
            anoms = TicketAnomalies('123456', 'default')
            anoms.extract(df=pd.DataFrame())

        with self.assertRaises(ValueError):
            anoms = EdnaAnomalies('123456', 'default')
            anoms.extract(df=pd.DataFrame())

    def test_invalid_anomaly(self):

        # Test with valid data, but invalid anomaly names

        with self.assertRaises(ValueError):
            anoms = ScadaAnomalies('123456', ['invalid_name'])
            anoms.extract(df=valid_scada)

        with self.assertRaises(ValueError):
            anoms = AmiAnomalies('123456', ['invalid_name'])
            anoms.extract(df=valid_ami, customers=100)

        with self.assertRaises(ValueError):
            anoms = TicketAnomalies('123456', ['invalid_name'])
            anoms.extract(df=valid_tix)

        with self.assertRaises(ValueError):
            anoms = EdnaAnomalies('123456', ['invalid_name'])
            anoms.extract(df=valid_edna)

    def test_anomaly(self):

        # This should create two anomalies, one breaker open, one breaker close
        anoms = ScadaAnomalies('123456', 'default')
        anoms.extract(df=valid_scada)
        self.assertTrue(len(anoms) == 2)
        self.assertTrue(anoms.to_df().shape == (2, 7))
        self.assertIsInstance(anoms.anomaly_times[0], pd.Timestamp)

        # This should create no anomalies as only two customers, below 10%
        anoms = AmiAnomalies('123456', 'default')
        anoms.extract(df=valid_ami, customers=100)
        self.assertTrue(len(anoms) == 0)
        self.assertTrue(anoms.to_df().shape == (0, 7))

        # This should create one anomaly as the customers is now above 10%
        anoms = AmiAnomalies('123456', 'default')
        anoms.extract(df=valid_ami, customers=15)
        self.assertTrue(len(anoms) == 1)
        self.assertTrue(anoms.to_df().shape == (1, 7))
        self.assertIsInstance(anoms.anomaly_times[0], pd.Timestamp)

        # This should create one FCI anomaly, and one THD anomaly
        anoms = EdnaAnomalies('123456', 'default')
        anoms.extract(df=valid_edna)
        self.assertTrue(len(anoms) == 2)
        self.assertTrue(anoms.to_df().shape == (2, 7))
        self.assertIsInstance(anoms.anomaly_times[0], pd.Timestamp)

        # This should create one THD anomaly
        anoms = EdnaAnomalies('123456', ['THD_SPIKES_V3'])
        anoms.extract(df=valid_edna)
        self.assertTrue(len(anoms) == 1)
        self.assertTrue(anoms.to_df().shape == (1, 7))
        self.assertIsInstance(anoms.anomaly_times[0], pd.Timestamp)
