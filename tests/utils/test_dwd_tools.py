import datetime
import unittest

import pandas as pd

from src.utils.dwd_tools import calculate_distance, get_decimals_from_minutes, get_station_id, \
    parse_kml_to_df, dwd_time_to_datetime, get_timestamp_from_runid, get_dwd_runid
from src.utils.static import test_mosmix_kml_file, dwd_mosmix_parameters_file


class MyTestCase(unittest.TestCase):

    def test_calculate_distance(self):
        lat_1, lon_1 = 49.75931543289453, 6.643681343223453  # Trier Porta Nigra
        lat_2, lon_2 = 51.21792544098937, 6.761529549616857  # Duesseldorf rheinturm
        lat_3, lon_3 = 49.74688606912671, 6.653033274910183  # Trier Aussichtspunkt Petrisberg
        lat_4, lon_4 = 40.74838948352278, -73.98569695911485  # New York Empire State Building

        self.assertAlmostEqual(calculate_distance(lat_1, lon_1, lat_2, lon_2), 162.40, 1)
        self.assertAlmostEqual(calculate_distance(lat_1, lon_1, lat_3, lon_3), 1.54, 1)
        self.assertAlmostEqual(calculate_distance(lat_1, lon_1, lat_4, lon_4), 6081.7, 1)
        self.assertAlmostEqual(calculate_distance(lat_2, lon_2, lat_4, lon_4), 6025.42, 1)

    def test_get_decimals_from_minutes(self):
        m1 = 12.17  # 12.2833
        m2 = 52.93  # 53.55
        m3 = 33.35  # 33.5833
        m4 = 22.46  # 22.7667

        self.assertAlmostEqual(get_decimals_from_minutes(m1), 12.2833, 2)
        self.assertAlmostEqual(get_decimals_from_minutes(m2), 53.55, 2)
        self.assertAlmostEqual(get_decimals_from_minutes(m3), 33.5833, 2)
        self.assertAlmostEqual(get_decimals_from_minutes(m4), 22.7667, 2)

    def test_get_station_id(self):
        lat_1, lon_1 = 49.75500703146231, 6.63988190608787  # Should be Petrisberg (10609)
        lat_2, lon_2 = 51.28940570113179, 6.774645818877019  # Should be Duesseldorf (10400)
        lat_3, lon_3 = 65.67149630356944, -18.120737797563685  # Should be AKUREYRI (04063)

        self.assertEqual(get_station_id(lat_1, lon_1), "10609")
        self.assertEqual(get_station_id(lat_2, lon_2), "10400")
        self.assertEqual(get_station_id(lat_3, lon_3), "04063")

    def test_parse_kml(self):
        params = pd.read_csv(dwd_mosmix_parameters_file, sep=';')['parameter'].tolist()

        mosmix_df = parse_kml_to_df(test_mosmix_kml_file, params)

        self.assertEqual(247, len(mosmix_df), "mosmix forecast should have 247(h) entries")

    def test_dwd_time_to_datetime(self):
        ts1 = "2024-01-29T01:00:00.000Z"
        ts2 = "2024-01-29T02:00:00.000Z"
        ts3 = "2024-01-29T03:00:00.000Z"

        self.assertEqual(dwd_time_to_datetime(ts1),
                         datetime.datetime(2024, 1, 29, 1, 0, 0, tzinfo=datetime.timezone.utc))
        self.assertEqual(dwd_time_to_datetime(ts2),
                         datetime.datetime(2024, 1, 29, 2, 0, 0, tzinfo=datetime.timezone.utc))
        self.assertEqual(dwd_time_to_datetime(ts3),
                         datetime.datetime(2024, 1, 29, 3, 0, 0, tzinfo=datetime.timezone.utc))

    def test_timestamp_from_runid(self):
        ts1 = "2022090100"
        ts2 = "1992110106"
        ts3 = "2010050518"

        self.assertEqual(get_timestamp_from_runid(ts1),
                         datetime.datetime(2022, 9, 1, 0, 0, 0,tzinfo=datetime.timezone.utc))
        self.assertEqual(get_timestamp_from_runid(ts2),
                         datetime.datetime(1992, 11, 1, 6, 0, 0,tzinfo=datetime.timezone.utc))
        self.assertEqual(get_timestamp_from_runid(ts3),
                         datetime.datetime(2010, 5, 5, 18, 0, 0,tzinfo=datetime.timezone.utc))

    def test_get_dwd_runid(self):
        filename = "MOSMIX_L_2022090100_10609.kmz"
        self.assertEqual(get_dwd_runid(filename), 2022090100)

        filename = "/root/MOSMIX_L_1992110106_10400.kmz"
        self.assertEqual(get_dwd_runid(filename), 1992110106)

        filename = "c:\\\\test_path\\MOSMIX_L_2010050518_04063.kmz"
        self.assertEqual(get_dwd_runid(filename), 2010050518)

        with self.assertRaises(ValueError):
            get_dwd_runid("no_digits_here.txt")

        self.assertEqual(get_dwd_runid("data/_1234567890_/archive/_1987654321_.zip"),1987654321)

        with self.assertRaises(ValueError):
            get_dwd_runid("too_many/_1234567890123_digits.txt")

        with self.assertRaises(ValueError):
            get_dwd_runid("too_few/_12345_digits.txt")


if __name__ == '__main__':
    unittest.main()
