"""
tools to for dwd data
"""

import math
import os
import re
from datetime import datetime, timezone
from zipfile import ZipFile

import pandas as pd
from lxml import etree

from src.utils.logging import get_default_logger
from src.utils.static import dwd_station_file

logger = get_default_logger(__name__)


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculates the distance between two coordinates
    :param lat1: latitude of first coordinate
    :param lon1: longitude of first coordinate
    :param lat2: latitude of second coordinate
    :param lon2: longitude of second coordinate
    :return: distance between the two coordinates in km
    """
    # Radius of the Earth in kilometers
    r = 6371

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculate the differences between the latitudes and longitudes
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Haversine formula
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(
        delta_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c

    return distance


def get_decimals_from_minutes(minutes: float) -> float:
    """
    Converts minutes to decimal degrees
    :param minutes: minutes
    :return: decimal degrees
    """
    m_str = str(minutes)
    nr, m = m_str.split(".")  # split the minutes into the integer part and the minutes part
    m = float("0." + m) * 100 / 60  # convert the minutes part to decimal degrees
    mi = float(nr) + m  # add the integer part to the decimal minutes
    return mi


def get_station_id(lat: float, lon: float) -> str:
    """
    Returns the station id of the closest station to the given coordinates
    :param lat: latitude
    :param lon: longitude
    :return: station id
    """
    df = pd.read_csv(dwd_station_file, sep=';')
    df['LON'] = df.apply(lambda x: (get_decimals_from_minutes(x['LON'])), axis=1)
    df['LAT'] = df.apply(lambda x: (get_decimals_from_minutes(x['LAT'])), axis=1)
    df['Distance'] = df.apply(
        lambda x: (calculate_distance(float(x['LAT']), float(x['LON']), lat, lon)), axis=1)
    df.sort_values(by=['Distance'], inplace=True)
    return df.iloc[0]['ID']


def parse_kml_to_df(kml_path: str, dwd_parameters: [str]) -> pd.DataFrame:
    """
    Parses a kml file and returns a dataframe with the data.
    :param kml_path: path to the kml file
    :param dwd_parameters: parameters which should be parsed
    :return: dataframe with parsed parameters and timestamps
    """

    # load kml
    tree = etree.parse(kml_path)

    # xml prefixes
    dwd = "{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}"
    kml = "{http://www.opengis.net/kml/2.2}"

    fts = tree.find(".//" + dwd + "ForecastTimeSteps").getchildren()
    timestamps = [dwd_time_to_datetime(t.text) for t in fts]
    output = pd.DataFrame(columns=['timestamp'], data=timestamps)

    data_series = {}
    station = tree.find(".//" + kml + "Placemark")
    for x in station.findall(".//" + dwd + "Forecast"):
        parameter = x.attrib[dwd + 'elementName']
        if parameter not in dwd_parameters:
            continue
        value_str = x.getchildren()[0].text
        data_series[parameter] = parse_dwd_string_to_list(value_str)

    # create dataframe and concatenate
    value_dataframe = pd.DataFrame(data_series)
    output = pd.concat((output, value_dataframe), axis=1)

    # convert missing values to 0
    output.replace(to_replace='-', value="0", inplace=True)

    # convert all columns to numeric
    for parameter in dwd_parameters:
        if parameter in output.columns:
            output[parameter] = pd.to_numeric(output[parameter], errors='coerce')
    return output


def parse_dwd_string_to_list(string: str) -> [str]:
    """
    Since someone at dwd thought it would be a good idea to store the data as a whitespace separated
    values,this function has to exist.
    :param string: string to parse
    :return: list of strings
    """
    out = []
    for x in string.split(' '):
        if x.strip() != '':
            out.append(x)

    return out


def dwd_time_to_datetime(dwd_time: str) -> datetime:
    """
    Returns a datetime object from a dwd time string
    :param dwd_time:
    :return:
    """
    return datetime.fromisoformat(dwd_time.replace("Z", "+00:00"))


def get_timestamp_from_runid(runid: str | int) -> datetime:
    """
    Returns the timestamp of a dwd runid
    :param runid: runid to extract timestamp from
    :return: timestamp
    """
    if isinstance(runid, int):
        runid = str(runid)

    if len(runid) != 10:
        raise ValueError(f"Runid {runid} has to be 10 characters long")

    return datetime.strptime(runid, '%Y%m%d%H').replace(tzinfo=timezone.utc)


def get_dwd_runid(filename: str) -> int:
    """
    Returns the runid of a dwd forecast file
    :param filename: name to extract id from
    :return: id
    """
    pattern = r'[^/\\]*?_+(\d{10})_+[^/\\]*?\.[^/\\]+$'
    matches = re.findall(pattern, filename)
    if len(matches) > 1:
        raise ValueError(f"Found more than one runid in {filename}: {matches}")
    if len(matches) == 0:
        raise ValueError(f"Could not extract runid from {filename}")

    return int(matches[0])


def is_correct_mosmix_station(station_id: str) -> bool:
    """
    Checks if the station id is a valid mosmix station id
    :param station_id: station id to check
    :return: true if valid
    """

    stations = pd.read_csv(dwd_station_file, sep=';')
    return station_id in stations['ID'].values


def extract_kml_from_kmz(kmz_path: str, kml_path: str):
    """
    Extracts a kml file from a kmz file
    :param kmz_path: file path to kmz file
    :param kml_path: file path to kml file
    """
    if not os.path.exists(kmz_path):
        raise FileNotFoundError(f"File {kmz_path} not found")

    with ZipFile(kmz_path, 'r') as kmz:
        # Assuming there is only one KML file in the KMZ archive
        kml_filename = [name for name in kmz.namelist() if name.endswith('.kml')][0]
        kml_content = kmz.read(kml_filename)
        with open(kml_path, 'wb') as kml_file:
            kml_file.write(kml_content)
