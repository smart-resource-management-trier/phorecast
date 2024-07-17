"""
This file contains static variables like paths or constants which are referenced from multiple
places in the code.
"""
import os
from pathlib import Path

# Persistent/dynamic data
# -Directories
project_root_path = Path(os.path.dirname(__file__)).parents[1]

persistent_data_root_path = os.path.join(project_root_path, "data")

model_data_path = os.path.join(persistent_data_root_path, "model-data")

server_storage_path = os.path.join(persistent_data_root_path, "server-data")

# -Files
user_data_db_file = os.path.join(server_storage_path, "userdata.db")

event_config_db_file = os.path.join(server_storage_path, "event_config.db")

# Static data
# -Directories
static_data_root_path = os.path.join(project_root_path, "resources")

docker_build_path = os.path.join(static_data_root_path, "build", "docker")

# -Files
dwd_mosmix_parameters_file = os.path.join(static_data_root_path, "dwd_mosmix_parameters.csv")

dwd_station_file = os.path.join(static_data_root_path, "dwd_mosmix_stations.csv")

pv_lib_parameters_file = os.path.join(static_data_root_path, "pv_lib_parameters.csv")

# Test data
# -Directories
test_root_path = os.path.join(project_root_path, "tests")

test_data_root_path = os.path.join(test_root_path, "resources")

# -Files
test_dataset_file = os.path.join(test_data_root_path, "test_dataset.csv")

test_mosmix_kml_file = os.path.join(test_data_root_path, "MOSMIX_L_2024012909_10609.kml")

test_target_backup_file = os.path.join(test_data_root_path, "backup_csv",
                                       "backup_target_loader.csv")

test_weather_backup_file = os.path.join(test_data_root_path, "backup_csv",
                                        "backup_weather_loader.csv")
