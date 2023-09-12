from ingest.send_mqtt import mqtt_connection
from ingest.messages import load_files, build_message

import xarray as xr

import json


class ingest_to_pipeline():
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(self, mqtt_conf: dict, uuid_prefix: str):
        self.mqtt = mqtt_connection(mqtt_conf["host"], mqtt_conf["topic"])

        self.uuid_prefix = uuid_prefix

    def setup_netcdf(self, path_to_json_map: str):
        with open(path_to_json_map, "r") as file:
            self.json_map = json.load(file)

    def ingest_message(self, message: [str, object], input_type: str = None):
        match input_type:
            case "netCDF":
                if isinstance(message, str):
                    return load_files(message, input_type=input_type,
                                      json_map=self.json_map, uuid_prefix=self.uuid_prefix)
                elif isinstance(message, xr.Dataset):
                    return build_message(message, input_type=input_type, json_map=self.json_map,
                                         uuid_prefix=self.uuid_prefix)
                else:
                    raise TypeError(
                        f"Unknown netCDF type, expected path or xarray.Dataset, got {type(message)}")
