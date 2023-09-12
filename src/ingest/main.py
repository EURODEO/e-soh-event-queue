from ingest.send_mqtt import mqtt_connection
from ingest.messages import load_files, build_message

import xarray as xr


class ingest_to_pipline():
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(self, mqtt_conf: dict, uuid_prefix: str):
        self.mqtt = mqtt_connection(mqtt_conf["host"], mqtt_conf["topic"])

        self.uuid_prefix = uuid_prefix

    def ingest_message(self, message: [str, object], input_type: str = None):
        match input_type:
            case "netCDF":
                if isinstance(message, str):
                    load_files(message, input_type=input_type, uuid_prefix=self.uuid_prefix)
                elif isinstance(message, xr.Dataset):
                    build_message(message, type=input_type, uuid_prefix=self.uuid_prefix)
                else:
                    raise TypeError(
                        f"Unknown netCDF type, expected path or xarray.Dataset, got {type(message)}")
