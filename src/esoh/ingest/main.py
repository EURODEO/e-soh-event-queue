from esoh.ingest.send_mqtt import mqtt_connection
from esoh.ingest.messages import load_files, build_message


import xarray as xr

from jsonschema import Draft202012Validator, ValidationError
import json


import logging


class ingest_to_pipline():
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(self, mqtt_conf: dict,
                 uuid_prefix: str,
                 testing: bool = False,
                 esoh_mqtt_schema="schemas/e-soh-message-spec.json"):
        self.uuid_prefix = uuid_prefix

        if testing:
            return

        self.mqtt = mqtt_connection(mqtt_conf["host"])

        with open(esoh_mqtt_schema, "r") as file:
            self.esoh_mqtt_schema = json.load(file)

        self.scheam_validator = Draft202012Validator(self.esoh_mqtt_schema)

    def ingest_message(self, message: [str, object], input_type: str = None):
        if not input_type:
            input_type = self.decide_input_type(message)

        return self.build_message(message, input_type)

    def decide_input_type(self, message):
        if isinstance(message, str):
            match message.split(".")[-1]:
                case "nc":
                    return "netCDF"
                case "bufr":
                    return "bufr"
                case _:
                    raise ValueError("Unknown filetype provided.")

    def build_messages(self, message: [str, object], input_type: str = None):
        if not input_type:
            if isinstance(message, str):
                input_type = self.decide_input_type()
            else:
                raise TypeError(
                    "Illegal use, can not provide objects without specifying input_type")
        match input_type:
            case "netCDF":
                if isinstance(message, str):
                    return load_files(message, input_type=input_type, uuid_prefix=self.uuid_prefix)
                elif isinstance(message, xr.Dataset):
                    return build_message(message,
                                         input_type=input_type,
                                         uuid_prefix=self.uuid_prefix)
                else:
                    raise TypeError("Unknown netCDF type, expected path"
                                    + f"or xarray.Dataset, got {type(message)}")
            case "bufr":
                raise NotImplementedError("Handeling of bufr not implemented")

    def publish_messages(self, messages: list):
        for msg in messages:
            try:
                self.scheam_validator.validate(msg)
                self.mqtt.send_message(msg)
            except ValidationError as v_error:
                logging.warning("Message did not pass schema validation, " + v_error)
                continue
            except Exception as e:
                raise e
