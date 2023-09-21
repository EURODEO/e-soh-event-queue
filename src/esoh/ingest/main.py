from esoh.ingest.send_mqtt import mqtt_connection
from esoh.ingest.messages import messages

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

    def ingest(self, message: [str, object], input_type: str = None):
        if not input_type:
            input_type = self.decide_input_type(message)

        self.build_message(message, input_type)

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

    def _decide_input_type(self, message):
        if isinstance(message, str):
            match message.split(".")[-1]:
                case "nc":
                    return "netCDF"
                case "bufr":
                    return "bufr"
                case _:
                    raise ValueError(f"Unknown filetype provided. Got {message.split('.')[-1]}")

    def _build_messages(self, message: [str, object], input_type: str = None):
        if not input_type:
            if isinstance(message, str):
                input_type = self._decide_input_type(message)
            else:
                raise TypeError("Illagel usage, not allowed to input"
                                + "objects without specifying input type")
        return messages(message, input_type, self.uuid_prefix)
