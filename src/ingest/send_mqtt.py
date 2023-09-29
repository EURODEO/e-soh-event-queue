from ingest.netCDF.extract_metadata_netcdf import build_all_json_payloads_from_netCDF

import paho.mqtt.client as mqtt
import xarray as xr

import json


class mqtt_connection():
    def __init__(self, mqtt_host, mqtt_topic):
        self.mqtt_host = mqtt_host
        self.mqtt_topic = mqtt_topic

        # Initiate MQTT Client
        self.pub_client = mqtt.Client(client_id="")

        # Connect with MQTT Broker
        self.pub_client.connect(self.mqtt_host)

    def send_message(self, message: str):
        try:
            self.pub_client.publish(self.mqtt_topic, message)
        except Exception as e:
            print(e.with_traceback())


if __name__ == "__main__":

    json_netcdf_def = "../schemas/netcdf_to_e_soh_message_metno.json"

    ds = xr.load_dataset(path)
    with open(json_netcdf_def, "r") as file:
        netcdf_def = json.load(file)

    messages = build_all_json_payloads_from_netCDF(ds, json)

    for m in messages:
        pub_client.publish(MQTT_TOPIC, json.dumps(m))

        MQTT_TOPIC = "topic/test"
