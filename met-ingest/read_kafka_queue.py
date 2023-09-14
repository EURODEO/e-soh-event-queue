from typing import Callable

import kafka
import json

import xml.etree.ElementTree as ET
import numpy as np


import xarray as xr


from parse_kfaka_messages import stinfosys_decoder

from ingest.send_mqtt import mqtt_connection
from ingest.main import ingest_to_pipeline


class read_kvalobs_topic():
    def __init__(self, topics: list, bootstrap_server: list):
        self.consumer = kafka.KafkaConsumer(
            bootstrap_servers=bootstrap_server, security_protocol="SSL")

        self.topics = topics
        self.subscribe(self.topics)

    def subscribe(self, topics: str):
        self.consumer.subscribe(topics)

    def listen_loop(self, message_handler: Callable[[dict], None], timeout_ms: int = 1000):
        """
        Might make this non blocking at some point, will not implement this as of yet.
        Threaded message handeling would probably be smart.
        """

        while True:
            data = self.consumer.poll(timeout_ms=timeout_ms)
            if data:
                message_handler(data)


def parse_kvalobs_ids(xml_doc: ET):

    parsed_observation = []

    stations = xml_doc.findall("station")
    # print(minidom.parseString(ET.tostring(xml_doc)).toprettyxml())

    for station in stations:
        for obstime in station.findall(".//obstime"):
            station_metadata = decoder.getStationWigos(station.attrib["val"])
            if not station_metadata:
                station_metadata = decoder.getStationMetadata(station.attrib["val"])
                if not station_metadata:
                    print(f"Stationid {station.attrib['val']} not found")
                    continue
                station_metadata = station_metadata[0]
                print(f"Stationid {station_metadata[0]} not found in wigos_station view.\n"
                      f"Station info:\n\tstation name: {station_metadata[9]}\n\t" +
                      f"station wmono: {station_metadata[11]}")
                continue
            station_metadata = station_metadata[0]
            observations = {}

            for obs in obstime.findall(".//kvdata"):
                if obs.attrib["paramid"] == 0:
                    continue
                obs_metadata = decoder.getParamData(obs.attrib["paramid"])[0]
                if obs_metadata[0] == 0:
                    continue
                # [print(i, j) for i, j in enumerate(obs_metadata)]
                observations[obs_metadata[1]] = (["time"],
                                                 [float(obs.find("corrected").text)],
                                                 {"units": obs_metadata[3],
                                                 "standard_name": obs_metadata[14],
                                                  })

                netcdf_payload = xr.Dataset(observations,
                                            coords={"time": [np.datetime64(
                                                obstime.get("val")).astype('datetime64[ns]')]},
                                            attrs={
                                                "license": "http//spdx.org/licenses/CC-BY-4.0(CC-BY-4.0)",
                                                "Conventions": "CF-1.10, ACDD-1.3",
                                                "naming_authority": "no.met",
                                                "institution": "Norwegian Meteorological Institute (MET Norway)",
                                                "institution_short_name": "MET Norway",
                                                "spatial_representation": "point",
                                                "access_constraint": "Open",
                                                "featureType": "point",
                                                "creator_institution": "Norwegian Meteorological Institute",
                                                "creator_name": "Norwegian Meteorological Institute",
                                                "creator_role": "Investigator",
                                                "creator_type": "institution",
                                                "creator_url": "https//www.met.no",
                                                "creator_email": "info@met.no",
                                                "processing_level": "Operational",
                                                "publisher_name": "Norwegian Meteorological Institute",
                                                "publisher_type": "institution",
                                                "publisher_email": "csw-services@met.no",
                                                "publisher_url": "https//www.met.no/",
                                                "geospatial_lat_min": station_metadata[2],
                                                "geospatial_lat_max": station_metadata[2],
                                                "geospatial_lon_min": station_metadata[3],
                                                "geospatial_lon_max": station_metadata[3],
                                                "source ": "In Situ Land-based station",
                                                "platform": station_metadata[11],
                                                "wmono": station_metadata[13],
                                                "wigos": station_metadata[0],
                                                "history": obstime.get("val") + ": Created from kvalobs.",
                                                "id": "no.met:placeholder",
                                                "title": station_metadata[10],
                                                "keywords": "Meteoroligical observations from ground stations",
                                                "summary": "Ground observation from kvalobs, station " + station_metadata[10],
                                                "source": "kvalobs",
                                                "references": "Insert documentation about E-SOH datastore"
                                            }
                                            )

                parsed_observation.append(netcdf_payload)
            # print(parsed_observation)

    return parsed_observation


def process_consumer_record(record):
    observations = []

    for Topic_partition in record:
        for consumer_record in record[Topic_partition]:

            observations.append(parse_kvalobs_ids(ET.fromstring(
                consumer_record.value.decode().replace("\n", ""))))

    for i in observations:
        for df in i:
            for msg in main_ingest.ingest_message(df, "netCDF"):
                # print(msg)
                publisher_rabbit.send_message(json.dumps(msg))
                publisher_verne.send_message(json.dumps(msg))
    return observations


if __name__ == "__main__":
    import sys

    with open("schemas/netcdf_to_e_soh_message_metno.json", "r") as file:
        json_map = json.load(file)

    publisher_rabbit = mqtt_connection("157.249.73.113", "topic/test")
    publisher_verne = mqtt_connection("157.249.72.58", "topic/test")

    main_ingest = ingest_to_pipeline(
        {"host": "157.249.73.113", "topic": "topic/test"}, "urn:x-wmo:md:norway:no.met")
    main_ingest.setup_netcdf("schemas/netcdf_to_e_soh_message_metno.json")

    decoder = stinfosys_decoder(db="stinfosys", host="stinfodb",
                                user=sys.argv[1], port=5432,  password=sys.argv[2])

    topic = 'kvalobs.production.checked'
    servers = ['kafka1-a1.kafka.met.no:9093',
               'kafka1-a2.kafka.met.no:9093',
               'kafka1-b1.kafka.met.no:9093',
               'kafka1-b2.kafka.met.no:9093']

    observer = read_kvalobs_topic(topic, servers)

    observer.listen_loop(process_consumer_record)
