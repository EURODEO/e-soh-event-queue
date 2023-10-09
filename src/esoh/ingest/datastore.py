import esoh.datastore_pb2 as dstore
import esoh.datastore_pb2_grpc as dstore_grpc


import grpc
import logging

from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from datetime import datetime

logger = logging.getLogger(__name__)


class datastore_connection():
    def __init__(self, DSHOST: str, DSPORT: str) -> None:
        self._channel = grpc.insecure_channel(DSHOST + ":" + DSPORT)
        self._stub = dstore_grpc.DatastoreStub(self._channel)

    def ingest(self, msg: str) -> None:
        print(msg)
        ts_metadata = dstore.TSMetadata()
        # json_format.ParseDict(msg, ts_metadata, ignore_unknown_fields=True,
        #   max_recursion_depth=100)
        # json_format.ParseDict(msg["properties"], ts_metadata, ignore_unknown_fields=True)

        for i in ['version',
                  'type',
                  'title',
                  'summary',
                  'keywords',
                  'keywords_vocabulary',
                  'license',
                  'conventions',
                  'naming_authority',
                  'creator_type',
                  'creator_name',
                  'creator_email',
                  'creator_url',
                  'institution',
                  'project',
                  'source',
                  'platform',
                  'platform_vocabulary',
                  'standard_name',
                  'unit',
                  'instrument',
                  'instrument_vocabulary']:
            if i in msg["properties"]:
                setattr(ts_metadata, i, msg["properties"][i])
            elif i in msg["properties"]["content"]:
                setattr(ts_metadata, i, msg["properties"]["content"][i])

        Observation_data = dstore.ObsMetadata(
            pubtime=Timestamp().FromDatetime(
                datetime.fromisoformat(msg["properties"]["pubtime"])),
            obstime_instant=Timestamp().FromDatetime(
                datetime.fromisoformat(msg["properties"]["datetime"])),
            geo_point=dstore.Point(lat=int(msg["geometry"]["coordinates"][0]),
                                   lon=int(msg["geometry"]["coordinates"][1])))

        for i in ['id',
                  'history',
                  'metadata_id',
                  'processing_level',
                  'data_id',
                  'value']:
            if i in msg:
                setattr(Observation_data, i, msg[i])
            elif i in msg['properties']:
                setattr(Observation_data, i, msg["properties"][i])
            elif i in msg["properties"]["content"]:
                setattr(Observation_data, i, msg["properties"]["content"][i])

        # json_format.ParseDict(msg, Observation_data,
        #                       ignore_unknown_fields=True, max_recursion_depth=100)
        # json_format.ParseDict(msg["properties"], Observation_data, ignore_unknown_fields=True)
        # json_format.ParseDict(msg["properties"]["content"],
        #                       Observation_data, ignore_unknown_fields=True)

        # Observation_data["obstime"] = Timestamp().FromDatetime(
        #     datetime.strptime(msg["properties"]["datetime"],
        #                       "%Y-%m-%dT%H:%M:%S"))

        print(Observation_data)

        print(ts_metadata)

        request = dstore.PutObsRequest(
            observations=[
                dstore.Metadata1(
                    ts_mdata=ts_metadata,
                    obs_mdata=Observation_data
                )
            ]
        )

        try:
            self._stub.PutObservations(request)
        except grpc.RpcError as e:
            logger.critical(str(e))

    def ingest_list(self, msg_list: list) -> None:
        for i in msg_list:
            self.ingest(i)
