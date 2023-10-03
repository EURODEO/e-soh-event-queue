import esoh.datastore_pb2 as dstore
import esoh.datastore_pb2_grpc as dstore_grpc

import grpc
import logging

from google.protobuf import json_format


logger = logging.getLogger(__name__)


class datastore_connection():
    def __init__(self, DSHOST: str, DSPORT: str) -> None:
        self._channel = grpc.insecure_channel(DSHOST + ":" + DSPORT)
        self._stub = dstore_grpc.Datastorestub(self._channel)

    def ingest(self, msg: str) -> None:
        ts_metadata = dstore.TSMetadata()
        json_format.Parse(msg, ts_metadata, ignore_unknown_fields=True)

        Observation_data = dstore.ObsMetadata()
        json_format.Parse(msg, Observation_data, ignore_unknown_fields=True)

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
