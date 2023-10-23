from esoh.ingest.main import ingest_to_pipeline

import pytest
import glob

import xarray as xr

from jsonschema import Draft202012Validator, ValidationError
import json

from esoh.ingest.bufr.bufresohmsg_py import bufresohmsg_py, \
    init_bufrtables_py, \
    init_oscar_py, \
    destroy_bufrtables_py

# SurfaceLand_subset_29.bufr: 48987 missing geolocation and Oscar info
@pytest.mark.parametrize("bufr_file_path", glob.glob("test/test_data/bufr/*[!9].buf[r]"))
def test_verify_json_payload_bufr(bufr_file_path):
    # Load the schema
    with open("src/esoh/schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    init_bufrtables_py("")
    init_oscar_py("./src/esoh/ingest/bufr/oscar/oscar_stations_all.json")
    msg_build = ingest_to_pipeline(None, None, "testing", testing=True, schema_file="e-soh-message-spec.json")

    json_payloads = msg_build._build_messages(bufr_file_path, input_type="bufr")
    destroy_bufrtables_py()

    for payload in json_payloads:
        try:
            assert Draft202012Validator(e_soh_mqtt_message_schema).validate(
                payload) is None
        except ValidationError as e:
            print(e.context)
            raise ValidationError(e.message)


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/met_norway/*.nc"))
def test_verify_json_payload_metno_netcdf(netcdf_file_path):
    # Load the schema
    with open("src/esoh/schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    msg_build = ingest_to_pipeline(None, None, "testing", testing=True)

    json_payloads = msg_build._build_messages(ds, input_type="netCDF")

    for payload in json_payloads:
        try:
            assert Draft202012Validator(e_soh_mqtt_message_schema).validate(
                payload) is None
        except ValidationError as e:
            print(e.context)
            raise ValidationError(e.message)


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/knmi/*.nc"))
def test_verify_json_payload_knmi_netcdf(netcdf_file_path):
    with open("src/esoh/schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    msg_build = ingest_to_pipeline(None, None, "testing", testing=True)

    json_payloads = msg_build._build_messages(ds, input_type="netCDF")

    for payload in json_payloads:
        try:
            assert Draft202012Validator(e_soh_mqtt_message_schema).validate(
                payload) is None
        except ValidationError as e:
            print(e.message, "\n\n", e.cause)
            raise ValidationError(e.message)


if __name__ == "__main__":
    [test_verify_json_payload_knmi_netcdf(i) for i in glob.glob("test/test_data/knmi/*nc")]
    pass
