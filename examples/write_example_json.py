from esoh.ingest.main import ingest_to_pipeline
import xarray as xr
import json

"""
Small script to wirte json payloads instead of sending them to a mqtt-broker.
Supply path to netCDF file at commandline.

"""

# Only writes last message from the list of messages created in ingest._build_messages

ingest = ingest_to_pipeline(None, "testing", testing=True)

print("Load METno data")
path = "test/test_data/air_temperature_gullingen_skisenter-parent.nc"
ds = xr.load_dataset(path)

with open("schemas/netcdf_to_e_soh_message_metno.json") as file:
    j_read_netcdf = json.load(file)

json_msg = ingest._build_messages(
    ds, j_read_netcdf)[0]
json_msg["version"] = "v04"

with open(f"examples/{path.split('/')[-1].strip('.nc')}_meta.json", "w") as file:
    file.write(json.dumps(json_msg, indent=4))

print("Load KNMI data")
path = "test/test_data/20221231.nc"
ds = xr.load_dataset(path)
with open("schemas/netcdf_to_e_soh_message_knmi.json") as file:
    j_read_netcdf = json.load(file)

for station in ds.station:
    json_msg = ingest._build_messages(
        ds.sel(station=station), j_read_netcdf)[0]
    break


with open(f"examples/{path.split('/')[-1].strip('.nc')}_meta.json", "w") as file:
    file.write(json.dumps(json_msg, indent=4))
