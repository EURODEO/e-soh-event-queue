from glob import glob
from pybind11.setup_helpers import Pybind11Extension

ext_modules = [
    Pybind11Extension(
        "esoh.ingest.bufr.esoh",
        sorted(glob("src/esoh/ingest/bufr/*.cpp")),  # Sort source files for reproducibility
    ),
]
