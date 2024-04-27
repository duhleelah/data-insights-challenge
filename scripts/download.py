import os
from urllib.request import urlretrieve
from zipfile import ZipFile
import opendatasets as op

OUTPUT_DIR = "landing/"
DOWNLOAD_URL =

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
