#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_adp_worker_management import SourceADPWorkerManagement

if __name__ == "__main__":
    source = SourceADPWorkerManagement()
    launch(source, sys.argv[1:])
