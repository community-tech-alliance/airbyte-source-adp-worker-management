#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

import requests
import tempfile
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_adp_worker_management.streams import (
    Workers
)

token_url = f"https://accounts.adp.com/auth/oauth/v2/token"

# Source
class SourceADPWorkerManagement(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Checks to see if a connection to ADP Worker Management  API can be created with given credentials.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        # Write the SSL certificate and key data to temporary files.
        ssl_cert_file = tempfile.NamedTemporaryFile(delete=False)
        ssl_cert_file.write(config['ssl_cert'].encode())
        ssl_cert_file.close()

        ssl_key_file = tempfile.NamedTemporaryFile(delete=False)
        ssl_key_file.write(config['ssl_key'].encode())
        ssl_key_file.close()

        ssl_cert = (ssl_cert_file.name, ssl_key_file.name)
        credentials = (config['client_id'], config['client_secret'])

        # Obtain short-lived access token
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {'grant_type': 'client_credentials'}
        try:
            response = requests.post(url=token_url, 
                                 headers=headers,
                                 data=body,
                                 auth=(credentials[0], credentials[1]),
                                 cert=ssl_cert
                                 )
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [
            Workers(config=config,
                    token_url=token_url
                    )
        ]
