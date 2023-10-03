#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from math import floor
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import logging
import requests
import tempfile
from airbyte_cdk.sources.streams.http import HttpStream

logging.basicConfig(level=logging.DEBUG)
token_url = f"https://accounts.adp.com/auth/oauth/v2/token"

# Basic full refresh stream
class ADPWorkerManagementStream(HttpStream, ABC):
    """
    Parent class extended by all stream-specific classes
    """

    def __init__(self, config, token_url, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.page = 0

        # Write the SSL certificate and key data to temporary files.
        ssl_cert_file = tempfile.NamedTemporaryFile(delete=False)
        ssl_cert_file.write(config['ssl_cert'].encode())
        ssl_cert_file.close()

        ssl_key_file = tempfile.NamedTemporaryFile(delete=False)
        ssl_key_file.write(config['ssl_key'].encode())
        ssl_key_file.close()

        self.ssl_cert = (ssl_cert_file.name, ssl_key_file.name)

        # Read credentials from config
        credentials = (config['client_id'], config['client_secret'])

        # Obtain short-lived access token
        # TODO(): implement a strat for refreshing this token as needed (probably a try-catch for auth errors?)
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {'grant_type': 'client_credentials'}
        response = requests.post(url=token_url, 
                                 headers=headers,
                                 data=body,
                                 auth=(credentials[0], credentials[1]),
                                 cert=self.ssl_cert
                                 )
        self.access_token = response.json()['access_token']

        # Get total records (use in pagination logic to avert disaster if total records are a multiple of 100)
        headers = {"Authorization": "Bearer " + self.access_token}
        records_count_url = "https://api.adp.com/hr/v2/workers?count=true"
        records_count_response = requests.get(records_count_url, headers=headers, cert=self.ssl_cert)      
        total_records = records_count_response.json()['meta']['totalNumber']

        # Each page has 100 records, so this logic determines the total number of pages to read
        if total_records%100 == 0:
            self.final_page = int((total_records/100) - 1)
        else:
            self.final_page = floor(total_records/100)

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": "Bearer " + self.access_token}
    
    def request_kwargs(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        """
        Override to return a mapping of keyword arguments to be used when creating the HTTP request.
        Any option listed in https://docs.python-requests.org/en/latest/api/#requests.adapters.BaseAdapter.send for can be returned from
        this method. Note that these options do not conflict with request-level options such as headers, request params, etc..
        """
        return {"cert": self.ssl_cert}

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        How pagination works for this API:

        Each request fetches up to 100 records at a time.
        The first call (where self.page==0) returns the first 100 records (using "$top=100").
        Every time the response returns 100 records:
            - increment self.page by 1
            - skip the first 100*{page} records
            - grab the next 100 by returning endpoint = f"workers?$skip={self.page}00&$top=100"
            (see path() in Workers class below - this method just increments self.page)

        """
   
        if self.page < self.final_page:
            self.page += 1
            return True
        else:
            return False

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        
        response_json = response.json()['workers']
        yield from response_json

class Workers(ADPWorkerManagementStream):
    
    primary_key = "meta" # this means nothing, but the HttpStream class requires something ¯\_(ツ)_/¯
    url_base = "https://api.adp.com/hr/v2/"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        if self.page == 0:
            endpoint = f"workers?$top=100"
        else:
            endpoint = f"workers?$skip={self.page}00&$top=100"

        return endpoint