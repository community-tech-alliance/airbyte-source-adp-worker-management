#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import logging
import requests
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class JazzHRStream(HttpStream, ABC):
    """
    Parent class extended by all stream-specific classes
    """

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.url_base = "https://api.resumatorapi.com/v1/"
        self.page = 1

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": "OAuth " + self.config["api_key"]}

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Pagination for all endpoints is achieved by incrementing `/page/#` in the request URL
        The response does not indicate how many pages there will be,
        so we just need to keep trying to fetch the next page of results

        Each page of results has a max of 100 records,
        so we only increment if the current page returned 100 records
        """

        # WHILE TESTING, limit to 5 pages of results
        # (or we're gonna be here forever)
        # if len(response.json())==100 and self.page<5:
        if len(response.json()) == 100:
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
        response_json = response.json()
        yield from response_json


class Activities(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"activities/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Applicants(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"applicants/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class ApplicantsToJobs(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"applicants2jobs/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class CategoriesToApplicants(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"categories2applicants/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Categories(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"categories/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Contacts(JazzHRStream):
    """
    No records returned with the API key used for development,
    so who knows what this stream contains?
    (not me)
    """

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"contacts/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Hires(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"hires/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Jobs(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"jobs/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class QuestionnaireAnswers(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"questionnaire_answers/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Tasks(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"tasks/page/{self.page}/?apikey={self.api_key}"

        return endpoint


class Users(JazzHRStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = f"users/page/{self.page}/?apikey={self.api_key}"

        return endpoint
