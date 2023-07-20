#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_jazzhr.streams import (
    Activities,
    Applicants,
    ApplicantsToJobs,
    CategoriesToApplicants,
    Categories,
    Contacts,
    Hires,
    Jobs,
    QuestionnaireAnswers,
    Tasks,
    Users,
)


# Source
class SourceADPWorkerManagement(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Checks to see if a connection to JazzHR API can be created with given credentials.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        api_key = config["api_key"]
        connection_url = f"https://api.resumatorapi.com/v1/categories?apikey={api_key}"
        try:
            response = requests.get(url=connection_url)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [
            Activities(config=config),
            Applicants(config=config),
            ApplicantsToJobs(config=config),
            CategoriesToApplicants(config=config),
            Categories(config=config),
            Contacts(config=config),
            Hires(config=config),
            Jobs(config=config),
            QuestionnaireAnswers(config=config),
            Tasks(config=config),
            Users(config=config),
        ]
