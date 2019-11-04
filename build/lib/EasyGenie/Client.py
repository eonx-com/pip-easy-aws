#!/usr/bin/env python
# -*- coding: utf-8 -*-

import opsgenie_sdk


class Client:
    def __init__(self, key):
        """
        Setup OpsGenie Client

        :type key: str
        :param key: API Key
        """
        self.config = opsgenie_sdk.configuration.Configuration()
        self.config.api_key['Authorization'] = key

        self.api_client = opsgenie_sdk.api_client.ApiClient(configuration=self.config)
        self.alert_api = opsgenie_sdk.AlertApi(api_client=self.api_client)

    def send_alert(self, team, priority, alias, message, details=None):
        """
        Generate OpsGenie Alert

        :type team: str
        :param team: Team to alert

        :type priority: str
        :param priority: Alert priority

        :type alias: str
        :param alias: Alert alias

        :type message: str
        :param message: Message

        :type details: dict or None
        :param details: Optional details

        :return: SuccessResponse
        """
        body = opsgenie_sdk.CreateAlertPayload(
            message=message,
            alias=alias,
            responders=[{
                'name': team,
                'type': 'team'
            }],
            details=details,
            priority=priority
        )

        response = self.alert_api.create_alert(create_alert_payload=body)
        return response
