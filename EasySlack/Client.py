#!/usr/bin/env python
# -*- coding: utf-8 -*-

import slack

from EasySlack.ClientError import ClientError


class Client:
    def __init__(self, token):
        """
        Setup slack client

        :type token: str
        :param token: Slack API token
        """
        self.__token__ = token
        self.__client__ = slack.WebClient(token=token)

    def send_file(self, channel, local_filename):
        """
        Send a file to a slack channel

        :type channel: str
        :param channel: The channel to send the message to

        :type local_filename: str
        :param local_filename: Path/filename to upload
        """
        print('Sending File To Slack Channel...')
        print('Channel: {channel}'.format(channel=channel))
        print('Filename: {local_filename}'.format(local_filename=local_filename))

        try:
            response = self.__client__.files_upload(channel=channel, file=local_filename)

            try:
                assert response['ok']
            except AssertionError:
                # Failed to asset message send correctly
                print(ClientError.ERROR_SLACK_SEND_FILE_FAILED)
        except Exception as send_exception:
            # Failed to send message due to an unhandled exception
            print(ClientError.ERROR_SLACK_SEND_FILE_UNHANDLED_EXCEPTION)
            print(send_exception)

    def send_message(self, channel, message):
        """
        Send a slack message toa  channel

        :type channel: str
        :param channel: The channel to send the message to

        :type message: str
        :param message: The message to send
        """
        print('Sending Message To Slack Channel...')
        print('Channel: {channel}'.format(channel=channel))
        print('Message: {message}'.format(message=message))

        try:
            response = self.__client__.chat_postMessage(channel=channel, message=message)

            try:
                assert response['ok']
                assert response['message']['text'] == message
            except AssertionError:
                # Failed to asset message send correctly
                print(ClientError.ERROR_SLACK_SEND_MESSAGE_FAILED)
        except Exception as send_exception:
            # Failed to send message due to an unhandled exception
            print(ClientError.ERROR_SLACK_SEND_MESSAGE_UNHANDLED_EXCEPTION)
            print(send_exception)
