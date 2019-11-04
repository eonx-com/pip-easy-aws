#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    ERROR_UNHANDLED_EXCEPTION = ' Please review additional error messages.'

    # Send Message Errors
    ERROR_SLACK_SEND = 'An unexpected error occurred while sending Slack message.'
    ERROR_SLACK_SEND_MESSAGE_UNHANDLED_EXCEPTION = ERROR_SLACK_SEND + ERROR_UNHANDLED_EXCEPTION
    ERROR_SLACK_SEND_MESSAGE_FAILED = ERROR_SLACK_SEND + ' Failed to assert message sent successfully'

    # Send File Errors
    ERROR_SLACK_SEND_FILE = 'An unexpected error occurred while sending a file to Slack.'
    ERROR_SLACK_SEND_FILE_FAILED = ERROR_SLACK_SEND_FILE + ' Failed to assert file sent successfully'
    ERROR_SLACK_SEND_FILE_UNHANDLED_EXCEPTION = ERROR_SLACK_SEND_FILE + ERROR_UNHANDLED_EXCEPTION
