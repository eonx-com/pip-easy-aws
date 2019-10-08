#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
import uuid

from datetime import datetime
from EasyLog.Log import Log


class Helpers:
    @staticmethod
    def datetime_iso_8601(date_time=None) -> str:
        """
        Return ISO-8601 formatted datetime string

        :type date_time: datetime or None
        :param date_time: The date/time to convert to ISO-8601. If not supplied the current time will be used

        :return:
        """
        if date_time is None:
            date_time = datetime.now()

        return date_time.strftime("%Y-%m-%d %H:%M:%S")

    # noinspection DuplicatedCode
    @staticmethod
    def create_unique_local_temp_path() -> str:
        """
        Create a new local path inside the system temp folder that is guaranteed to be unique

        :return: str
        """
        count = 0
        temp_path = tempfile.gettempdir()

        while True:
            count = count + 1
            local_path = '{temp_path}/{uuid}'.format(temp_path=temp_path, uuid=uuid.uuid4())
            if os.path.exists(local_path) is False:
                os.makedirs(local_path, exist_ok=False)
                Log.debug('Created unique local temporary path: {local_path}'.format(local_path=local_path))
                return local_path
            if count > 10:
                raise Exception('Failed to create unique local filepath')
