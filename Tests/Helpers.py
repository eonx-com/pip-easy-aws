#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import os
import tempfile
import uuid

from datetime import datetime
from EasyLog.Log import Log


# noinspection PyBroadException
class Helpers:
    # Error constants
    ERROR_CLASS_NOT_FOUND = 'The specified class does not exist'
    ERROR_MODULE_NOT_FOUND = 'The specified module does not exist'

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

    @staticmethod
    def is_class(module_name, class_name):
        """
        Return flag indicating the specified module/class exists

        :type module_name: str
        :param module_name: Name of the module

        :type class_name: str
        :param class_name: Name of the class

        :return: bool
        """
        try:
            Helpers.get_class(module_name=module_name, class_name=class_name)
            return True
        except Exception:
            return False

    @staticmethod
    def is_class_method(module_name, class_name, method_name):
        """
        Return flag indicating the specified method exists in the specified module/class

        :type module_name: str
        :param module_name: Name of the module

        :type class_name: str
        :param class_name: Name of the class

        :type method_name: str
        :param method_name: Name of the method

        :return: bool
        """
        Log.trace('Searching for class method: {method_name}'.format(method_name=method_name))
        try:
            __class__ = Helpers.get_class(module_name=module_name, class_name=class_name)
            class_methods = dir(__class__)
            for method_current in class_methods:
                if method_current == method_name:
                    Log.debug('Method found: {method_name}'.format(method_name=method_name))
                    return True
        except Exception as find_exception:
            Log.exception('Unhandled exception while searching for method', find_exception)
            return False

        Log.debug('Method not found: {method_name}'.format(method_name=method_name))
        return False

    @staticmethod
    def get_class(module_name, class_name):
        """
        Return a class from the specified module/class name strings
        
        :type module_name: str
        :param module_name: Name of the module

        :type class_name: str
        :param class_name: Name of the class

        :return: 
        """
        Log.trace('Instantiating class from string: {module_name}.{class_name}...'.format(
            module_name=module_name,
            class_name=class_name
        ))
        try:
            # Attempt to import the module
            __module__ = importlib.import_module(module_name)
        except ImportError as module_exception:
            # The module could not be instantiated
            Log.exception(Helpers.ERROR_MODULE_NOT_FOUND, module_exception)
            raise Exception(Helpers.ERROR_MODULE_NOT_FOUND)

        try:
            # Attempt to create the class
            __class__ = getattr(__module__, class_name)()
        except AttributeError as class_exception:
            # The class could not be instantiated
            Log.exception(Helpers.ERROR_CLASS_NOT_FOUND, class_exception)
            raise class_exception

        # Return the class
        return __class__

    @staticmethod
    def create_local_test_file(prefix='test') -> str:
        """
        Create a dummy local file filled with 100 UUIDs

        :type prefix: str
        :param prefix: Filename prefix (defaults to 'test')

        :return: str The filename
        """
        contents = ''
        for i in range(1, 100):
            contents += str(uuid.uuid4())

        filename = Helpers.create_unique_local_filename(prefix=prefix)
        file = open(filename, 'wt')
        file.write(contents)
        file.close()

        # Make sure the file exists

        if os.path.exists(filename) is False:
            raise Exception('Failed to create test file, the file could not be found')

        # Make sure the file can be opened and read

        file = open(filename, 'r')
        file_readable = file.readable()
        file.close()

        if file_readable is False:
            raise Exception('Failed to create test file, the resulting file is not readable')

        # Return the filename

        return filename

    @staticmethod
    def create_unique_local_filename(prefix='test') -> str:
        """
        Create a unique local filename

        :type prefix: str
        :param prefix: Optional filename prefix

        :return: str
        """
        temp_folder = Helpers.create_unique_local_temp_path()

        while True:
            filename = '{temp_folder}/{prefix}.{uuid}.txt'.format(
                temp_folder=temp_folder,
                prefix=prefix,
                uuid=uuid.uuid4()
            )
            if os.path.exists(filename) is False:
                break

        return filename

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
