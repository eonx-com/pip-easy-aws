import importlib
import os
import tempfile
import uuid

from datetime import datetime
from EasyLambda.EasyLog import EasyLog


# noinspection PyBroadException
class EasyHelpers:
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
            EasyHelpers.get_class(module_name=module_name, class_name=class_name)
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
        EasyLog.trace('Searching for class method: {method_name}'.format(method_name=method_name))
        try:
            __class__ = EasyHelpers.get_class(module_name=module_name, class_name=class_name)
            class_methods = dir(__class__)
            for method_current in class_methods:
                if method_current == method_name:
                    EasyLog.debug('Method found: {method_name}'.format(method_name=method_name))
                    return True
        except Exception as find_exception:
            EasyLog.exception('Unhandled exception while searching for method', find_exception)
            return False

        EasyLog.debug('Method not found: {method_name}'.format(method_name=method_name))
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
        EasyLog.trace('Instantiating class from string: {module_name}.{class_name}...'.format(
            module_name=module_name,
            class_name=class_name
        ))
        try:
            # Attempt to import the module
            __module__ = importlib.import_module(module_name)
        except ImportError as module_exception:
            # The module could not be instantiated
            EasyLog.exception(EasyHelpers.ERROR_MODULE_NOT_FOUND, module_exception)
            raise Exception(EasyHelpers.ERROR_MODULE_NOT_FOUND)

        try:
            # Attempt to create the class
            __class__ = getattr(__module__, class_name)()
        except AttributeError as class_exception:
            # The class could not be instantiated
            EasyLog.exception(EasyHelpers.ERROR_CLASS_NOT_FOUND, class_exception)
            raise class_exception

        # Return the class
        return __class__

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
                EasyLog.debug('Created unique local temporary path: {local_path}'.format(local_path=local_path))
                return local_path
            if count > 10:
                raise Exception('Failed to create unique local filepath')
