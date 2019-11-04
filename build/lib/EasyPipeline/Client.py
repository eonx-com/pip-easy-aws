#!/usr/bin/env python
# -*- coding: utf-8 -*-
import importlib

from EasyLog.Log import Log
from EasyPipeline.ClientError import ClientError


# noinspection DuplicatedCode
class Client:
    # Pipeline storage
    __pipelines__ = {}

    @staticmethod
    def load_pipeline(reference, configuration) -> None:
        """
        Load pipeline into the stack

        :type reference: str
        :param reference: Unique user-defined reference to assign to the pipeline

        :type configuration: dict
        :param configuration: The pipelines configuration

        :return:
        """
        Log.trace('Loading pipeline: {reference}...')

        # Make sure the reference hasn't already been used
        if reference in Client.__pipelines__:
            raise Exception(ClientError.ERROR_PIPELINE_REFERENCE_ALREADY_EXISTS)

        # Validate the pipeline
        if Client.is_pipeline_valid(configuration) is False:
            Log.error('The pipeline could not be validated'.format(reference=reference))
            raise Exception(ClientError.ERROR_PIPELINE_VALIDATION_FAILED)

        # If there are no pipelines, add empty lists
        if 'success_pipelines' not in configuration:
            configuration['success_pipelines'] = []

        if 'failure_pipelines' not in configuration:
            configuration['failure_pipelines'] = []

        # Instantiate the pipeline and store it
        Client.__pipelines__[reference] = {
            'success_pipelines': configuration['success_pipelines'],
            'failure_pipelines': configuration['failure_pipelines'],
            'callable': Client.get_class(
                module_name=configuration['module'],
                class_name=configuration['class']
            )
        }

        if 'inputs' in configuration:
            if isinstance(configuration['inputs'], dict) is True:
                for input_name in configuration['inputs'].keys():
                    Log.debug('Loading input value: {input_name}'.format(input_name=input_name))
                    Client.__pipelines__[reference]['callable'].set_input(
                        name=input_name,
                        value=configuration['inputs'][input_name]
                    )

    @staticmethod
    def clear_pipelines() -> None:
        """
        Clear all previously loaded pipelines

        :return: None
        """
        Client.__pipelines__ = {}

    @staticmethod
    def get_pipelines() -> dict:
        """
        Return all pipelines

        :return: dict
        """
        return Client.__pipelines__

    @staticmethod
    def get_pipeline(reference) -> callable:
        """
        Return all pipelines

        :type reference: str
        :param reference: The name of the pipeline

        :return: dict
        """
        if reference not in Client.__pipelines__:
            Log.error('Could not find required pipeline: {reference}'.format(reference=reference))
            raise Exception(ClientError.ERROR_PIPELINE_REFERENCE_NOT_FOUND)

        return Client.__pipelines__[reference]

    @staticmethod
    def load_pipelines(pipelines) -> None:
        """
        Load all pipelines from a list that are going to be executed

        :type pipelines: list
        :param pipelines: List of pipelines to be executed

        :return: None
        """
        # Start by loading all of the pipelines
        for pipeline in pipelines:
            Client.load_pipeline(
                reference=pipeline['reference'],
                configuration=pipeline['configuration']
            )

    @staticmethod
    def is_pipeline_list_valid(pipeline_list) -> bool:
        """
        Ensure that the pipeline list is valid

        :type pipeline_list: list
        :param pipeline_list: The list of pipelines to test

        :return: bool
        """
        error = False

        for pipeline in pipeline_list:
            if 'reference' not in pipeline:
                Log.error('Required pipeline list parameter not found: reference')
                error = True

            # If inputs are defined, ensure they are in a dictionary
            if 'inputs' in pipeline:
                if pipeline['inputs'] is not None:
                    if isinstance(pipeline['inputs'], dict) is False:
                        Log.error('The pipeline contains an input value that is not a valid dictionary')
                        error = True

            if 'failure_pipelines' in pipeline:
                if 'failure_pipelines' is not None:
                    if isinstance(pipeline['failure_pipelines'], list) is False:
                        Log.error('failure pipelines was not a valid list type')
                        error = True
                    else:
                        for failure_pipeline in pipeline['failure_pipelines']:
                            if Client.has_pipeline(failure_pipeline) is False:
                                Log.error('Required pipeline specified in next_pipeline does not exist: "{reference}"'.format(reference=pipeline['failure_pipelines']))
                                error = True

            if 'success_pipelines' in pipeline:
                if 'success_pipelines' is not None:
                    if isinstance(pipeline['success_pipelines'], list) is False:
                        Log.error('Success pipelines was not a valid list type')
                        error = True
                    else:
                        for success_pipeline in pipeline['success_pipelines']:
                            if Client.has_pipeline(success_pipeline) is False:
                                Log.error('Required pipeline specified in next_pipeline does not exist: "{reference}"'.format(reference=pipeline['success_pipelines']))
                                error = True

        return error is False

    @staticmethod
    def execute_pipeline(reference, inputs=None):
        """
        Execute a chain of pipelines

        :type reference: str
        :param reference: The reference to the pipeline that should be executed

        :type inputs: dict
        :param inputs: Optional dictionary of inputs
        """
        Log.trace('Executing pipeline: {reference}...'.format(reference=reference))

        pipeline = Client.get_pipeline(reference)
        pipeline_callable = pipeline['callable']

        if inputs is not None:
            for key in inputs.keys():
                pipeline_callable.set_input(name=key, value=inputs[key])

        # Call the setup function
        pipeline_callable.setup()

        # Execute the pipeline and get the result
        result = pipeline_callable.execute()

        # Get the outputs of execution
        outputs = pipeline_callable.get_outputs()

        # Check the result of execution, it should be either True/False or the name of another pipeline to run
        if isinstance(result, str):
            # If a string is supplied it should be the name of another pipeline to execute
            if Client.has_pipeline(result) is False:
                Log.error('Execution of pipeline returned a string that could not be resolved to a known pipeline: {result}'.format(result=result))
                raise Exception(ClientError.ERROR_PIPELINE_STRING_RESULT_INVALID)

            # Execute the pipeline indicated in the string
            outputs.update(Client.execute_pipeline(result, outputs))
        elif result is True:
            # Execution was successfully, if there are success pipelines, execute them
            Log.debug('Execution successful, searching for success pipelines...')
            if 'success_pipelines' in pipeline:
                for success_pipeline in pipeline['success_pipelines']:
                    Log.debug('Executing success pipeline: {success_pipeline}'.format(success_pipeline=success_pipeline))
                    outputs.update(Client.execute_pipeline(success_pipeline, outputs))
        elif result is False:
            # Execution was failed, if there are failure pipelines, execute them
            Log.debug('Execution failed, searching for failure pipelines...')
            if 'failure_pipelines' in pipeline:
                for failure_pipeline in pipeline['failure_pipelines']:
                    Log.debug('Executing failure pipeline: {failure_pipeline}'.format(failure_pipeline=failure_pipeline))
                    outputs.update(Client.execute_pipeline(failure_pipeline, outputs))
        else:
            # Unknown type of result supplied
            Log.error('Execution of pipeline returned an invalid result, all pipelines must return a boolean value')
            raise Exception(ClientError.ERROR_PIPELINE_RESULT_INVALID)

        Log.debug('Pipeline execution completed: {reference}'.format(reference=reference))

        # Pass outputs of each function along the execution chain
        return outputs

    @staticmethod
    def has_pipeline(reference) -> bool:
        """
        Return boolean flag indicating whether the specified pipeline reference exists

        :type reference: str
        :param reference: The pipeline reference to search for

        :return: bool
        """
        return reference in Client.__pipelines__

    @staticmethod
    def is_pipeline_valid(configuration) -> bool:
        """
        Validate that the configuration is syntactically correct

        :type configuration: dict
        :param configuration: The pipelines configuration

        :return: bool
        """
        Log.trace('Validating pipeline...')

        error = False

        # Make sure the module is specified
        if 'module' not in configuration:
            Log.error(ClientError.ERROR_PIPELINE_MODULE_NAME_NOT_FOUND)
            error = True

        # Make sure the class is specified
        if 'class' not in configuration:
            Log.error(ClientError.ERROR_PIPELINE_CLASS_NAME_NOT_FOUND)
            error = True

        # Make sure all required methods exist in the pipeline class
        for method_name in (
                # Setup pipeline function
                'setup',
                # List all of the pipelines inputs
                'list_inputs',
                # Check if input exists
                'has_input',
                # Set an input value
                'set_input',
                # Get an input value
                'get_input',
                # Get all inputs
                'get_inputs',
                # List all of the pipelines output
                'list_outputs',
                # Check if output exists
                'has_output',
                # Set an output value
                'set_output',
                # Get an output value
                'get_output',
                # Get all outputs
                'get_outputs',
                # Execute the pipeline
                'execute'
        ):
            if Client.is_class_method(
                    module_name=configuration['module'],
                    class_name=configuration['class'],
                    method_name=method_name,
            ) is False:
                Log.error('Could not locate required class method: {method_name}'.format(method_name=method_name))
                Log.error(ClientError.ERROR_PIPELINE_CLASS_METHOD_NOT_FOUND)
                error = True

        # Return validation result
        return error is False

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
            __class__ = Client.get_class(module_name=module_name, class_name=class_name)
            class_methods = dir(__class__)
            for method_current in class_methods:
                if method_current == method_name:
                    Log.debug('Method found: {method_name}'.format(method_name=method_name))
                    return True
        except Exception as find_exception:
            Log.exception('Unhandled base_exception while searching for method', find_exception)
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
            Log.exception(ClientError.ERROR_PIPELINE_MODULE_NOT_FOUND, module_exception)
            raise Exception(ClientError.ERROR_PIPELINE_MODULE_NOT_FOUND)

        try:
            # Attempt to create the class
            __class__ = getattr(__module__, class_name)()
        except AttributeError as class_exception:
            # The class could not be instantiated
            Log.exception(ClientError.ERROR_PIPELINE_CLASS_NOT_FOUND, class_exception)
            raise class_exception

        # Return the class
        return __class__
