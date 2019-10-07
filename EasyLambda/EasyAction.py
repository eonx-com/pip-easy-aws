from abc import abstractmethod

from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.EasyLog import EasyLog


class EasyAction:
    # Error constants
    ERROR_ACTION_VALIDATION_FAILED = 'One or more actions failed validation'
    ERROR_ACTION_LIST_VALIDATION_FAILED = 'The action list failed validation'
    ERROR_ACTION_REFERENCE_ALREADY_EXISTS = 'The specified action reference already exists'
    ERROR_ACTION_MODULE_NAME_NOT_FOUND = 'Required module name could not be found in action configuration'
    ERROR_ACTION_CLASS_NAME_NOT_FOUND = 'Required class name could not be found in action configuration'
    ERROR_ACTION_CLASS_METHOD_NOT_FOUND = 'One or more required class action methods could not be found'
    ERROR_ACTION_REFERENCE_NOT_FOUND = 'One or more required actions could not be found'
    ERROR_ACTION_RESULT_INVALID = 'The action did not return a valid boolean value'
    ERROR_ACTION_STRING_RESULT_INVALID = 'The action returned a string that could not be resolved to a known action'

    # Action storage
    __actions__ = {}

    @staticmethod
    def load_action(reference, configuration) -> None:
        """
        Load an action into the stack

        :type reference: str
        :param reference: Unique user-defined reference to assign to the action

        :type configuration: dict
        :param configuration: The actions configuration

        :return:
        """
        EasyLog.trace('Loading action: {reference}...')

        # Make sure the reference hasn't already been used
        if reference in EasyAction.__actions__:
            raise Exception(EasyAction.ERROR_ACTION_REFERENCE_ALREADY_EXISTS)

        # Validate the action
        if EasyAction.is_action_valid(configuration) is False:
            EasyLog.error('The action could not be validated'.format(reference=reference))
            raise Exception(EasyAction.ERROR_ACTION_VALIDATION_FAILED)

        # If there are no actions, add empty lists
        if 'success_actions' not in configuration:
            configuration['success_actions'] = []

        if 'failure_actions' not in configuration:
            configuration['failure_actions'] = []

        # Instantiate the action and store it
        EasyAction.__actions__[reference] = {
            'success_actions': configuration['success_actions'],
            'failure_actions': configuration['failure_actions'],
            'callable': EasyHelpers.get_class(
                module_name=configuration['module'],
                class_name=configuration['class']
            )
        }

        if 'inputs' in configuration:
            if isinstance(configuration['inputs'], dict) is True:
                for input_name in configuration['inputs'].keys():
                    EasyLog.debug('Loading input value: {input_name}'.format(input_name=input_name))
                    EasyAction.__actions__[reference]['callable'].set_input(
                        name=input_name,
                        value=configuration['inputs'][input_name]
                    )

    @staticmethod
    def clear_actions() -> None:
        """
        Clear all previously loaded actions

        :return: None
        """
        EasyAction.__actions__ = {}

    @staticmethod
    def get_actions() -> dict:
        """
        Return all actions

        :return: dict
        """
        return EasyAction.__actions__

    @staticmethod
    def get_action(reference) -> callable:
        """
        Return all actions

        :type reference: str
        :param reference: The name of the action

        :return: dict
        """
        if reference not in EasyAction.__actions__:
            EasyLog.error('Could not find required action: {reference}'.format(reference=reference))
            raise Exception(EasyAction.ERROR_ACTION_REFERENCE_NOT_FOUND)

        return EasyAction.__actions__[reference]

    @staticmethod
    def load_actions(actions) -> None:
        """
        Load all actions from a list that are going to be executed

        :type actions: list
        :param actions: List of actions to be executed

        :return: None
        """
        # Start by loading all of the actions
        for action in actions:
            EasyAction.load_action(
                reference=action['reference'],
                configuration=action['configuration']
            )

    @staticmethod
    def is_action_list_valid(action_list) -> bool:
        """
        Ensure that the action list is valid

        :type action_list: list
        :param action_list: The list of actions to test

        :return: bool
        """
        error = False

        for action in action_list:
            if 'reference' not in action:
                EasyLog.error('Required action list parameter not found: reference')
                error = True

            # If inputs are defined, ensure they are in a dictionary
            if 'inputs' in action:
                if action['inputs'] is not None:
                    if isinstance(action['inputs'], dict) is False:
                        EasyLog.error('The action contains an input value that is not a valid dictionary')
                        error = True

            if 'failure_actions' in action:
                if 'failure_actions' is not None:
                    if isinstance(action['failure_actions'], list) is False:
                        EasyLog.error('failure actions was not a valid list type')
                        error = True
                    else:
                        for failure_action in action['failure_actions']:
                            if EasyAction.has_action(failure_action) is False:
                                EasyLog.error('Required action specified in next_action does not exist: "{reference}"'.format(reference=action['failure_actions']))
                                error = True

            if 'success_actions' in action:
                if 'success_actions' is not None:
                    if isinstance(action['success_actions'], list) is False:
                        EasyLog.error('Success actions was not a valid list type')
                        error = True
                    else:
                        for success_action in action['success_actions']:
                            if EasyAction.has_action(success_action) is False:
                                EasyLog.error('Required action specified in next_action does not exist: "{reference}"'.format(reference=action['success_actions']))
                                error = True

        return error is False

    @staticmethod
    def execute_action(reference, inputs=None):
        """
        Execute a chain of actions

        :type reference: str
        :param reference: The reference to the action that should be executed

        :type inputs: dict
        :param inputs: Optional dictionary of inputs
        """
        EasyLog.trace('Executing action: {reference}...'.format(reference=reference))

        action = EasyAction.get_action(reference)
        action_callable = action['callable']

        if inputs is not None:
            for key in inputs.keys():
                action_callable.set_input(name=key, value=inputs[key])

        # Call the setup function
        action_callable.setup()

        # Execute the action and get the result
        result = action_callable.execute()

        # Get the outputs of execution
        outputs = action_callable.get_outputs()

        # Check the result of execution, it should be either True/False or the name of another action to run
        if isinstance(result, str):
            # If a string is supplied it should be the name of another action to execute
            if EasyAction.has_action(result) is False:
                EasyLog.error('Execution of action returned a string that could not be resolved to a known action: {result}'.format(result=result))
                raise Exception(EasyAction.ERROR_ACTION_STRING_RESULT_INVALID)

            # Execute the action indicated in the string
            outputs.update(EasyAction.execute_action(result, outputs))
        elif result is True:
            # Execution was successfully, if there are success actions, execute them
            EasyLog.debug('Execution successful, searching for success actions...')
            if 'success_actions' in action:
                for success_action in action['success_actions']:
                    EasyLog.debug('Executing success action: {success_action}'.format(success_action=success_action))
                    outputs.update(EasyAction.execute_action(success_action, outputs))
        elif result is False:
            # Execution was failed, if there are failure actions, execute them
            EasyLog.debug('Execution failed, searching for failure actions...')
            if 'failure_actions' in action:
                for failure_action in action['failure_actions']:
                    EasyLog.debug('Executing failure action: {failure_action}'.format(failure_action=failure_action))
                    outputs.update(EasyAction.execute_action(failure_action, outputs))
        else:
            # Unknown type of result supplied
            EasyLog.error('Execution of action returned an invalid result, all actions must return a boolean value')
            raise Exception(EasyAction.ERROR_ACTION_RESULT_INVALID)

        EasyLog.debug('Action execution completed: {reference}'.format(reference=reference))

        # Pass outputs of each function along the execution chain
        return outputs

    @staticmethod
    def has_action(reference) -> bool:
        """
        Return boolean flag indicating whether the specified action reference exists

        :type reference: str
        :param reference: The action reference to search for

        :return: bool
        """
        return reference in EasyAction.__actions__

    @staticmethod
    def is_action_valid(configuration) -> bool:
        """
        Validate that the configuration is syntactically correct

        :type configuration: dict
        :param configuration: The actions configuration

        :return: bool
        """
        EasyLog.trace('Validating action...')

        error = False

        # Make sure the module is specified
        if 'module' not in configuration:
            EasyLog.error(EasyAction.ERROR_ACTION_MODULE_NAME_NOT_FOUND)
            error = True

        # Make sure the class is specified
        if 'class' not in configuration:
            EasyLog.error(EasyAction.ERROR_ACTION_CLASS_NAME_NOT_FOUND)
            error = True

        # Make sure all required methods exist in the action class
        for method_name in (
                # Setup action function
                'setup',
                # List all of the actions inputs
                'list_inputs',
                # Check if input exists
                'has_input',
                # Set an input value
                'set_input',
                # Get an input value
                'get_input',
                # Get all inputs
                'get_inputs',
                # List all of the actions output
                'list_outputs',
                # Check if output exists
                'has_output',
                # Set an output value
                'set_output',
                # Get an output value
                'get_output',
                # Get all outputs
                'get_outputs',
                # Execute the action
                'execute'
        ):
            if EasyHelpers.is_class_method(
                    module_name=configuration['module'],
                    class_name=configuration['class'],
                    method_name=method_name,
            ) is False:
                EasyLog.error('Could not locate required class method: {method_name}'.format(method_name=method_name))
                EasyLog.error(EasyAction.ERROR_ACTION_CLASS_METHOD_NOT_FOUND)
                error = True

        # Return validation result
        return error is False


class BaseAction:
    def __init__(self):
        """
        """
        self.__inputs__ = {}
        self.__outputs__ = {}

        self.setup()

    def setup(self) -> None:
        """
        Perform action setup

        :return: None
        """
        pass

    @abstractmethod
    def execute(self):
        """
        Perform the action

        :return: bool or str
        """
        pass

    def list_inputs(self) -> list:
        """
        List all available inputs

        :return: list
        """
        return list(self.__inputs__.keys())

    def has_input(self, name) -> bool:
        """
        Check if specified input exists

        :type name: str
        :param name: Name of the input

        :return: bool
        """
        return name in self.__inputs__

    def get_input(self, name):
        """
        Return specified input (or None if does not exist)

        :type name: str
        :param name: Name of the input

        :return:
        """
        if self.has_input(name) is False:
            return None

        return self.__inputs__[name]

    def get_inputs(self) -> dict:
        """
        Return all inputs
        
        :return: dict 
        """
        return self.__inputs__

    def set_input(self, name, value) -> None:
        """
        Set an input value

        :type name: str
        :param name: Name of the input to set

        :param value: The value to set

        :return:EasyAction None
        """
        self.__inputs__[name] = value

    def list_outputs(self) -> list:
        """
        List all available outputs

        :return: list
        """
        return list(self.__outputs__.keys())

    def has_output(self, name) -> bool:
        """
        Check if specified output exists

        :type name: str
        :param name: Name of the output

        :return: bool
        """
        return name in self.__outputs__

    def get_output(self, name):
        """
        Return specified output (or None if does not exist)

        :type name: str
        :param name: Name of the output

        :return:
        """
        if self.has_output(name) is False:
            return None

        return self.__outputs__[name]

    def set_output(self, name, value) -> None:
        """
        Set an output value

        :type name: str
        :param name: Name of the output to set

        :param value: The value to set

        :return:EasyAction None
        """
        self.__outputs__[name] = value

    def get_outputs(self) -> dict:
        """
        Return all outputs

        :return: dict 
        """
        return self.__outputs__
