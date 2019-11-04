#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyLog.Log import Log


class Client:
    # Validation rule types
    RULE_ALL = 'ALL'
    RULE_ALL_OR_NOTHING = 'ALL_OR_NOTHING'
    RULE_ANY = 'ANY'
    RULE_NONE = 'NONE'
    RULE_TYPE = 'TYPE'
    RULE_CALLBACK = 'CALLBACK'

    # Error constants
    ERROR_MALFORMED_RULESET = 'The validation ruleset was invalid'

    @staticmethod
    def validate(ruleset, data) -> bool:
        """
        Perform validation

        :type ruleset: list
        :param ruleset: The rules to be applied

        :type data: dict
        :param data: The data to be validated

        :return: bool
        """
        Log.trace('Validating data...')

        error = False

        # Validate the ruleset before we start validation
        Client.validate_ruleset(ruleset)

        # Iterate through each rule in ruleset
        for rule in ruleset:
            # Get rule type and its configuration
            rule_type = rule['type']

            # Validate each rule passes, as soon as one fails- everything fails
            if rule_type == Client.RULE_ANY:
                Log.debug('Validating any rule...')
                if Client.validate_any(rule=rule, data=data) is False:
                    Log.debug('Validation of any rule failed')
                    error = True
            elif rule_type == Client.RULE_ALL:
                Log.debug('Validating all rule...')
                if Client.validate_all(rule=rule, data=data) is False:
                    Log.debug('Validation of all rule failed')
                    error = True
            elif rule_type == Client.RULE_ALL_OR_NOTHING:
                Log.debug('Validating all or nothing rule...')
                if Client.validate_all_any_nothing(rule=rule, data=data) is False:
                    Log.debug('Validation of all or nothing rule failed')
                    error = True
            elif rule_type == Client.RULE_NONE:
                Log.debug('Validating none rule...')
                if Client.validate_none(rule=rule, data=data) is False:
                    Log.debug('Validation of none rule failed')
                    error = True
            elif rule_type == Client.RULE_TYPE:
                Log.debug('Validating type rule...')
                if Client.validate_type(rule=rule, data=data) is False:
                    Log.debug('Validation of type rule failed')
                    error = True
            elif rule_type == Client.RULE_CALLBACK:
                Log.debug('Validating using custom callback_staked...')
                validation_function = rule['validation_function']
                if validation_function() is False:
                    Log.debug('Validation by custom callback_staked failed')
                    error = True

        # Validation was successful
        return error

    @staticmethod
    def validate_ruleset(ruleset) -> bool:
        """
        Validate set of rules are syntactically correct

        :type ruleset: list
        :param ruleset: The ruleset to test

        :return:
        """
        Log.trace('Validating ruleset...')

        # Make sure we received a list
        if isinstance(ruleset, list) is False:
            Log.error('Ruleset was not the expected list type')
            return False

        # Iterate through each rule in the list
        for rule in ruleset:
            # Make sure rule is a dictionary type object
            if isinstance(rule, dict) is False:
                Log.error(Client.ERROR_MALFORMED_RULESET)
                return False

            # Each rule must have a type specified
            if 'type' not in rule:
                Log.error(Client.ERROR_MALFORMED_RULESET)
                return False

            # Ensure rule is of a known type
            if rule['type'] not in (
                    Client.RULE_ALL,
                    Client.RULE_ALL_OR_NOTHING,
                    Client.RULE_ANY,
                    Client.RULE_CALLBACK,
                    Client.RULE_NONE
            ):
                Log.error(Client.ERROR_MALFORMED_RULESET)
                return False

            # Make sure values are defined for these rule types
            if rule['type'] in (
                    Client.RULE_ALL,
                    Client.RULE_ALL_OR_NOTHING,
                    Client.RULE_ANY,
                    Client.RULE_NONE
            ):
                if 'values' not in rule:
                    Log.error(Client.ERROR_MALFORMED_RULESET)
                    return False

            # Make sure a callback_staked function is supplied for this rule type
            if rule['type'] == Client.RULE_CALLBACK:
                if callable(rule['validation_function']) is False:
                    Log.error(Client.ERROR_MALFORMED_RULESET)
                    return False

        return True

    @staticmethod
    def validate_type(rule, data) -> bool:
        """
        Validate the type of variables

        :type rule: dict
        :param rule: The rule to be validated

        :type data: dict
        :param data: The user data to validate

        :return: bool
        """
        rule_values = rule['values']
        rule_allow_none = rule['allow_none']
        rule_type = rule['type']

        # Iterate through all values
        for expected_value in rule_values:
            # Check if the type is correct
            if expected_value in data.keys():
                Log.debug('Current key: {expected_value}'.format(expected_value=expected_value))
                Log.debug('Current type: {type}'.format(type=type(data[expected_value])))
                Log.debug('Expected type: {rule_type}'.format(rule_type=rule_type))

                if type(data[expected_value]) == rule_type:
                    continue
                elif data[expected_value] is None:
                    if rule_allow_none is False:
                        # Value was None but that is not allowed
                        return False
                else:
                    return False

        # None of the values were found, validation was successful
        Log.debug('Validation successful')
        return True

    @staticmethod
    def validate_any(rule, data) -> bool:
        """
        Ensure at least one of the values is present

        :type rule: dict
        :param rule: The rule to be validated

        :type data: dict
        :param data: The user data to validate

        :return: bool
        """
        rule_values = rule['values']

        # Iterate through all expected values
        for expected_value in rule_values:
            # Check if the value is present in the supplied data
            if expected_value in data.keys():
                # Found at least one of the allowed values, validation was successful
                return True

        # None of the values were found, validation has failed
        return False

    @staticmethod
    def validate_all(rule, data) -> bool:
        """
        Ensure all of the values are present

        :type rule: dict
        :param rule: The rule to be validated

        :type data: dict
        :param data: The user data to validate

        :return: bool
        """
        rule_values = rule['values']

        # Iterate through all expected values
        for expected_value in rule_values:
            # Make sure the value is present in the supplied data
            if expected_value not in data.keys():
                # If any expected value is missing, validation has failed
                return False

        # All values were found, validation was successful
        return True

    @staticmethod
    def validate_none(rule, data) -> bool:
        """
        Ensure none of the values are present

        :type rule: dict
        :param rule: The rule to be validated

        :type data: dict
        :param data: The user data to validate

        :return: bool
        """
        rule_values = rule['values']

        # Iterate through all expected values
        for expected_value in rule_values:
            # Make sure each value is missing
            if expected_value in data.keys():
                # If any value is found, validation has failed
                return False

        # None of the values were found, validation was successful
        return True

    @staticmethod
    def validate_all_any_nothing(rule, data) -> bool:
        """
        Either all of the values, or none of the values must be present the supplied data
        
        :type rule: dict
        :param rule: The rule to be validated
        
        :type data: dict
        :param data: The user data to validate
        
        :return: 
        """
        rule_values = rule['values']

        count_found = 0
        count_maximum = len(rule_values)

        # Iterate through all expected values
        for expected_value in rule_values:
            # Check if the value is present in the supplied data
            if expected_value in data.keys():
                # Count how many values we find
                count_found += 1

        # Return validation result
        return count_found == 0 or count_found == count_maximum
