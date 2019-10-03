class EasyValidator:
    # Error constants
    ERROR_MALFORMED_REQUIREMENT = 'The validation requirement specified contains an unknown conjugation method name'

    # Validation rules
    RULE_AND = 'AND'
    RULE_OR = 'OR'
    RULE_ALL_OR_NOTHING = 'ALL_OR_NOTHING'
    RULE_CUSTOM = 'CUSTOM'

    @staticmethod
    def validate_parameters(requirements, parameters) -> bool:
        """
        Validate all required parameters were passed to the function

        :type requirements: tuple
        :param requirements: Parameter requirements

        :type parameters: dict
        :param parameters: The parameters to be tested

        :return: bool
        """
        for requirement in requirements:
            if requirement is dict:
                # Dictionary should contain either 'and'/'or' as the key
                for conjugation in requirement.keys():
                    # One of the parameters must be set
                    if conjugation == EasyValidator.RULE_OR:
                        or_found = False
                        or_requirement = requirement[conjugation]
                        for or_field in or_requirement:
                            if or_field in parameters.keys():
                                if parameters[or_field] is not None:
                                    or_found = True
                                    break
                        # If none of the parameters was found, raise an exception
                        if or_found is False:
                            return False
                    elif conjugation == EasyValidator.RULE_AND:
                        # All of the parameters in the dictionary must be set
                        for and_field in requirement[conjugation]:
                            # If any of the parameters is not set, raise an exception
                            if and_field not in parameters.keys():
                                return False
                            # If the current parameter exists but is set to None, raise an exception
                            if parameters[and_field] is None:
                                return False
                    elif conjugation == EasyValidator.RULE_ALL_OR_NOTHING:
                        # None of them, or all of them must be present
                        count_found = 0
                        count_required = len(requirement[conjugation])
                        for field in [conjugation]:
                            if field in parameters:
                                count_found += 1

                        if count_found != count_required and count_found != 0:
                            return False
                    elif conjugation == EasyValidator.RULE_CUSTOM:
                        # Custom callback function
                        if requirement[conjugation](parameters) is False:
                            return False
                    else:
                        # Malformed parameter syntax
                        raise Exception(EasyValidator.ERROR_MALFORMED_REQUIREMENT)
            else:
                # If the parameter is not present, raise an exception
                if requirement not in parameters:
                    return False
                # If the parameter exists but is set to None, raise an exception
                if parameters[requirement] is None:
                    return False

        return True
