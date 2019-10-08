from EasyLambda.Action.BaseAction import BaseAction
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyValidator import EasyValidator


class ListFiles(BaseAction):
    # Error constants
    ERROR_BUCKET_NAME_NOT_FOUND = 'Missing required bucket_name parameter'

    def setup(self):
        EasyLog.trace('Setting up S3.ListFiles action...')
        EasyLog.debug('Validating required inputs...')

        ruleset = [
            {EasyValidator.RULE_ALL: ['bucket_name', 'base_path', 'recursive']},
            {EasyValidator.RULE_TYPE: {
                'type': bool,
                'allow_none': True,
                'values': ['recursive']
            }}
        ]

        if EasyValidator.validate(ruleset=ruleset, data=self.get_inputs()) is False:
            EasyLog.error(ListFiles.ERROR_BUCKET_NAME_NOT_FOUND)
            raise Exception(ListFiles.ERROR_BUCKET_NAME_NOT_FOUND)

        # If recursion was not specified- set it to false
        if self.has_input('recursive') is False:
            self.set_input('recursive', False)

    def execute(self):
        EasyLog.trace('Executing S3.ListFiles action...')
