import unittest

from EasyLambda.EasyAction import EasyAction

# noinspection PyMethodMayBeStatic
from EasyLambda.EasyLog import EasyLog


# noinspection DuplicatedCode
class EasyActionTest(unittest.TestCase):
    def test_is_action_valid_success(self):
        """
        Assert that is_action_valid() succeeds with a valid action class
        """
        EasyLog.test('Testing action validation on valid action definition...')
        self.assertTrue(
            EasyAction.is_action_valid({
                'module': 'EasyLambda.Actions.Test.ActionValid',
                'class': 'Action'
            })
        )

    def test_is_action_valid_failure(self):
        """
        Assert that the is_action_valid() function fails when required methods are missing
        """
        EasyLog.test('Test action validation on invalid action definitions...')

        EasyLog.test('Testing validation fails on missing setup() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetup'
        }))

        EasyLog.test('Testing validation fails on missing execute() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingExecute'
        }))

        EasyLog.test('Testing validation fails on missing list_inputs() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListInputs'
        }))

        EasyLog.test('Testing validation fails on missing has_input() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasInput'
        }))

        EasyLog.test('Testing validation fails on missing get_input() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInput'
        }))

        EasyLog.test('Testing validation fails on missing setup function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInputs'
        }))

        EasyLog.test('Testing validation fails on missing set_input() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetInput'
        }))

        EasyLog.test('Testing validation fails on missing list_outputs() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListOutputs'
        }))

        EasyLog.test('Testing validation fails on missing has_output() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasOutput'
        }))

        EasyLog.test('Testing validation fails on missing get_output() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutput'
        }))

        EasyLog.test('Testing validation fails on missing set_output() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetOutput'
        }))

        EasyLog.test('Testing validation fails on missing get_outputs() function...')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutputs'
        }))

    def test_is_action_list_valid_success(self):
        """
        Test valid action list passes validation
        """
        EasyLog.test('Testing validation of valid action list...')

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Loading actions...')
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'next_action_failure': None,
                'next_action_success': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            },
            {
                'reference': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo'
                }
            }
        ])

        EasyLog.test('Validating action list using previously loaded actions...')
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'next_action': 'action_two'
        }]))

    def test_is_action_list_invalid_inputs_failure(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        EasyLog.test('Testing action list validation fails with invalid inputs...')

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Loading actions...')
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'next_action_failure': None,
                'next_action_success': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            },
            {
                'reference': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo'
                }
            }
        ])

        # Ensure validation fails due to 'action_two' not being loaded
        EasyLog.test('Asserting validation fails with invalid list input type...')
        self.assertFalse(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': ['this', 'is', 'not', 'a', 'dictionary']
        }]))

        EasyLog.test('Asserting validation fails with invalid string input type...')
        self.assertFalse(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': 'this is not either'
        }]))

    def test_is_action_list_valid_inputs_success(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        EasyLog.test('Testing action list validation success with valid inputs...')

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Loading actions...')
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'next_action_failure': None,
                'next_action_success': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            },
            {
                'reference': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo'
                }
            }
        ])

        # Ensure validation succeeds with valid inputs
        EasyLog.test('Asserting validation passes with valid input dictionary...')
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': {
                'input_one': 1,
                'input_two': 2
            }
        }]))

        EasyLog.test('Asserting validation passes with valid empty input dictionary...')
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': {}
        }]))

        EasyLog.test('Asserting validation passes with NoneType inputs...')
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': None
        }]))

        EasyLog.test('Asserting validation passes with missing inputs parameter...')
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one'
        }]))

    def test_load_action_success(self):
        """
        Test that an action is loaded without any issue
        """
        EasyLog.test('Testing loading of action is successful...')

        reference = 'test_action'

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Validating action does not exist...')
        self.assertFalse(EasyAction.has_action(reference))

        EasyLog.test('Creating new test_action action...')
        EasyAction.load_action(
            reference=reference,
            configuration={
                'module': 'EasyLambda.Actions.Test.ActionValid',
                'class': 'Action'
            }
        )

        EasyLog.test('Validation action exists...')
        self.assertTrue(EasyAction.has_action(reference))

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Validating action does not exist...')
        self.assertFalse(EasyAction.has_action(reference))

    def test_load_actions_success(self):
        """
        Test loading of multiple actions
        """
        EasyLog.test('Testing successful load of multiple actions...')

        reference_one = 'action_one'
        reference_two = 'action_two'

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Validating actions do not exist...')
        self.assertFalse(EasyAction.has_action(reference_one))
        self.assertFalse(EasyAction.has_action(reference_two))

        # Load the actions
        EasyAction.load_actions([
            {
                'reference': reference_one,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            },
            {
                'reference': reference_two,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo'
                }
            }
        ])

        # Assert the actions now exist
        EasyLog.test('Validating actions exist...')
        self.assertTrue(EasyAction.has_action(reference_one))
        self.assertTrue(EasyAction.has_action(reference_two))

    def test_clear_actions_success(self):
        """
        Test clearing of actions works
        """
        EasyLog.test('Testing clearing of actions...')

        reference_one = 'action_one'
        reference_two = 'action_two'

        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        EasyLog.test('Validating actions do not exist...')
        self.assertFalse(EasyAction.has_action(reference_one))
        self.assertFalse(EasyAction.has_action(reference_two))

        # Load the actions
        EasyAction.load_actions([
            {
                'reference': reference_one,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            },
            {
                'reference': reference_two,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo'
                }
            }
        ])

        # Assert the actions now exist
        EasyLog.test('Validating actions exist...')
        self.assertTrue(EasyAction.has_action(reference_one))
        self.assertTrue(EasyAction.has_action(reference_two))

        # Clear the actions
        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        # Assert they don't exist again
        EasyLog.test('Validating actions do not exist...')
        self.assertFalse(EasyAction.has_action(reference_one))
        self.assertFalse(EasyAction.has_action(reference_two))

    def test_execute_action_list(self):
        """
        Test successful execution of a chain of actions
        """
        EasyLog.test('Testing successful execution of action list...')

        # Clear all actions
        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        reference_one = 'action_one'
        reference_two = 'action_two'

        input_one = 17
        input_two = 23

        # Load the actions
        EasyLog.test('Loading actions...')
        EasyAction.load_actions([
            {
                'reference': reference_one,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne',
                    'success_actions': [reference_two],
                    'failure_actions': []
                }
            },
            {
                'reference': reference_two,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo',
                    'success_actions': [],
                    'failure_actions': [],
                }
            }
        ])

        # Execute the action and
        EasyLog.test('Executing first action...')
        result = EasyAction.execute_action(reference_one, {
            'input_one': input_one,
            'input_two': input_two
        })

        # Check the output of action one
        EasyLog.test('Asserting first action output exists...')
        self.assertTrue('output_one' in result)
        EasyLog.test('Asserting first action output value...')
        self.assertEqual(result['output_one'], (input_one + input_two))

        # Check the output of action two
        EasyLog.test('Asserting second action output exists...')
        self.assertTrue('output_two' in result)
        EasyLog.test('Asserting second action output value...')
        self.assertEqual(result['output_two'], (input_one + input_two) * 2)

    def test_execute_result_string(self):
        """
        Test successful execution of a chain of actions by returning the next action name as a string
        """
        EasyLog.test('Testing action returning string reference...')

        reference_one = 'action_string_launch'
        reference_two = 'action_success'

        # Clear all actions
        EasyLog.test('Clearing existing actions...')
        EasyAction.clear_actions()

        # Load the action
        EasyLog.test('Loading actions...')
        EasyAction.load_actions([
            {
                'reference': reference_one,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionStringLaunch',
                    'inputs': {
                        'something': 'irrelevant'
                    }
                }
            },
            {
                'reference': reference_two,
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionSuccess'
                }
            }
        ])

        # Execute the action and
        EasyLog.test('Executing first action...')
        result = EasyAction.execute_action('action_string_launch', {
            'next_action': reference_two
        })

        # We should have executed 2 actions
        EasyLog.test('Asserting count of executions...')
        self.assertTrue('count' in result)
        self.assertEqual(result['count'], 2)

        # First should have been the ActionStringLaunch function
        EasyLog.test('Asserting first function called in order...')
        self.assertTrue(reference_one in result)
        self.assertEqual(result[reference_one], 1)

        # The second should have been the ActionSuccess function
        EasyLog.test('Asserting second function called in order...')
        self.assertTrue(reference_two in result)
        self.assertEqual(result[reference_two], 2)


if __name__ == '__main__':
    unittest.main()
