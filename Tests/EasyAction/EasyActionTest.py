import unittest

from EasyPipeline.EasyAction import EasyPipeline
from EasyLog.Log import Log


# noinspection DuplicatedCode, PyMethodMayBeStatic
class EasyActionTest(unittest.TestCase):
    def test_is_action_valid_success(self):
        """
        Assert that is_action_valid() succeeds with a valid action class
        """
        Log.test('Testing action validation on valid action definition...')
        self.assertTrue(
            EasyPipeline.is_action_valid({
                'module': 'EasyLambda.Actions.Test.ActionValid',
                'class': 'Action'
            })
        )

    def test_is_action_valid_failure(self):
        """
        Assert that the is_action_valid() function fails when required methods are missing
        """
        Log.test('Test action validation on invalid action definitions...')

        Log.test('Testing validation fails on missing setup() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetup'
        }))

        Log.test('Testing validation fails on missing execute() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingExecute'
        }))

        Log.test('Testing validation fails on missing list_inputs() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListInputs'
        }))

        Log.test('Testing validation fails on missing has_input() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasInput'
        }))

        Log.test('Testing validation fails on missing get_input() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInput'
        }))

        Log.test('Testing validation fails on missing setup function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInputs'
        }))

        Log.test('Testing validation fails on missing set_input() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetInput'
        }))

        Log.test('Testing validation fails on missing list_outputs() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListOutputs'
        }))

        Log.test('Testing validation fails on missing has_output() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasOutput'
        }))

        Log.test('Testing validation fails on missing get_output() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutput'
        }))

        Log.test('Testing validation fails on missing set_output() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetOutput'
        }))

        Log.test('Testing validation fails on missing get_outputs() function...')
        self.assertFalse(EasyPipeline.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutputs'
        }))

    def test_is_action_list_valid_success(self):
        """
        Test valid action list passes validation
        """
        Log.test('Testing validation of valid action list...')

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Loading actions...')
        EasyPipeline.load_actions([
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

        Log.test('Validating action list using previously loaded actions...')
        self.assertTrue(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'next_action': 'action_two'
        }]))

    def test_is_action_list_invalid_inputs_failure(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        Log.test('Testing action list validation fails with invalid inputs...')

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Loading actions...')
        EasyPipeline.load_actions([
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
        Log.test('Asserting validation fails with invalid list input type...')
        self.assertFalse(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': ['this', 'is', 'not', 'a', 'dictionary']
        }]))

        Log.test('Asserting validation fails with invalid string input type...')
        self.assertFalse(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': 'this is not either'
        }]))

    def test_is_action_list_valid_inputs_success(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        Log.test('Testing action list validation success with valid inputs...')

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Loading actions...')
        EasyPipeline.load_actions([
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
        Log.test('Asserting validation passes with valid input dictionary...')
        self.assertTrue(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': {
                'input_one': 1,
                'input_two': 2
            }
        }]))

        Log.test('Asserting validation passes with valid empty input dictionary...')
        self.assertTrue(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': {}
        }]))

        Log.test('Asserting validation passes with NoneType inputs...')
        self.assertTrue(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': None
        }]))

        Log.test('Asserting validation passes with missing inputs parameter...')
        self.assertTrue(EasyPipeline.is_action_list_valid([{
            'reference': 'action_one'
        }]))

    def test_load_action_success(self):
        """
        Test that an action is loaded without any issue
        """
        Log.test('Testing loading of action is successful...')

        reference = 'test_action'

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Validating action does not exist...')
        self.assertFalse(EasyPipeline.has_action(reference))

        Log.test('Creating new test_action action...')
        EasyPipeline.load_action(
            reference=reference,
            configuration={
                'module': 'EasyLambda.Actions.Test.ActionValid',
                'class': 'Action'
            }
        )

        Log.test('Validation action exists...')
        self.assertTrue(EasyPipeline.has_action(reference))

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Validating action does not exist...')
        self.assertFalse(EasyPipeline.has_action(reference))

    def test_load_actions_success(self):
        """
        Test loading of multiple actions
        """
        Log.test('Testing successful load of multiple actions...')

        reference_one = 'action_one'
        reference_two = 'action_two'

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Validating actions do not exist...')
        self.assertFalse(EasyPipeline.has_action(reference_one))
        self.assertFalse(EasyPipeline.has_action(reference_two))

        # Load the actions
        EasyPipeline.load_actions([
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
        Log.test('Validating actions exist...')
        self.assertTrue(EasyPipeline.has_action(reference_one))
        self.assertTrue(EasyPipeline.has_action(reference_two))

    def test_clear_actions_success(self):
        """
        Test clearing of actions works
        """
        Log.test('Testing clearing of actions...')

        reference_one = 'action_one'
        reference_two = 'action_two'

        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        Log.test('Validating actions do not exist...')
        self.assertFalse(EasyPipeline.has_action(reference_one))
        self.assertFalse(EasyPipeline.has_action(reference_two))

        # Load the actions
        EasyPipeline.load_actions([
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
        Log.test('Validating actions exist...')
        self.assertTrue(EasyPipeline.has_action(reference_one))
        self.assertTrue(EasyPipeline.has_action(reference_two))

        # Clear the actions
        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        # Assert they don't exist again
        Log.test('Validating actions do not exist...')
        self.assertFalse(EasyPipeline.has_action(reference_one))
        self.assertFalse(EasyPipeline.has_action(reference_two))

    def test_execute_action_list(self):
        """
        Test successful execution of a chain of actions
        """
        Log.test('Testing successful execution of action list...')

        # Clear all actions
        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        reference_one = 'action_one'
        reference_two = 'action_two'

        input_one = 17
        input_two = 23

        # Load the actions
        Log.test('Loading actions...')
        EasyPipeline.load_actions([
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
        Log.test('Executing first action...')
        result = EasyPipeline.execute_action(reference_one, {
            'input_one': input_one,
            'input_two': input_two
        })

        # Check the output of action one
        Log.test('Asserting first action output exists...')
        self.assertTrue('output_one' in result)
        Log.test('Asserting first action output value...')
        self.assertEqual(result['output_one'], (input_one + input_two))

        # Check the output of action two
        Log.test('Asserting second action output exists...')
        self.assertTrue('output_two' in result)
        Log.test('Asserting second action output value...')
        self.assertEqual(result['output_two'], (input_one + input_two) * 2)

    def test_execute_result_string(self):
        """
        Test successful execution of a chain of actions by returning the next action name as a string
        """
        Log.test('Testing action returning string reference...')

        reference_one = 'action_string_launch'
        reference_two = 'action_success'

        # Clear all actions
        Log.test('Clearing existing actions...')
        EasyPipeline.clear_actions()

        # Load the action
        Log.test('Loading actions...')
        EasyPipeline.load_actions([
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
        Log.test('Executing first action...')
        result = EasyPipeline.execute_action('action_string_launch', {
            'next_action': reference_two
        })

        # We should have executed 2 actions
        Log.test('Asserting count of executions...')
        self.assertTrue('count' in result)
        self.assertEqual(result['count'], 2)

        # First should have been the ActionStringLaunch function
        Log.test('Asserting first function called in order...')
        self.assertTrue(reference_one in result)
        self.assertEqual(result[reference_one], 1)

        # The second should have been the ActionSuccess function
        Log.test('Asserting second function called in order...')
        self.assertTrue(reference_two in result)
        self.assertEqual(result[reference_two], 2)


if __name__ == '__main__':
    unittest.main()
