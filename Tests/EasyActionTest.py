import unittest

from EasyLambda.EasyAction import EasyAction


# noinspection PyMethodMayBeStatic
class EasyActionTest(unittest.TestCase):
    def test_is_action_valid_success(self):
        """
        Assert that is_action_valid() succeeds with a valid action class
        """
        print('test_is_action_valid_success')
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
        print('test_is_action_valid_failure_setup')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetup'
        }))

        print('test_is_action_valid_failure_execute')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingExecute'
        }))

        print('test_is_action_valid_failure_list_inputs')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListInputs'
        }))

        print('test_is_action_valid_failure_has_input')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasInput'
        }))

        print('test_is_action_valid_failure_get_input')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInput'
        }))

        print('test_is_action_valid_failure_get_inputs')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetInputs'
        }))

        print('test_is_action_valid_failure_set_input')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetInput'
        }))

        print('test_is_action_valid_failure_list_outputs')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingListOutputs'
        }))

        print('test_is_action_valid_failure_has_output')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingHasOutput'
        }))

        print('test_is_action_valid_failure_get_output')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutput'
        }))

        print('test_is_action_valid_failure_set_output')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingSetOutput'
        }))

        print('test_is_action_valid_failure_get_outputs')
        self.assertFalse(EasyAction.is_action_valid({
            'module': 'EasyLambda.Actions.Test.ActionInvalid',
            'class': 'ActionMissingGetOutputs'
        }))

    def test_is_action_list_valid_success(self):
        """
        Test valid action list passes validation
        """
        print('test_is_action_list_valid_success')
        EasyAction.clear_actions()
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

        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'next_action': 'action_two'
        }]))

    def test_is_action_list_valid_next_action_failure(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        print('test_is_action_list_valid_next_action_failure')
        EasyAction.clear_actions()
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
        self.assertFalse(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': ['this', 'is', 'not', 'a', 'dictionary'],
            'next_action': 'action_two'
        }]))

    def test_is_action_list_valid_inputs_failure(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        print('test_is_action_list_valid_inputs_failure')
        EasyAction.clear_actions()
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'next_action_failure': None,
                'next_action_success': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne'
                }
            }
        ])

        # Ensure validation fails due to 'action_two' not being loaded
        self.assertFalse(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'next_action': 'action_two'
        }]))

    def test_is_action_list_valid_inputs_success(self):
        """
        Test valid action list fails when a next_action reference does not exist
        """
        print('test_is_action_list_valid_inputs_success')
        EasyAction.clear_actions()
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'success_actions': None,
                'failure_actions': 'action_two',
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
        self.assertTrue(EasyAction.is_action_list_valid([{
            'reference': 'action_one',
            'inputs': {
                'input_one': 'input_value_one'
            },
            'next_action': 'action_two'
        }]))

    def test_load_action_success(self):
        """
        Test that an action is loaded without any issue
        """
        print('test_load_action_success')
        reference = 'test_action'

        EasyAction.clear_actions()
        EasyAction.load_action(
            reference=reference,
            configuration={
                'module': 'EasyLambda.Actions.Test.ActionValid',
                'class': 'Action'
            }
        )

        self.assertTrue(EasyAction.has_action(reference))

    def test_load_actions_success(self):
        """
        Test loading of multiple actions
        """
        print('test_load_actions_success')

        # Clear all actions
        EasyAction.clear_actions()

        # Assert they don't exist
        self.assertFalse(EasyAction.has_action('action_one'))
        self.assertFalse(EasyAction.has_action('action_two'))

        # Load the actions
        EasyAction.load_actions([
            {
                'reference': 'action_one',
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

        # Assert the actions now exist
        self.assertTrue(EasyAction.has_action('action_one'))
        self.assertTrue(EasyAction.has_action('action_two'))

    def test_clear_actions_success(self):
        """
        Test clearing of actions works
        """
        print('test_clear_actions_success')

        # Clear all actions
        EasyAction.clear_actions()

        # Assert they don't exist
        self.assertFalse(EasyAction.has_action('action_one'))
        self.assertFalse(EasyAction.has_action('action_two'))

        # Load the actions
        EasyAction.load_actions([
            {
                'reference': 'action_one',
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

        # Assert the actions now exist
        self.assertTrue(EasyAction.has_action('action_one'))
        self.assertTrue(EasyAction.has_action('action_two'))

        # Clear the actions
        EasyAction.clear_actions()

        # Assert they don't exist again
        self.assertFalse(EasyAction.has_action('action_one'))
        self.assertFalse(EasyAction.has_action('action_two'))

    def test_execute_action_list(self):
        """
        Test successful execution of a chain of actions
        """
        print('test_execute_action_list')

        # Clear all actions
        EasyAction.clear_actions()

        input_one = 17
        input_two = 23

        # Load the actions
        EasyAction.load_actions([
            {
                'reference': 'action_one',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionOne',
                    'success_actions': ['action_two'],
                    'failure_actions': []
                }
            },
            {
                'reference': 'action_two',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionTwo',
                    'success_actions': [],
                    'failure_actions': [],
                }
            }
        ])

        # Execute the action and
        result = EasyAction.execute_action('action_one', {
            'input_one': input_one,
            'input_two': input_two
        })

        # Check the output of action one
        self.assertTrue('output_one' in result)
        self.assertEqual(result['output_one'], (input_one + input_two))

        # Check the output of action two
        self.assertTrue('output_two' in result)
        self.assertEqual(result['output_two'], (input_one + input_two) * 2)

    def test_execute_result_string(self):
        """
        Test successful execution of a chain of actions by returning the next action name as a string
        """
        print('test_execute_action_list')

        # Clear all actions
        EasyAction.clear_actions()

        # Load the action
        EasyAction.load_actions([
            {
                'reference': 'action_string_launch',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionStringLaunch'
                }
            },
            {
                'reference': 'action_success',
                'configuration': {
                    'module': 'EasyLambda.Actions.Test.ActionValid',
                    'class': 'ActionSuccess'
                }
            }
        ])

        # Execute the action and
        result = EasyAction.execute_action('action_string_launch')

        # We should have executed 2 actions
        self.assertTrue('count' in result)
        self.assertEqual(result['count'], 2)

        # First should have been the ActionStringLaunch function
        self.assertTrue('action_string_launch' in result)
        self.assertEqual(result['action_string_launch'], 1)

        # The second should have been the ActionSuccess function
        self.assertTrue('action_success' in result)
        self.assertEqual(result['action_success'], 2)


if __name__ == '__main__':
    unittest.main()
