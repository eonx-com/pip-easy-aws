#!/usr/bin/env python
# -*- coding: utf-8 -*-

from EasyPipeline import BaseAction


class Action(BaseAction):
    # noinspection PyMethodMayBeStatic
    def execute(self):
        return True


class ActionOne(BaseAction):
    def execute(self):
        """
        Take two inputs add them together and pass them on to the second action

        :return: bool
        """
        # Make sure the input one exists
        if self.has_input('input_one') is False:
            raise Exception('Missing input one')

        # Make sure the input two exists
        if self.has_input('input_two') is False:
            raise Exception('Missing input one')

        # Retrieve the inputs
        input_one = self.get_input('input_one')
        input_two = self.get_input('input_two')

        # Add them together
        output_one = input_one + input_two

        # Pass the result as output to the next function
        self.set_output('output_one', output_one)
        return True


class ActionTwo(BaseAction):
    def execute(self):
        """
        Take the output from the first action and multiply it by two

        :return: bool
        """
        if self.has_input('output_one') is False:
            raise Exception('Missing input from action one')

        output_one = self.get_input('output_one')

        self.set_output('output_two', output_one * 2)
        return True


class ActionStringLaunch(BaseAction):
    """
    Action that launches 'action_success' by returning a string
    """

    def execute(self):
        next_action = 'action_success'

        if self.has_input('next_action') is False:
            next_action = self.get_input('next_action')

        if self.has_input('count') is False:
            self.set_output('count', 1)
        else:
            self.set_output('count', self.get_input('count') + 1)
        self.set_output('action_string_launch', self.get_output('count'))

        return next_action


class ActionFailure(BaseAction):
    """
    Action that returns failure
    """

    def execute(self):
        if self.has_input('count') is False:
            self.set_output('count', 1)
        else:
            self.set_output('count', self.get_input('count') + 1)

        self.set_output('action_failure', self.get_output('count'))
        return False


class ActionSuccess(BaseAction):
    """
    Action that returns success
    """

    def execute(self):
        if self.has_input('count') is False:
            self.set_output('count', 1)
        else:
            self.set_output('count', self.get_input('count') + 1)

        self.set_output('action_success', self.get_output('count'))
        return True
