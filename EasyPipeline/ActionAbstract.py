#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod


class ActionAbstract:
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
