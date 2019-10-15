#!/usr/bin/env python
# -*- coding: utf-8 -*-


class ClientError:
    # Pipeline Errors
    ERROR_PIPELINE_VALIDATION_FAILED = 'One or more pipelines failed validation.'
    ERROR_PIPELINE_LIST_VALIDATION_FAILED = 'The pipeline list failed validation.'
    ERROR_PIPELINE_REFERENCE_ALREADY_EXISTS = 'The specified pipeline reference already exists.'
    ERROR_PIPELINE_MODULE_NAME_NOT_FOUND = 'Required module name could not be found in pipeline configuration.'
    ERROR_PIPELINE_CLASS_NOT_FOUND = 'The specified class could not be found.'
    ERROR_PIPELINE_CLASS_NAME_NOT_FOUND = 'Required class name could not be found in pipeline configuration.'
    ERROR_PIPELINE_CLASS_METHOD_NOT_FOUND = 'One or more required class pipeline methods could not be found.'
    ERROR_PIPELINE_MODULE_NOT_FOUND = 'The specified module could not be found.'
    ERROR_PIPELINE_REFERENCE_NOT_FOUND = 'One or more required pipelines could not be found.'
    ERROR_PIPELINE_RESULT_INVALID = 'The pipeline did not return a valid boolean value.'
    ERROR_PIPELINE_STRING_RESULT_INVALID = 'The pipeline returned a string that could not be resolved to a known pipeline.'
