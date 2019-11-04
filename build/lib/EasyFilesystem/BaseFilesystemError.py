#!/usr/bin/env python
# -*- coding: utf-8 -*-


class BaseFilesystemError:
    ERROR_UNHANDLED_EXCEPTION = ' Please review additional error messages.'

    ERROR_ITERATE = 'An unexpected error occurred during sftp_filesystem iteration.'
    ERROR_ITERATE_CALLBACK_NOT_CALLABLE = ERROR_ITERATE + ' The user callback_staked function provided was not a callable object.'
    ERROR_ITERATE_STRATEGY_UNKNOWN = ERROR_ITERATE + ' The specified iteration staking_strategy was unknown.'
