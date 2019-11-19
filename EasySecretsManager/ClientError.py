class ClientError:
    ERROR_SECRET_LIST_UNHANDLED_EXCEPTION = 'An unexpected error occurred during listing of secrets'
    ERROR_SECRET_CREATE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during creation of secrets'
    ERROR_SECRET_CREATE_ALREADY_EXISTS = ERROR_SECRET_CREATE_UNHANDLED_EXCEPTION + ' The specified secret already exists'
    ERROR_SECRET_UPDATE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during update of secrets'
    ERROR_SECRET_UPDATE_NOT_FOUND = ERROR_SECRET_UPDATE_UNHANDLED_EXCEPTION + ' The requested secret could not be found'
    ERROR_SECRET_DELETE_UNHANDLED_EXCEPTION = 'An unexpected error occurred during deletion of secrets'
    ERROR_SECRET_DELETE_NOT_FOUND = ERROR_SECRET_DELETE_UNHANDLED_EXCEPTION + ' The requested secret could not be found'
