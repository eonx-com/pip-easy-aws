class ClientError:
    ERROR_UNHANDLED_EXCEPTION = ' Please review additional error messages.'

    # File List Errors
    ERROR_FILE_LIST = 'An unexpected error occurred during listing of files in SFTP sftp_filesystem.'
    ERROR_FILE_LIST_UNHANDLED_EXCEPTION = ERROR_FILE_LIST + ERROR_UNHANDLED_EXCEPTION

    # File Exists Errors
    ERROR_FILE_EXISTS = 'An unexpected error occurred during test of file existence in SFTP sftp_filesystem.'
    ERROR_FILE_EXISTS_UNHANDLED_EXCEPTION = ERROR_FILE_EXISTS

    # Path Exists Errors
    ERROR_PATH_EXISTS = 'An unexpected error occurred during test of path existence in SFTP sftp_filesystem.'
    ERROR_PATH_EXISTS_UNHANDLED_EXCEPTION = ERROR_PATH_EXISTS

    # File Delete Errors
    ERROR_FILE_DELETE = 'An unexpected error occurred while deleting SFTP file.'
    ERROR_FILE_DELETE_UNHANDLED_EXCEPTION = ERROR_FILE_DELETE + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_DELETE_FAILED = ERROR_FILE_DELETE + ' The deleted file still exists.'
    ERROR_FILE_DELETE_NOT_FOUND = ERROR_FILE_DELETE + ' The file to delete could not be found.'

    # Path Delete Errors
    ERROR_PATH_DELETE = 'An unexpected error occurred while deleting SFTP path.'
    ERROR_PATH_DELETE_UNHANDLED_EXCEPTION = ERROR_PATH_DELETE + ERROR_UNHANDLED_EXCEPTION
    ERROR_PATH_DELETE_FAILED = ERROR_PATH_DELETE + ' The deleted path still exists.'
    ERROR_PATH_DELETE_NOT_FOUND = ERROR_PATH_DELETE + ' The path to delete could not be found.'

    # File Copy Errors
    ERROR_FILE_COPY = 'An unexpected error occurred while copying SFTP file.'
    ERROR_FILE_COPY_UNHANDLED_EXCEPTION = ERROR_FILE_COPY + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_COPY_FAILED = ERROR_FILE_COPY + ' Failed to copy the requested file.'
    ERROR_FILE_COPY_SOURCE_DESTINATION_SAME = ERROR_FILE_COPY + ' The source and destination of the file copy cannot be the same.'
    ERROR_FILE_COPY_SOURCE_NOT_FOUND = ERROR_FILE_COPY + ' The requested file did not exist in the source sftp_filesystem.'
    ERROR_FILE_COPY_ALREADY_EXISTS = ERROR_FILE_COPY + ' The requested file already exists in the destination sftp_filesystem.'

    # File Move Errors
    ERROR_FILE_MOVE = 'An unexpected error occurred while moving SFTP file.'
    ERROR_FILE_MOVE_UNHANDLED_EXCEPTION = ERROR_FILE_MOVE + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_MOVE_COPY_FAILED = ERROR_FILE_MOVE + ' Copying to the destination failed.'
    ERROR_FILE_MOVE_SOURCE_NOT_FOUND = ERROR_FILE_MOVE + ' The source file could not be found.'
    ERROR_FILE_MOVE_ALREADY_EXISTS = ERROR_FILE_MOVE + ' The destination file already exists.'
    ERROR_FILE_MOVE_DELETE_FAILED = ERROR_FILE_MOVE + ' Deleting the source file failed.'
    ERROR_FILE_MOVE_SOURCE_DESTINATION_SAME = ERROR_FILE_MOVE + ' The source and destination of the file move cannot be the same.'

    # File Upload Errors
    ERROR_FILE_UPLOAD = 'An unexpected error occurred while uploading file to SFTP.'
    ERROR_FILE_UPLOAD_UNHANDLED_EXCEPTION = ERROR_FILE_UPLOAD + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_UPLOAD_SOURCE_NOT_FOUND = ERROR_FILE_UPLOAD + ' The source file could not be found.'
    ERROR_FILE_UPLOAD_ALREADY_EXISTS = ERROR_FILE_UPLOAD + ' The destination file already exists.'
    ERROR_FILE_UPLOAD_FAILED = ERROR_FILE_UPLOAD + ' The upload failed.'

    # File Download Errors
    ERROR_FILE_DOWNLOAD = 'An unexpected error occurred while download a file from SFTP.'
    ERROR_FILE_DOWNLOAD_UNHANDLED_EXCEPTION = ERROR_FILE_DOWNLOAD + ERROR_UNHANDLED_EXCEPTION
    ERROR_FILE_DOWNLOAD_SOURCE_NOT_FOUND = ERROR_FILE_DOWNLOAD + ' The source file could not be found.'
    ERROR_FILE_DOWNLOAD_CALLBACK_NOT_CALLABLE = ERROR_FILE_DOWNLOAD + ' The callback_staked function was not a callable object.'
    ERROR_FILE_DOWNLOAD_ALREADY_EXISTS = ERROR_FILE_DOWNLOAD + ' The destination file already exists.'
    ERROR_FILE_DOWNLOAD_FAILED = ERROR_FILE_DOWNLOAD + ' The download failed.'

    # Create Path Errors
    ERROR_CREATE_PATH = 'An unexpected error occurred while attempting to create a path in SFTP.'
    ERROR_CREATE_PATH_UNHANDLED_EXCEPTION = ERROR_CREATE_PATH + ERROR_UNHANDLED_EXCEPTION
    ERROR_CREATE_PATH_ALREADY_EXISTS = ERROR_CREATE_PATH + ' The path already exists.'
    ERROR_CREATE_PATH_FAILED = ERROR_CREATE_PATH + ' Failed to create the requested path.'

    # Create Temp Path Errors
    ERROR_CREATE_TEMP_PATH = 'An unexpected error occurred while attempting to create a unique temp path in SFTP.'
    ERROR_CREATE_TEMP_PATH_FOLDER_NOT_FOUND = ERROR_CREATE_PATH + ' The base temp folder path specified did not exist.'
    ERROR_CREATE_TEMP_PATH_FAILED = ERROR_CREATE_PATH + ' Failed to create a unique temporary path.'

    # Server Connect Errors
    ERROR_CONNECT = 'An unexpected error occurred while attempting to connect to the SFTP server.'
    ERROR_CONNECT_FAILED = ERROR_CONNECT + ' Failed to connect successfully.'
    ERROR_CONNECT_INVALID_FINGERPRINT = ERROR_CONNECT + ' An invalid server fingerprint was detected.'
    ERROR_CONNECT_SANITIZE_ADDRESS = ERROR_CONNECT + ' The specified hostname could not be resolved to a valid address.'
    ERROR_CONNECT_SANITIZE_FINGERPRINT = ERROR_CONNECT + ' The specified fingerprint was not valid'
    ERROR_CONNECT_SANITIZE_PRIVATE_KEY = ERROR_CONNECT + ' The specified private key was not valid'
    ERROR_CONNECT_SANITIZE_FINGERPRINT_TYPE = ERROR_CONNECT + ' The specified fingerprint type was not valid'
    ERROR_CONNECT_SANITIZE_PORT = ERROR_CONNECT + ' The specified SFTP port was not valid'
    ERROR_CONNECT_SANITIZE_USERNAME = ERROR_CONNECT + ' No username was supplied.'
