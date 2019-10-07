from EasyLambda.EasyFilesystemDriver import EasyFilesystemDriver
from EasyLambda.EasyLog import EasyLog


class EasyIteratorSource:
    # Error constants
    ERROR_STAKING_STRATEGY_INVALID = 'The requested staking strategy was invalid'
    ERROR_STAKING_STRATEGY_UNSUPPORTED = 'The selected staking strategy is not supported by the filesystem'

    # Staking strategies
    # Ignore requirement for unique staking, assume that by downloading the file has been uniquely staked (e.g. Direct Link)
    STRATEGY_IGNORE = 'IGNORE'
    # Rename the file on the source filesystem, then check to make sure the rename was successful
    STRATEGY_RENAME = 'RENAME'
    # Add a unique value to a common staking property on the file, then check to make sure the value is the one assigned by the current process
    STRATEGY_PROPERTY = 'PROPERTY'

    def __init__(
            self,
            filesystem_driver,
            recursive,
            delete_on_success,
            delete_on_failure,
            staking_strategy=None,
            success_destinations=None,
            failure_destinations=None
    ):
        """
        This should not be called directly, use the SourceFactory methods to create a source filesystem

        :type filesystem_driver: Filesystem
        :param filesystem_driver: The underlying filesystem this destination is using

        :type staking_strategy: str
        :param staking_strategy: The type of staking strategy to use

        :type recursive: bool
        :param recursive: Flag indicating iteration should be performed recursively

        :type success_destinations: list of EasyIteratorDestination or None
        :param success_destinations: If defined, the destination filesystem where each files will be copied following their successful completion

        :type delete_on_success: bool
        :param delete_on_success: If True, files will be deleted from the source on successful iteration

        :type failure_destinations: list of EasyIteratorDestination or None
        :param failure_destinations: If defined, the destination filesystem where each files will be copied following their failure during iteration

        :type delete_on_failure: bool
        :param delete_on_failure: If True, files will be deleted from the source if an error occurs during iteration
        """
        EasyLog.trace('Initializing EasyIteratorSource class...')

        if staking_strategy is None:
            # If no staking strategy is selected, ignoring unique staking and just assume downloaded files are staked
            staking_strategy = EasyIteratorSource.STRATEGY_IGNORE
        elif staking_strategy not in (EasyIteratorSource.STRATEGY_IGNORE, EasyIteratorSource.STRATEGY_RENAME, EasyIteratorSource.STRATEGY_PROPERTY):
            # Otherwise ensure we received a known strategy
            EasyLog.error('Unknown staking strategy requested: {staking_strategy}'.format(staking_strategy=staking_strategy))
            raise Exception(EasyIteratorSource.ERROR_STAKING_STRATEGY_INVALID)

        self.__filesystem_driver__ = filesystem_driver
        self.__recursive__ = recursive
        self.__success_destinations__ = success_destinations
        self.__failure_destinations__ = failure_destinations
        self.__delete_on_success__ = delete_on_success
        self.__delete_on_failure__ = delete_on_failure
        self.__staking_strategy__ = staking_strategy

    def iterate_files(self, callback, maximum_files=None) -> int:
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate


        :return: int The number of files iterated
        """
        # Pass iteration down to the specific driver class
        self.__filesystem_driver__: EasyFilesystemDriver
        return self.__filesystem_driver__.iterate_files(
            callback=callback,
            maximum_files=maximum_files,
            recursive=self.__recursive__,
            success_destinations=self.__success_destinations__,
            failure_destinations=self.__failure_destinations__,
            delete_on_success=self.__delete_on_success__,
            delete_on_failure=self.__delete_on_failure__,
            staking_strategy=self.__staking_strategy__
        )
